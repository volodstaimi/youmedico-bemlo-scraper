"""
Bemlo Vacancy Scraper API - Enhanced Version
Deployed on Google Cloud Run
"""

import os
import json
import base64
import sqlite3
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from flask import Flask, jsonify, request, Response
import requests

# ============================================
# CONFIGURATION
# ============================================

BEMLO_API_URL = "https://api.bemlo.ai/graphql"
BEMLO_AUTH_URL = "https://api.bemlo.ai/auth/signin"
BEMLO_REFRESH_URL = "https://api.bemlo.ai/auth/session/refresh"

# From environment (set via GCP Secret Manager)
BEMLO_EMAIL = os.environ.get("BEMLO_EMAIL", "")
BEMLO_PASSWORD = os.environ.get("BEMLO_PASSWORD", "")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "")  # Optional Slack webhook

DB_PATH = "/tmp/bemlo_vacancies.db"

app = Flask(__name__)

# ============================================
# GRAPHQL QUERIES
# ============================================

VACANCIES_LIST_QUERY = """
query VacanciesList($first: Int!, $after: String) {
  vacancies(first: $first, after: $after, filter: { reviewStatus: { eq: APPROVED } }) {
    pageInfo {
      hasNextPage
      startCursor
      endCursor
    }
    edges {
      cursor
      node {
        id
        title
        createdAt
        profession
        specializations
        isViewed
        jobType
        jobEndsAt
        jobStartsAt
        lastApplicationDate
        hasLastApplicationDate
        reviewStatus
        procuredAmount
        procuredAmountCurrency
        municipality
        region
        tender {
          id
          title
          extensionForId
          announcedAt
          startsAt
          endsAt
          scope
          lastPresentationDate
          pricing
          scheduleType
          dynamicStatus
          urgentShiftsCount
          fillRate
          unit {
            id
            name
            municipality
          }
          orderer {
            id
            displayName
          }
        }
      }
    }
  }
}
"""

VACANCY_DETAIL_QUERY = """
query VacancyById($id: String!) {
  vacancy(id: $id) {
    id
    title
    createdAt
    description
    department
    driversLicenseRequirements
    employerId
    expectedCompensation { high low }
    freeTextRequirements
    hasLastApplicationDate
    internalComment
    isViewed
    jobEndsAt
    jobStartsAt
    jobType
    lastApplicationDate
    municipality
    profession
    isSpecializationRequired
    country
    region
    reviewStatus
    specializations
    status
    procuredAmount
    procuredAmountCurrency
    experienceRequired
    journalSystems
    tags
    tenderId
    tender {
      id
      title
      announcedAt
      dynamicStatus
      pricing
      assignedAt
      doesAcceptPresentations
      lastPresentationDate
      maxPresentations
      specializationPriority
      scope
      scheduleType
      scheduleDescription
      fillRate
      rows
      enforceRows
      startsAt
      endsAt
      createdAt
      updatedAt
      department
      isTimeReportable
      vacancyDescription
      candidateDescription
      contactPerson {
        email
        fullName
        id
        org { phoneNumber }
      }
      unit {
        id
        country
        region
        name
        municipality
        description
        address
        contactName
        contactRole
        contactEmail
        contactPhoneNumber
      }
      orderer {
        id
        displayName
      }
      billingInformation {
        title
        costCenter
        organizationName
        organizationNumber
        billingReference
        comment
      }
      requirements {
        id
        customRequirements {
          id
          title
          heading
          isRequired
          comment
        }
        documentRequirements {
          id
          document
          isRequired
          maxAge
          type
          comment
        }
        driversLicenseRequirements {
          id
          driversLicense
          isRequired
          comment
        }
        experienceRequirements {
          id
          department
          numberOfYears
          isRequired
          comment
        }
        journalSystemRequirements {
          id
          journalSystem
          isRequired
          comment
        }
        specializationsRequirements {
          id
          specializations
          isRequired
          comment
        }
      }
      availablePriceGroups {
        id
        title
        pricing
        specializations
        basePay
        currency
      }
    }
  }
}
"""

TENDER_SCHEDULE_QUERY = """
query TenderSchedule($id: String!) {
  tender(id: $id) {
    id
    title
    scheduleType
    enforceRows
    rows
    startsAt
    endsAt
    scheduleDescription
    unit {
      id
      name
      address
      municipality
      hasOnCall
      timeZone
    }
    vacantSchedule {
      id
      isUrgent
      scheduleSlotId
      tenderId
      shiftIds
      activity
      startDateTime
      endDateTime
      date
      startTime
      endTime
      breakTime
      row
      status
      comment
    }
  }
}
"""

# ============================================
# DATA CLASSES
# ============================================

@dataclass
class AuthTokens:
    access_token: str = ""
    refresh_token: str = ""
    front_token: str = ""
    expires_at: int = 0
    
    @classmethod
    def from_headers(cls, headers: dict) -> 'AuthTokens':
        access_token = headers.get('st-access-token', '')
        refresh_token = headers.get('st-refresh-token', '')
        front_token = headers.get('front-token', '')
        
        expires_at = 0
        if access_token:
            try:
                payload_part = access_token.split('.')[1]
                payload_part += '=' * (4 - len(payload_part) % 4)
                decoded = json.loads(base64.urlsafe_b64decode(payload_part))
                expires_at = decoded.get('exp', 0)
            except:
                expires_at = int(datetime.now().timestamp()) + 3600
        
        return cls(
            access_token=access_token,
            refresh_token=refresh_token,
            front_token=front_token,
            expires_at=expires_at
        )
    
    def is_expired(self) -> bool:
        return datetime.now().timestamp() >= (self.expires_at - 60)


# ============================================
# DATABASE SETUP
# ============================================

def init_db():
    """Initialize SQLite database with enhanced schema"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Main vacancies table (enhanced)
    c.execute('''
        CREATE TABLE IF NOT EXISTS vacancies (
            id TEXT PRIMARY KEY,
            tender_id TEXT,
            title TEXT,
            
            -- Job details
            profession TEXT,
            specializations TEXT,
            department TEXT,
            job_type TEXT,
            status TEXT,
            review_status TEXT,
            dynamic_status TEXT,
            
            -- Location
            country TEXT DEFAULT 'SWEDEN',
            region TEXT,
            municipality TEXT,
            
            -- Dates (unix timestamps)
            created_at INTEGER,
            announced_at INTEGER,
            job_starts_at INTEGER,
            job_ends_at INTEGER,
            last_application_date INTEGER,
            last_presentation_date INTEGER,
            
            -- Pricing
            procured_amount REAL,
            procured_amount_currency TEXT DEFAULT 'SEK',
            pricing_type TEXT,
            
            -- Scope
            scope_hours REAL,
            fill_rate REAL,
            urgent_shifts_count INTEGER DEFAULT 0,
            
            -- Description
            description TEXT,
            vacancy_description TEXT,
            candidate_description TEXT,
            schedule_description TEXT,
            free_text_requirements TEXT,
            
            -- Requirements summary
            experience_required TEXT,
            drivers_license_required TEXT,
            journal_systems TEXT,
            tags TEXT,
            
            -- Unit info
            unit_id TEXT,
            unit_name TEXT,
            unit_address TEXT,
            unit_municipality TEXT,
            
            -- Orderer info
            orderer_id TEXT,
            orderer_name TEXT,
            
            -- Contact
            contact_person_name TEXT,
            contact_person_email TEXT,
            contact_person_phone TEXT,
            
            -- Billing
            billing_cost_center TEXT,
            billing_organization_name TEXT,
            billing_organization_number TEXT,
            billing_reference TEXT,
            
            -- Flags
            is_time_reportable INTEGER,
            has_last_application_date INTEGER,
            is_viewed INTEGER DEFAULT 0,
            
            -- Tracking
            first_seen_at INTEGER,
            last_updated_at INTEGER,
            details_fetched_at INTEGER,
            
            -- URL
            url TEXT
        )
    ''')
    
    # Shifts table
    c.execute('''
        CREATE TABLE IF NOT EXISTS shifts (
            id TEXT PRIMARY KEY,
            vacancy_id TEXT,
            tender_id TEXT,
            
            date TEXT,
            start_time TEXT,
            end_time TEXT,
            start_datetime INTEGER,
            end_datetime INTEGER,
            break_time INTEGER,
            
            activity TEXT,
            status TEXT,
            is_urgent INTEGER DEFAULT 0,
            row_number INTEGER,
            comment TEXT,
            
            created_at INTEGER DEFAULT (strftime('%s', 'now')),
            
            FOREIGN KEY (vacancy_id) REFERENCES vacancies(id)
        )
    ''')
    
    # Requirements table
    c.execute('''
        CREATE TABLE IF NOT EXISTS requirements (
            id TEXT PRIMARY KEY,
            vacancy_id TEXT,
            
            requirement_type TEXT,
            title TEXT,
            heading TEXT,
            is_required INTEGER,
            comment TEXT,
            
            document_type TEXT,
            document_max_age INTEGER,
            experience_department TEXT,
            experience_years INTEGER,
            journal_system TEXT,
            drivers_license TEXT,
            specializations TEXT,
            
            created_at INTEGER DEFAULT (strftime('%s', 'now')),
            
            FOREIGN KEY (vacancy_id) REFERENCES vacancies(id)
        )
    ''')
    
    # Price groups table
    c.execute('''
        CREATE TABLE IF NOT EXISTS price_groups (
            id TEXT PRIMARY KEY,
            vacancy_id TEXT,
            tender_id TEXT,
            
            title TEXT,
            pricing TEXT,
            base_pay REAL,
            currency TEXT DEFAULT 'SEK',
            specializations TEXT,
            
            created_at INTEGER DEFAULT (strftime('%s', 'now')),
            
            FOREIGN KEY (vacancy_id) REFERENCES vacancies(id)
        )
    ''')
    
    # Scrape logs
    c.execute('''
        CREATE TABLE IF NOT EXISTS scrape_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER,
            total_fetched INTEGER,
            new_count INTEGER,
            updated_count INTEGER,
            details_fetched INTEGER DEFAULT 0,
            shifts_fetched INTEGER DEFAULT 0,
            duration_seconds REAL,
            error TEXT
        )
    ''')
    
    # Create indexes
    c.execute('CREATE INDEX IF NOT EXISTS idx_vac_profession ON vacancies(profession)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_vac_region ON vacancies(region)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_vac_status ON vacancies(dynamic_status)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_vac_announced ON vacancies(announced_at DESC)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_vac_rate ON vacancies(procured_amount DESC)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_shifts_vacancy ON shifts(vacancy_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_shifts_date ON shifts(date)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_req_vacancy ON requirements(vacancy_id)')
    
    conn.commit()
    conn.close()


# ============================================
# BEMLO API CLIENT
# ============================================

class BemloClient:
    def __init__(self, email: str, password: str):
        self.email = email
        self.password = password
        self.session = requests.Session()
        self.tokens: Optional[AuthTokens] = None
        self._setup_session()
    
    def _setup_session(self):
        """Configure session headers"""
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Origin": "https://app.bemlo.com",
            "Referer": "https://app.bemlo.com/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        })
    
    def login(self) -> AuthTokens:
        """Login to Bemlo and get fresh tokens"""
        print(f"[{datetime.now()}] Logging in to Bemlo...")
        
        headers = {
            "Content-Type": "application/json",
            "Origin": "https://app.bemlo.com",
            "Referer": "https://app.bemlo.com/",
            "rid": "emailpassword",
        }
        
        payload = {
            "formFields": [
                {"id": "email", "value": self.email},
                {"id": "password", "value": self.password}
            ]
        }
        
        response = self.session.post(BEMLO_AUTH_URL, json=payload, headers=headers)
        
        if response.status_code != 200:
            raise Exception(f"Login failed: {response.status_code} - {response.text}")
        
        data = response.json()
        if data.get("status") != "OK":
            raise Exception(f"Login failed: {data}")
        
        self.tokens = AuthTokens.from_headers(dict(response.headers))
        
        if not self.tokens.access_token:
            raise Exception("No access token in login response")
        
        print(f"[{datetime.now()}] Login successful, token expires at {datetime.fromtimestamp(self.tokens.expires_at)}")
        return self.tokens
    
    def refresh(self) -> AuthTokens:
        """Refresh the access token"""
        if not self.tokens or not self.tokens.refresh_token:
            print("No refresh token available, doing fresh login...")
            return self.login()
        
        print(f"[{datetime.now()}] Refreshing token...")
        
        headers = {
            "Content-Type": "application/json",
            "Origin": "https://app.bemlo.com",
            "Referer": "https://app.bemlo.com/",
            "rid": "session",
            "Authorization": f"Bearer {self.tokens.refresh_token}",
        }
        
        response = self.session.post(BEMLO_REFRESH_URL, headers=headers)
        
        if response.status_code != 200:
            print(f"Refresh failed ({response.status_code}), doing fresh login...")
            return self.login()
        
        access_token = response.headers.get('st-access-token', '')
        refresh_token = response.headers.get('st-refresh-token', self.tokens.refresh_token)
        front_token = response.headers.get('front-token', self.tokens.front_token)
        
        if not access_token:
            print("No access token in refresh response, doing fresh login...")
            return self.login()
        
        expires_at = 0
        try:
            payload_part = access_token.split('.')[1]
            payload_part += '=' * (4 - len(payload_part) % 4)
            decoded = json.loads(base64.urlsafe_b64decode(payload_part))
            expires_at = decoded.get('exp', 0)
        except Exception as e:
            print(f"Warning: Failed to decode JWT expiry: {e}")
            expires_at = int(datetime.now().timestamp()) + 3600
        
        self.tokens = AuthTokens(
            access_token=access_token,
            refresh_token=refresh_token,
            front_token=front_token,
            expires_at=expires_at
        )
        
        print(f"[{datetime.now()}] Token refreshed, expires at {datetime.fromtimestamp(expires_at)}")
        return self.tokens
    
    def ensure_valid_token(self):
        """Ensure we have a valid token"""
        if not self.tokens:
            self.login()
        elif self.tokens.is_expired():
            self.refresh()
    
    def _make_request(self, query: str, variables: dict) -> dict:
        """Make authenticated GraphQL request"""
        self.ensure_valid_token()
        
        headers = {
            "Authorization": f"Bearer {self.tokens.access_token}",
            "Content-Type": "application/json",
        }
        
        payload = {
            "query": query,
            "variables": variables
        }
        
        response = self.session.post(BEMLO_API_URL, json=payload, headers=headers)
        
        if response.status_code == 401:
            self.refresh()
            headers["Authorization"] = f"Bearer {self.tokens.access_token}"
            response = self.session.post(BEMLO_API_URL, json=payload, headers=headers)
        
        if response.status_code != 200:
            raise Exception(f"GraphQL request failed: {response.status_code} - {response.text}")
        
        return response.json()
    
    def fetch_vacancies_page(self, first: int = 30, after: str = None) -> dict:
        """Fetch a page of vacancies with enhanced data"""
        variables = {"first": first}
        if after:
            variables["after"] = after
        
        return self._make_request(VACANCIES_LIST_QUERY, variables)
    
    def fetch_vacancy_details(self, vacancy_id: str) -> dict:
        """Fetch detailed info for a single vacancy"""
        return self._make_request(VACANCY_DETAIL_QUERY, {"id": vacancy_id})
    
    def fetch_tender_schedule(self, tender_id: str) -> dict:
        """Fetch shifts/schedule for a tender"""
        return self._make_request(TENDER_SCHEDULE_QUERY, {"id": tender_id})
    
    def fetch_all_vacancies(self) -> List[dict]:
        """Fetch all vacancies with pagination"""
        all_vacancies = []
        after = None
        page = 0
        
        while True:
            page += 1
            print(f"[{datetime.now()}] Fetching page {page}...")
            
            result = self.fetch_vacancies_page(first=30, after=after)
            
            edges = result.get("data", {}).get("vacancies", {}).get("edges", [])
            page_info = result.get("data", {}).get("vacancies", {}).get("pageInfo", {})
            
            for edge in edges:
                node = edge.get("node", {})
                if node:
                    all_vacancies.append(node)
            
            print(f"[{datetime.now()}] Got {len(edges)} vacancies (total: {len(all_vacancies)})")
            
            if not page_info.get("hasNextPage"):
                break
            
            after = page_info.get("endCursor")
        
        return all_vacancies


# ============================================
# DATABASE OPERATIONS
# ============================================

def save_vacancy(conn, vacancy: dict, is_new: bool = False):
    """Save or update a vacancy in the database"""
    c = conn.cursor()
    now = int(datetime.now().timestamp())
    
    tender = vacancy.get("tender", {}) or {}
    unit = tender.get("unit", {}) or {}
    orderer = tender.get("orderer", {}) or {}
    
    # Build URL
    url = f"https://app.bemlo.com/vacancies/{vacancy.get('id')}"
    
    c.execute('''
        INSERT OR REPLACE INTO vacancies (
            id, tender_id, title,
            profession, specializations, job_type, review_status, dynamic_status,
            region, municipality,
            created_at, announced_at, job_starts_at, job_ends_at,
            last_application_date, last_presentation_date,
            procured_amount, procured_amount_currency, pricing_type,
            scope_hours, fill_rate, urgent_shifts_count,
            has_last_application_date, is_viewed,
            unit_id, unit_name, unit_municipality,
            orderer_id, orderer_name,
            first_seen_at, last_updated_at, url
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        vacancy.get("id"),
        tender.get("id"),
        vacancy.get("title"),
        vacancy.get("profession"),
        json.dumps(vacancy.get("specializations", [])),
        vacancy.get("jobType"),
        vacancy.get("reviewStatus"),
        tender.get("dynamicStatus"),
        vacancy.get("region"),
        vacancy.get("municipality") or unit.get("municipality"),
        vacancy.get("createdAt"),
        tender.get("announcedAt"),
        vacancy.get("jobStartsAt"),
        vacancy.get("jobEndsAt"),
        vacancy.get("lastApplicationDate"),
        tender.get("lastPresentationDate"),
        vacancy.get("procuredAmount"),
        vacancy.get("procuredAmountCurrency", "SEK"),
        tender.get("pricing"),
        tender.get("scope"),
        tender.get("fillRate"),
        tender.get("urgentShiftsCount", 0),
        1 if vacancy.get("hasLastApplicationDate") else 0,
        1 if vacancy.get("isViewed") else 0,
        unit.get("id"),
        unit.get("name"),
        unit.get("municipality"),
        orderer.get("id"),
        orderer.get("displayName"),
        now if is_new else c.execute("SELECT first_seen_at FROM vacancies WHERE id = ?", (vacancy.get("id"),)).fetchone()[0] if not is_new else now,
        now,
        url
    ))


def save_vacancy_details(conn, vacancy_id: str, details: dict):
    """Save detailed vacancy info including requirements, shifts, etc."""
    c = conn.cursor()
    now = int(datetime.now().timestamp())
    
    vacancy = details.get("data", {}).get("vacancy", {})
    if not vacancy:
        return
    
    tender = vacancy.get("tender", {}) or {}
    unit = tender.get("unit", {}) or {}
    contact = tender.get("contactPerson", {}) or {}
    billing = tender.get("billingInformation", {}) or {}
    
    # Update vacancy with detailed info
    c.execute('''
        UPDATE vacancies SET
            description = ?,
            vacancy_description = ?,
            candidate_description = ?,
            schedule_description = ?,
            free_text_requirements = ?,
            department = ?,
            status = ?,
            country = ?,
            experience_required = ?,
            drivers_license_required = ?,
            journal_systems = ?,
            tags = ?,
            unit_address = ?,
            contact_person_name = ?,
            contact_person_email = ?,
            contact_person_phone = ?,
            billing_cost_center = ?,
            billing_organization_name = ?,
            billing_organization_number = ?,
            billing_reference = ?,
            is_time_reportable = ?,
            details_fetched_at = ?
        WHERE id = ?
    ''', (
        vacancy.get("description"),
        tender.get("vacancyDescription"),
        tender.get("candidateDescription"),
        tender.get("scheduleDescription"),
        vacancy.get("freeTextRequirements"),
        vacancy.get("department"),
        vacancy.get("status"),
        vacancy.get("country"),
        vacancy.get("experienceRequired"),
        vacancy.get("driversLicenseRequirements"),
        json.dumps(vacancy.get("journalSystems", [])),
        json.dumps(vacancy.get("tags", [])),
        unit.get("address"),
        contact.get("fullName"),
        contact.get("email"),
        contact.get("org", {}).get("phoneNumber") if contact.get("org") else None,
        billing.get("costCenter"),
        billing.get("organizationName"),
        billing.get("organizationNumber"),
        billing.get("billingReference"),
        1 if tender.get("isTimeReportable") else 0,
        now,
        vacancy_id
    ))
    
    # Save requirements
    requirements = tender.get("requirements", {}) or {}
    
    # Delete old requirements
    c.execute("DELETE FROM requirements WHERE vacancy_id = ?", (vacancy_id,))
    
    # Custom requirements
    for req in requirements.get("customRequirements", []):
        c.execute('''
            INSERT INTO requirements (id, vacancy_id, requirement_type, title, heading, is_required, comment)
            VALUES (?, ?, 'CUSTOM', ?, ?, ?, ?)
        ''', (req.get("id"), vacancy_id, req.get("title"), req.get("heading"), 
              1 if req.get("isRequired") else 0, req.get("comment")))
    
    # Document requirements
    for req in requirements.get("documentRequirements", []):
        c.execute('''
            INSERT INTO requirements (id, vacancy_id, requirement_type, title, is_required, comment, document_type, document_max_age)
            VALUES (?, ?, 'DOCUMENT', ?, ?, ?, ?, ?)
        ''', (req.get("id"), vacancy_id, req.get("document"), 
              1 if req.get("isRequired") else 0, req.get("comment"),
              req.get("type"), req.get("maxAge")))
    
    # Experience requirements
    for req in requirements.get("experienceRequirements", []):
        c.execute('''
            INSERT INTO requirements (id, vacancy_id, requirement_type, is_required, comment, experience_department, experience_years)
            VALUES (?, ?, 'EXPERIENCE', ?, ?, ?, ?)
        ''', (req.get("id"), vacancy_id, 1 if req.get("isRequired") else 0, 
              req.get("comment"), req.get("department"), req.get("numberOfYears")))
    
    # Journal system requirements
    for req in requirements.get("journalSystemRequirements", []):
        c.execute('''
            INSERT INTO requirements (id, vacancy_id, requirement_type, is_required, comment, journal_system)
            VALUES (?, ?, 'JOURNAL_SYSTEM', ?, ?, ?)
        ''', (req.get("id"), vacancy_id, 1 if req.get("isRequired") else 0,
              req.get("comment"), req.get("journalSystem")))
    
    # Specialization requirements
    for req in requirements.get("specializationsRequirements", []):
        c.execute('''
            INSERT INTO requirements (id, vacancy_id, requirement_type, is_required, comment, specializations)
            VALUES (?, ?, 'SPECIALIZATION', ?, ?, ?)
        ''', (req.get("id"), vacancy_id, 1 if req.get("isRequired") else 0,
              req.get("comment"), json.dumps(req.get("specializations", []))))
    
    # Save price groups
    c.execute("DELETE FROM price_groups WHERE vacancy_id = ?", (vacancy_id,))
    for pg in tender.get("availablePriceGroups", []):
        c.execute('''
            INSERT INTO price_groups (id, vacancy_id, tender_id, title, pricing, base_pay, currency, specializations)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (pg.get("id"), vacancy_id, tender.get("id"), pg.get("title"),
              pg.get("pricing"), pg.get("basePay"), pg.get("currency"),
              json.dumps(pg.get("specializations", []))))


def save_shifts(conn, vacancy_id: str, tender_id: str, schedule_data: dict):
    """Save shifts from tender schedule"""
    c = conn.cursor()
    
    tender = schedule_data.get("data", {}).get("tender", {})
    if not tender:
        return 0
    
    vacant_schedule = tender.get("vacantSchedule", [])
    
    # Delete old shifts
    c.execute("DELETE FROM shifts WHERE vacancy_id = ?", (vacancy_id,))
    
    for shift in vacant_schedule:
        c.execute('''
            INSERT INTO shifts (
                id, vacancy_id, tender_id,
                date, start_time, end_time,
                start_datetime, end_datetime, break_time,
                activity, status, is_urgent, row_number, comment
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            shift.get("id"),
            vacancy_id,
            tender_id,
            shift.get("date"),
            shift.get("startTime"),
            shift.get("endTime"),
            shift.get("startDateTime"),
            shift.get("endDateTime"),
            shift.get("breakTime"),
            shift.get("activity"),
            shift.get("status"),
            1 if shift.get("isUrgent") else 0,
            shift.get("row"),
            shift.get("comment")
        ))
    
    return len(vacant_schedule)


def process_vacancies(client: BemloClient, vacancies: List[dict], fetch_details: bool = True) -> dict:
    """Process and save vacancies, return stats"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    new_count = 0
    updated_count = 0
    unchanged_count = 0
    details_fetched = 0
    shifts_fetched = 0
    
    new_vacancies = []
    updates = []
    
    for vacancy in vacancies:
        vacancy_id = vacancy.get("id")
        tender = vacancy.get("tender", {}) or {}
        
        # Check if exists
        c.execute("SELECT id, fill_rate, dynamic_status FROM vacancies WHERE id = ?", (vacancy_id,))
        existing = c.fetchone()
        
        if not existing:
            # New vacancy
            save_vacancy(conn, vacancy, is_new=True)
            new_count += 1
            new_vacancies.append({
                "id": vacancy_id,
                "title": vacancy.get("title"),
                "profession": vacancy.get("profession"),
                "municipality": vacancy.get("municipality") or tender.get("unit", {}).get("municipality"),
                "region": vacancy.get("region"),
                "rate": vacancy.get("procuredAmount"),
                "scope_hours": tender.get("scope"),
                "fill_rate": tender.get("fillRate"),
                "dynamic_status": tender.get("dynamicStatus"),
                "unit_name": tender.get("unit", {}).get("name") if tender.get("unit") else None,
                "orderer_name": tender.get("orderer", {}).get("displayName") if tender.get("orderer") else None,
                "url": f"https://app.bemlo.com/vacancies/{vacancy_id}"
            })
            
            # Fetch details for new vacancies
            if fetch_details and tender.get("id"):
                try:
                    details = client.fetch_vacancy_details(vacancy_id)
                    save_vacancy_details(conn, vacancy_id, details)
                    details_fetched += 1
                    
                    schedule = client.fetch_tender_schedule(tender.get("id"))
                    shifts_fetched += save_shifts(conn, vacancy_id, tender.get("id"), schedule)
                except Exception as e:
                    print(f"Error fetching details for {vacancy_id}: {e}")
        else:
            # Check for updates
            old_fill_rate = existing[1]
            old_status = existing[2]
            new_fill_rate = tender.get("fillRate")
            new_status = tender.get("dynamicStatus")
            
            if old_fill_rate != new_fill_rate or old_status != new_status:
                save_vacancy(conn, vacancy, is_new=False)
                updated_count += 1
                updates.append({
                    "id": vacancy_id,
                    "title": vacancy.get("title"),
                    "changes": {
                        "fill_rate": {"old": old_fill_rate, "new": new_fill_rate},
                        "status": {"old": old_status, "new": new_status}
                    }
                })
                
                # Refetch details if status changed
                if fetch_details and old_status != new_status and tender.get("id"):
                    try:
                        details = client.fetch_vacancy_details(vacancy_id)
                        save_vacancy_details(conn, vacancy_id, details)
                        details_fetched += 1
                        
                        schedule = client.fetch_tender_schedule(tender.get("id"))
                        shifts_fetched += save_shifts(conn, vacancy_id, tender.get("id"), schedule)
                    except Exception as e:
                        print(f"Error fetching details for {vacancy_id}: {e}")
            else:
                unchanged_count += 1
    
    conn.commit()
    conn.close()
    
    return {
        "new_count": new_count,
        "updated_count": updated_count,
        "unchanged_count": unchanged_count,
        "details_fetched": details_fetched,
        "shifts_fetched": shifts_fetched,
        "new_vacancies": new_vacancies,
        "updates": updates
    }


# ============================================
# API ENDPOINTS
# ============================================

@app.route("/")
def index():
    return jsonify({
        "service": "Bemlo Vacancy Scraper (Enhanced)",
        "version": "2.0.0",
        "endpoints": {
            "GET /": "This info",
            "GET /health": "Health check",
            "GET /stats": "Database statistics",
            "GET /vacancies": "Get vacancies (query params: profession, region, limit, offset)",
            "GET /vacancy/<id>": "Get single vacancy with details",
            "GET /vacancy/<id>/shifts": "Get shifts for a vacancy",
            "GET /export": "Download CSV",
            "POST /scrape": "Trigger scrape"
        }
    })


@app.route("/health")
def health():
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "configured": bool(BEMLO_EMAIL and BEMLO_PASSWORD)
    })


@app.route("/stats")
def stats():
    init_db()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Total count
    c.execute("SELECT COUNT(*) FROM vacancies")
    total = c.fetchone()[0]
    
    # By profession
    c.execute("SELECT profession, COUNT(*) FROM vacancies GROUP BY profession")
    by_profession = dict(c.fetchall())
    
    # By region (top 10)
    c.execute("SELECT region, COUNT(*) FROM vacancies GROUP BY region ORDER BY COUNT(*) DESC LIMIT 10")
    by_region = dict(c.fetchall())
    
    # By status
    c.execute("SELECT dynamic_status, COUNT(*) FROM vacancies GROUP BY dynamic_status")
    by_status = dict(c.fetchall())
    
    # Average rates
    c.execute("SELECT AVG(procured_amount) FROM vacancies WHERE profession = 'DOCTOR'")
    avg_doctor = c.fetchone()[0] or 0
    
    c.execute("SELECT AVG(procured_amount) FROM vacancies WHERE profession = 'NURSE'")
    avg_nurse = c.fetchone()[0] or 0
    
    # Recent scrapes
    c.execute("""
        SELECT timestamp, total_fetched, new_count, updated_count, details_fetched, shifts_fetched 
        FROM scrape_logs ORDER BY timestamp DESC LIMIT 5
    """)
    recent = [{"timestamp": r[0], "total": r[1], "new": r[2], "updated": r[3], 
               "details": r[4], "shifts": r[5]} for r in c.fetchall()]
    
    conn.close()
    
    return jsonify({
        "total_vacancies": total,
        "by_profession": by_profession,
        "by_region": by_region,
        "by_status": by_status,
        "avg_doctor_rate": avg_doctor,
        "avg_nurse_rate": avg_nurse,
        "recent_scrapes": recent
    })


@app.route("/vacancies")
def get_vacancies():
    init_db()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    
    # Query params
    profession = request.args.get("profession")
    region = request.args.get("region")
    status = request.args.get("status")
    limit = int(request.args.get("limit", 100))
    offset = int(request.args.get("offset", 0))
    
    query = "SELECT * FROM vacancies WHERE 1=1"
    params = []
    
    if profession:
        query += " AND profession = ?"
        params.append(profession)
    if region:
        query += " AND region = ?"
        params.append(region)
    if status:
        query += " AND dynamic_status = ?"
        params.append(status)
    
    query += " ORDER BY announced_at DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    
    c.execute(query, params)
    rows = c.fetchall()
    
    vacancies = []
    for row in rows:
        v = dict(row)
        # Parse JSON fields
        for field in ['specializations', 'journal_systems', 'tags']:
            if v.get(field):
                try:
                    v[field] = json.loads(v[field])
                except:
                    pass
        vacancies.append(v)
    
    conn.close()
    return jsonify(vacancies)


@app.route("/vacancy/<vacancy_id>")
def get_vacancy(vacancy_id):
    init_db()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    
    # Get vacancy
    c.execute("SELECT * FROM vacancies WHERE id = ?", (vacancy_id,))
    row = c.fetchone()
    
    if not row:
        return jsonify({"error": "Vacancy not found"}), 404
    
    vacancy = dict(row)
    
    # Parse JSON fields
    for field in ['specializations', 'journal_systems', 'tags']:
        if vacancy.get(field):
            try:
                vacancy[field] = json.loads(vacancy[field])
            except:
                pass
    
    # Get requirements
    c.execute("SELECT * FROM requirements WHERE vacancy_id = ?", (vacancy_id,))
    vacancy["requirements"] = [dict(r) for r in c.fetchall()]
    
    # Get price groups
    c.execute("SELECT * FROM price_groups WHERE vacancy_id = ?", (vacancy_id,))
    vacancy["price_groups"] = [dict(p) for p in c.fetchall()]
    
    # Get shifts
    c.execute("SELECT * FROM shifts WHERE vacancy_id = ? ORDER BY date, start_time", (vacancy_id,))
    vacancy["shifts"] = [dict(s) for s in c.fetchall()]
    
    conn.close()
    return jsonify(vacancy)


@app.route("/vacancy/<vacancy_id>/shifts")
def get_vacancy_shifts(vacancy_id):
    init_db()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    
    c.execute("SELECT * FROM shifts WHERE vacancy_id = ? ORDER BY date, start_time", (vacancy_id,))
    shifts = [dict(s) for s in c.fetchall()]
    
    conn.close()
    return jsonify(shifts)


@app.route("/export")
def export_csv():
    init_db()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    c.execute("""
        SELECT id, title, profession, specializations, region, municipality,
               procured_amount, procured_amount_currency, scope_hours, fill_rate,
               dynamic_status, unit_name, orderer_name,
               job_starts_at, job_ends_at, announced_at, url
        FROM vacancies 
        ORDER BY announced_at DESC
    """)
    
    rows = c.fetchall()
    conn.close()
    
    # Build CSV
    headers = ["id", "title", "profession", "specializations", "region", "municipality",
               "rate", "currency", "scope_hours", "fill_rate", "status",
               "unit_name", "orderer_name", "job_starts_at", "job_ends_at", "announced_at", "url"]
    
    csv_lines = [",".join(headers)]
    for row in rows:
        csv_lines.append(",".join([
            f'"{str(v).replace('"', '""') if v else ""}"' for v in row
        ]))
    
    return Response(
        "\n".join(csv_lines),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment;filename=bemlo_vacancies.csv"}
    )


@app.route("/scrape", methods=["POST"])
def scrape():
    if not BEMLO_EMAIL or not BEMLO_PASSWORD:
        return jsonify({"error": "Bemlo credentials not configured"}), 500
    
    init_db()
    start_time = datetime.now()
    
    try:
        client = BemloClient(BEMLO_EMAIL, BEMLO_PASSWORD)
        client.login()
        
        vacancies = client.fetch_all_vacancies()
        
        # Process with detail fetching enabled
        result = process_vacancies(client, vacancies, fetch_details=True)
        
        duration = (datetime.now() - start_time).total_seconds()
        
        # Log scrape
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""
            INSERT INTO scrape_logs (timestamp, total_fetched, new_count, updated_count, 
                                    details_fetched, shifts_fetched, duration_seconds)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (int(start_time.timestamp()), len(vacancies), result["new_count"], 
              result["updated_count"], result["details_fetched"], result["shifts_fetched"], duration))
        conn.commit()
        conn.close()
        
        response = {
            "timestamp": start_time.isoformat(),
            "duration_seconds": duration,
            "total_fetched": len(vacancies),
            "new_count": result["new_count"],
            "updated_count": result["updated_count"],
            "unchanged_count": result["unchanged_count"],
            "details_fetched": result["details_fetched"],
            "shifts_fetched": result["shifts_fetched"],
            "new_vacancies": result["new_vacancies"][:50],  # Limit response size
            "updates": result["updates"]
        }
        
        # Optional: Send webhook notification
        if WEBHOOK_URL and result["new_count"] > 0:
            try:
                requests.post(WEBHOOK_URL, json={
                    "text": f"🏥 Bemlo Scraper: {result['new_count']} new vacancies found"
                })
            except:
                pass
        
        return jsonify(response)
        
    except Exception as e:
        # Log error
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""
            INSERT INTO scrape_logs (timestamp, total_fetched, new_count, updated_count, 
                                    duration_seconds, error)
            VALUES (?, 0, 0, 0, ?, ?)
        """, (int(start_time.timestamp()), (datetime.now() - start_time).total_seconds(), str(e)))
        conn.commit()
        conn.close()
        
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
