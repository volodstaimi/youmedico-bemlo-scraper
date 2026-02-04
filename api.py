"""
Bemlo Vacancy Scraper API - v3
Deployed on Google Cloud Run

Endpoints:
- GET /health - Health check
- GET /scrape - Fetch newest vacancies (basic list)
- GET /vacancy/<id>/details - Fetch full details for a single vacancy
"""

import os
import json
import base64
import random
import time
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from flask import Flask, jsonify, request
import requests

# ============================================
# CONFIGURATION
# ============================================

BEMLO_API_URL = "https://api.bemlo.ai/graphql"
BEMLO_AUTH_URL = "https://api.bemlo.ai/auth/signin"
BEMLO_REFRESH_URL = "https://api.bemlo.ai/auth/session/refresh"

BEMLO_EMAIL = os.environ.get("BEMLO_EMAIL", "")
BEMLO_PASSWORD = os.environ.get("BEMLO_PASSWORD", "")

app = Flask(__name__)

# ============================================
# BROWSER-LIKE HEADERS (to avoid detection)
# ============================================

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
]

def get_browser_headers(include_auth: str = None) -> dict:
    """Get realistic browser headers"""
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9,sv;q=0.8",
        "Cache-Control": "no-cache",
        "Content-Type": "application/json",
        "Origin": "https://app.bemlo.com",
        "Pragma": "no-cache",
        "Referer": "https://app.bemlo.com/",
        "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "User-Agent": random.choice(USER_AGENTS),
    }
    if include_auth:
        headers["Authorization"] = f"Bearer {include_auth}"
        headers["st-auth-mode"] = "header"
    return headers


# ============================================
# GRAPHQL QUERIES
# ============================================

VACANCIES_LIST_QUERY = """
query VacanciesList($afterCursor: String, $filter: VacancyFilter!, $orderBy: VacancyOrderBy!, $orderDir: OrderByDirection!, $take: Int!) {
  allVacancies(
    afterCursor: $afterCursor
    filter: $filter
    orderBy: $orderBy
    orderDir: $orderDir
    take: $take
  ) {
    pageInfo {
      hasNextPage
      startCursor
      endCursor
      __typename
    }
    edges {
      cursor
      node {
        createdAt
        hasLastApplicationDate
        id
        profession
        specializations
        isViewed
        jobType
        jobEndsAt
        jobStartsAt
        lastApplicationDate
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
            __typename
          }
          orderer {
            id
            displayName
            graphicProfile {
              logoUrl
              avatarUrl
              primaryColor
              secondaryColor
              __typename
            }
            __typename
          }
          __typename
        }
        title
        __typename
      }
      __typename
    }
    __typename
  }
}
"""

VACANCY_DETAIL_QUERY = """
query VacancyById($id: String!) {
  vacancy(id: $id) {
    assignedTo
    createdAt
    description
    department
    driversLicenseRequirements
    employerId
    expectedCompensation {
      high
      low
      __typename
    }
    freeTextRequirements
    hasLastApplicationDate
    id
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
    tenderId
    tender {
      bid {
        id
        score
        scoreComponents
        scoredAt
        numberOfPresentations
        numberOfHours
        specializationRank
        __typename
      }
      extensionFor {
        id
        presentation {
          id
          name
          __typename
        }
        __typename
      }
      rows
      enforceRows
      id
      announcedAt
      dynamicStatus
      pricing
      assignedAt
      doesAcceptPresentations
      lastPresentationDate
      maxPresentations
      specializationPriority
      scope
      profession
      scheduleType
      scheduleDescription
      title
      fillRate
      startsAt
      endsAt
      createdAt
      updatedAt
      department
      vacancyDescription
      candidateDescription
      isTimeReportable
      contactPersonUserId
      contactPerson {
        avatarUrl
        email
        fullName
        id
        org {
          phoneNumber
          __typename
        }
        __typename
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
        openingHours {
          id
          day
          openingHours
          closingHours
          __typename
        }
        __typename
      }
      orderer {
        id
        displayName
        graphicProfile {
          logoUrl
          avatarUrl
          primaryColor
          secondaryColor
          __typename
        }
        __typename
      }
      attachments {
        id
        createdAt
        description
        referenceUrl
        title
        __typename
      }
      attributionCriteria {
        id
        type
        weight
        __typename
      }
      billingInformation {
        id
        title
        billingAddressStreet1
        billingAddressStreet2
        billingAddressPostalCode
        billingAddressPlace
        billingAddressCountry
        costCenter
        organizationName
        organizationNumber
        billingEmail
        billingReference
        comment
        __typename
      }
      requirements {
        id
        customRequirements {
          id
          comment
          isRequired
          title
          heading
          __typename
        }
        documentRequirements {
          id
          comment
          isRequired
          document
          maxAge
          type
          __typename
        }
        driversLicenseRequirements {
          id
          comment
          isRequired
          driversLicense
          __typename
        }
        experienceRequirements {
          id
          comment
          isRequired
          department
          numberOfYears
          __typename
        }
        journalSystemRequirements {
          id
          comment
          isRequired
          journalSystem
          __typename
        }
        specializationsRequirements {
          id
          comment
          isRequired
          specializations
          __typename
        }
        __typename
      }
      availablePriceGroups {
        id
        supplierId
        priceGroupId
        tenderId
        pricing
        title
        specializations
        basePay
        currency
        compensationModelId
        __typename
      }
      shifts {
        shift {
          id
          activity
          date
          startTime
          endTime
          startDateTime
          endDateTime
          breakTime
          comment
          onCallTime {
            id
            type
            startDateTime
            endDateTime
            startTime
            endTime
            __typename
          }
          __typename
        }
        id
        row
        status
        tenderId
        __typename
      }
      __typename
    }
    title
    procuredAmount
    procuredAmountCurrency
    experienceRequired
    journalSystems
    tags
    __typename
  }
}
"""


# ============================================
# AUTH TOKEN HANDLING
# ============================================

@dataclass
class AuthTokens:
    access_token: str
    refresh_token: Optional[str] = None
    front_token: Optional[str] = None
    expires_at: int = 0
    
    def is_expired(self, buffer_seconds: int = 300) -> bool:
        return datetime.now().timestamp() > (self.expires_at - buffer_seconds)
    
    @classmethod
    def from_headers(cls, headers: Dict[str, str]) -> 'AuthTokens':
        access_token = headers.get('st-access-token', '')
        refresh_token = headers.get('st-refresh-token', '')
        front_token = headers.get('front-token', '')
        
        expires_at = 0
        if access_token:
            try:
                payload = access_token.split('.')[1]
                payload += '=' * (4 - len(payload) % 4)
                decoded = json.loads(base64.urlsafe_b64decode(payload))
                expires_at = decoded.get('exp', 0)
            except:
                pass
        
        return cls(
            access_token=access_token,
            refresh_token=refresh_token,
            front_token=front_token,
            expires_at=expires_at
        )


# ============================================
# BEMLO API CLIENT
# ============================================

class BemloClient:
    def __init__(self, email: str, password: str):
        self.email = email
        self.password = password
        self.session = requests.Session()
        self.tokens: Optional[AuthTokens] = None
    
    def login(self) -> AuthTokens:
        """Login to Bemlo with browser-like headers"""
        print(f"[{datetime.now()}] Logging in to Bemlo...")
        
        headers = get_browser_headers()
        headers["rid"] = "emailpassword"
        
        payload = {
            "formFields": [
                {"id": "email", "value": self.email},
                {"id": "password", "value": self.password}
            ]
        }
        
        # Small random delay to seem more human
        time.sleep(random.uniform(0.1, 0.3))
        
        response = self.session.post(BEMLO_AUTH_URL, json=payload, headers=headers)
        
        if response.status_code != 200:
            raise Exception(f"Login failed: {response.status_code} - {response.text}")
        
        data = response.json()
        if data.get("status") != "OK":
            raise Exception(f"Login failed: {data}")
        
        self.tokens = AuthTokens.from_headers(dict(response.headers))
        
        if not self.tokens.access_token:
            raise Exception("No access token in login response")
        
        print(f"[{datetime.now()}] Login successful, expires at {datetime.fromtimestamp(self.tokens.expires_at)}")
        return self.tokens
    
    def refresh(self) -> AuthTokens:
        """Refresh token with browser-like headers"""
        if not self.tokens or not self.tokens.refresh_token:
            return self.login()
        
        print(f"[{datetime.now()}] Refreshing token...")
        
        headers = get_browser_headers()
        headers["rid"] = "session"
        headers["Authorization"] = f"Bearer {self.tokens.refresh_token}"
        
        response = self.session.post(BEMLO_REFRESH_URL, headers=headers)
        
        if response.status_code != 200:
            print(f"Refresh failed, doing fresh login...")
            return self.login()
        
        self.tokens = AuthTokens.from_headers(dict(response.headers))
        print(f"[{datetime.now()}] Token refreshed")
        return self.tokens
    
    def ensure_valid_token(self):
        """Ensure we have a valid token"""
        if not self.tokens:
            self.login()
        elif self.tokens.is_expired():
            self.refresh()
    
    def _graphql_request(self, operation_name: str, query: str, variables: dict) -> dict:
        """Make GraphQL request with browser-like headers"""
        self.ensure_valid_token()
        
        headers = get_browser_headers(include_auth=self.tokens.access_token)
        
        payload = {
            "operationName": operation_name,
            "query": query,
            "variables": variables
        }
        
        # Small random delay between requests
        time.sleep(random.uniform(0.1, 0.5))
        
        response = self.session.post(BEMLO_API_URL, json=payload, headers=headers)
        
        # Handle 401 by refreshing
        if response.status_code == 401:
            self.refresh()
            headers = get_browser_headers(include_auth=self.tokens.access_token)
            response = self.session.post(BEMLO_API_URL, json=payload, headers=headers)
        
        if response.status_code != 200:
            raise Exception(f"GraphQL failed: {response.status_code} - {response.text}")
        
        result = response.json()
        if "errors" in result:
            raise Exception(f"GraphQL errors: {result['errors']}")
        
        return result
    
    def fetch_vacancies_list(self, take: int = 30, after_cursor: str = None) -> dict:
        """Fetch list of vacancies (basic info)"""
        variables = {
            "filter": {
                "AND": [
                    {"tender": {"doesAcceptPresentations": True}},
                    {}
                ]
            },
            "take": take,
            "orderBy": "CREATED_AT",
            "orderDir": "DESC"
        }
        if after_cursor:
            variables["afterCursor"] = after_cursor
        
        return self._graphql_request("VacanciesList", VACANCIES_LIST_QUERY, variables)
    
    def fetch_vacancy_details(self, vacancy_id: str) -> dict:
        """Fetch full details for a single vacancy"""
        return self._graphql_request("VacancyById", VACANCY_DETAIL_QUERY, {"id": vacancy_id})


# Global client instance (reused across requests to maintain session)
_client: Optional[BemloClient] = None

def get_client() -> BemloClient:
    global _client
    if _client is None:
        if not BEMLO_EMAIL or not BEMLO_PASSWORD:
            raise Exception("BEMLO_EMAIL and BEMLO_PASSWORD not configured")
        _client = BemloClient(BEMLO_EMAIL, BEMLO_PASSWORD)
    return _client


# ============================================
# API ENDPOINTS
# ============================================

@app.route("/", methods=["GET"])
def index():
    return jsonify({
        "service": "Bemlo Vacancy Scraper",
        "version": "3.0",
        "endpoints": {
            "GET /health": "Health check",
            "GET /scrape": "Fetch newest vacancies (params: take, cursor)",
            "GET /scrape/full": "Fetch ALL vacancies",
            "GET /vacancy/<id>/details": "Fetch full details for a vacancy"
        }
    })


@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "configured": bool(BEMLO_EMAIL and BEMLO_PASSWORD)
    })


@app.route("/scrape", methods=["GET", "POST"])
def scrape():
    """
    Fetch newest vacancies (basic list info)
    
    Query params:
        take: Number of vacancies to fetch (default: 30, max: 100)
        cursor: Pagination cursor for next page
    
    Returns basic vacancy list - Supabase decides which need details
    """
    try:
        take = min(int(request.args.get("take", 30)), 100)
        cursor = request.args.get("cursor")
        
        client = get_client()
        client.ensure_valid_token()
        
        result = client.fetch_vacancies_list(take=take, after_cursor=cursor)
        
        all_vac = result.get("data", {}).get("allVacancies", {})
        edges = all_vac.get("edges", [])
        page_info = all_vac.get("pageInfo", {})
        
        # Extract vacancy data
        vacancies = []
        for edge in edges:
            node = edge.get("node", {})
            if not node:
                continue
            
            tender = node.get("tender", {}) or {}
            unit = tender.get("unit", {}) or {}
            orderer = tender.get("orderer", {}) or {}
            
            vacancies.append({
                "id": node.get("id"),
                "title": node.get("title"),
                "created_at": node.get("createdAt"),
                "profession": node.get("profession"),
                "specializations": node.get("specializations", []),
                "job_type": node.get("jobType"),
                "review_status": node.get("reviewStatus"),
                "job_starts_at": node.get("jobStartsAt"),
                "job_ends_at": node.get("jobEndsAt"),
                "last_application_date": node.get("lastApplicationDate"),
                "has_last_application_date": node.get("hasLastApplicationDate"),
                "procured_amount": node.get("procuredAmount"),
                "procured_amount_currency": node.get("procuredAmountCurrency"),
                "municipality": node.get("municipality"),
                "region": node.get("region"),
                "tender_id": tender.get("id"),
                "tender_title": tender.get("title"),
                "announced_at": tender.get("announcedAt"),
                "starts_at": tender.get("startsAt"),
                "ends_at": tender.get("endsAt"),
                "scope": tender.get("scope"),
                "pricing": tender.get("pricing"),
                "schedule_type": tender.get("scheduleType"),
                "dynamic_status": tender.get("dynamicStatus"),
                "fill_rate": tender.get("fillRate"),
                "urgent_shifts_count": tender.get("urgentShiftsCount"),
                "last_presentation_date": tender.get("lastPresentationDate"),
                "unit_id": unit.get("id"),
                "unit_name": unit.get("name"),
                "unit_municipality": unit.get("municipality"),
                "orderer_id": orderer.get("id"),
                "orderer_name": orderer.get("displayName"),
                "url": f"https://app.bemlo.com/vacancies/{node.get('id')}"
            })
        
        return jsonify({
            "success": True,
            "timestamp": datetime.now().isoformat(),
            "count": len(vacancies),
            "has_next_page": page_info.get("hasNextPage", False),
            "next_cursor": page_info.get("endCursor"),
            "vacancies": vacancies
        })
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/scrape/full", methods=["GET", "POST"])
def scrape_full():
    """
    Fetch ALL vacancies (for initial sync)
    Warning: This can take a while and returns a lot of data
    """
    try:
        client = get_client()
        client.ensure_valid_token()
        
        all_vacancies = []
        cursor = None
        page = 0
        
        while True:
            page += 1
            print(f"[{datetime.now()}] Fetching page {page}...")
            
            result = client.fetch_vacancies_list(take=30, after_cursor=cursor)
            
            all_vac = result.get("data", {}).get("allVacancies", {})
            edges = all_vac.get("edges", [])
            page_info = all_vac.get("pageInfo", {})
            
            for edge in edges:
                node = edge.get("node", {})
                if not node:
                    continue
                
                tender = node.get("tender", {}) or {}
                unit = tender.get("unit", {}) or {}
                orderer = tender.get("orderer", {}) or {}
                
                all_vacancies.append({
                    "id": node.get("id"),
                    "title": node.get("title"),
                    "created_at": node.get("createdAt"),
                    "profession": node.get("profession"),
                    "specializations": node.get("specializations", []),
                    "job_type": node.get("jobType"),
                    "review_status": node.get("reviewStatus"),
                    "job_starts_at": node.get("jobStartsAt"),
                    "job_ends_at": node.get("jobEndsAt"),
                    "last_application_date": node.get("lastApplicationDate"),
                    "procured_amount": node.get("procuredAmount"),
                    "procured_amount_currency": node.get("procuredAmountCurrency"),
                    "municipality": node.get("municipality"),
                    "region": node.get("region"),
                    "tender_id": tender.get("id"),
                    "tender_title": tender.get("title"),
                    "announced_at": tender.get("announcedAt"),
                    "starts_at": tender.get("startsAt"),
                    "ends_at": tender.get("endsAt"),
                    "scope": tender.get("scope"),
                    "pricing": tender.get("pricing"),
                    "schedule_type": tender.get("scheduleType"),
                    "dynamic_status": tender.get("dynamicStatus"),
                    "fill_rate": tender.get("fillRate"),
                    "urgent_shifts_count": tender.get("urgentShiftsCount"),
                    "unit_id": unit.get("id"),
                    "unit_name": unit.get("name"),
                    "orderer_id": orderer.get("id"),
                    "orderer_name": orderer.get("displayName"),
                    "url": f"https://app.bemlo.com/vacancies/{node.get('id')}"
                })
            
            print(f"[{datetime.now()}] Got {len(edges)} (total: {len(all_vacancies)})")
            
            if not page_info.get("hasNextPage"):
                break
            
            cursor = page_info.get("endCursor")
        
        return jsonify({
            "success": True,
            "timestamp": datetime.now().isoformat(),
            "total_pages": page,
            "count": len(all_vacancies),
            "vacancies": all_vacancies
        })
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/vacancy/<vacancy_id>/details", methods=["GET"])
def get_vacancy_details(vacancy_id: str):
    """
    Fetch full details for a single vacancy
    
    Returns:
        - Full vacancy info with description
        - All shifts/schedule
        - Requirements
        - Price groups
        - Contact info
        - Unit details
    """
    try:
        client = get_client()
        client.ensure_valid_token()
        
        result = client.fetch_vacancy_details(vacancy_id)
        
        vacancy = result.get("data", {}).get("vacancy")
        if not vacancy:
            return jsonify({"success": False, "error": "Vacancy not found"}), 404
        
        tender = vacancy.get("tender", {}) or {}
        unit = tender.get("unit", {}) or {}
        orderer = tender.get("orderer", {}) or {}
        contact = tender.get("contactPerson", {}) or {}
        requirements = tender.get("requirements", {}) or {}
        billing = tender.get("billingInformation", {}) or {}
        
        # Parse shifts
        shifts = []
        for ts in tender.get("shifts", []) or []:
            shift = ts.get("shift", {}) or {}
            on_call = shift.get("onCallTime", {}) or {}
            shifts.append({
                "id": ts.get("id"),
                "shift_id": shift.get("id"),
                "tender_id": ts.get("tenderId"),
                "row": ts.get("row"),
                "status": ts.get("status"),
                "date": shift.get("date"),
                "start_time": shift.get("startTime"),
                "end_time": shift.get("endTime"),
                "start_datetime": shift.get("startDateTime"),
                "end_datetime": shift.get("endDateTime"),
                "break_time": shift.get("breakTime"),
                "activity": shift.get("activity"),
                "comment": shift.get("comment"),
                "on_call_type": on_call.get("type"),
                "on_call_start": on_call.get("startDateTime"),
                "on_call_end": on_call.get("endDateTime"),
            })
        
        # Parse requirements
        all_requirements = []
        
        for req in requirements.get("customRequirements", []) or []:
            all_requirements.append({
                "type": "custom",
                "id": req.get("id"),
                "title": req.get("title"),
                "heading": req.get("heading"),
                "is_required": req.get("isRequired"),
                "comment": req.get("comment"),
            })
        
        for req in requirements.get("documentRequirements", []) or []:
            all_requirements.append({
                "type": "document",
                "id": req.get("id"),
                "document": req.get("document"),
                "document_type": req.get("type"),
                "max_age": req.get("maxAge"),
                "is_required": req.get("isRequired"),
                "comment": req.get("comment"),
            })
        
        for req in requirements.get("experienceRequirements", []) or []:
            all_requirements.append({
                "type": "experience",
                "id": req.get("id"),
                "department": req.get("department"),
                "years": req.get("numberOfYears"),
                "is_required": req.get("isRequired"),
                "comment": req.get("comment"),
            })
        
        for req in requirements.get("journalSystemRequirements", []) or []:
            all_requirements.append({
                "type": "journal_system",
                "id": req.get("id"),
                "journal_system": req.get("journalSystem"),
                "is_required": req.get("isRequired"),
                "comment": req.get("comment"),
            })
        
        for req in requirements.get("driversLicenseRequirements", []) or []:
            all_requirements.append({
                "type": "drivers_license",
                "id": req.get("id"),
                "license": req.get("driversLicense"),
                "is_required": req.get("isRequired"),
                "comment": req.get("comment"),
            })
        
        for req in requirements.get("specializationsRequirements", []) or []:
            all_requirements.append({
                "type": "specialization",
                "id": req.get("id"),
                "specializations": req.get("specializations"),
                "is_required": req.get("isRequired"),
                "comment": req.get("comment"),
            })
        
        # Parse price groups
        price_groups = []
        for pg in tender.get("availablePriceGroups", []) or []:
            price_groups.append({
                "id": pg.get("id"),
                "tender_id": pg.get("tenderId"),
                "title": pg.get("title"),
                "pricing": pg.get("pricing"),
                "base_pay": pg.get("basePay"),
                "currency": pg.get("currency"),
                "specializations": pg.get("specializations"),
            })
        
        # Parse attachments
        attachments = []
        for att in tender.get("attachments", []) or []:
            attachments.append({
                "id": att.get("id"),
                "title": att.get("title"),
                "description": att.get("description"),
                "url": att.get("referenceUrl"),
                "created_at": att.get("createdAt"),
            })
        
        # Build response
        response = {
            "success": True,
            "timestamp": datetime.now().isoformat(),
            "vacancy": {
                "id": vacancy.get("id"),
                "title": vacancy.get("title"),
                "description": vacancy.get("description"),
                "vacancy_description": tender.get("vacancyDescription"),
                "candidate_description": tender.get("candidateDescription"),
                "schedule_description": tender.get("scheduleDescription"),
                "created_at": vacancy.get("createdAt"),
                "profession": vacancy.get("profession"),
                "specializations": vacancy.get("specializations", []),
                "is_specialization_required": vacancy.get("isSpecializationRequired"),
                "job_type": vacancy.get("jobType"),
                "status": vacancy.get("status"),
                "review_status": vacancy.get("reviewStatus"),
                "job_starts_at": vacancy.get("jobStartsAt"),
                "job_ends_at": vacancy.get("jobEndsAt"),
                "last_application_date": vacancy.get("lastApplicationDate"),
                "procured_amount": vacancy.get("procuredAmount"),
                "procured_amount_currency": vacancy.get("procuredAmountCurrency"),
                "expected_compensation_low": vacancy.get("expectedCompensation", {}).get("low") if vacancy.get("expectedCompensation") else None,
                "expected_compensation_high": vacancy.get("expectedCompensation", {}).get("high") if vacancy.get("expectedCompensation") else None,
                "experience_required": vacancy.get("experienceRequired"),
                "journal_systems": vacancy.get("journalSystems"),
                "drivers_license_requirements": vacancy.get("driversLicenseRequirements"),
                "free_text_requirements": vacancy.get("freeTextRequirements"),
                "department": vacancy.get("department"),
                "municipality": vacancy.get("municipality"),
                "region": vacancy.get("region"),
                "country": vacancy.get("country"),
                "tags": vacancy.get("tags"),
                "internal_comment": vacancy.get("internalComment"),
                "url": f"https://app.bemlo.com/vacancies/{vacancy.get('id')}"
            },
            "tender": {
                "id": tender.get("id"),
                "title": tender.get("title"),
                "announced_at": tender.get("announcedAt"),
                "starts_at": tender.get("startsAt"),
                "ends_at": tender.get("endsAt"),
                "created_at": tender.get("createdAt"),
                "updated_at": tender.get("updatedAt"),
                "scope": tender.get("scope"),
                "pricing": tender.get("pricing"),
                "schedule_type": tender.get("scheduleType"),
                "dynamic_status": tender.get("dynamicStatus"),
                "fill_rate": tender.get("fillRate"),
                "rows": tender.get("rows"),
                "enforce_rows": tender.get("enforceRows"),
                "max_presentations": tender.get("maxPresentations"),
                "specialization_priority": tender.get("specializationPriority"),
                "does_accept_presentations": tender.get("doesAcceptPresentations"),
                "last_presentation_date": tender.get("lastPresentationDate"),
                "is_time_reportable": tender.get("isTimeReportable"),
            },
            "unit": {
                "id": unit.get("id"),
                "name": unit.get("name"),
                "description": unit.get("description"),
                "address": unit.get("address"),
                "municipality": unit.get("municipality"),
                "region": unit.get("region"),
                "country": unit.get("country"),
                "contact_name": unit.get("contactName"),
                "contact_role": unit.get("contactRole"),
                "contact_email": unit.get("contactEmail"),
                "contact_phone": unit.get("contactPhoneNumber"),
                "opening_hours": unit.get("openingHours"),
            },
            "orderer": {
                "id": orderer.get("id"),
                "name": orderer.get("displayName"),
                "logo_url": orderer.get("graphicProfile", {}).get("logoUrl") if orderer.get("graphicProfile") else None,
            },
            "contact_person": {
                "id": contact.get("id"),
                "name": contact.get("fullName"),
                "email": contact.get("email"),
                "phone": contact.get("org", {}).get("phoneNumber") if contact.get("org") else None,
                "avatar_url": contact.get("avatarUrl"),
            },
            "shifts": shifts,
            "shifts_count": len(shifts),
            "requirements": all_requirements,
            "requirements_count": len(all_requirements),
            "price_groups": price_groups,
            "attachments": attachments,
        }
        
        return jsonify(response)
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
