"""
Bemlo Scraper - GCP Cloud Run API
Handles authentication with SuperTokens and scrapes Bemlo GraphQL API
"""

import os
import json
import sqlite3
import requests
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Optional, Dict, List, Any, Tuple
from dataclasses import dataclass, asdict
import base64

# Configuration
PORT = int(os.environ.get("PORT", 8080))
BEMLO_EMAIL = os.environ.get("BEMLO_EMAIL")
BEMLO_PASSWORD = os.environ.get("BEMLO_PASSWORD")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL")
DB_PATH = os.environ.get("DB_PATH", "/tmp/bemlo_vacancies.db")

# Bemlo API endpoints
BEMLO_GRAPHQL_URL = "https://api.bemlo.ai/graphql"
BEMLO_AUTH_URL = "https://api.bemlo.ai/auth/signin"
BEMLO_REFRESH_URL = "https://api.bemlo.ai/auth/session/refresh"

# Request headers needed for SuperTokens
SUPERTOKENS_HEADERS = {
    "Content-Type": "application/json",
    "Origin": "https://app.bemlo.com",
    "Referer": "https://app.bemlo.com/",
}

# GraphQL query
VACANCIES_QUERY = """
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
          }
          orderer {
            id
            displayName
          }
        }
        title
      }
    }
  }
}
"""


@dataclass
class AuthTokens:
    """Stores Bemlo authentication tokens"""
    access_token: str
    refresh_token: Optional[str] = None
    front_token: Optional[str] = None
    expires_at: int = 0  # Unix timestamp
    
    def is_expired(self, buffer_seconds: int = 300) -> bool:
        """Check if token is expired (with buffer)"""
        return datetime.now().timestamp() > (self.expires_at - buffer_seconds)
    
    @classmethod
    def from_headers(cls, headers: Dict[str, str]) -> 'AuthTokens':
        """Extract tokens from response headers"""
        access_token = headers.get('st-access-token', '')
        refresh_token = headers.get('st-refresh-token', '')
        front_token = headers.get('front-token', '')
        
        # Decode JWT to get expiry
        expires_at = 0
        if access_token:
            try:
                # JWT is base64 encoded in 3 parts: header.payload.signature
                payload = access_token.split('.')[1]
                # Add padding if needed
                payload += '=' * (4 - len(payload) % 4)
                decoded = json.loads(base64.urlsafe_b64decode(payload))
                expires_at = decoded.get('exp', 0)
            except Exception as e:
                print(f"Failed to decode JWT: {e}")
        
        return cls(
            access_token=access_token,
            refresh_token=refresh_token,
            front_token=front_token,
            expires_at=expires_at
        )


class BemloAuth:
    """Handles Bemlo SuperTokens authentication"""
    
    def __init__(self, email: str, password: str):
        self.email = email
        self.password = password
        self.tokens: Optional[AuthTokens] = None
        self.session = requests.Session()
        self._setup_session()
    
    def _setup_session(self):
        """Configure session headers"""
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Origin": "https://app.bemlo.com",
            "Referer": "https://app.bemlo.com/",
        })
    
    def login(self) -> AuthTokens:
        """Login to Bemlo and get fresh tokens"""
        print(f"[{datetime.now()}] Logging in to Bemlo...")
        
        # SuperTokens emailpassword signin
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
        
        # Extract tokens from response headers (SuperTokens style)
        access_token = response.headers.get('st-access-token', '')
        refresh_token = response.headers.get('st-refresh-token', '')
        front_token = response.headers.get('front-token', '')
        
        if not access_token:
            raise Exception("No st-access-token in login response headers")
        
        # Decode JWT to get expiry
        expires_at = 0
        try:
            payload_part = access_token.split('.')[1]
            # Add padding for base64
            payload_part += '=' * (4 - len(payload_part) % 4)
            decoded = json.loads(base64.urlsafe_b64decode(payload_part))
            expires_at = decoded.get('exp', 0)
        except Exception as e:
            print(f"Warning: Failed to decode JWT expiry: {e}")
            # Default to 1 hour from now
            expires_at = int(datetime.now().timestamp()) + 3600
        
        self.tokens = AuthTokens(
            access_token=access_token,
            refresh_token=refresh_token,
            front_token=front_token,
            expires_at=expires_at
        )
        
        print(f"[{datetime.now()}] Login successful, token expires at {datetime.fromtimestamp(expires_at)}")
        return self.tokens
    
    def refresh(self) -> AuthTokens:
        """Refresh the access token using refresh token"""
        if not self.tokens or not self.tokens.refresh_token:
            print("No refresh token available, doing fresh login...")
            return self.login()
        
        print(f"[{datetime.now()}] Refreshing token...")
        
        # SuperTokens refresh uses the refresh token in a header
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
        
        # Extract new tokens from response headers
        access_token = response.headers.get('st-access-token', '')
        refresh_token = response.headers.get('st-refresh-token', self.tokens.refresh_token)
        front_token = response.headers.get('front-token', self.tokens.front_token)
        
        if not access_token:
            print("No access token in refresh response, doing fresh login...")
            return self.login()
        
        # Decode JWT to get expiry
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
    
    def get_valid_token(self) -> str:
        """Get a valid access token, refreshing if needed"""
        if not self.tokens:
            self.login()
        elif self.tokens.is_expired():
            self.refresh()
        
        return self.tokens.access_token


class BemloScraper:
    """Scrapes Bemlo vacancies via GraphQL API"""
    
    def __init__(self, auth: BemloAuth):
        self.auth = auth
        self.session = requests.Session()
        self._init_database()
    
    def _init_database(self):
        """Initialize SQLite database"""
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS vacancies (
                id TEXT PRIMARY KEY,
                title TEXT,
                profession TEXT,
                specializations TEXT,
                municipality TEXT,
                region TEXT,
                job_starts_at INTEGER,
                job_ends_at INTEGER,
                procured_amount REAL,
                procured_amount_currency TEXT,
                scope_hours REAL,
                fill_rate REAL,
                dynamic_status TEXT,
                tender_id TEXT,
                tender_title TEXT,
                unit_id TEXT,
                unit_name TEXT,
                orderer_id TEXT,
                orderer_name TEXT,
                last_application_date INTEGER,
                created_at INTEGER,
                announced_at INTEGER,
                scraped_at INTEGER,
                first_seen_at INTEGER,
                last_updated_at INTEGER,
                raw_data TEXT
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS scrape_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scraped_at INTEGER,
                total_fetched INTEGER,
                new_count INTEGER,
                updated_count INTEGER,
                duration_seconds REAL
            )
        """)
        
        conn.commit()
        conn.close()
    
    def _graphql_request(self, query: str, variables: Dict, retry_on_401: bool = True) -> Dict:
        """Execute GraphQL request with auth"""
        token = self.auth.get_valid_token()
        
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
            "Origin": "https://app.bemlo.com",
            "Referer": "https://app.bemlo.com/",
            "st-auth-mode": "header",
        }
        
        payload = {
            "operationName": "VacanciesList",
            "query": query,
            "variables": variables
        }
        
        response = self.session.post(BEMLO_GRAPHQL_URL, json=payload, headers=headers)
        
        # Handle 401 by refreshing token and retrying
        if response.status_code == 401 and retry_on_401:
            print("Got 401, refreshing token...")
            self.auth.refresh()
            return self._graphql_request(query, variables, retry_on_401=False)
        
        response.raise_for_status()
        
        data = response.json()
        if "errors" in data:
            raise Exception(f"GraphQL errors: {data['errors']}")
        
        return data.get("data", {})
    
    def fetch_vacancies_page(self, after_cursor: str = None) -> Tuple[List[Dict], bool, str]:
        """Fetch a single page of vacancies"""
        variables = {
            "filter": {
                "AND": [
                    {"tender": {"doesAcceptPresentations": True}},
                    {}
                ]
            },
            "take": 30,
            "orderBy": "CREATED_AT",
            "orderDir": "DESC"
        }
        
        if after_cursor:
            variables["afterCursor"] = after_cursor
        
        data = self._graphql_request(VACANCIES_QUERY, variables)
        
        all_vacancies = data.get("allVacancies", {})
        edges = all_vacancies.get("edges", [])
        page_info = all_vacancies.get("pageInfo", {})
        
        vacancies = [edge["node"] for edge in edges]
        has_next_page = page_info.get("hasNextPage", False)
        end_cursor = page_info.get("endCursor")
        
        return vacancies, has_next_page, end_cursor
    
    def fetch_all_vacancies(self, max_pages: int = 20) -> List[Dict]:
        """Fetch all vacancies with pagination"""
        all_vacancies = []
        cursor = None
        page = 0
        
        while page < max_pages:
            print(f"Fetching page {page + 1}...")
            vacancies, has_next_page, cursor = self.fetch_vacancies_page(cursor)
            all_vacancies.extend(vacancies)
            page += 1
            
            if not has_next_page:
                break
        
        print(f"Fetched {len(all_vacancies)} vacancies across {page} pages")
        return all_vacancies
    
    def parse_vacancy(self, raw: Dict) -> Dict:
        """Parse raw vacancy into flat structure"""
        tender = raw.get("tender", {})
        unit = tender.get("unit", {})
        orderer = tender.get("orderer", {})
        
        return {
            "id": raw["id"],
            "title": raw["title"],
            "profession": raw["profession"],
            "specializations": json.dumps(raw.get("specializations", [])),
            "municipality": raw.get("municipality", ""),
            "region": raw.get("region", ""),
            "job_starts_at": raw.get("jobStartsAt", 0),
            "job_ends_at": raw.get("jobEndsAt", 0),
            "procured_amount": raw.get("procuredAmount", 0),
            "procured_amount_currency": raw.get("procuredAmountCurrency", "SEK"),
            "scope_hours": tender.get("scope", 0),
            "fill_rate": tender.get("fillRate", 0),
            "dynamic_status": tender.get("dynamicStatus", ""),
            "tender_id": tender.get("id", ""),
            "tender_title": tender.get("title", ""),
            "unit_id": unit.get("id", ""),
            "unit_name": unit.get("name", ""),
            "orderer_id": orderer.get("id", ""),
            "orderer_name": orderer.get("displayName", ""),
            "last_application_date": raw.get("lastApplicationDate", 0),
            "created_at": raw.get("createdAt", 0),
            "announced_at": tender.get("announcedAt", 0),
            "raw_data": json.dumps(raw)
        }
    
    def save_vacancy(self, vacancy: Dict) -> Tuple[bool, List[str]]:
        """Save vacancy to database, returns (is_new, changed_fields)"""
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        now = int(datetime.now().timestamp())
        
        cursor.execute("SELECT fill_rate, dynamic_status, procured_amount FROM vacancies WHERE id = ?", 
                      (vacancy["id"],))
        existing = cursor.fetchone()
        
        if existing is None:
            # New vacancy
            vacancy["scraped_at"] = now
            vacancy["first_seen_at"] = now
            vacancy["last_updated_at"] = now
            
            columns = ", ".join(vacancy.keys())
            placeholders = ", ".join(["?" for _ in vacancy])
            cursor.execute(f"INSERT INTO vacancies ({columns}) VALUES ({placeholders})", 
                          list(vacancy.values()))
            conn.commit()
            conn.close()
            return True, []
        
        # Check for changes
        old_fill_rate, old_status, old_amount = existing
        changed_fields = []
        
        if old_fill_rate != vacancy["fill_rate"]:
            changed_fields.append("fill_rate")
        if old_status != vacancy["dynamic_status"]:
            changed_fields.append("dynamic_status")
        if old_amount != vacancy["procured_amount"]:
            changed_fields.append("procured_amount")
        
        # Update record
        cursor.execute("""
            UPDATE vacancies SET
                fill_rate = ?, dynamic_status = ?, procured_amount = ?,
                scraped_at = ?, last_updated_at = ?, raw_data = ?
            WHERE id = ?
        """, (vacancy["fill_rate"], vacancy["dynamic_status"], vacancy["procured_amount"],
              now, now, vacancy["raw_data"], vacancy["id"]))
        
        conn.commit()
        conn.close()
        return False, changed_fields
    
    def scrape(self) -> Dict[str, Any]:
        """Main scrape function"""
        start_time = datetime.now()
        
        raw_vacancies = self.fetch_all_vacancies()
        
        new_count = 0
        updated_count = 0
        new_vacancies = []
        updates = []
        
        for raw in raw_vacancies:
            vacancy = self.parse_vacancy(raw)
            is_new, changed_fields = self.save_vacancy(vacancy)
            
            if is_new:
                new_count += 1
                new_vacancies.append(vacancy)
            elif changed_fields:
                updated_count += 1
                updates.append({"vacancy": vacancy, "changes": changed_fields})
        
        duration = (datetime.now() - start_time).total_seconds()
        
        # Log scrape history
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO scrape_history (scraped_at, total_fetched, new_count, updated_count, duration_seconds)
            VALUES (?, ?, ?, ?, ?)
        """, (int(start_time.timestamp()), len(raw_vacancies), new_count, updated_count, duration))
        conn.commit()
        conn.close()
        
        return {
            "timestamp": start_time.isoformat(),
            "duration_seconds": duration,
            "total_fetched": len(raw_vacancies),
            "new_count": new_count,
            "updated_count": updated_count,
            "unchanged_count": len(raw_vacancies) - new_count - updated_count,
            "new_vacancies": [
                {
                    "id": v["id"],
                    "title": v["title"],
                    "profession": v["profession"],
                    "municipality": v["municipality"],
                    "region": v["region"],
                    "rate": v["procured_amount"],
                    "scope_hours": v["scope_hours"],
                    "unit_name": v["unit_name"],
                    "orderer_name": v["orderer_name"],
                    "url": f"https://app.bemlo.com/vacancies/{v['id']}"
                }
                for v in new_vacancies
            ],
            "updates": [
                {
                    "id": u["vacancy"]["id"],
                    "title": u["vacancy"]["title"],
                    "changes": u["changes"]
                }
                for u in updates[:10]
            ]
        }
    
    def get_stats(self) -> Dict:
        """Get database statistics"""
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        stats = {}
        
        cursor.execute("SELECT COUNT(*) FROM vacancies")
        stats["total_vacancies"] = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT profession, COUNT(*) as count 
            FROM vacancies GROUP BY profession
        """)
        stats["by_profession"] = dict(cursor.fetchall())
        
        cursor.execute("""
            SELECT region, COUNT(*) as count 
            FROM vacancies GROUP BY region ORDER BY count DESC LIMIT 10
        """)
        stats["by_region"] = dict(cursor.fetchall())
        
        cursor.execute("SELECT AVG(procured_amount) FROM vacancies WHERE profession = 'DOCTOR'")
        stats["avg_doctor_rate"] = cursor.fetchone()[0] or 0
        
        cursor.execute("SELECT AVG(procured_amount) FROM vacancies WHERE profession = 'NURSE'")
        stats["avg_nurse_rate"] = cursor.fetchone()[0] or 0
        
        cursor.execute("""
            SELECT scraped_at, total_fetched, new_count, updated_count 
            FROM scrape_history ORDER BY scraped_at DESC LIMIT 5
        """)
        stats["recent_scrapes"] = [
            {"timestamp": row[0], "total": row[1], "new": row[2], "updated": row[3]}
            for row in cursor.fetchall()
        ]
        
        conn.close()
        return stats
    
    def export_csv(self) -> bytes:
        """Export all vacancies as CSV"""
        import csv
        import io
        
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, title, profession, specializations, municipality, region,
                   job_starts_at, job_ends_at, procured_amount, procured_amount_currency,
                   scope_hours, fill_rate, dynamic_status, unit_name, orderer_name,
                   last_application_date, created_at, first_seen_at
            FROM vacancies ORDER BY created_at DESC
        """)
        
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        conn.close()
        
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(columns)
        writer.writerows(rows)
        
        return output.getvalue().encode('utf-8')
    
    def get_vacancies_json(self, limit: int = 100, profession: str = None) -> List[Dict]:
        """Get vacancies as JSON"""
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        query = """
            SELECT id, title, profession, specializations, municipality, region,
                   job_starts_at, job_ends_at, procured_amount, procured_amount_currency,
                   scope_hours, fill_rate, dynamic_status, tender_id, tender_title,
                   unit_id, unit_name, orderer_id, orderer_name,
                   last_application_date, created_at, announced_at, first_seen_at
            FROM vacancies
        """
        params = []
        
        if profession:
            query += " WHERE profession = ?"
            params.append(profession)
        
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        
        cursor.execute(query, params)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        conn.close()
        
        return [dict(zip(columns, row)) for row in rows]


def send_notification(message: str):
    """Send notification to webhook"""
    if not WEBHOOK_URL:
        return
    try:
        requests.post(WEBHOOK_URL, json={"text": message}, timeout=10)
    except Exception as e:
        print(f"Webhook failed: {e}")


# Global instances (initialized on first request)
_auth: Optional[BemloAuth] = None
_scraper: Optional[BemloScraper] = None


def get_scraper() -> BemloScraper:
    """Get or create scraper instance"""
    global _auth, _scraper
    
    if _scraper is None:
        if not BEMLO_EMAIL or not BEMLO_PASSWORD:
            raise ValueError("BEMLO_EMAIL and BEMLO_PASSWORD required")
        _auth = BemloAuth(BEMLO_EMAIL, BEMLO_PASSWORD)
        _scraper = BemloScraper(_auth)
    
    return _scraper


class RequestHandler(BaseHTTPRequestHandler):
    """HTTP request handler"""
    
    def _send_json(self, status: int, data: Dict):
        body = json.dumps(data, indent=2, default=str).encode('utf-8')
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)
    
    def _send_csv(self, data: bytes, filename: str):
        self.send_response(200)
        self.send_header("Content-Type", "text/csv")
        self.send_header("Content-Disposition", f"attachment; filename={filename}")
        self.send_header("Content-Length", len(data))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(data)
    
    def do_OPTIONS(self):
        """Handle CORS preflight"""
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.end_headers()
    
    def do_GET(self):
        path = self.path.split("?")[0]
        
        if path == "/":
            self._send_json(200, {
                "service": "Bemlo Vacancy Scraper",
                "version": "1.0.0",
                "endpoints": {
                    "GET /": "This help",
                    "GET /health": "Health check",
                    "GET /stats": "Database statistics",
                    "GET /vacancies": "Get vacancies JSON (params: limit, profession)",
                    "GET /export": "Download CSV",
                    "POST /scrape": "Trigger scrape"
                }
            })
        
        elif path == "/health":
            self._send_json(200, {
                "status": "healthy",
                "timestamp": datetime.now().isoformat(),
                "configured": bool(BEMLO_EMAIL and BEMLO_PASSWORD)
            })
        
        elif path == "/stats":
            try:
                scraper = get_scraper()
                stats = scraper.get_stats()
                self._send_json(200, stats)
            except Exception as e:
                self._send_json(500, {"error": str(e)})
        
        elif path == "/vacancies":
            try:
                # Parse query params
                from urllib.parse import urlparse, parse_qs
                params = parse_qs(urlparse(self.path).query)
                limit = int(params.get("limit", [100])[0])
                profession = params.get("profession", [None])[0]
                
                scraper = get_scraper()
                vacancies = scraper.get_vacancies_json(limit=limit, profession=profession)
                self._send_json(200, {"count": len(vacancies), "vacancies": vacancies})
            except Exception as e:
                self._send_json(500, {"error": str(e)})
        
        elif path == "/export":
            try:
                scraper = get_scraper()
                csv_data = scraper.export_csv()
                filename = f"bemlo_vacancies_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
                self._send_csv(csv_data, filename)
            except Exception as e:
                self._send_json(500, {"error": str(e)})
        
        else:
            self._send_json(404, {"error": "Not found"})
    
    def do_POST(self):
        path = self.path.split("?")[0]
        
        if path == "/scrape":
            try:
                print(f"[{datetime.now()}] Scrape triggered")
                scraper = get_scraper()
                results = scraper.scrape()
                
                # Send notification if new vacancies
                if results["new_count"] > 0:
                    msg = f"🏥 *{results['new_count']} new Bemlo vacancies*\n"
                    for v in results["new_vacancies"][:5]:
                        msg += f"• {v['title']} - {v['profession']} @ {v['municipality']} ({v['rate']} SEK)\n"
                    send_notification(msg)
                
                self._send_json(200, results)
            except Exception as e:
                print(f"Scrape error: {e}")
                self._send_json(500, {"error": str(e)})
        
        else:
            self._send_json(404, {"error": "Not found"})
    
    def log_message(self, format, *args):
        print(f"[{datetime.now()}] {args[0]}")


def main():
    print(f"Starting Bemlo Scraper API on port {PORT}")
    print(f"Email configured: {bool(BEMLO_EMAIL)}")
    print(f"Webhook configured: {bool(WEBHOOK_URL)}")
    
    server = HTTPServer(("0.0.0.0", PORT), RequestHandler)
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.shutdown()


if __name__ == "__main__":
    main()
