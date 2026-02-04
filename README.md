# Bemlo Vacancy Scraper v2.0 (Enhanced)

Scrapes healthcare staffing tenders from Bemlo with full details.

## Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | API info |
| GET | `/health` | Health check |
| GET | `/stats` | Database statistics |
| GET | `/vacancies` | List vacancies (params: profession, region, status, limit, offset) |
| GET | `/vacancy/{id}` | Single vacancy with requirements, shifts, price groups |
| GET | `/vacancy/{id}/shifts` | Shifts for a vacancy |
| GET | `/export` | Download CSV |
| POST | `/scrape` | Trigger scrape |

## Deploy

```bash
gcloud run deploy bemlo-scraper \
  --source . \
  --region europe-north1 \
  --allow-unauthenticated \
  --memory 512Mi \
  --timeout 300 \
  --set-secrets "BEMLO_EMAIL=bemlo-email:latest,BEMLO_PASSWORD=bemlo-password:latest"
```

## Data Captured

- Vacancies: title, profession, specializations, region, rate, scope, fill_rate, status
- Details: description, requirements, contact info, billing info
- Shifts: date, time, duration, status (VACANT/BOOKED)
- Requirements: documents, experience, journal systems, specializations
- Price groups: by specialization
