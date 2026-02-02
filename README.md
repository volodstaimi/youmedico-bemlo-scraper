# Bemlo Scraper - GCP Deployment

## Files to upload to GitHub:
- api.py
- Dockerfile
- .gitignore

## GCP Console Commands (run in Cloud Shell)

```bash
# 1. Clone your repo
git clone https://github.com/YOUR_USERNAME/bemlo-scraper.git
cd bemlo-scraper

# 2. Set your project
gcloud config set project YOUR_PROJECT_ID

# 3. Enable APIs
gcloud services enable run.googleapis.com secretmanager.googleapis.com cloudbuild.googleapis.com

# 4. Create secrets (one-time)
echo -n "karl@youpal.se" | gcloud secrets create bemlo-email --data-file=-
echo -n "@Energy2025!" | gcloud secrets create bemlo-password --data-file=-

# 5. Build and deploy
gcloud run deploy bemlo-scraper \
  --source . \
  --region europe-north1 \
  --allow-unauthenticated \
  --memory 512Mi \
  --timeout 300 \
  --set-secrets "BEMLO_EMAIL=bemlo-email:latest,BEMLO_PASSWORD=bemlo-password:latest"

# 6. Test it
SERVICE_URL=$(gcloud run services describe bemlo-scraper --region europe-north1 --format 'value(status.url)')
echo $SERVICE_URL
curl -X POST $SERVICE_URL/scrape
```

## Endpoints
- GET  /health    - Health check
- GET  /stats     - Statistics  
- GET  /vacancies - Get data as JSON
- GET  /export    - Download CSV
- POST /scrape    - Trigger scrape
