from fastapi import FastAPI
from app.core.config import settings
from app.api.routes_health import router as health_router
from app.api.routes_jobs import router as jobs_router
from app.api.routes_webhooks import router as webhooks_router

app = FastAPI(title=settings.APP_NAME)

app.include_router(webhooks_router)
app.include_router(health_router)
app.include_router(jobs_router)

@app.get("/")
def root():
    return {
        "app": settings.APP_NAME,
        "environment": settings.APP_ENV,
        "status": "running"
    }