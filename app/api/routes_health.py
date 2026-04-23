from fastapi import APIRouter
from app.core.db import test_connection

router = APIRouter(prefix="/health", tags=["Health"])

@router.get("")
def health():
    db_ok = test_connection()
    return {
        "status": "ok",
        "database": db_ok
    }