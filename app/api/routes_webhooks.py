from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session
from app.core.db import get_db
from app.services.webhook_bank_movement import process_bank_movement_webhook


router = APIRouter(prefix="/webhooks", tags=["Webhooks"])

@router.post("/sienge/bank-movement")
async def receive_bank_movement_webhook(
    request: Request,
    db: Session = Depends(get_db)
):
    try:
        payload = await request.json()
        result = await process_bank_movement_webhook(db=db, payload=payload)

        return {
            "success": True,
            "result": result
        }

    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))