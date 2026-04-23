from fastapi import APIRouter, Depends, HTTPException, Request, Query
from sqlalchemy.orm import Session
from app.core.db import get_db
from app.services.sync_cost_centers import sync_cost_centers
from app.services.sync_cp import sync_cp_bills
from app.services.sync_cr import sync_cr_bills
from app.services.sync_companies import sync_companies
from app.services.sync_creditors import sync_creditors
from app.services.sync_customers import sync_customers
from app.services.sync_bank_movement_bulk import sync_bank_movement_bulk
from app.services.sync_financial_categories import sync_financial_categories


router = APIRouter(prefix="/jobs", tags=["Jobs"])

@router.post("/sync/cp/bills")
async def run_sync_cp_bills(
    request: Request,
    startDate: str = Query(...),
    endDate: str = Query(...),
    db: Session = Depends(get_db)
):
    try:
        filters = dict(request.query_params)
        filters.pop("startDate", None)
        filters.pop("endDate", None)

        result = await sync_cp_bills(
            db,
            start_date=startDate,
            end_date=endDate,
            extra_filters=filters
        )
        return {"success": True, "result": result}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

# @router.post("/sync/bulk/bank-movement")
# async def run_sync_bank_movement_bulk(
#     request: Request,
#     startDate: str = Query(...),
#     endDate: str = Query(...),
#     db: Session = Depends(get_db)
# ):
#     try:
#         query_filters = dict(request.query_params)

#         body_payload = {}
#         raw_body = await request.body()

#         if raw_body and raw_body.strip():
#             body_payload = await request.json()

#         payload = {
#             **body_payload,
#             "startDate": startDate,
#             "endDate": endDate
#         }

#         result = await sync_bank_movement_bulk(db, payload=payload)
#         return {"success": True, "result": result}

#     except Exception as e:
#         db.rollback()
#         raise HTTPException(status_code=500, detail=str(e))

@router.post("/sync/bulk/bank-movement")
async def run_sync_bank_movement_bulk(
    request: Request,
    startDate: str = Query(...),
    endDate: str = Query(...),
    db: Session = Depends(get_db)
):
    try:
        filters = dict(request.query_params)

        result = await sync_bank_movement_bulk(db, params=filters)
        return {"success": True, "result": result}

    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    
@router.post("/sync/rest/companies")
async def run_sync_companies(
    db: Session = Depends(get_db)
):
    try:
        result = await sync_companies(db)
        return {"success": True, "result": result}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/sync/rest/creditors")
async def run_sync_creditors(
    db: Session = Depends(get_db)
):
    try:
        result = await sync_creditors(db)
        return {"success": True, "result": result}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/sync/rest/customers")
async def run_sync_customers(
    db: Session = Depends(get_db)
):
    try:
        result = await sync_customers(db)
        return {"success": True, "result": result}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    
@router.post("/sync/rest/cost-centers")
async def run_sync_cost_centers(
    db: Session = Depends(get_db)
):
    try:
        result = await sync_cost_centers(db)
        return {"success": True, "result": result}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
 
@router.post("/sync/rest/financial-categories")
async def run_sync_financial_categories(
    db: Session = Depends(get_db)
):
    try:
        result = await sync_financial_categories(db)
        return {"success": True, "result": result}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))