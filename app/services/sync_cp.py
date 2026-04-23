from sqlalchemy.orm import Session
from app.services.sienge_client import SiengeClient
from app.repositories.raw_repository import insert_raw_cp_bill, upsert_stg_cp_titulo

CP_BILLS_PATH = "/v1/bills"

async def sync_cp_bills(
    db: Session,
    start_date: str,
    end_date: str,
    extra_filters: dict | None = None
) -> dict:
    client = SiengeClient()

    filters = {
        "startDate": start_date,
        "endDate": end_date,
        **(extra_filters or {})
    }

    bills = await client.fetch_all_pages(
        CP_BILLS_PATH,
        limit=20,
        extra_params=filters,
        pause_seconds=0.8,
        max_pages=50,
        max_total_seconds=180
    )

    total = 0

    for bill in bills:
        bill_id = bill.get("id")
        if not bill_id:
            continue

        insert_raw_cp_bill(db, bill_id, CP_BILLS_PATH, bill)
        upsert_stg_cp_titulo(db, bill)
        total += 1

    db.commit()

    return {
        "domain": "cp",
        "entity": "bills",
        "processed": total,
        "filters": filters
    }