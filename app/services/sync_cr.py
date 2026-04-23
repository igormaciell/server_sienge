from sqlalchemy.orm import Session
from app.services.sienge_client import SiengeClient
from app.repositories.raw_repository import insert_raw_cr_bill, upsert_stg_cr_titulo

CR_BILLS_PATH = "/v1/accounts-receivable/receivable-bills"

async def sync_cr_bills(
    db: Session,
    extra_filters: dict | None = None
) -> dict:
    client = SiengeClient()

    bills = await client.fetch_all_pages(
        CR_BILLS_PATH,
        limit=20,
        extra_params=extra_filters or {},
        pause_seconds=1.0,
        max_pages=50,
        max_total_seconds=180
    )

    total = 0

    for bill in bills:
        receivable_bill_id = bill.get("receivableBillId")
        if not receivable_bill_id:
            continue

        insert_raw_cr_bill(db, receivable_bill_id, CR_BILLS_PATH, bill)
        upsert_stg_cr_titulo(db, bill)
        total += 1

    db.commit()

    return {
        "domain": "cr",
        "entity": "bills",
        "processed": total,
        "filters": extra_filters or {}
    }