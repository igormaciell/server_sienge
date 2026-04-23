from sqlalchemy.orm import Session
from app.services.bulk_client import SiengeBulkClient
from app.repositories.bank_movement_repository import (
    insert_raw_bank_movement_bulk,
    upsert_stg_bank_movement,
    refresh_bank_movement_children
)
import json

BANK_MOVEMENT_BULK_PATH = "/bulk-data/v1/bank-movement"

async def sync_bank_movement_bulk(
    db: Session,
    params: dict
) -> dict:
    client = SiengeBulkClient()
    response = await client.get(BANK_MOVEMENT_BULK_PATH, params=params)

    print("[BANK_MOVEMENT_BULK] Tipo da resposta:", type(response).__name__)
    print("[BANK_MOVEMENT_BULK] Resposta completa:")
    print(json.dumps(response, ensure_ascii=False, indent=2)[:5000])

    items = _extract_items(response)

    total = 0
    skipped = 0

    for item in items:
        movement_id = item.get("bankMovementId")

        if movement_id is None:
            print("[BANK_MOVEMENT_BULK] Item ignorado por não ter bankMovementId:")
            print(json.dumps(item, ensure_ascii=False, indent=2)[:2000])
            skipped += 1
            continue

        insert_raw_bank_movement_bulk(
            db=db,
            movement_id=movement_id,
            source_endpoint=BANK_MOVEMENT_BULK_PATH,
            request_context=params,
            payload=item
        )
        upsert_stg_bank_movement(db, item)
        refresh_bank_movement_children(db, movement_id, item)
        total += 1

    db.commit()

    return {
        "domain": "bank_movement",
        "entity": "bulk",
        "processed": total,
        "skipped": skipped,
        "filters": params
    }

def _extract_items(response):
    if isinstance(response, list):
        return response

    if isinstance(response, dict):
        if "results" in response and isinstance(response["results"], list):
            return response["results"]

        if "data" in response and isinstance(response["data"], list):
            return response["data"]

        if "items" in response and isinstance(response["items"], list):
            return response["items"]

    return []