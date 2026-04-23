from sqlalchemy.orm import Session
from sqlalchemy import text
from app.utils.hash_utils import generate_payload_hash
from app.services.sync_bank_movement_by_ids import sync_bank_movement_by_ids
import json


WEBHOOK_EVENT_DOMAIN = "bank_movement"

async def process_bank_movement_webhook(db: Session, payload: dict) -> dict:
    payload_hash = generate_payload_hash(payload)

    bank_movement_id = (
        payload.get("bankMovmentId")   # doc antiga/artigo
        or payload.get("bankMovementId")  # doc swagger
        or payload.get("id")
    )

    db.execute(text("""
        insert into integration.webhook_event (
            source_system,
            event_domain,
            event_type,
            external_id,
            payload_hash,
            payload_json,
            status
        )
        values (
            'sienge',
            :event_domain,
            :event_type,
            :external_id,
            :payload_hash,
            cast(:payload_json as jsonb),
            'received'
        )
        on conflict do nothing
    """), {
        "event_domain": WEBHOOK_EVENT_DOMAIN,
        "event_type": "BANK_MOVEMENT_UPDATED",
        "external_id": str(bank_movement_id) if bank_movement_id is not None else None,
        "payload_hash": payload_hash,
        "payload_json": json.dumps(payload, ensure_ascii=False)
    })

    if bank_movement_id is None:
        db.commit()
        return {
            "message": "Webhook recebido sem bankMovementId",
            "processed": False
        }

    result = await sync_bank_movement_by_ids(
        db=db,
        movement_ids=[int(bank_movement_id)]
    )

    db.execute(text("""
        update integration.webhook_event
        set status = 'processed',
            processed_at = now()
        where event_domain = :event_domain
          and payload_hash = :payload_hash
    """), {
        "event_domain": WEBHOOK_EVENT_DOMAIN,
        "payload_hash": payload_hash
    })

    db.commit()

    return {
        "message": "Webhook processado com sucesso",
        "bank_movement_id": bank_movement_id,
        "processed": True,
        "sync_result": result
    }
