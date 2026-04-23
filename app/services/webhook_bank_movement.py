from sqlalchemy.orm import Session
from sqlalchemy import text
from app.utils.hash_utils import generate_payload_hash
from app.services.bulk_client import SiengeBulkClient
from app.services.sync_bank_movement_bulk import sync_bank_movement_bulk
import json

WEBHOOK_EVENT_DOMAIN = "bank_movement"
BANK_MOVEMENT_LIST_BULK_PATH = "/bulk-data/v1/bank-movement"

async def process_bank_movement_webhook(db: Session, payload: dict) -> dict:
    payload_hash = generate_payload_hash(payload)

    bank_movement_id = (
        payload.get("bankMovmentId")   # nome do artigo do Sienge
        or payload.get("bankMovementId")
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
        "event_type": "bank_movement_changed",
        "external_id": str(bank_movement_id) if bank_movement_id is not None else None,
        "payload_hash": payload_hash,
        "payload_json": json.dumps(payload, ensure_ascii=False)
    })

    if bank_movement_id is None:
        db.commit()
        return {
            "message": "Webhook recebido sem bankMovmentId",
            "processed": False
        }

    # Estratégia recomendada:
    # usar a API Bulk de movimentos específicos/lista de movimentos
    # em vez de revarrer janela inteira
    params = {
        "bankMovementIds": str(bank_movement_id)
    }

    await sync_bank_movement_bulk(db=db, params=params)

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
        "processed": True
    }