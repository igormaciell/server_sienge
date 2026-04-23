from sqlalchemy import text
from sqlalchemy.orm import Session
from app.utils.hash_utils import generate_payload_hash
import json

def insert_raw_cp_bill(db: Session, external_id: int, source_endpoint: str, payload: dict) -> None:
    payload_hash = generate_payload_hash(payload)

    sql = text("""
        insert into raw.cp_bill (external_id, source_endpoint, payload_hash, payload_json)
        values (:external_id, :source_endpoint, :payload_hash, cast(:payload_json as jsonb))
        on conflict do nothing
    """)

    db.execute(sql, {
        "external_id": external_id,
        "source_endpoint": source_endpoint,
        "payload_hash": payload_hash,
        "payload_json": json.dumps(payload, ensure_ascii=False)
    })

def insert_raw_cr_bill(db: Session, external_id: int, source_endpoint: str, payload: dict) -> None:
    payload_hash = generate_payload_hash(payload)

    sql = text("""
        insert into raw.cr_bill (external_id, source_endpoint, payload_hash, payload_json)
        values (:external_id, :source_endpoint, :payload_hash, cast(:payload_json as jsonb))
        on conflict do nothing
    """)

    db.execute(sql, {
        "external_id": external_id,
        "source_endpoint": source_endpoint,
        "payload_hash": payload_hash,
        "payload_json": json.dumps(payload, ensure_ascii=False)
    })

def upsert_stg_cp_titulo(db: Session, payload: dict) -> None:
    sql = text("""
        insert into stg.cp_titulo (
            bill_id,
            debtor_id,
            creditor_id,
            company_id,
            document_identification_id,
            document_number,
            issue_date,
            installments_number,
            total_invoice_amount,
            discount,
            status,
            origin_id,
            registered_user_id,
            registered_by,
            registered_date,
            changed_user_id,
            changed_by,
            changed_date,
            notes,
            updated_at
        )
        values (
            :bill_id,
            :debtor_id,
            :creditor_id,
            :company_id,
            :document_identification_id,
            :document_number,
            :issue_date,
            :installments_number,
            :total_invoice_amount,
            :discount,
            :status,
            :origin_id,
            :registered_user_id,
            :registered_by,
            :registered_date,
            :changed_user_id,
            :changed_by,
            :changed_date,
            :notes,
            now()
        )
        on conflict (bill_id) do update set
            debtor_id = excluded.debtor_id,
            creditor_id = excluded.creditor_id,
            company_id = excluded.company_id,
            document_identification_id = excluded.document_identification_id,
            document_number = excluded.document_number,
            issue_date = excluded.issue_date,
            installments_number = excluded.installments_number,
            total_invoice_amount = excluded.total_invoice_amount,
            discount = excluded.discount,
            status = excluded.status,
            origin_id = excluded.origin_id,
            registered_user_id = excluded.registered_user_id,
            registered_by = excluded.registered_by,
            registered_date = excluded.registered_date,
            changed_user_id = excluded.changed_user_id,
            changed_by = excluded.changed_by,
            changed_date = excluded.changed_date,
            notes = excluded.notes,
            updated_at = now()
    """)

    db.execute(sql, {
        "bill_id": payload.get("id"),
        "debtor_id": payload.get("debtorId"),
        "creditor_id": payload.get("creditorId"),
        "company_id": _extract_company_id(payload),
        "document_identification_id": payload.get("documentIdentificationId"),
        "document_number": payload.get("documentNumber"),
        "issue_date": payload.get("issueDate"),
        "installments_number": payload.get("installmentsNumber"),
        "total_invoice_amount": payload.get("totalInvoiceAmount"),
        "discount": payload.get("discount"),
        "status": payload.get("status"),
        "origin_id": payload.get("originId"),
        "registered_user_id": payload.get("registeredUserId"),
        "registered_by": payload.get("registeredBy"),
        "registered_date": payload.get("registeredDate"),
        "changed_user_id": payload.get("changedUserId"),
        "changed_by": payload.get("changedBy"),
        "changed_date": payload.get("changedDate"),
        "notes": payload.get("notes"),
    })

def upsert_stg_cr_titulo(db: Session, payload: dict) -> None:
    sql = text("""
        insert into stg.cr_titulo (
            receivable_bill_id,
            customer_id,
            company_id,
            document_id,
            document_number,
            issue_date,
            receivable_bill_value,
            defaulting,
            subjudice,
            note,
            pay_off_date,
            updated_at
        )
        values (
            :receivable_bill_id,
            :customer_id,
            :company_id,
            :document_id,
            :document_number,
            :issue_date,
            :receivable_bill_value,
            :defaulting,
            :subjudice,
            :note,
            :pay_off_date,
            now()
        )
        on conflict (receivable_bill_id) do update set
            customer_id = excluded.customer_id,
            company_id = excluded.company_id,
            document_id = excluded.document_id,
            document_number = excluded.document_number,
            issue_date = excluded.issue_date,
            receivable_bill_value = excluded.receivable_bill_value,
            defaulting = excluded.defaulting,
            subjudice = excluded.subjudice,
            note = excluded.note,
            pay_off_date = excluded.pay_off_date,
            updated_at = now()
    """)

    db.execute(sql, {
        "receivable_bill_id": payload.get("receivableBillId"),
        "customer_id": payload.get("customerId"),
        "company_id": payload.get("companyId"),
        "document_id": payload.get("documentId"),
        "document_number": payload.get("documentNumber"),
        "issue_date": payload.get("issueDate"),
        "receivable_bill_value": payload.get("receivableBillValue"),
        "defaulting": payload.get("defaulting"),
        "subjudice": payload.get("subjudice"),
        "note": payload.get("note"),
        "pay_off_date": payload.get("payOffDate"),
    })

def _extract_company_id(payload: dict):
    links = payload.get("links", [])
    for link in links:
        if link.get("rel") == "company":
            href = link.get("href", "")
            try:
                return int(href.rstrip("/").split("/")[-1])
            except Exception:
                return None
    return None