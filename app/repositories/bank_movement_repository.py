from sqlalchemy import text
from sqlalchemy.orm import Session
from app.utils.hash_utils import generate_payload_hash
import json

def insert_raw_bank_movement_bulk(
    db: Session,
    movement_id: int | None,
    source_endpoint: str,
    request_context: dict | None,
    payload: dict
) -> None:
    payload_hash = generate_payload_hash(payload)

    sql = text("""
        insert into raw.bank_movement_bulk (
            movement_id,
            source_endpoint,
            request_context,
            payload_hash,
            payload_json
        )
        values (
            :movement_id,
            :source_endpoint,
            cast(:request_context as jsonb),
            :payload_hash,
            cast(:payload_json as jsonb)
        )
        on conflict do nothing
    """)

    db.execute(sql, {
        "movement_id": movement_id,
        "source_endpoint": source_endpoint,
        "request_context": json.dumps(request_context or {}, ensure_ascii=False),
        "payload_hash": payload_hash,
        "payload_json": json.dumps(payload, ensure_ascii=False)
    })


def upsert_stg_bank_movement(db: Session, payload: dict) -> None:
    movement_id = payload.get("bankMovementId")

    if movement_id is None:
        raise ValueError(
            f"Item sem bankMovementId. Payload: {json.dumps(payload, ensure_ascii=False)[:1200]}"
        )

    sql = text("""
        insert into stg.bank_movement (
            movement_id,
            bill_id,
            installment_id,
            bank_movement_amount,
            document_identification_id,
            document_identification_name,
            document_identification_number,
            bank_movement_origin_id,
            bank_movement_historic_id,
            bank_movement_historic_name,
            bank_movement_operation_id,
            bank_movement_operation_name,
            bank_movement_operation_type,
            bank_movement_reconcile,
            bank_movement_date,
            bill_date,
            account_number,
            company_id,
            company_name,
            group_company_id,
            group_company_name,
            holding_id,
            holding_name,
            subsidiary_id,
            subsidiary_name,
            creditor_id,
            creditor_name,
            client_id,
            client_name,
            updated_at
        )
        values (
            :movement_id,
            :bill_id,
            :installment_id,
            :bank_movement_amount,
            :document_identification_id,
            :document_identification_name,
            :document_identification_number,
            :bank_movement_origin_id,
            :bank_movement_historic_id,
            :bank_movement_historic_name,
            :bank_movement_operation_id,
            :bank_movement_operation_name,
            :bank_movement_operation_type,
            :bank_movement_reconcile,
            :bank_movement_date,
            :bill_date,
            :account_number,
            :company_id,
            :company_name,
            :group_company_id,
            :group_company_name,
            :holding_id,
            :holding_name,
            :subsidiary_id,
            :subsidiary_name,
            :creditor_id,
            :creditor_name,
            :client_id,
            :client_name,
            now()
        )
        on conflict (movement_id) do update set
            bill_id = excluded.bill_id,
            installment_id = excluded.installment_id,
            bank_movement_amount = excluded.bank_movement_amount,
            document_identification_id = excluded.document_identification_id,
            document_identification_name = excluded.document_identification_name,
            document_identification_number = excluded.document_identification_number,
            bank_movement_origin_id = excluded.bank_movement_origin_id,
            bank_movement_historic_id = excluded.bank_movement_historic_id,
            bank_movement_historic_name = excluded.bank_movement_historic_name,
            bank_movement_operation_id = excluded.bank_movement_operation_id,
            bank_movement_operation_name = excluded.bank_movement_operation_name,
            bank_movement_operation_type = excluded.bank_movement_operation_type,
            bank_movement_reconcile = excluded.bank_movement_reconcile,
            bank_movement_date = excluded.bank_movement_date,
            bill_date = excluded.bill_date,
            account_number = excluded.account_number,
            company_id = excluded.company_id,
            company_name = excluded.company_name,
            group_company_id = excluded.group_company_id,
            group_company_name = excluded.group_company_name,
            holding_id = excluded.holding_id,
            holding_name = excluded.holding_name,
            subsidiary_id = excluded.subsidiary_id,
            subsidiary_name = excluded.subsidiary_name,
            creditor_id = excluded.creditor_id,
            creditor_name = excluded.creditor_name,
            client_id = excluded.client_id,
            client_name = excluded.client_name,
            updated_at = now()
    """)

    db.execute(sql, {
        "movement_id": payload.get("bankMovementId"),
        "bill_id": payload.get("billId"),
        "installment_id": payload.get("installmentId"),
        "bank_movement_amount": payload.get("bankMovementAmount"),
        "document_identification_id": payload.get("documentIdentificationId"),
        "document_identification_name": payload.get("documentIdentificationName"),
        "document_identification_number": payload.get("documentIdentificationNumber"),
        "bank_movement_origin_id": payload.get("bankMovementOriginId"),
        "bank_movement_historic_id": str(payload.get("bankMovementHistoricId")) if payload.get("bankMovementHistoricId") is not None else None,
        "bank_movement_historic_name": payload.get("bankMovementHistoricName"),
        "bank_movement_operation_id": payload.get("bankMovementOperationId"),
        "bank_movement_operation_name": payload.get("bankMovementOperationName"),
        "bank_movement_operation_type": payload.get("bankMovementOperationType"),
        "bank_movement_reconcile": payload.get("bankMovementReconcile"),
        "bank_movement_date": payload.get("bankMovementDate"),
        "bill_date": payload.get("billDate"),
        "account_number": payload.get("accountNumber"),
        "company_id": payload.get("companyId"),
        "company_name": payload.get("companyName"),
        "group_company_id": payload.get("groupCompanyId"),
        "group_company_name": payload.get("groupCompanyName"),
        "holding_id": payload.get("holdingId"),
        "holding_name": payload.get("holdingName"),
        "subsidiary_id": payload.get("subsidiaryId"),
        "subsidiary_name": payload.get("subsidiaryName"),
        "creditor_id": payload.get("creditorId"),
        "creditor_name": payload.get("creditorName"),
        "client_id": payload.get("clientId"),
        "client_name": payload.get("clientName"),
    })


def refresh_bank_movement_children(db: Session, movement_id: int, payload: dict) -> None:
    db.execute(
        text("delete from stg.bank_movement_financial_category where movement_id = :movement_id"),
        {"movement_id": movement_id}
    )
    db.execute(
        text("delete from stg.bank_movement_department_cost where movement_id = :movement_id"),
        {"movement_id": movement_id}
    )
    db.execute(
        text("delete from stg.bank_movement_building_cost where movement_id = :movement_id"),
        {"movement_id": movement_id}
    )

    for fc in (payload.get("financialCategories") or []):
        db.execute(text("""
            insert into stg.bank_movement_financial_category (
                movement_id,
                company_id,
                company_name,
                cost_center_id,
                cost_center_name,
                financial_category_id,
                financial_category_name,
                financial_category_reducer,
                financial_category_type,
                financial_category_rate,
                business_area_id,
                business_area_name,
                project_id,
                project_name,
                business_type_id,
                business_type_name,
                group_company_id,
                group_company_name,
                holding_id,
                holding_name,
                subsidiary_id,
                subsidiary_name
            )
            values (
                :movement_id,
                :company_id,
                :company_name,
                :cost_center_id,
                :cost_center_name,
                :financial_category_id,
                :financial_category_name,
                :financial_category_reducer,
                :financial_category_type,
                :financial_category_rate,
                :business_area_id,
                :business_area_name,
                :project_id,
                :project_name,
                :business_type_id,
                :business_type_name,
                :group_company_id,
                :group_company_name,
                :holding_id,
                :holding_name,
                :subsidiary_id,
                :subsidiary_name
            )
            on conflict do nothing
        """), {
            "movement_id": movement_id,
            "company_id": fc.get("companyId"),
            "company_name": fc.get("companyName"),
            "cost_center_id": fc.get("costCenterId"),
            "cost_center_name": fc.get("costCenterName"),
            "financial_category_id": fc.get("financialCategoryId"),
            "financial_category_name": fc.get("financialCategoryName"),
            "financial_category_reducer": fc.get("financialCategoryReducer"),
            "financial_category_type": fc.get("financialCategoryType"),
            "financial_category_rate": fc.get("financialCategoryRate"),
            "business_area_id": fc.get("businessAreaId"),
            "business_area_name": fc.get("businessAreaName"),
            "project_id": fc.get("projectId"),
            "project_name": fc.get("projectName"),
            "business_type_id": fc.get("businessTypeId"),
            "business_type_name": fc.get("businessTypeName"),
            "group_company_id": fc.get("groupCompanyId"),
            "group_company_name": fc.get("groupCompanyName"),
            "holding_id": fc.get("holdingId"),
            "holding_name": fc.get("holdingName"),
            "subsidiary_id": fc.get("subsidiaryId"),
            "subsidiary_name": fc.get("subsidiaryName"),
        })

    for dc in (payload.get("departamentCosts") or []):
        db.execute(text("""
            insert into stg.bank_movement_department_cost (
                movement_id,
                department_cost_id,
                department_cost_name,
                rate
            )
            values (
                :movement_id,
                :department_cost_id,
                :department_cost_name,
                :rate
            )
            on conflict do nothing
        """), {
            "movement_id": movement_id,
            "department_cost_id": dc.get("id"),
            "department_cost_name": dc.get("name"),
            "rate": dc.get("rate"),
        })

    for bc in (payload.get("buldingCosts") or []):
        db.execute(text("""
            insert into stg.bank_movement_building_cost (
                movement_id,
                building_id,
                building_name,
                building_unit_id,
                building_unit_name,
                cost_estimation_sheet_id,
                cost_estimation_sheet_name,
                rate
            )
            values (
                :movement_id,
                :building_id,
                :building_name,
                :building_unit_id,
                :building_unit_name,
                :cost_estimation_sheet_id,
                :cost_estimation_sheet_name,
                :rate
            )
            on conflict do nothing
        """), {
            "movement_id": movement_id,
            "building_id": bc.get("buildingId"),
            "building_name": bc.get("name"),
            "building_unit_id": bc.get("buildingUnitId"),
            "building_unit_name": bc.get("buildingUnitName"),
            "cost_estimation_sheet_id": bc.get("costEstimationSheetId"),
            "cost_estimation_sheet_name": bc.get("costEstimationSheetName"),
            "rate": bc.get("rate"),
        })