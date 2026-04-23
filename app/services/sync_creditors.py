from sqlalchemy.orm import Session
from app.services.sienge_client import SiengeClient
from app.repositories.dimension_repository import upsert_dim_credor, refresh_credor_children

CREDITORS_PATH = "/v1/creditors"

async def sync_creditors(db: Session) -> dict:
    client = SiengeClient()
    items = await client.fetch_all_pages(CREDITORS_PATH, limit=100)

    total = 0

    for item in items:
        creditor_id = item.get("id") or item.get("creditorId")
        if not creditor_id:
            continue

        address = item.get("address") or {}
        phones = item.get("phones") or []
        contacts = item.get("contacts") or []

        cnpj = item.get("cnpj")
        cpf = item.get("cpf")
        creditor_document = cnpj or cpf or item.get("document") or item.get("documentNumber")

        upsert_dim_credor(
            db=db,
            creditor_id=creditor_id,
            creditor_name=item.get("name") or item.get("creditorName") or item.get("businessName"),
            creditor_document=creditor_document,
            trade_name=item.get("tradeName"),
            cpf=cpf,
            cnpj=cnpj,
            supplier=item.get("supplier"),
            broker=item.get("broker"),
            employee=item.get("employee"),
            active=item.get("active"),
            state_registration_number=item.get("stateRegistrationNumber"),
            state_registration_type=item.get("stateRegistrationType"),
            tax_classification_id=item.get("taxClassificationId"),
            tax_classification=item.get("taxClassification"),
            payment_type_id=item.get("paymentTypeId"),
            creation_date_source=item.get("creationDate"),
            last_modification_date_source=item.get("lastModificationDate"),
            city_id=address.get("cityId"),
            city_name=address.get("cityName"),
            street_name=address.get("streetName"),
            address_number=address.get("number"),
            address_complement=address.get("complement"),
            neighborhood=address.get("neighborhood"),
            state=address.get("state"),
            zip_code=address.get("zipCode"),
        )

        refresh_credor_children(
            db=db,
            creditor_id=creditor_id,
            phones=phones,
            contacts=contacts
        )

        total += 1

    db.commit()

    return {
        "entity": "creditors",
        "processed": total
    }