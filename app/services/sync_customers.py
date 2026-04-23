from sqlalchemy.orm import Session
from app.services.sienge_client import SiengeClient
from app.repositories.dimension_repository import upsert_dim_cliente, refresh_cliente_children

CUSTOMERS_PATH = "/v1/customers"

async def sync_customers(db: Session) -> dict:
    client = SiengeClient()
    items = await client.fetch_all_pages(CUSTOMERS_PATH, limit=100)

    total = 0

    for item in items:
        client_id = item.get("id") or item.get("customerId")
        if not client_id:
            continue

        phones = item.get("phones") or []
        addresses = item.get("addresses") or []

        main_phone = next((p for p in phones if p.get("main") is True), phones[0] if phones else None)
        main_address = next((a for a in addresses if a.get("mail") is True), addresses[0] if addresses else None)

        cnpj = item.get("cnpj")
        cpf = item.get("cpf")
        client_document = cnpj or cpf or item.get("document") or item.get("documentNumber")

        upsert_dim_cliente(
            db=db,
            client_id=client_id,
            client_name=item.get("name"),
            client_document=client_document,
            person_type=item.get("personType"),
            client_type=item.get("clientType"),
            foreigner=item.get("foreigner"),
            international_id=item.get("internationalId"),
            cnpj=cnpj,
            cpf=cpf,
            email=item.get("email"),
            fantasy_name=item.get("fantasyName"),
            state_registration_number=item.get("stateRegistrationNumber"),
            city_registration_number=item.get("cityRegistrationNumber"),
            contact_name=item.get("contactName"),
            site=item.get("site"),
            note=item.get("note"),
            created_at_source=item.get("createdAt"),
            modified_at_source=item.get("modifiedAt"),
            main_phone=main_phone.get("number") if main_phone else None,
            main_address_type=main_address.get("type") if main_address else None,
            street_name=main_address.get("streetName") if main_address else None,
            address_number=main_address.get("number") if main_address else None,
            address_complement=main_address.get("complement") if main_address else None,
            neighborhood=main_address.get("neighborhood") if main_address else None,
            city_id=main_address.get("cityId") if main_address else None,
            city_name=main_address.get("city") if main_address else None,
            state=main_address.get("state") if main_address else None,
            zip_code=main_address.get("zipCode") if main_address else None,
        )

        refresh_cliente_children(
            db=db,
            client_id=client_id,
            phones=phones,
            addresses=addresses
        )

        total += 1

    db.commit()

    return {
        "entity": "customers",
        "processed": total
    }