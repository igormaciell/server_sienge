from sqlalchemy.orm import Session
from app.services.sienge_client import SiengeClient
from app.repositories.dimension_repository import upsert_dim_empresa


COMPANIES_PATH = "/v1/companies"

print("DEBUG SiengeClient methods:", dir(SiengeClient))

async def sync_companies(db: Session) -> dict:
    client = SiengeClient()
    items = await client.fetch_all_pages(COMPANIES_PATH, limit=100)

    total = 0

    for item in items:
        company_id = item.get("id") or item.get("companyId")
        if not company_id:
            continue

        upsert_dim_empresa(
            db=db,
            company_id=company_id,
            company_name=item.get("name") or item.get("companyName"),
            group_company_id=item.get("groupCompanyId"),
            group_company_name=item.get("groupCompanyName"),
            holding_id=item.get("holdingId"),
            holding_name=item.get("holdingName"),
            subsidiary_id=item.get("subsidiaryId"),
            subsidiary_name=item.get("subsidiaryName"),
        )
        total += 1

    db.commit()

    return {
        "entity": "companies",
        "processed": total
    }