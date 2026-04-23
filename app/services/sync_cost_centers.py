from sqlalchemy.orm import Session
from app.services.sienge_client import SiengeClient
from app.repositories.dimension_repository import (
    upsert_dim_centro_custo,
    refresh_centro_custo_children
)

COST_CENTERS_PATH = "/v1/cost-centers/"

async def sync_cost_centers(db: Session) -> dict:
    client = SiengeClient()
    items = await client.fetch_all_pages(COST_CENTERS_PATH, limit=100)

    total = 0

    for item in items:
        cost_center_id = item.get("id")
        if not cost_center_id:
            continue

        upsert_dim_centro_custo(
            db=db,
            cost_center_id=cost_center_id,
            cost_center_name=item.get("name"),
            cnpj=item.get("cnpj"),
            id_company=item.get("idCompany"),
            company_id=item.get("idCompany")
        )

        refresh_centro_custo_children(
            db=db,
            cost_center_id=cost_center_id,
            building_sectors=item.get("buildingSectors") or []
        )

        total += 1

    db.commit()

    return {
        "entity": "cost_centers",
        "processed": total
    }