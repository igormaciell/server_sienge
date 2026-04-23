from sqlalchemy.orm import Session
from app.services.sienge_client import SiengeClient
from app.repositories.dimension_repository import upsert_dim_categoria_financeira

FINANCIAL_CATEGORIES_PATH = "/v1/payment-categories/"

async def sync_financial_categories(db: Session) -> dict:
    client = SiengeClient()

    data = await client.get(FINANCIAL_CATEGORIES_PATH)

    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        items = data.get("results") or data.get("data") or []
    else:
        items = []

    total = 0

    for item in items:
        financial_category_id = item.get("id")
        if not financial_category_id:
            continue

        upsert_dim_categoria_financeira(
            db=db,
            financial_category_id=str(item.get("id")),
            financial_category_name=item.get("name"),
            tp_conta=item.get("tpConta"),
            fl_redutora=item.get("flRedutora"),
            fl_ativa=item.get("flAtiva"),
            fl_adiantamento=item.get("flAdiantamento"),
            fl_imposto=item.get("flImposto"),
        )
        total += 1

    db.commit()

    return {
        "entity": "financial_categories",
        "processed": total
    }