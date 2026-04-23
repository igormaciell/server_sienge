import asyncio
import base64
import httpx
from app.core.config import settings


class SiengeClient:
    def __init__(self):
        self.base_url = settings.SIENGE_REST_BASE_URL
        auth_string = f"{settings.SIENGE_USERNAME}:{settings.SIENGE_PASSWORD}"
        auth_b64 = base64.b64encode(auth_string.encode("utf-8")).decode("utf-8")
        self.headers = {
            "Authorization": f"Basic {auth_b64}",
            "Accept": "application/json",
        }

    async def get(self, path: str, params: dict | None = None):
        url = f"{self.base_url}{path}"

        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.get(url, headers=self.headers, params=params)

            print(f"[SIENGE REST] GET {url} | params={params} | status={response.status_code}")

            if response.status_code >= 400:
                raise Exception(
                    f"Sienge REST retornou {response.status_code} em {url}\n"
                    f"Params: {params}\n"
                    f"Resposta: {response.text}"
                )

            return response.json()

    async def fetch_all_pages(
        self,
        path: str,
        limit: int = 100,
        extra_params: dict | None = None,
        pause_seconds: float = 0.3,
        max_pages: int = 200,
    ) -> list[dict]:
        offset = 0
        page = 0
        all_results = []
        extra_params = extra_params or {}
        previous_first_item = None

        while True:
            if page >= max_pages:
                raise Exception(f"Paginação abortada: excedeu {max_pages} páginas.")

            params = {
                **extra_params,
                "offset": offset,
                "limit": limit,
            }

            data = await self.get(path, params=params)

            if isinstance(data, dict):
                metadata = data.get("resultSetMetadata", {})
                results = data.get("results", [])
            elif isinstance(data, list):
                metadata = {}
                results = data
            else:
                results = []

            print(f"[SIENGE REST] página={page + 1} | offset={offset} | retornados={len(results)}")

            if not results:
                break

            current_first_item = str(results[0])

            if previous_first_item is not None and current_first_item == previous_first_item:
                print("[SIENGE REST] Paginação interrompida: a API retornou a mesma página novamente.")
                break

            previous_first_item = current_first_item
            all_results.extend(results)

            count = metadata.get("count")
            current_limit = metadata.get("limit", limit)

            page += 1
            offset += current_limit

            if count is not None and offset >= count:
                break

            if len(results) < current_limit:
                break

            await asyncio.sleep(pause_seconds)

        return all_results