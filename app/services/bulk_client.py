import httpx
import base64
from app.core.config import settings


class SiengeBulkClient:
    def __init__(self):
        self.base_url = settings.SIENGE_BASE_URL
        auth_string = f"{settings.SIENGE_USERNAME}:{settings.SIENGE_PASSWORD}"
        auth_b64 = base64.b64encode(auth_string.encode("utf-8")).decode("utf-8")
        self.headers = {
            "Authorization": f"Basic {auth_b64}",
            "Accept": "application/json"
        }

    async def get(self, path: str, params: dict | None = None) -> dict:
        url = f"{self.base_url}{path}"

        async with httpx.AsyncClient(timeout=120.0) as client:
            response = await client.get(url, headers=self.headers, params=params)

            print(f"[SIENGE BULK] GET {url} | params={params} | status={response.status_code}")
            print(f"[SIENGE BULK] response body: {response.text[:1000]}")

            if response.status_code >= 400:
                raise Exception(
                    f"Sienge Bulk retornou {response.status_code} em {url}\n"
                    f"Params: {params}\n"
                    f"Resposta: {response.text}"
                )

            return response.json()