import asyncio
import aiohttp
from typing import Optional

BASE_URL = "http://localhost:8080"

class AirflowClient:
    def __init__(self, base_url: str = BASE_URL, username: str = "airflow", password: str = "airflow"):
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password

        self._stop_event = asyncio.Event()
        self._session: aiohttp.ClientSession | None = None
        self._token: Optional[str] = None

    async def start(self) -> None:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        # já pega token ao iniciar
        await self._ensure_token()

    async def stop(self) -> None:
        self._stop_event.set()
        if self._session and not self._session.closed:
            await self._session.close()

    async def _fetch_token(self) -> str:
        assert self._session is not None

        url = f"{self.base_url}/auth/token"

        payload = {
            "username": self.username,
            "password": self.password,
        }

        async with self._session.post(url, json=payload) as resp:
            body_text = await resp.text()
            if resp.status >= 400:
                raise RuntimeError(f"Token error {resp.status}: {body_text}")

            data = await resp.json()

        token = data.get("access_token") or data.get("token")
        if not token:
            raise RuntimeError(f"Resposta inesperada do /auth/token: {data}")
        return token


    async def _ensure_token(self) -> None:
        if not self._token:
            self._token = await self._fetch_token()

    async def _request_json(self, method: str, path: str) -> dict:
        if not self._session:
            raise RuntimeError("Client not started. Call: await airflow_client.start()")

        await self._ensure_token()
        url = f"{self.base_url}{path}"

        headers = {"Authorization": f"Bearer {self._token}"}

        async with self._session.request(method, url, headers=headers) as resp:
            # se expirou / inválido, tenta 1 refresh
            if resp.status == 401:
                self._token = await self._fetch_token()
                headers = {"Authorization": f"Bearer {self._token}"}
                async with self._session.request(method, url, headers=headers) as resp2:
                    resp2.raise_for_status()
                    return await resp2.json()

            resp.raise_for_status()
            return await resp.json()

    async def get_health(self) -> dict:
        return await self._request_json("GET", "/api/v2/monitor/health")

    async def get_dags(self) -> dict:
        return await self._request_json("GET", "/api/v2/dags")

    async def monitor(self, interval_s: int = 10) -> None:
        while not self._stop_event.is_set():
            health, dags = await asyncio.gather(
                self.get_health(),
                self.get_dags(),
                return_exceptions=True
            )

            if isinstance(health, Exception):
                print("Health erro:", repr(health))
            else:
                print("Health:", health)

            if isinstance(dags, Exception):
                print("DAGs erro:", repr(dags))
            else:
                print("DAGs:", dags)

            await asyncio.sleep(interval_s)
