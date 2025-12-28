from airflowClient import AirflowClient

class Metadabase:
    def __init__(self):
        self.status = ''
    def set_status(self, status: str):
        self.status = status

class Scheduler:
    def __init__(self):
        self.status = ''
        self.last_heart_beat = ''
    def set_status(self, status: str):
        self.status = status
    def set_last_heart_beat(self, last_heart_beat: str):
        self.last_heart_beat = last_heart_beat

class Triggerer:
    def __init__(self):
        self.status = ''
        self.last_heart_beat = ''
    def set_status(self, status: str):
        self.status = status
    def set_last_heart_beat(self, last_heart_beat: str):
        self.last_heart_beat = last_heart_beat

class DagProcessor:
    def __init__(self):
        self.status = ''
        self.last_heart_beat = ''
    def set_status(self, status: str):
        self.status = status
    def set_last_heart_beat(self, last_heart_beat: str):
        self.last_heart_beat = last_heart_beat

class Airflow:
    def __init__(self):
        self.status = "n/d"
        self.metadabase = Metadabase()
        self.triggerer = Triggerer()
        self.scheduler = Scheduler()
        self.dag_processor = DagProcessor()
        self.airflow_client = AirflowClient()

    # async def get_airflow_status(self) -> None:
    #     try:
    #         async with aiohttp.ClientSession() as session:
    #             while not self._stop_event.is_set():
    #                 print("Getting airflow status")
    #                 async with session.get(f"{BASE_URL}/api/v2/monitor/health") as resp:
    #                     data = await resp.json()
    #                     self.metadabase.set_status(data.get("metadatabase").get("status"))
    #                     self.scheduler.set_status(data.get("dag_processor").get("status"))
    #                     self.triggerer.set_status(data.get("triggerer").get("status"))
    #                     self.dag_processor.set_status(data.get("dag_processor").get("status"))

    #                 await asyncio.sleep(10)
    #     except asyncio.CancelledError:
    #         pass
    #     finally:
    #         print("Health checker stopped")


    