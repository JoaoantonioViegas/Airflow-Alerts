import asyncio
import signal
from airflow import Airflow

async def main():
    airflow_instance = Airflow()

    await airflow_instance.airflow_client.start()

    task = asyncio.create_task(airflow_instance.airflow_client.monitor(interval_s=10))

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(airflow_instance.airflow_client.stop()))

    try:
        await task  # roda até receber stop
    finally:
        await airflow_instance.airflow_client.stop()
        # garante que a task finaliza sem ficar pendurada
        if not task.done():
            task.cancel()
            with asyncio.CancelledError:
                pass

if __name__ == "__main__":
    asyncio.run(main())
