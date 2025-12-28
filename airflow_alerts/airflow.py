import yaml
import os
from airflowClient import AirflowClient

def load_config(config_path: str = "../config.yaml") -> dict:
    """
    Load configuration from YAML file
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    full_config_path = os.path.join(script_dir, config_path)

    with open(full_config_path, 'r') as f:
        return yaml.safe_load(f)

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
    def __init__(self, config_path: str = "../config.yaml"):
        self.status = "n/d"
        self.metadabase = Metadabase()
        self.triggerer = Triggerer()
        self.scheduler = Scheduler()
        self.dag_processor = DagProcessor()

        # Load configuration
        config = load_config(config_path)
        airflow_config = config.get('airflow', {})

        # Initialize client with credentials from config
        self.airflow_client = AirflowClient(
            base_url=airflow_config.get('base_url', 'http://localhost:8080'),
            username=airflow_config.get('username', 'airflow'),
            password=airflow_config.get('password', 'airflow')
        )


    