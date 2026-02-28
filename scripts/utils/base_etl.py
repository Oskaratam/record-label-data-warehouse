import time
import requests 
from datetime import datetime
from scripts.utils.db_client import DatabaseClient
from scripts.utils.db_config import API_TRIAL_THRESHOLD

class BaseEtl():

    def __init__(self, source_system: str, db_client: DatabaseClient | None = None):
        self.database = db_client or DatabaseClient()
        self._load_tries_count = 0
        self.source_system = source_system

    def run(self):
        watermark_value = self.database.get_watermark_value(self.source_system)
        while(self._load_tries_count < API_TRIAL_THRESHOLD):
            try:
                json_data = self._get_data(watermark_value)
                ##self.database.load_to_db(json_data)
                print('Data loaded successfully')
                return
            except (ConnectionError, TimeoutError, KeyError, requests.HTTPError) as error:
                print(f'Error occured while loading: {error}')
                self._load_tries_count += 1
                self.save_failed_load(str(error))
                if(self._load_tries_count < API_TRIAL_THRESHOLD):
                    print(f'Retrying the Load..... [{self._load_tries_count} / {API_TRIAL_THRESHOLD}]')
                    time.sleep(4)
                else:
                    print(f'Error occured while loading: {error}')
                    print('Number of trials exceeded the allowed threshold')

    def is_valid_date(self, string: str | None) -> bool:
        if not string: return False
        try:
            datetime.fromisoformat(string)
            return True
        except ValueError:
            print(f"Value '{string}' is not a valid ISO format datetime")
            return False 

    def save_failed_load(self, error_message: str):
        self.database.load_to_control_table(error_message)

    def _get_data(self,  watermark: str):
        return
    
