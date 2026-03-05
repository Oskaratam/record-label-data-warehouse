import time
import requests 
import json
from datetime import datetime
from scripts.utils.db_client import DatabaseClient
from scripts.utils.etl_config import API_TRIAL_THRESHOLD

class BaseEtl():

    def __init__(self, source_system: str, data_category: str, db_client: DatabaseClient | None = None):
        self.database = db_client or DatabaseClient()
        self._load_tries_count = 0
        self.source_system = source_system
        self.data_category = data_category

    def run(self):
        watermark_value = self.database.get_watermark_value(self.source_system)
        raw_data = [{}]
        while(self._load_tries_count < API_TRIAL_THRESHOLD ):
            try:
                raw_data = self._get_data(watermark_value)
                break
            except (ConnectionError, TimeoutError, KeyError, requests.HTTPError, Exception) as error:
                print(f'Error occured while loading: {error}')
                self._load_tries_count += 1
                self.save_failed_load(str(error))
                if(self._load_tries_count < API_TRIAL_THRESHOLD):
                    print(f'Retrying the Load..... [{self._load_tries_count} / {API_TRIAL_THRESHOLD}]')
                    time.sleep(4)
                else:
                    print(f'Error occured while loading: {error}')
                    print('Number of trials exceeded the allowed threshold')
                    return
        self.database.load_to_bronze(raw_data, self.source_system, self.data_category) 

    @classmethod
    def is_valid_date(cls, string: str | None) -> bool:
        if not string: return False
        try:
            datetime.fromisoformat(string)
            return True
        except ValueError:
            print(f"Value '{string}' is not a valid ISO format datetime")
            return False 

    def save_failed_load(self, error_message: str):
        self.database.load_to_control_table(error_message)

    def _get_data(self,  watermark: str) -> list[dict]:
        return [{}]
    
