import time
import requests 
import json
from typing import Any
from datetime import datetime
from scripts.utils.db_client import DatabaseClient
from scripts.utils.etl_config import API_TRIAL_THRESHOLD
from scripts.utils.decorators import with_metadata

class BaseEtl():

    def __init__(self, source_system: str, data_category: str, db_client: DatabaseClient | None = None):
        self.database = db_client or DatabaseClient()
        self._load_tries_count = 0
        self.source_system = source_system
        self.data_category = data_category

    def run(self):
        watermark_value = self.database.get_watermark_value(self.source_system)
        data = {}
        while(self._load_tries_count < API_TRIAL_THRESHOLD ):
            data : dict = self._get_data(watermark_value)

    
            if(data['metadata']['status'] == "Success"):
                self.database.load_to_bronze(data["raw_data"], self.source_system, self.data_category) 
                self.database.load_to_control_table(data["metadata"])
                return 
            else:
                print(f'Error occured while loading: {data['metadata']['error_message']}')
                self._load_tries_count += 1
                self.database.load_to_control_table(data['metadata'])
                if(self._load_tries_count < API_TRIAL_THRESHOLD):
                    print(f'Retrying the Load..... [{self._load_tries_count} / {API_TRIAL_THRESHOLD}]')
                    time.sleep(4)
                else:
                    print(f'Error occured while loading: {data['metadata']['error_message']}')
                    print('Number of trials exceeded the allowed threshold')
                    return


    @classmethod
    def is_valid_date(cls, string: str | None) -> bool:
        if not string: return False
        try:
            datetime.fromisoformat(string)
            return True
        except ValueError:
            print(f"Value '{string}' is not a valid ISO format datetime")
            return False 

    @with_metadata
    def _get_data(self,  watermark: str) -> dict:
        return {"raw_data" : [], "new_watermark": None}
    
