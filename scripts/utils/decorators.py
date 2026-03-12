from datetime import datetime
import time
import requests
def with_metadata(get_data_func):
    def wrapper(self, watermark) -> dict:
        start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        timer_start = time.perf_counter()
        metadata = {
            "source_system": self.source_system,
            "status": "Fail",
            "items_count": 0,
            "watermark": None,
            "start_time": start_time,
            "end_time": None,
            "execution_time": None,
            "error_message": None
        }
        try:
            data: dict = get_data_func(self, watermark)
            metadata.update({
                'execution_time' : time.perf_counter() - timer_start,
                'items_count' : len(data['raw_data']),
                'watermark' : data.get('new_watermark', ""),
                'end_time' : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'status' : "Success"
            })
            return {"raw_data": data['raw_data'],
                    "metadata": metadata
                    }
        except (ConnectionError, TimeoutError, KeyError, requests.HTTPError, Exception) as e:
            metadata.update({
                'execution_time' : time.perf_counter() - timer_start,
                'end_time' : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'error_message' : str(e)
            })
            return {
                    "raw_data": [],
                    "metadata": metadata
                    }
           
    return wrapper