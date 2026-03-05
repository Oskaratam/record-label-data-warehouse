import os 
import pyodbc
import json 
import scripts.utils.db_config as db_config
import scripts.utils.etl_config as etl_config
from dotenv import load_dotenv

class DatabaseClient:

    def __init__(self):
        load_dotenv('.env')
        self.SERVER_NAME = os.getenv('SERVER_NAME')
        self.DATABASE_NAME = os.getenv('DATABASE_NAME')
        self.UID = os.getenv('UID')
        self.PASSWORD = os.getenv('PASSWORD')
        self.TABLES = db_config.TABLES

    
    def load_to_bronze(self, raw_data : list[dict], source_system : str, data_category : str):
        connection = self._connect_to_db()
        target_table = etl_config.category_to_table_name_map[data_category]
        try:
            if not isinstance(raw_data, list):
                raw_data = [raw_data]
            
            params = [(json.dumps(record), source_system) for record in raw_data]
            with connection.cursor() as cursor:
                query = f"""
                INSERT INTO {target_table} (raw_content, meta_source_system) 
                VALUES (?, ?)
                """
                cursor.fast_executemany = True  
                cursor.executemany(query, params)
                print(f'Loaded {len(raw_data)} rows into table "{target_table}"')
        except pyodbc.Error as e:
            print(f"Database ERROR during loading to bronze: {e}")
            connection.rollback()
        finally:
            connection.close()



    def load_to_control_table(self, error_message):
        print()
        

    def get_watermark_value(self, source_system: str) -> str:
        connection : pyodbc.Connection = self._connect_to_db()
        watermark = ""
        control_table = self.TABLES["bronze_control_table"]
        try:
            with connection.cursor() as cursor:
                watermark = cursor.execute(
                    f"""
                    SELECT TOP 1 {control_table['columns']['watermark']}
                    FROM {control_table['name']}
                    WHERE source_system = ? AND {control_table['columns']['status']} = 'COMPLETED'
                    ORDER BY {control_table['columns']['end']} DESC
                    """, 
                    source_system
                    ).fetchone()
        except pyodbc.Error as e:
            print(f"Database ERROR during watermark retrieval: {e}")
        finally:
            connection.close()
            return str(watermark[0]) if isinstance(watermark, pyodbc.Row) and len(watermark) > 0 else ""
        

    def _load_json(self, watermark=""):
        print()

    def _load_html(self, watermark=""):
        print()

    def  _connect_to_db(self) -> pyodbc.Connection: # type: ignore
        try:
            connection = pyodbc.connect(
                f'DRIVER={{ODBC Driver 18 for SQL Server}};'
                f'SERVER={self.SERVER_NAME};'
                f'DATABASE={self.DATABASE_NAME};'
                f'UID={self.UID};' 
                f'PWD={self.PASSWORD};'
                'TrustServerCertificate=yes;'
            )  
            return connection
        except pyodbc.Error as e:
            print('!!!!!!!!!!!!!!!!!')
            print(f'Connection to the database failed \n Error Message: {e}')

if __name__ == "__main__":
    database = DatabaseClient()
    watermark = database.get_watermark_value('SAP_ERP')
    print(watermark)
    




