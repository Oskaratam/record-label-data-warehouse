import os 
import pyodbc
import db_config
from dotenv import load_dotenv

class DatabaseClient:
    def __init__(self):
        load_dotenv('.env')
        self.SERVER_NAME = os.getenv('SERVER_NAME')
        self.DATABASE_NAME = os.getenv('DATABASE_NAME')
        self.UID = os.getenv('UID')
        self.PASSWORD = os.getenv('PASSWORD')
        self.TABLES = db_config.TABLES

    
    def load_to_db(self, raw_data, source_system, source_url=None):
        connection = self._connect_to_db()
        cursor = connection.cursor()


        print('Loaded to db')
        connection.close()


    def _load_to_control_table():
        print()
        

    def get_watermark_value(self, source_system) -> str:
        connection : pyodbc.Connection = self._connect_to_db()
        cursor = connection.cursor()
        watermark = None
        control_table = self.TABLES["bronze_control_table"]
        try:
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
            print(f"Database error: {e}")
        finally:
            connection.close()
            return watermark
        

    def _load_json(watermark=None):
        print()

    def _load_html(watermark=None):
        print()

    def  _connect_to_db(self) -> pyodbc.Connection:
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


client = DatabaseClient()
print(client.get_watermark_value("SAP_ERP"))



