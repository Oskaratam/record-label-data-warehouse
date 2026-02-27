TABLES = {
    "bronze_control_table" : {
        "name": "bronze.ctrl_load",
       "columns": {
        "system": "source_system",
        "type": "source_type",
        "status": "load_status",
        "target": "target_table_name",
        "rows": "loaded_rows",
        "hash": "hash_value",
        "watermark": "watermark_value",
        "start": "start_time",
        "end": "finish_time",
        "error": "error_message"
        }
    }
}
