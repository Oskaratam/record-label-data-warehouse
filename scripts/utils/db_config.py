TABLES = {
    "bronze_control_table" : {
        "name": "bronze.ctrl_load",
       "columns": {
        "system": "source_system",
        "status": "load_status",
        "rows": "loaded_rows",
        "watermark": "watermark_value",
        "start": "start_time",
        "end": "finish_time",
        "load_time": "load_time_seconds",
        "error": "error_message",
        "target": "target_table_name",
        }
    }
}
