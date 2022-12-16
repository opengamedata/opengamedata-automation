from google.cloud import bigquery

# Definition for our BigQuery table schema - used for table creation
# SchemaField docs - https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.schema.SchemaField
# field_type docs - https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#TableFieldSchema.FIELDS.type
schema = [
    bigquery.SchemaField("session_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("user_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("user_data", "JSON", mode="NULLABLE"),
    bigquery.SchemaField("client_time", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("client_offset", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("server_time", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("event_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("event_data", "JSON", mode="REQUIRED"),
    bigquery.SchemaField("event_source", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("game_state", "JSON", mode="NULLABLE"),
    bigquery.SchemaField("app_version", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("app_branch", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("log_version", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("event_sequence_index", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("remote_addr", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("http_user_agent", "STRING", mode="NULLABLE")
]
