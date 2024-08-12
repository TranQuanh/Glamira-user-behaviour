from google.cloud import bigquery, storage
import json
def create_schema_from_json(schema_json): 
    schema = [] 
    for column in schema_json: 
        if column['type'] == 'RECORD': 
            # Recursively handle nested schemas 
            fields = create_schema_from_json(column['fields']) 
            schema_field = bigquery.SchemaField(column['name'], column['type'], column['mode'], fields=fields) 
        else: 
            schema_field = bigquery.SchemaField(column['name'], column['type'], column['mode']) 
         
        schema.append(schema_field) 
     
    return schema
def gcs_to_bigquery(event, context):
    client = bigquery.Client()
    storage_client = storage.Client()

    bucket_name = event['bucket']
    file_name = event['name']
    
    dataset_id = 'Glamira'  # Thay bằng dataset của bạn
    table_id = file_name.split('.')[0]
    table_id = table_id.split("/")[1]
    table_ref = client.dataset(dataset_id).table(table_id)

    # Xác định đường dẫn đến tệp JSON schema
    schema_uri =f"{table_id}_schema.json"
    print("load schema")
    with open(schema_uri, 'r') as schema_file:
        content = schema_file.read()
        schema_json = json.loads(content)
        # schema = [bigquery.SchemaField(**field) for field in schema_json]
        schema = create_schema_from_json(schema_json)
    print("Schema loaded successfully")
    print(schema)
    # schema = [bigquery.SchemaField(**field) for field in schema_json]
    # Kiểm tra và tạo bảng nếu chưa tồn tại
    try:
        client.get_table(table_ref)
        print(f"Table {table_id} already exists.")
    except:
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        print(f"Created table {table_id} in dataset {dataset_id}.")

    # Tạo URI cho tệp JSONL trên GCS
    uri = f"gs://{bucket_name}/{file_name}"

    # Cấu hình job tải dữ liệu
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=schema,
        write_disposition = 'WRITE_APPEND'
    )

    load_job = client.load_table_from_uri(
        uri,
        table_ref,
        job_config=job_config
    )
    load_job.result()
    print(f"Loaded {file_name} into {dataset_id}.{table_id}")