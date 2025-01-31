import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from datetime import datetime
import json
from custom_logger import setup_logger

# Initialize the logger
logger = setup_logger("loadtos3")

def upload_to_s3(data, file_name, bucket, folder, s3_client):
    df = pd.DataFrame(data)

    if not df.empty:
        # Serialize JSON-like data in columns
        for col in df.columns:
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)
        
        # Add timestamp to the file name
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name_with_timestamp = f"{file_name.split('.')[0]}_{timestamp}.parquet"
        
        # Convert DataFrame to Parquet format
        table = pa.Table.from_pandas(df)
        buffer = BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)
        
        # Ensure folder exists in S3
        folder_path = folder if folder.endswith('/') else folder + '/'
        try:
            s3_client.head_object(Bucket=bucket, Key=folder_path)
        except s3_client.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                logger.info(f"Folder {folder_path} does not exist. Creating it in S3.")
                s3_client.put_object(Bucket=bucket, Key=folder_path)
        
        # Upload file to S3
        s3_client.put_object(Bucket=bucket, Key=folder_path + file_name_with_timestamp, Body=buffer)
        logger.info(f"Uploaded {file_name_with_timestamp} to S3: {folder_path + file_name_with_timestamp}")
    else:
        logger.warning(f"No data to upload for {file_name}, DataFrame is empty.")