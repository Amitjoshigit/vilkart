import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from datetime import datetime
import json
import logging

# Configure logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def upload_to_s3(data, file_name, bucket, s3_client):
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
        
        # Upload to S3
        s3_client.put_object(Bucket=bucket, + file_name_with_timestamp, Body=buffer)
        logger.info(f"Uploaded {file_name_with_timestamp} to S3: {folder + file_name_with_timestamp}")
    else:
        logger.warning(f"No data to upload for {file_name}, DataFrame is empty.")
