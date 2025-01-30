import os
import json
import boto3
from f1_extraction import get_kinesis_records, get_secret
from f2_transformation import process_kinesis_records
from f3_load import upload_to_s3
from custom_logger import setup_logger

logger = setup_logger("LambdaLogger")

def lambda_handler(event, context):
    # Retrieve environment variables
    #secret_name = os.environ.get('SECRETS_MANAGER_SECRET_NAME')
    secret_name = "kinesis-sequence_shard-secret"
    logger.info("Starting Lambda function execution.")
    
    # Initialize AWS SDK clients
    try:
        kinesis_client = boto3.client('kinesis', region_name='ap-south-1')
        s3_client = boto3.client('s3')
        secrets_client = boto3.client('secretsmanager', region_name='ap-south-1')
        logger.info("AWS SDK clients initialized successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize AWS SDK clients: {e}")
        raise

    try:
        # Fetch secret values
        logger.info(f"Fetching secret values from Secrets Manager: {secret_name}")
        secret_values = get_secret(secrets_client, secret_name)

        # Extract necessary variables from the secret
        kinesis_stream_name = secret_values.get('KINESIS_STREAM_NAME')
        bucket_name = secret_values.get('S3_BUCKET_NAME')
      #

        # Ensure all variables are retrieved
        if not all([kinesis_stream_name, bucket_name):
            logger.error("Failed to load configuration from Secrets Manager. Exiting.")
            raise ValueError("Missing required secret values")
        
        logger.info("Secret values retrieved successfully.")

        # Get records from Kinesis
        logger.info(f"Fetching records from Kinesis stream: {kinesis_stream_name}")
        records = get_kinesis_records(kinesis_client, secrets_client, secret_name, kinesis_stream_name)
        logger.info(f"Successfully fetched {len(records)} records from Kinesis stream.")
        
        # Process Kinesis records
        logger.info("Processing Kinesis records.")
        order_data, created_by_data = process_kinesis_records(records)
        logger.info(f"Processing complete. Order data: {len(order_data)} records, Created by data: {len(created_by_data)} records.")
        
        # Upload processed data to S3
        logger.info("Uploading processed data to S3.")
        upload_to_s3(order_data, "order_data.parquet", bucket_name, s3_client)
        upload_to_s3(created_by_data, "created_by_data.parquet", bucket_name, s3_client)
        logger.info("Data uploaded to S3 successfully.")
        
        logger.info("Lambda function completed.")
        return {
            'statusCode': 200,
            'body': json.dumps('Processing completed successfully')
        }
    except Exception as e:
        logger.error(f"Error in Lambda function: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error processing Kinesis records: {str(e)}')
        }
