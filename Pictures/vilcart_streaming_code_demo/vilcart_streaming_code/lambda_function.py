# import os
# import json
# import boto3
# from datetime import datetime
# from f1_extraction import get_kinesis_records, get_secret
# from f2_transformation import process_kinesis_records
# from f3_load import upload_to_s3
# from custom_logger import setup_logger

# logger = setup_logger("LambdaLogger")

# def lambda_handler(event, context):
#     # Retrieve environment variables
#     secret_name = os.environ.get('SECRETS_MANAGER_SECRET_NAME')
#     # secret_name = event.get('SECRETS_MANAGER_SECRET_NAME')
#     logger.info("Starting Lambda function execution.")
    
#     # Initialize AWS SDK clients
#     try:
#         kinesis_client = boto3.client('kinesis', region_name='ap-south-1')
#         s3_client = boto3.client('s3')
#         secrets_client = boto3.client('secretsmanager', region_name='ap-south-1')
#         logger.info("AWS SDK clients initialized successfully.")
#     except Exception as e:
#         logger.error(f"Failed to initialize AWS SDK clients: {e}")
#         raise

#     try:
#         # Fetch secret values
#         logger.info(f"Fetching secret values from Secrets Manager: {secret_name}")
#         secret_values = get_secret(secrets_client, secret_name)

#         # Extract necessary variables from the secret
#         kinesis_stream_name = secret_values.get('KINESIS_STREAM_NAME')
#         bucket_name = secret_values.get('S3_BUCKET_NAME')

#         # Ensure all variables are retrieved
#         if not all([kinesis_stream_name, bucket_name]):
#             logger.error("Failed to load configuration from Secrets Manager. Exiting.")
#             raise ValueError("Missing required secret values")
        
#         logger.info("Secret values retrieved successfully.")

#         # Get records from Kinesis
#         logger.info(f"Fetching records from Kinesis stream: {kinesis_stream_name}")
#         records = get_kinesis_records(kinesis_client, secrets_client, secret_name, kinesis_stream_name)
#         logger.info(f"Successfully fetched {len(records)} records from Kinesis stream.")
        
#         # Process Kinesis records
#         logger.info("Processing Kinesis records.")
#         order_data, created_by_data = process_kinesis_records(records)
#         logger.info(f"Processing complete. Order data: {len(order_data)} records, Created by data: {len(created_by_data)} records.")
        
#         # Generate dynamic folder names
#         order_folder = "order_Latest/"
#         created_by_folder = "sales_Latest/"
        
#         # Upload processed data to S3
#         logger.info("Uploading processed data to S3.")
#         upload_to_s3(order_data, "order_data.parquet", bucket_name, order_folder, s3_client)
#         upload_to_s3(created_by_data, "created_by_data.parquet", bucket_name, created_by_folder, s3_client)
#         logger.info("Data uploaded to S3 successfully.")
        
#         logger.info("Lambda function completed.")
#         return {
#             'statusCode': 200,
#             'body': json.dumps('Processing completed successfully')
#         }
#     except Exception as e:
#         logger.error(f"Error in Lambda function: {e}")
#         return {
#             'statusCode': 500,
#             'body': json.dumps(f'Error processing Kinesis records: {str(e)}')
#         }

# # lambda_handler(event = {"SECRETS_MANAGER_SECRET_NAME": "vlc-realtime-secrets"}, context=None)









import os
import json
import boto3
from datetime import datetime
from f1_extraction import get_kinesis_records, get_secret
from f2_transformation import process_kinesis_records
from f3_load import upload_to_s3
from custom_logger import setup_logger

logger = setup_logger("LambdaLogger")

def lambda_handler(event, context):
    # Get secret name from environment variable or event
    secret_name = os.environ.get('SECRETS_MANAGER_SECRET_NAME') or event.get('SECRETS_MANAGER_SECRET_NAME')
    
    if not secret_name:
        logger.error("Secret name not provided in environment variables or event")
        raise ValueError("SECRETS_MANAGER_SECRET_NAME is required")
    
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
        
        if not secret_values:
            logger.error("No secret values retrieved from Secrets Manager")
            raise ValueError("Empty secret values")
            
        # Extract necessary variables from the secret
        kinesis_stream_name = secret_values.get('KINESIS_STREAM_NAME')
        bucket_name = secret_values.get('S3_BUCKET_NAME')
        
        # Ensure all variables are retrieved
        if not all([kinesis_stream_name, bucket_name]):
            missing_values = []
            if not kinesis_stream_name:
                missing_values.append('KINESIS_STREAM_NAME')
            if not bucket_name:
                missing_values.append('S3_BUCKET_NAME')
            logger.error(f"Missing required secret values: {', '.join(missing_values)}")
            raise ValueError(f"Missing required secret values: {', '.join(missing_values)}")
        
        logger.info("Secret values retrieved successfully.")
        
        # Get records from Kinesis
        logger.info(f"Fetching records from Kinesis stream: {kinesis_stream_name}")
        records = get_kinesis_records(kinesis_client, secrets_client, secret_name, kinesis_stream_name)
        
        if not records:
            logger.info("No records found in Kinesis stream")
            return {
                'statusCode': 200,
                'body': json.dumps('No records to process')
            }
            
        logger.info(f"Successfully fetched {len(records)} records from Kinesis stream.")
        
        # Process Kinesis records
        logger.info("Processing Kinesis records.")
        order_data, created_by_data = process_kinesis_records(records)
        logger.info(f"Processing complete. Order data: {len(order_data)} records, Created by data: {len(created_by_data)} records.")
        
        # Generate dynamic folder names with timestamp for versioning
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        order_folder = f"order_Latest/{timestamp}/"
        created_by_folder = f"sales_Latest/{timestamp}/"
        
        # Upload processed data to S3
        logger.info("Uploading processed data to S3.")
        upload_to_s3(order_data, "order_data.parquet", bucket_name, order_folder, s3_client)
        upload_to_s3(created_by_data, "created_by_data.parquet", bucket_name, created_by_folder, s3_client)
        logger.info("Data uploaded to S3 successfully.")
        
        logger.info("Lambda function completed.")
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Processing completed successfully',
                'records_processed': len(records),
                'order_data_count': len(order_data),
                'created_by_data_count': len(created_by_data),
                'order_folder': order_folder,
                'created_by_folder': created_by_folder
            })
        }
    except Exception as e:
        logger.error(f"Error in Lambda function: {e}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'error_type': type(e).__name__
            })
        }

























