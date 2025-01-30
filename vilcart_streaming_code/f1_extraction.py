import boto3
import json
from custom_logger import setup_logger

# Initialize the logger
logger = setup_logger("KinesisLogger")

def update_sequence_number(secret_name, secrets_client, shard_id, sequence_number):
    try:
        # Retrieve the existing secret data
        response = secrets_client.get_secret_value(SecretId=secret_name)
        secret_data = json.loads(response['SecretString'])

        # Update the shard-related keys while keeping the rest intact
        secret_data["last_shard_id"] = shard_id
        secret_data["last_shard_sequence"] = sequence_number

        # Update the secret in AWS Secrets Manager
        secrets_client.update_secret(
            SecretId=secret_name,
            SecretString=json.dumps(secret_data)
        )
        logger.info(f"Updated shard ID to {shard_id} and sequence number to {sequence_number}.")
    except secrets_client.exceptions.ResourceNotFoundException:
        # If the secret doesn't exist, create it with the new shard data
        secret_data = {
            "last_shard_id": shard_id,
            "last_shard_sequence": sequence_number
        }
        secrets_client.create_secret(
            Name=secret_name,
            SecretString=json.dumps(secret_data)
        )
        logger.warning(f"Secret {secret_name} not found. Creating a new secret.")
    except Exception as e:
        logger.error(f"Error updating shard ID and sequence number: {e}")

def get_last_sequence_number(secret_name, secrets_client):
    try:
        # Retrieve the last shard data from Secrets Manager
        response = secrets_client.get_secret_value(SecretId=secret_name)
        secret_data = json.loads(response['SecretString'])
        logger.info(f"Retrieved last sequence number from secret {secret_name}.")
        return secret_data
    except secrets_client.exceptions.ResourceNotFoundException:
        # If the secret doesn't exist, create it with empty data
        secrets_client.create_secret(
            Name=secret_name,
            SecretString=json.dumps({})
        )
        logger.warning(f"Secret {secret_name} not found. Creating an empty secret.")
        return None
    except Exception as e:
        logger.error(f"Error retrieving last shard ID and sequence number: {e}")
        return None

def list_shards(kinesis_client, stream_name):
    try:
        response = kinesis_client.list_shards(StreamName=stream_name)
        shards = response.get("Shards", [])
        logger.info(f"Retrieved {len(shards)} shards from stream {stream_name}.")
        return [shard["ShardId"] for shard in shards]
    except Exception as e:
        logger.error(f"Error listing shards: {e}")
        return []

def get_kinesis_records(kinesis_client, secrets_client, secret_name, stream_name):
    shards = list_shards(kinesis_client, stream_name)
    all_records = []
    last_shard_id = None
    last_processed_sequence = None

    for shard_id in shards:
        # Fetch the last sequence number and shard ID
        last_sequence_data = get_last_sequence_number(secret_name, secrets_client)
        last_sequence_number = None

        if last_sequence_data and last_sequence_data.get("last_shard_id") == shard_id:
            last_sequence_number = last_sequence_data.get("last_shard_sequence")
            logger.info(f"Retrieved last sequence number: {last_sequence_number} from shard: {shard_id}")

        # Determine the shard iterator type
        if last_sequence_number:
            iterator_args = {
                'StreamName': stream_name,
                'ShardId': shard_id,
                'ShardIteratorType': 'AFTER_SEQUENCE_NUMBER',
                'StartingSequenceNumber': last_sequence_number
            }
        else:
            iterator_args = {
                'StreamName': stream_name,
                'ShardId': shard_id,
                'ShardIteratorType': 'TRIM_HORIZON'
            }

        try:
            response = kinesis_client.get_shard_iterator(**iterator_args)
            shard_iterator = response['ShardIterator']
        except Exception as e:
            logger.error(f"Error getting shard iterator for shard {shard_id}: {e}")
            continue

        records = []

        while shard_iterator:
            try:
                response = kinesis_client.get_records(
                    ShardIterator=shard_iterator
                )
            except Exception as e:
                logger.error(f"Error fetching records from shard {shard_id}: {e}")
                break

            if response['Records']:
                records.extend(response['Records'])
                # Keep track of the last shard ID and sequence number
                last_shard_id = shard_id
                last_processed_sequence = response['Records'][-1]['SequenceNumber']
                logger.info(f"Retrieved {len(response['Records'])} records from shard {shard_id}.")

            shard_iterator = response.get('NextShardIterator', None)
            if len(response['Records']) == 0:
                break

        all_records.extend(records)

    # Update the last processed shard ID and sequence number
    if last_shard_id and last_processed_sequence:
        update_sequence_number(secret_name, secrets_client, last_shard_id, last_processed_sequence)

    return all_records

def get_secret(secrets_client, secret_name):
    """
    Retrieve and parse the secret value from AWS Secrets Manager.
    """
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        secret = response.get('SecretString') or response.get('SecretBinary')
        logger.info(f"Successfully retrieved secret {secret_name}.")
        return json.loads(secret) if isinstance(secret, str) else secret
    except Exception as e:
        logger.error(f"Failed to retrieve secret {secret_name}: {e}")
        raise
