import json
import boto3
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# AWS Kinesis configuration
STREAM_NAME = "vlc-realtime-events-test"
AWS_REGION = "ap-south-1"

# Initialize Kinesis client
kinesis_client = boto3.client("kinesis", region_name=AWS_REGION)

def chunk_records(records, max_chunk_size=5000000, max_chunk_count=500):
    """
    Split records into smaller chunks to stay within Kinesis API limits.

    Args:
        records (list): List of records to split.
        max_chunk_size (int): Maximum chunk size in bytes (default: 5 MB).
        max_chunk_count (int): Maximum number of records per chunk (default: 500).

    Returns:
        list: List of record chunks.
    """
    chunks = []
    current_chunk = []
    current_size = 0

    for record in records:
        record_size = len(record["Data"].encode("utf-8")) + len(record["PartitionKey"].encode("utf-8"))
        # Check if adding this record exceeds size or count limits
        if (current_size + record_size > max_chunk_size) or (len(current_chunk) >= max_chunk_count):
            chunks.append(current_chunk)
            current_chunk = []
            current_size = 0
        current_chunk.append(record)
        current_size += record_size

    if current_chunk:
        chunks.append(current_chunk)

    return chunks

def extract_payload_and_stream(json_file_path, stream_name):
    """
    Extract 'payload' from a JSON file and stream to Kinesis Data Stream.

    Args:
        json_file_path (str): Path to the local JSON file.
        stream_name (str): Name of the Kinesis Data Stream.
    """
    try:
        # Load the JSON data from the file
        with open(json_file_path, "r") as file:
            data = json.load(file)

        # Check if data is a list or a single object
        if isinstance(data, dict):  # If it's a single object, wrap it in a list
            data = [data]

        # Prepare records for batching
        records = []
        for obj in data:
            object_id = obj.get("_id", {}).get("$oid", "unknown_id")
            payload = obj.get("payload", {})
            
            # Skip if no payload exists
            if not payload:
                logging.warning(f"No payload found for object ID {object_id}, skipping...")
                continue
            
            # Prepare record
            partition_key = object_id
            payload_data = json.dumps(payload)
            records.append({
                "Data": payload_data,
                "PartitionKey": partition_key
            })

        # Chunk and send records to Kinesis
        if records:
            chunks = chunk_records(records)
            for chunk in chunks:
                response = kinesis_client.put_records(StreamName=stream_name, Records=chunk)
                logging.info(f"Successfully sent {len(chunk)} records to stream {stream_name}. Response: {response}")
        else:
            logging.warning("No valid records to send to Kinesis.")
    
    except FileNotFoundError:
        logging.error(f"File not found: {json_file_path}")
    except json.JSONDecodeError:
        logging.error("Error decoding JSON file.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

# Local JSON file path
json_file_path = "/home/vijaylaxmi/Documents/Vilcart_documents/latest_firstHope.KA_KUMTA.json"
# json_file_path = "/home/vijaylaxmi/Documents/Vilcart_documents/latest_firstHope.activities_TS_WARANGAL.json"
# json_file_path = "/home/vijaylaxmi/Documents/Vilcart_documents/firstHope.activities.json"

# Call the function to extract and stream data
extract_payload_and_stream(json_file_path, STREAM_NAME)