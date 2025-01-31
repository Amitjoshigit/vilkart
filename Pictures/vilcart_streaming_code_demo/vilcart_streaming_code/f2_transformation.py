import json
from datetime import datetime
from typing import List, Dict
from custom_logger import setup_logger

# Initialize the logger
logger = setup_logger("KinesisLogger")

def extract_category_data(items: List[Dict]) -> tuple:
    """Extract category and subcategory names from items"""
    try:
        if not items:
            return '', ''
        
        first_item = items[0]
        if not isinstance(first_item, dict):
            return '', ''
            
        category = first_item.get('category', {})
        if not isinstance(category, dict):
            return '', ''
            
        category_name = category.get('name', '')
        
        subcats = category.get('subCats', {})
        if not isinstance(subcats, dict):
            return category_name, ''
            
        subcat_name = subcats.get('name', '')
        
        return category_name, subcat_name
    except Exception as e:
        print(f"Error extracting category data: {str(e)}")
        return '', ''

def process_kinesis_records(records):
    order_data = []
    created_by_data = []
    
    for record in records:
        try:
            if 'Data' in record:
                payload_data = record['Data']
                
                try:
                    payload = json.loads(payload_data.decode("utf-8"))
                except Exception as e:
                    logger.error(f"Error decoding payload as JSON: {e}")
                    continue
                
                order_info = {
                    "order_id": payload.get("id", ""),
                    "createdAt": datetime.fromtimestamp(payload.get("createdAt", 0) / 1000) if payload.get("createdAt") else None,
                    "dcName": payload.get("dcName", ""),
                    "updatedAt": payload.get("updatedAt", ""),
                    "orderNumber": payload.get("orderNumber", None),
                    "packed": payload.get("packed", ""),
                    "status": payload.get("status", ""),
                    "total": payload.get("total", 0),
                    "source": payload.get('source', '')
                }
                order_data.append(order_info)
                
                created_by = payload.get("createdBy", {})
                items = payload.get("items", [])  # Assuming 'items' exists in the payload
                category_name, subcat_name = extract_category_data(items)
                
                if created_by:
                    created_by_info = {
                        "order_id": payload.get("id", ""),
                        "created_By_id": created_by.get("_id", ""),
                        "status": created_by.get("status", ""),
                        "dcName": created_by.get("dcName", ""),
                        "displayName": created_by.get("displayName", ""),
                        "role": created_by.get("role", ""),
                        "email": created_by.get("email", ""),
                        "roleName": created_by.get("roleName", ""),
                        "created_on": created_by.get("created_on", ""),
                        "category_name": category_name,
                        "subcategory_name": subcat_name,
                        "total": payload.get("total", 0)
                    }
                    created_by_data.append(created_by_info)
        
        except Exception as e:
            logger.error(f"Error processing record: {e}")
            continue
    
    logger.info(f"Order data length: {len(order_data)}")
    logger.info(f"Created by data length: {len(created_by_data)}")
    return order_data, created_by_data