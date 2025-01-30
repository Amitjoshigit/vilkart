import json
from datetime import datetime
from custom_logger import setup_logger

# Initialize the logger
logger = setup_logger("KinesisLogger")

def process_kinesis_records(records):
    order_data = []
    created_by_data = []
    
    for record in records:
        try:
            if 'Data' in record:
                payload_data = record['Data']
                
                try:
                    payload = json.loads(payload_data.decode("utf-8"))
                    logger.info(f"Processing record with sequence number: {record['SequenceNumber']}")
                except Exception as e:
                    logger.error(f"Error decoding payload as JSON: {e}")
                    continue
                
                order_info = {
                    "id": payload.get("id", ""),
                    "_id": payload["_id"]["$oid"] if isinstance(payload.get("_id"), dict) else payload.get("_id", ""),
                    "address": payload.get("address", ""),
                    "balanceAmount": payload.get("balanceAmount", 0),
                    "bankName": payload.get("bankName", ""),
                    "bankPaidAmount": payload.get("bankPaidAmount", ""),
                    "cashAmount": payload.get("cashAmount", 0),
                    "cashSettled": payload.get("cashSettled", 0),
                    "complete": payload.get("complete", ""),
                    "createdAt": datetime.fromtimestamp(payload.get("createdAt", 0) / 1000) if payload.get("createdAt") else None,
                    "dcName": payload.get("dcName", ""),
                    "delivering": payload.get("delivering", ""),
                    "deliveryAt": payload.get("deliveryAt", ""),
                    "diffAmt": payload.get("diffAmt", 0),
                    "discountAmount": payload.get("discountAmount", 0),
                    "dispatchAt": payload.get("dispatchAt", ""),
                    "invAmount": payload.get("invAmount", 0),
                    "invoice": payload.get("invoice", ""),
                    "invoiceAt": payload.get("invoiceAt", ""),
                    "updatedAt": payload.get("updatedAt", ""),
                    "invoiceNumber": payload.get("invoiceNumber", 0),
                    "isPrinted": payload.get("isPrinted", False),
                    "orderNumber": payload.get("orderNumber", None),
                    "packed": payload.get("packed", ""),
                    "packedAt": payload.get("packedAt", ""),
                    "paidAmount": payload.get("paidAmount", 0),
                    "prevBalance": payload.get("prevBalance", 0),
                    "remarks": payload.get("remarks", ""),
                    "returnAmount": payload.get("returnAmount", 0),
                    "route": payload.get("route", ""),
                    "status": payload.get("status", ""),
                    "total": payload.get("total", 0),
                    "vehicleNumber": payload.get("vehicleNumber", ""),
                    "villageName": payload.get("villageName", ""),
                    "created_on": payload.get("created_on", ""),
                    "subTotal": payload.get("subTotal", ""),
                    "handlingFee": payload.get("handlingFee", 0),
                    "isCascaded": payload.get("isCascaded", False),
                    "cascadedAt": payload.get("cascadedAt", ""),
                    "cascadedOn": payload.get("cascadedOn", ""),
                    "transactionId": payload.get("transactionId", 0),
                }
                order_data.append(order_info)

                created_by = payload.get("createdBy", {})
                if created_by:
                    created_by_info = {
                        "order_id": payload.get("_id", ""),
                        "_id": created_by.get("_id", ""),
                        "status": created_by.get("status", ""),
                        "dcName": created_by.get("dcName", ""),
                        "displayName": created_by.get("displayName", ""),
                        "role": created_by.get("role", ""),
                        "email": created_by.get("email", ""),
                        "roleName": created_by.get("roleName", ""),
                        "userName": created_by.get("userName", ""),
                        "created_on": created_by.get("created_on", "")
                    }
                    created_by_data.append(created_by_info)

        except Exception as e:
            logger.error(f"Error processing record: {e}")
            continue
    
    logger.info(f"Order data length: {len(order_data)}")
    logger.info(f"Created by data length: {len(created_by_data)}")
    return order_data, created_by_data
