import json
from exceptions.table_insert_error import TableInsertError
from service.table_service import save

def lambda_handler(event, context):
    event_type = event.get("event")
    data = event.get("data")
    user = event.get("user")
    
    if event_type == "add_table" or event_type == "update_table":
        try:
            message = save(data, user)
            status_code = 200
        except TableInsertError as e:
            message = str(e)
            status_code = 500
            
        return {
            "statusCode": status_code,
            "body": json.dumps({"message": message})
        }
    
    else:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "Invalid event type"})
        }