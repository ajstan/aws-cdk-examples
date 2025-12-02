# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all

patch_all()

import boto3
import os
import json
import logging
import uuid

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb_client = boto3.client("dynamodb")


def handler(event, context):
    table = os.environ.get("TABLE_NAME")
    request_id = context.request_id
    
    # Log request context (structured)
    logger.info(json.dumps({
        "event": "request_received",
        "request_id": request_id,
        "table_name": table,
        "http_method": event.get("httpMethod"),
        "source_ip": event.get("requestContext", {}).get("identity", {}).get("sourceIp"),
    }))
    
    try:
        if event.get("body"):
            item = json.loads(event["body"])
            logger.info(json.dumps({
                "event": "processing_request",
                "request_id": request_id,
                "has_payload": True,
            }))
            
            year = str(item["year"])
            title = str(item["title"])
            id = str(item["id"])
            
            dynamodb_client.put_item(
                TableName=table,
                Item={"year": {"N": year}, "title": {"S": title}, "id": {"S": id}},
            )
            
            logger.info(json.dumps({
                "event": "dynamodb_put_success",
                "request_id": request_id,
                "item_id": id,
            }))
            
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"message": "Successfully inserted data!"}),
            }
        else:
            logger.info(json.dumps({
                "event": "processing_default_request",
                "request_id": request_id,
            }))
            
            item_id = str(uuid.uuid4())
            dynamodb_client.put_item(
                TableName=table,
                Item={
                    "year": {"N": "2012"},
                    "title": {"S": "The Amazing Spider-Man 2"},
                    "id": {"S": item_id},
                },
            )
            
            logger.info(json.dumps({
                "event": "dynamodb_put_success",
                "request_id": request_id,
                "item_id": item_id,
            }))
            
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"message": "Successfully inserted data!"}),
            }
    except Exception as e:
        logger.error(json.dumps({
            "event": "error",
            "request_id": request_id,
            "error_type": type(e).__name__,
            "error_message": str(e),
        }))
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"message": "Internal server error"}),
        }
