import os
import json
import boto3
import requests
import certifi
from entsoe import *


s3 = boto3.client("s3")
BUCKET_NAME =  os.environ["BUCKET_NAME"]



def lambda_handler(event, context):
    if event and event.get("period_start") and event.get("period_end"):
        period_start = event["period_start"]
        period_end = event["period_end"]
    else:
        period_start, period_end = get_hourly_window()

    result = run_entsoe_bronze_ingestion(
        period_start=period_start,
        period_end=period_end,
    )

    return {
        "statusCode": 200,
        "body": json.dumps(result),
    }


#if __name__ == "__main__":
#    period_start, period_end = get_last_closed_hour_window()

#    result = run_entsoe_bronze_ingestion(
#        period_start=period_start,
#        period_end=period_end,
#    )
#
#       print(json.dumps(result, indent=2))