import os
import json
import boto3
import requests
import certifi

s3 = boto3.client("s3")
BUCKET_NAME = os.environ["BUCKET_NAME"]

def lambda_handler(event, context):
    url = "https://pokeapi.co/api/v2/pokemon/pikachu"

    try:
        response = requests.get(url, timeout=30, verify=certifi.where())
        response.raise_for_status()
        data = response.json()

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key="raw/pokemon/pikachu.json",
            Body=json.dumps(data).encode("utf-8"),
            ContentType="application/json"
        )

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": f"Saved data to s3://{BUCKET_NAME}/raw/pokemon/pikachu.json"
            })
        }

    except requests.exceptions.SSLError as e:
        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": f"SSL error: {str(e)}"
            })
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": str(e)
            })
        }