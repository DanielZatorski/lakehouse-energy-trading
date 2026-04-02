from s3_manager import *
from openweather import *




def lambda_handler(event, context):
    run_time = datetime.now(timezone.utc)
    ingested_at = run_time.isoformat()

    weather_records = fetch_current_weather(weather_points)

    if not weather_records:
        return {
            "statusCode": 500,
            "body": json.dumps(
                {
                    "message": "No weather records fetched.",
                    "record_count": 0,
                }
            ),
        }

    s3_key = build_s3_key(run_time)
    body = to_jsonl(weather_records, ingested_at)

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "Weather data written to bronze layer.",
                "bucket": BUCKET_NAME,
                "key": s3_key,
                "record_count": len(weather_records),
            }
        ),
    }


#if __name__ == "__main__":
#    event = {}
#   context = {}
#    result = lambda_handler(event, context)
#    print(result)