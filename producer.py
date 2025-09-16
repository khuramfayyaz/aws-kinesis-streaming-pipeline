import boto3
import json
import time
import random
from datetime import datetime

# Reuse your simulator function
def make_reading(device_id):
    return {
        "device_id": device_id,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "temperature": round(random.uniform(18.0, 36.0), 2),
        "humidity": round(random.uniform(20.0, 80.0), 1),
        "battery": round(random.uniform(2.5, 4.2), 2)
    }

# Initialize Kinesis client
kinesis = boto3.client("kinesis", region_name="us-east-1")  # change if your region is different

STREAM_NAME = "iot-kinesis-stream"   # must match your stream name

# Send 10 readings as a test
for i in range(10):
    reading = make_reading("device-1")
    print("Sending:", reading)

    # Put record into Kinesis
    response = kinesis.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(reading),     # convert dict to JSON string
        PartitionKey=reading["device_id"]  # partition by device_id
    )

    print("Response:", response)
    time.sleep(1)  # wait 1 second between messages
