
import os
import boto3
from botocore.config import Config

_REGION = os.getenv("AWS_REGION", "us-east-1")
_TABLE  = os.getenv("DDB_TABLE_NAME", "MyProjectTableDev")

# Optional: support LocalStack via AWS_DDB_ENDPOINT
_ENDPOINT = os.getenv("AWS_DDB_ENDPOINT")  # e.g., http://localhost:4566

_session = boto3.Session(profile_name=os.getenv("AWS_PROFILE"))
_config = Config(retries={"max_attempts": 10, "mode": "standard"})

_dynamodb = boto3.resource(
    "dynamodb",
    region_name=_REGION,
    endpoint_url=_ENDPOINT,
    config=_config,
)

_table = _dynamodb.Table(_TABLE)

def put_item(item: dict) -> None:
    if "pk" not in item:
        raise ValueError("Item must contain 'pk'")
    _table.put_item(Item=item)

def get_item(pk: str):
    resp = _table.get_item(Key={"pk": pk})
    return resp.get("Item")

def ping() -> bool:
    # quick health check
    _table.load()
    return True
