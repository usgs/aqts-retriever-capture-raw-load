"""
ETL loads content S3 and persist it into an RDS
"""
import json
import logging
import os

import boto3

from .etl.event_processor import TriggerEvent
from .etl.rds import RDS
from .etl.config import CONFIG

log_level = os.getenv('LOG_LEVEL', logging.ERROR)
logger = logging.getLogger(__name__)
logger.setLevel(log_level)

NWCAPTURE_TEST = 'NWCAPTURE-DB-TEST'
secrets_client = boto3.client('secretsmanager', os.environ['AWS_DEPLOYMENT_REGION'])

def etl(trigger_event):
    aws_region = CONFIG['aws']['region']
    event = TriggerEvent(aws_region)
    event.extract(trigger_event)
    original = secrets_client.get_secret_value(
        SecretId=NWCAPTURE_TEST,
    )
    secret_string = json.loads(original['SecretString'])
    db_host = secret_string['DATABASE_ADDRESS']
    db_user = secret_string['SCHEMA_OWNER_USERNAME']
    db_name = secret_string['DATABASE_NAME']
    db_password = secret_string['SCHEMA_OWNER_PASSWORD']

    rds = RDS(db_host, db_user, db_name, db_password)
    datum = event.data
    try:
        record_id = rds.persist_data(datum)
    except Exception as e:
        logger.debug(repr(e), exc_info=True)
        raise RuntimeError(repr(e))
    finally:
        rds.disconnect()
    return record_id


def lambda_handler(event, context):
    """
    takes an AWS S3 event and processes it
    :param event: AWS S3 JSON event containing a Record list of messages
    :param context:
    :return: success or fail status
    """
    response = None
    try:
        logger.debug(event)
        size = int(event['Record']['s3']['object']['size'])
        if size < int(os.getenv('S3_OBJECT_SIZE_LIMIT', 150000000)):
            record_id = etl(event)
        else:
            raise Exception(f"File too large to process for event {event}")
    except Exception as e:
        logger.info(f'About to exit with this exception: {repr(e)}')
        raise e
    else:
        response = {'id': record_id[0], 'partitionNumber': record_id[1]}
    return response
