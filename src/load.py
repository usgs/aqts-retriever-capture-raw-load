"""
ETL loads content S3 and persist it into an RDS
"""
import logging
import os

from .etl.event_processor import TriggerEvent
from .etl.rds import RDS
from .etl.config import CONFIG

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def etl(trigger_event):
    aws_region = CONFIG['aws']['region']
    event = TriggerEvent(aws_region)
    event.extract(trigger_event)

    rds = RDS()
    datum = event.data
    try:
        record_id = rds.persist_data(datum)
    except Exception as e:
        logger.debug(repr(e), exc_info=True)
        raise RuntimeError(repr(e))
    finally:
        rds.disconnect()
    return record_id


def etl_big(trigger_event):
    rds = RDS()
    try:
        record_id = rds.insert_from_s3(trigger_event['s3']['bucket']['name'], trigger_event['s3']['object']['key'])
    except Exception as e:
        logger.debug(repr(e), exc_info=True)
        raise RuntimeError(repr(e))
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
            logger.debug(f"Processing small S3 object {size}")
            record_id = etl(event)
        else:
            logger.debug(f"Processing large S3 object {size}")
            record_id = etl_big(event)
    except Exception as e:
        logger.info(f'About to exit with this exception: {repr(e)}')
        raise e
    else:
        response = {'id': record_id[0], 'partitionNumber': record_id[1]}
    return response
