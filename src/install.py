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


def aws_s3(trigger_event):
    rds = RDS()
    try:
        rds.install_aws_s3()
    except Exception as e:
        logger.debug(repr(e), exc_info=True)
        raise RuntimeError(repr(e))
    finally:
        rds.disconnect()


def lambda_handler(event, context):
    """
    Creates the aws_s3 extension for transfering S3 data directly to RDS.
    :param event: AWS S3 JSON event containing a Record list of messages
    :param context:
    :return: success or fail status
    """
    response = None
    try:
        logger.debug(event)
        aws_s3()
    except Exception as e:
        logger.info(f'About to exit with this exception: {repr(e)}')
        raise e
    else:
        response = {'statusCode': 201, 'body': "aws_s3 extension installed correctly"}
    return response
