"""
ETL loads content S3 and persist it into an RDS
"""
import logging

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
    try:
        rds.connect()
        datum = event.data
        record_id = rds.persist_data(datum)
    except Exception as e:
        logger.debug(repr(e), exc_info=True)
        raise e
    else:
        return record_id
    finally:
        rds.disconnect()
        logger.debug('Disconnected from database.')


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
        record_id = etl(event)
    except Exception as e:
        logger.info(f'About to exit with this exception: {repr(e)}')
        raise e
    else:
        response = {'id': record_id}
    return response
