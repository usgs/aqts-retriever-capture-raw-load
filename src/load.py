"""
ETL loads content S3 and persist it into an RDS
"""
from .etl.event_processor import TriggerEvent
from .etl.rds import RDS
from .etl.config import CONFIG

import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def etl(trigger_event):
    aws_region = CONFIG['aws']['region']
    event = TriggerEvent(aws_region)
    event.extract(trigger_event)

    rds = RDS()
    try:
        rds.connect()
        for datum in event.data:
            logger.debug(f'Data to be persisted: {datum}.')
            rds.persist_data(datum)
    except Exception as e:
        logger.debug(repr(e), exc_info=True)
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
    try:
        logger.debug(event)
        etl(event)
        return {
            'statusCode': 200,
            'body': 'Successfully processed S3 Event'
        }
    except Exception as e:
        logger.info(f'About to exit with this exception: {repr(e)}')
        return {
            'statusCode': 500,
            'body': 'Error processing S3 Event %s' % repr(e)
        }
