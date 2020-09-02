"""
This module handles processing of a file object in S3.

"""
import json
import logging

from .s3 import S3

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

BODY = 'body'
STRING_VALUE = 'StringValue'


class TriggerEvent:

    def __init__(self, region):
        self.data = []
        self.region = region

    def extract(self, event):
        record = event['Record']
        record_data = CapturedData(record, self.region)
        record_data.extract_attributes()
        record_data.fetch_body()
        self.data = record_data


class CapturedData:
    """
        this class is more of a macro to store data values
        it makes the code cleaner when accessing data properties
    """

    def __init__(self, record, region):
        self.s3 = S3(region)
        self.event = record
        self._bucket_name = record['s3']['bucket']['name']
        self._object_key = record['s3']['object']['key']
        logger.debug(f'Pulling data from the {self._object_key} in the {self._bucket_name} bucket.')
        self._data = json.loads(self.s3.get_file(self._bucket_name, self._object_key))
        self.metadata = self._data['metadata']
        self.content = self._data['content']
        proto_uuid = self._object_key.replace(".json", "")
        self.uuid = proto_uuid[-36:]

    def fetch_body(self):
        self.put(BODY, self.content)

    def extract_attributes(self):
        """ This migrates the metadata values into a local convenience class """

        # getting an individual attribute value

        self.put_attribute("url", 'URL', STRING_VALUE)
        self.put_attribute("api", 'API', STRING_VALUE)
        self.put_attribute("parameters", 'Parameters', STRING_VALUE)
        self.put_attribute("start_time", 'StartTime', STRING_VALUE)
        self.put_attribute("script_pid", 'PID', STRING_VALUE)
        self.put_attribute("script_name", 'ScriptName', STRING_VALUE)
        self.put_attribute("response_time", 'ResponseTime', STRING_VALUE)
        self.put_attribute("response_code", 'ResponseCode', STRING_VALUE)
        return self

    def put(self, name, value):
        self.__dict__[name] = value

    def put_attribute(self, name, in_name, attrs, value=STRING_VALUE):
        try:
            attrs = self.metadata
            self.put(name, attrs.get(in_name).get(value))
        except (ValueError, AttributeError):
            self.put(name, "")
