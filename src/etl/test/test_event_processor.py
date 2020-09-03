import json
from unittest import TestCase, mock

import src.etl.event_processor as sqs
from src.etl.event_processor import TriggerEvent, CapturedData
from src.etl.s3 import S3


@mock.patch('src.etl.s3.boto3.client')
class TestCaptureData(TestCase):

    def setUp(self):
        self.record = {
            's3': {
                'bucket': {'name': 'some-s3-name'},
                'object': {'key': 'body_getTSData_24640_mod_444abb55-afe0-40f7-9791-c824ac396a75.json'}
            }
        }
        self.region = 'us-south-1'

    def test_this_test_class_works(self, _):
        self.assertTrue(True, "works")

    @mock.patch.object(S3, 'get_file')
    def test_captured_body(self, m_method, _):
        fake_data = {'content': 'this is the body', 'metadata': {}}
        m_method.return_value = json.dumps(fake_data)
        message = CapturedData(self.record, self.region).extract_attributes()
        self.assertEqual(fake_data['content'], message.content)

    @mock.patch.object(S3, 'get_file')
    def test_attribute_url(self, m_method, _):
        the_value = "the url"
        fake_data = {'content': 'this is the body', 'metadata': {'URL': {sqs.STRING_VALUE: the_value}}}
        m_method.return_value = json.dumps(fake_data)
        message = CapturedData(self.record, self.region).extract_attributes()
        self.assertEqual(the_value, message.url)

    @mock.patch.object(S3, 'get_file')
    def test_attribute_uuid(self, m_method, _):
        the_value = "444abb55-afe0-40f7-9791-c824ac396a75"
        fake_data = {'content': 'this is the body', 'metadata': {'URL': {sqs.STRING_VALUE: the_value}}}
        m_method.return_value = json.dumps(fake_data)
        message = CapturedData(self.record, self.region).extract_attributes()
        self.assertEqual(the_value, message.uuid)


@mock.patch('src.etl.s3.boto3.client')
class TestTriggerEvent(TestCase):

    def setUp(self):
        self.region = 'us-south-10'
        self.event_1 = {
            'Record':
                {
                    "eventVersion": "2.0",
                    "eventSource": "aws:s3",
                    "awsRegion": "us-west-2",
                    "eventTime": "1970-01-01T00:00:00.000Z",
                    "eventName": "ObjectCreated:Put",
                    "userIdentity": {
                        "principalId": "EXAMPLE"
                    },
                    "requestParameters": {
                        "sourceIPAddress": "127.0.0.1"
                    },
                    "responseElements": {
                        "x-amz-request-id": "EXAMPLE123456789",
                        "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH"
                    },
                    "s3": {
                        "s3SchemaVersion": "1.0",
                        "configurationId": "testConfigRule",
                        "bucket": {
                            "name": "example-bucket",
                            "ownerIdentity": {
                                "principalId": "EXAMPLE"
                            },
                            "arn": "arn:aws:s3:::example-bucket"
                        },
                        "object": {
                            "key": "body_getTSData_24640_mod_444abb55-afe0-40f7-9791-c824ac396a75.json",
                            "size": 1024,
                            "eTag": "0123456789abcdef0123456789abcdef",
                            "sequencer": "0A1B2C3D4E5F678901"
                        }
                    }
                }

        }

    @mock.patch.object(S3, 'get_file')
    def test_single_record(self, m_method, _):
        fake_data = {
            'content': 'some content',
            'metadata': {
                'URL': {sqs.STRING_VALUE: 'the URL'},
            }
        }
        m_method.return_value = json.dumps(fake_data)

        te = TriggerEvent(self.region)
        te.extract(self.event_1)
        datum = te.data
        self.assertEqual('some content', datum.content)
        self.assertEqual("the URL", datum.url)
