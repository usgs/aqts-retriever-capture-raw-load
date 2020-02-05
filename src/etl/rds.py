"""
This module manages persisting data from a message into the RDS Resource.
"""

# the postgresql connection module
from psycopg2 import connect
from psycopg2 import OperationalError, DataError, IntegrityError
# json allows us to convert between dictionaries and json.
import json
import datetime

# project specific configuration parameters.
from .config import CONFIG

# allows for logging information
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def convert_total_seconds_to_datetime(total_seconds):
    dt = datetime.datetime.utcfromtimestamp(float(total_seconds))
    return dt


class RDS:

    def __init__(self):
        """
        connect to the database resource
        """
        self.connection_parameters = {
            'host': CONFIG['rds']['host'],
            'database': CONFIG['rds']['database'],
            'user': CONFIG['rds']['user'],
            'password': CONFIG['rds']['password']
        }
        logger.debug("created RDS instance %s" % self.connection_parameters)
        self.conn = None
        self.cursor = None

    def connect(self):
        logger.debug('Connecting to database.')
        conn_params = self.connection_parameters
        self.conn = connect(**conn_params)
        logger.debug(f'Connection object: {repr(self.conn)}.')
        # Interestingly, autocommit seemed necessary for create table too.
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()

    def disconnect(self):
        self.conn.close()

    def _execute_sql(self, sql, params=()):
        """
        shared method to connect and run an SQL statement.
        note that this is not efficient for most uses but this script has limited statements
        """
        logger.debug(f'SQL: {self.cursor.mogrify(sql, params)}.')
        try:
            if len(params):
                self.cursor.execute(sql, params)
            else:
                self.cursor.execute(sql)
        except (OperationalError, DataError, IntegrityError) as e:
            logger.debug(f'Error during SQL execution: {repr(e)}', exc_info=True)
            logger.debug('Transaction will be rolled back.')
            self.conn.rollback()
        else:
            id_of_new_record = self.cursor.fetchone()[0]
            logger.debug(f'New record ID: {id_of_new_record}')
            return id_of_new_record

    def persist_data(self, datum):
        """
        validates each value and 
        persists the message to RDS.
        """
        # ensure time is well formatted
        self.validate_time("Start Time", datum.start_time)
        self.validate_time("Response Time", datum.response_time)

        # WIP: only accept 200s ?
        self.validate_code(datum.response_code)
        self.validate_pid(datum.script_pid)

        # ensure that url is at least a url pattern
        self.validate_url(datum.url)
        # if api is missing then pull it from the url
        api = self.validate_api(datum.api, datum.url)

        # ensure valid json using json module testing
        self.validate_json("Parameters", datum.parameters)
        self.validate_json("JSON Data", datum.content)

        insert_json_data = """
            INSERT INTO capture.json_data 
            (start_time, response_time, response_code, url, api,
             script_name, script_pid,
             parameters, json_content)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING json_data_id;"""
        values = (
            convert_total_seconds_to_datetime(datum.start_time),
            convert_total_seconds_to_datetime(datum.response_time),
            int(datum.response_code),
            datum.url,
            api,
            datum.script_name,
            int(datum.script_pid),
            datum.parameters,
            datum.content
        )
        logger.debug(f'Values: {values}')
        logger.debug('Inserting data in the database.')
        db_resp = self._execute_sql(
            insert_json_data, (
                convert_total_seconds_to_datetime(datum.start_time),
                convert_total_seconds_to_datetime(datum.response_time), int(datum.response_code),
                datum.url, api, datum.script_name, int(datum.script_pid),
                datum.parameters, datum.content
            )
        )
        return db_resp

    @classmethod
    def validate_contains(cls, variable_name, actual):
        """
        ensures a variable contains text
        """
        if not (actual and actual.strip()):
            raise ValidationException("Variable value required", variable_name,
                                      "Not Blank", actual)

    def validate_time(self, variable_name, actual):
        """
        ensures a variable contains timestamp
        WIP
        """
        self.validate_contains(variable_name, actual)
        # WIP: validate timestamp pattern

    def validate_int(self, variable_name, actual, expected_small, expected_large):
        """
        ensures a variable contains an integer
        """
        self.validate_contains(variable_name, actual)

        try:
            if expected_small <= int(actual) < expected_large:
                return  # valid int just return now
        except ValueError:
            pass  # this means we have a non-integer

        raise ValidationException("Integer value required", variable_name,
                                  "must be a number from %d to %d"
                                  % (expected_small, expected_large),
                                  actual)

    def validate_code(self, actual):
        """
        ensures a variable contains HTTP response code
        """
        self.validate_int("Response Code", actual, 100, 599)

    def validate_pid(self, actual):
        """
        ensures a variable contains unix PID
        """
        self.validate_int("PIDs", actual, 2, 50000)

    def validate_url(self, actual):
        """
        ensures a variable contains a URL
        WIP
        """
        self.validate_contains("URL", actual)
        # WIP: URL pattern check

    def validate_api(self, actual, url):
        """
        ensures a variable contains an Aquarius API and that it is in the URL
        If blank it will return the API call found in the URL
        WIP
        """
        try:
            self.validate_contains("API Call", actual)
        except ValidationException:
            # WIP: extract API from URL if not provided.
            return "API"

        if actual not in url:
            raise ValidationException("API-URL mismatch", "API", "must match that in the URL", actual)

        return actual

    def validate_json(self, variable_name, actual):
        """
        ensures a variable contains JSON
        """
        self.validate_contains(variable_name, actual)
        try:
            # attempt to convert JSON string to dictionary
            json.loads(actual)
        except Exception:
            raise ValidationException("Must be JSON", variable_name, "expected valid JSON", actual)


class ValidationException(Exception):
    """
    Validation Exception class
    """
    
    def __init__(self, message, variable_name, expected, actual):
        """
        creates an instance
        """
        self.message = message
        self.variable_name = variable_name
        self.expected = expected
        self.actual = actual
    
    def message(self):
        """
        create an error message string from the properties
        """
        return "%s. %s should be '%s' but was '%s'" \
            % (self.message, self.variable_name, self.expected, self.actual)
