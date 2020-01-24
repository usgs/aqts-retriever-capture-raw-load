import os


def env(param, default_value=""):
    """ convenience method for fetching environment values """
    if default_value == "":
        default_value = f'{param} is not set'
    return os.environ.get(param, default_value)


""" AWS configuration from environment """
CONFIG = {
    'aws': {
        # everything is in one region right now
        'region': env('AWS_DEPLOYMENT_REGION', 'us-west-2'),
        'endpoint-base': env('AWS-BASE-ENDPOINT', '.c8adwxz9sely.us-west-2.rds.amazonaws.com'),
    },
    'rds': {
        'host': env('DB_HOST'),
        'port': env('DB_PORT', '5432'),
        'database': env('DB_NAME', 'retriever_capture'),
        # default to postgres but could be a schema owner instead
        'user': env('DB_USER', 'postgres'),
        'password': env('DB_PASSWORD')
    }
}
