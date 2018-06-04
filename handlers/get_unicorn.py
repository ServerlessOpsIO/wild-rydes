'''Request a ride'''

import logging
import json
import os
import random

import boto3

log_level = os.environ.get('LOG_LEVEL', 'INFO')
logging.root.setLevel(logging.getLevelName(log_level))  # type:ignore
_logger = logging.getLogger(__name__)

# DynamoDB
DYNAMODB_TABLE = os.environ.get('DYNAMODB_TABLE')
UNICORN_HASH_KEY = os.environ.get('UNICORN_HASH_KEY')
dynamodb = boto3.resource('dynamodb')
DDT = dynamodb.Table(DYNAMODB_TABLE)


def _get_unicorn():
    '''Return a unicorn from the fleet'''
    # Get a few of them and return one at random. Need to eventually randomize
    # where in the table we start our lookup.
    results = DDT.scan(
        Limit=5,
    )
    unicorns = results.get('Items')
    unicorn = unicorns[random.randint(0, len(unicorns) - 1)]

    return unicorn


def handler(event, context):
    '''Function entry'''
    _logger.debug('Request: {}'.format(json.dumps(event)))

    resp = _get_unicorn()

    resp = {
        'statusCode': 201,
        'body': json.dumps(resp),
    }

    _logger.debug('Response: {}'.format(json.dumps(resp)))
    return resp

