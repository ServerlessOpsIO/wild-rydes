'''Request a ride'''

from datetime import datetime
import logging
import json
import os
import random
import uuid

import boto3

logging.root.setLevel(logging.INFO)
_logger = logging.getLogger(__name__)

DYNAMODB_TABLE = os.environ.get('DYNAMODB_TABLE')
UNICORN_HASH_KEY = os.environ.get('UNICORN_HASH_KEY')
dynamodb = boto3.resource('dynamodb')
DDT = dynamodb.Table(DYNAMODB_TABLE)


def _generate_ride_id():
    '''Generate a ride ID.'''
    return uuid.uuid1()


def _get_ride(pickup_location):
    '''Get a ride.'''
    ride_id = _generate_ride_id()
    unicorn = _get_unicorn()

    # NOTE: upstream they replace Rider with User but that seems silly.
    resp = {
        'RideId': str(ride_id),
        'Unicorn': unicorn,
        'RequestTime': str(_get_timestamp_from_uuid(ride_id)),
    }
    return resp


def _get_timestamp_from_uuid(u):
    '''Return a timestamp from the given UUID'''
    return datetime.fromtimestamp((u.time - 0x01b21dd213814000) * 100 / 1e9)


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


def _get_pickup_location(body):
    '''Return pickup location from event'''
    return body.get('PickupLocation')


def handler(event, context):
    '''Function entry'''
    _logger.info('Request: {}'.format(json.dumps(event)))

    body = json.loads(event.get('body'))
    pickup_location = _get_pickup_location(body)
    ride_resp = _get_ride(pickup_location)

    resp = {
        'statusCode': 201,
        'body': json.dumps(ride_resp),
        'headers': {
            "Access-Control-Allow-Origin": "*",
        }
    }

    _logger.info('Response: {}'.format(json.dumps(resp)))
    return resp

