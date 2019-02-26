'''Request a ride'''

from datetime import datetime
import logging
import json
import os
import uuid

import boto3

log_level = os.environ.get('LOG_LEVEL', 'INFO')
logging.root.setLevel(logging.getLevelName(log_level))  # type:ignore
_logger = logging.getLogger(__name__)

REQUEST_UNICORN_ARN = os.environ.get('REQUEST_UNICORN_ARN')
LAMBDA_CLIENT = boto3.client('lambda')

RIDES_SNS_TOPIC_ARN = os.environ.get('RIDES_SNS_TOPIC_ARN')
SNS_CLIENT = boto3.client('sns')


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


def _get_unicorn(arn=REQUEST_UNICORN_ARN):
    '''Return a unicorn from the fleet'''
    response = LAMBDA_CLIENT.invoke(
        FunctionName=arn
    )
    unicorn = response.get('Payload').read().decode()
    _logger.info('Unicorn: {}'.format(unicorn))
    return json.loads(unicorn)


def _get_pickup_location(body):
    '''Return pickup location from event'''
    return body.get('PickupLocation')


def _publish_ride_record(ride, sns_topic_arn=RIDES_SNS_TOPIC_ARN):
    '''Publish ride info to SNS'''
    SNS_CLIENT.publish(
        TopicArn=sns_topic_arn,
        Message=json.dumps(ride)
    )


def handler(event, context):
    '''Function entry'''
    _logger.info('Request: {}'.format(json.dumps(event)))

    body = json.loads(event.get('body'))
    pickup_location = _get_pickup_location(body)
    ride_resp = _get_ride(pickup_location)
    _publish_ride_record(ride_resp)

    resp = {
        'statusCode': 201,
        'body': json.dumps(ride_resp),
        'headers': {
            "Access-Control-Allow-Origin": "*",
        }
    }

    _logger.info('Response: {}'.format(json.dumps(resp)))
    return resp

