'''Request a ride'''

from datetime import datetime
import logging
import json
import os
import uuid

import boto3
from botocore.vendored import requests

from thundra.thundra_agent import Thundra
from thundra.plugins.trace.traceable import Traceable

THUNDRA_API_KEY = os.environ.get('THUNDRA_API_KEY', '')
thundra = Thundra(api_key=THUNDRA_API_KEY)


from thundra.plugins.log.thundra_log_handler import ThundraLogHandler
log_level = os.environ.get('LOG_LEVEL', 'INFO')
logging.root.setLevel(logging.getLevelName(log_level))  # type:ignore
_logger = logging.getLogger(__name__)
_logger.addHandler(ThundraLogHandler())

REQUEST_UNICORN_URL = os.environ.get('REQUEST_UNICORN_URL')

RIDES_SNS_TOPIC_ARN = os.environ.get('RIDES_SNS_TOPIC_ARN')
SNS_CLIENT = boto3.client('sns')


@Traceable(trace_args=True, trace_return_value=True)
def _generate_ride_id():
    '''Generate a ride ID.'''
    return uuid.uuid1()


@Traceable(trace_args=True, trace_return_value=True)
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


@Traceable(trace_args=True, trace_return_value=True)
def _get_timestamp_from_uuid(u):
    '''Return a timestamp from the given UUID'''
    return datetime.fromtimestamp((u.time - 0x01b21dd213814000) * 100 / 1e9)


@Traceable(trace_args=True, trace_return_value=True)
def _get_unicorn(url=REQUEST_UNICORN_URL):
    '''Return a unicorn from the fleet'''
    unicorn = requests.get(REQUEST_UNICORN_URL)
    return unicorn.json()


@Traceable(trace_args=True, trace_return_value=True)
def _get_pickup_location(body):
    '''Return pickup location from event'''
    return body.get('PickupLocation')


@Traceable(trace_args=True, trace_return_value=True)
def _publish_ride_record(ride, sns_topic_arn=RIDES_SNS_TOPIC_ARN):
    '''Publish ride info to SNS'''
    SNS_CLIENT.publish(
        TopicArn=sns_topic_arn,
        Message=json.dumps(ride)
    )


@thundra
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

