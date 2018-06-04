'''Update backend config'''

import json
import logging
import os

import boto3
import cfn_resource

log_level = os.environ.get('LOG_LEVEL', 'INFO')
logging.root.setLevel(logging.getLevelName(log_level))  # type: ignore
_logger = logging.getLogger(__name__)

S3 = boto3.resource('s3')

CONFIG = {
    'cognito': {
        'userPoolId': '',
        'userPoolClientId': '',
        'region': '',
        'disabled': True
    },
    'api': {
        'invokeUrl': ''
    }
}

handler = cfn_resource.Resource()


def _create_config_from_properties(properties, config):
    '''Create a config string'''
    config['api']['invokeUrl'] = properties['RequestRideUrl']
    config_body = 'window._config = {}'.format(json.dumps(config, indent=2))

    return config_body


def _delete_backend(s3_bucket, s3_object_name):
    '''delete config objest'''
    # Needed to cleanup bucket so stack tear down works.
    config_object = S3.Object(s3_bucket, s3_object_name)
    config_object.delete()


def _get_properties_from_event(event):
    '''Return ResourceProperties from event.'''
    return event.get('ResourceProperties')


def _get_s3_info_from_properties(properties):
    '''Get the S3 info from the event properties.'''
    s3_bucket = properties['S3Bucket']
    s3_object_name = properties['S3Object']

    return (s3_bucket, s3_object_name)


def _write_configuration(s3_bucket, s3_object_name, s3_body):
    '''Add the invoke URL to frontend config.'''

    config_object = S3.Object(s3_bucket, s3_object_name)
    config_object.put(Body=s3_body)


@handler.create
def create(event, context):
    '''Create'''
    _logger.info('Create event: {}'.format(json.dumps(event)))

    properties = _get_properties_from_event(event)
    s3_bucket, s3_object_name = _get_s3_info_from_properties(properties)
    config_body = _create_config_from_properties(properties, CONFIG)

    _write_configuration(s3_bucket, s3_object_name, config_body)

    resp = {
        "ResourceProperties": properties,
        "PhysicalResourceId": context.log_stream_name or event.get("RequestId")
    }

    return resp


@handler.update
def update(event, context):
    '''Update'''
    _logger.info('Update event: {}'.format(json.dumps(event)))

    properties = _get_properties_from_event(event)
    s3_bucket, s3_object_name = _get_s3_info_from_properties(properties)
    config_body = _create_config_from_properties(properties, CONFIG)

    _write_configuration(s3_bucket, s3_object_name, config_body)

    resp = {
        "ResourceProperties": properties,
        "PhysicalResourceId": context.log_stream_name or event.get("RequestId")
    }

    return resp


@handler.delete
def delete(event, context):
    '''delete contents of bucket'''
    _logger.info('Update event: {}'.format(json.dumps(event)))

    properties = _get_properties_from_event(event)
    s3_bucket, s3_object_name = _get_s3_info_from_properties(properties)

    _delete_backend(s3_bucket, s3_object_name)

    resp = {
        "ResourceProperties": properties,
        "PhysicalResourceId": context.log_stream_name or event.get("RequestId")
    }

    return resp


