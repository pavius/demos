import json
import os
import sys
import base64

import nuclio_sdk
from google.oauth2 import service_account
from googleapiclient import discovery


def handler(context, event):
    if event.path == '/devices' and event.method == 'GET':
        return _get_devices(context)
    if event.path == '/configurations' and event.method == 'POST':
        return _post_configurations(context, event.body)


def init_context(context):
    setattr(context, 'config', {
        'project_id': os.environ['DEMO_API_PROJECT_ID'],
        'region_name': os.environ['DEMO_API_REGION_NAME'],
        'registry_id': os.environ['DEMO_API_REGISTRY_ID'],
        'service_account': json.loads(os.environ['DEMO_API_SERVICE_ACCOUNT']),
    })

    setattr(context, 'iotcore_client', _create_iotcore_client(context.config['service_account']))


def _get_devices(context):
    devices = context.iotcore_client. \
        projects(). \
        locations(). \
        registries(). \
        devices(). \
        list(parent=_get_registry_name(context), fieldMask="name,metadata"). \
        execute(). \
        get('devices', [])

    for device in devices:

        # get the state
        device['state'] = context.iotcore_client. \
            projects(). \
            locations(). \
            registries(). \
            devices(). \
            states(). \
            list(name=device['name']). \
            execute()

        # get the state
        device['config'] = context.iotcore_client. \
            projects(). \
            locations(). \
            registries(). \
            devices(). \
            configVersions(). \
            list(name=device['name']). \
            execute()['deviceConfigs'][0]

    return devices


def _post_configurations(context, request):
    context.logger.info_with('Updating configuration',
                             location_prefix=request['location_prefix'],
                             configuration=request['configuration'])

    encoded_configuration = json.dumps(request['configuration'])

    # iterate over devices
    for device in _get_devices(context):

        # skip if device isn't in location prefix
        if not device['metadata']['location'].startswith(request['location_prefix']):
            continue

        config_body = {
            'versionToUpdate': "0",
            'binaryData': base64.urlsafe_b64encode(encoded_configuration.encode('utf-8')).decode('ascii')
        }

        context.logger.debug_with('Updating device configuration', device_name=device['name'])

        context.iotcore_client. \
            projects(). \
            locations(). \
            registries(). \
            devices(). \
            modifyCloudToDeviceConfig(name=device['name'], body=config_body) \
            .execute()


def _get_registry_name(context):
    return 'projects/{}/locations/{}/registries/{}'.format(context.config['project_id'],
                                                           context.config['region_name'],
                                                           context.config['registry_id'])


def _create_iotcore_client(service_account_info):
    credentials = service_account.Credentials.from_service_account_info(service_account_info)

    scoped_credentials = credentials.with_scopes(['https://www.googleapis.com/auth/cloud-platform'])
    discovery_url = '{}?version={}'.format('https://cloudiot.googleapis.com/$discovery/rest', 'v1')

    return discovery.build(
        'cloudiotcore',
        'v1',
        discoveryServiceUrl=discovery_url,
        credentials=scoped_credentials)
