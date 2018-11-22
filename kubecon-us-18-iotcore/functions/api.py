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
        device['states'] = context.iotcore_client. \
            projects(). \
            locations(). \
            registries(). \
            devices(). \
            states(). \
            list(name=device['name']). \
            execute()['deviceStates']

        # iterate over state and convert binary data
        for state in device['states']:
            try:
                state['value'] = json.loads(base64.b64decode(state['binaryData']).decode('ascii'))
                del state['binaryData']
            except:
                pass

        # get the state
        device['config'] = context.iotcore_client. \
            projects(). \
            locations(). \
            registries(). \
            devices(). \
            configVersions(). \
            list(name=device['name']). \
            execute()['deviceConfigs'][0]

        try:
            device['config']['value'] = json.loads(base64.b64decode(device['config']['binaryData']).decode('ascii'))
            del device['config']['binaryData']
        except:
            pass

    return devices


def _post_configurations(context, configurations):
    context.logger.info_with('Updating device configurations',
                             configurations=configurations)

    # get devices
    devices = _get_devices(context)

    # get per device configurations
    device_configurations = _generate_device_configurations(context, devices, configurations)

    context.logger.debug_with('Generated device configurations',
                              device_configurations=device_configurations)

    # iterate over devices
    for device_name, device_configuration in device_configurations.items():
        encoded_device_configuration = json.dumps(device_configuration)

        config_body = {
            'versionToUpdate': "0",
            'binaryData': base64.urlsafe_b64encode(encoded_device_configuration.encode('utf-8')).decode('ascii')
        }

        context.logger.debug_with('Updating device configuration', device_name=device_name)

        context.iotcore_client. \
            projects(). \
            locations(). \
            registries(). \
            devices(). \
            modifyCloudToDeviceConfig(name=device_name, body=config_body) \
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


def _generate_device_configurations(context, devices, configuration):
    device_configurations = {}

    # iterate over devices
    for device in devices:
        device_configuration = {}

        # get the labels
        device_labels = device['metadata']

        # iterate over services in the configuration
        for service_name, service_configurations in configuration['services'].items():

            for service_configuration in service_configurations:

                # check if all selectors match the labels
                if _check_selectors_match(context, service_configuration.get('selectors'), device_labels):

                    # add the current configuration to the service
                    device_configuration[service_name] = {
                        'source': service_configuration['source']
                    }

                    # we're done for this service
                    break

        device_configurations[device['name']] = device_configuration

    return device_configurations


def _check_selectors_match(context, selectors, labels):

    # if no selectors - auto match
    if selectors is None:
        return True

    # iterate over selectors
    for selector in selectors:
        selector_key, selector_value = selector.split('=')

        if selector_key not in labels:
            return False

        if not labels[selector_key].startswith(selector_value):
            return False

    return True
