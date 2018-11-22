

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


def _test1():

    devices = [
        {
            'name': 'd0',
            'metadata': {
                'location': 'us/colorado/boulder',
                'gpu': '2'
            }
        },
        {
            'name': 'd1',
            'metadata': {
                'location': 'us/colorado/denver',
                'gpu': '0'
            }
        },
        {
            'name': 'd2',
            'metadata': {
                'location': 'us/arizona/phoenix',
                'gpu': '2'
            }
        }
    ]

    configuration = {
        'services': {
            'apiservice': [
                {
                    'selectors': [
                        'gpu=2'
                    ],
                    'source': 'pavius/tdemo:0.0.2'
                },
                {
                    'source': 'pavius/tdemo:0.0.1'
                }
            ],

            'detector': [
                {
                    'source': 'pavius/tdemo:0.0.1'
                }
            ]
        }
    }

    device_configurations = _generate_device_configurations(None, devices, configuration)
    print(device_configurations)


_test1()