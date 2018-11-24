import json
import os
import kubernetes

import nuclio_sdk

# @nuclio.configure
#
# function.yaml:
#   apiVersion: "nuclio.io/v1"
#   kind: Function
#   spec:
#     build:
#       commands:
#       - apk add --no-cache build-base openssl-dev libffi-dev
#       - pip install kubernetes nuclio_sdk


def handler(context, event):
    context.logger.debug_with('Got event', body=event.body.decode('utf-8'))
    event.body = json.loads(event.body.decode('utf-8'))

    # iterate over services
    for service_name, service_config in event.body.items():

        # update service configuration
        _update_service_config(context,
                               event.body.get('namespace') or context.namespace,
                               service_name,
                               service_config)


def init_context(context):

    # initialize in-cluster config
    kubernetes.config.load_incluster_config()

    # read current namespace
    current_namespace = open('/var/run/secrets/kubernetes.io/serviceaccount/namespace').read()

    # get current namespace
    setattr(context, 'namespace', current_namespace)
    setattr(context, 'config', {
        'index': os.environ['CONFIG_READER_INDEX'],
        'local_registry_url': os.environ['CONFIG_READER_LOCAL_REGISTRY_URL'],
        'local_registry_username': os.environ['CONFIG_READER_LOCAL_REGISTRY_USERNAME'],
        'local_registry_password': os.environ['CONFIG_READER_LOCAL_REGISTRY_PASSWORD']
    })


def _update_service_config(context, namespace, service_name, service_config):
    source_url, source_image = service_config['source'].split('/')

    context.logger.info_with('Syncing service image to local repository',
                             name=service_name,
                             source=service_config['source'],
                             source_url=source_url,
                             source_image=source_image,
                             service_config=service_config)

    # sync the docker image
    context.platform.call_function('sync-docker-image', nuclio_sdk.Event(body={
        'source': {
            'url': source_url,
            'image': source_image
        },
        'dest': {
            'url': context.config['local_registry_url'],
            'creds': {
                'username': context.config['local_registry_username'],
                'password': context.config['local_registry_password']
            }
        }
    }))

    local_registry_url = context.config['local_registry_url']

    # get service namespace, name and image
    deployment_namespace = namespace
    deployment_name = service_name + '-' + context.config['index']
    deployment_image = f'{local_registry_url}/{source_image}'

    # update the deployment to use the version
    context.logger.info_with('Updating deployment image',
                             deployment_namespace=deployment_namespace,
                             deployment_name=deployment_name,
                             deployment_image=deployment_image)

    # update the deployment
    kubernetes.client.AppsV1Api().patch_namespaced_deployment(deployment_name, deployment_namespace, {
        'spec': {
            'template': {
                'spec': {
                    'containers': [
                        {
                            'name': 'tdemo',
                            'image': deployment_image
                        }
                    ]
                }
            }
        }
    })
