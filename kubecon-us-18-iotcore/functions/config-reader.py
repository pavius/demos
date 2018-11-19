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

    # sync the docker image
    context.platform.call_function('sync-docker-image', nuclio_sdk.Event(body={
        'source': event.body['source'],
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
    deployment_namespace = event.body.get('namespace') or context.namespace
    deployment_name = event.body['name'] + '-' + context.config['index']
    deployment_image = f'{local_registry_url}/{event.body["source"]["image"]}'

    # update the deployment to use the version
    context.logger.debug_with('Updating deployment image', 
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
                            'name': event.body['name'],
                            'image': deployment_image
                        }
                    ]
                }
            }
        }
    })


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
