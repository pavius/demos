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
            'url': os.environ.get('LOCAL_REGISTRY_URL'),
            'creds': {
                'username': os.environ.get('LOCAL_REGISTRY_USERNAME'),
                'password': os.environ.get('LOCAL_REGISTRY_PASSWORD')
            }
        }
    }))
    
    # get service namespace, name and image
    service_namespace = event.body.get('namespace') or context.namespace
    service_name = event.body['name']
    service_image = f'{os.environ.get("LOCAL_REGISTRY_URL")}/{event.body["source"]["image"]}'

    # update the deployment to use the version
    context.logger.debug_with('Updating service image', 
        service_namespace=service_namespace,
        service_name=service_name,
        service_image=service_image)

    # update the deployment
    kubernetes.client.AppsV1Api().patch_namespaced_deployment(event.body['name'], service_namespace, {
        'spec': {
            'template': {
                'spec': {
                    'containers': [
                        {
                            'name': event.body['name'],
                            'image': service_image
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
