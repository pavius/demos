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
#     env:
#     - name: DEPLOYMENT_NAME
#       value: tdemo
#     triggers:
#       periodic:
#         kind: cron
#         attributes:
#           interval: 3s
# 

def handler(context, event):
    deployment_name = context.config['deployment_name']

    # get the current version
    current_version = _get_deployment_version(context, deployment_name)

    # have we reported it before?
    if current_version == context.last_updated_version:
        return

    context.logger.info_with('Updating', current_version=current_version)

    context.platform.call_function('iotcore-mqtt-dispatcher', nuclio_sdk.Event(path='/publish', body={
        'topic': 'state',
        'payload': json.dumps({
            'versions': {
                deployment_name: current_version
            }
        })
    }))

    context.last_updated_version = current_version

def init_context(context):

    # initialize in-cluster config
    kubernetes.config.load_incluster_config()

    # read current namespace
    current_namespace = open('/var/run/secrets/kubernetes.io/serviceaccount/namespace').read()

    # get current namespace
    setattr(context, 'namespace', current_namespace)
    setattr(context, 'last_updated_version', '')

    # set configuration
    setattr(context, 'config', {
        'deployment_name': os.environ['DEPLOYMENT_NAME']
    })


def _get_deployment_version(context, name):
    deployment = kubernetes.client.AppsV1Api().read_namespaced_deployment(name, context.namespace)

    return deployment.spec.template.spec.containers[0].image.split(':')[1]
