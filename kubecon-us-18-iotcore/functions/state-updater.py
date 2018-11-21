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
#     triggers:
#       periodic:
#         kind: cron
#         attributes:
#           interval: 3s
# 


def handler(context, event):

    # get the current version
    deployment_states = _get_deployment_states(context, context.config['label_selector'])

    # have we reported it before?
    if deployment_states == context.last_deployment_states:
        return

    context.logger.info_with('Updating', deployment_states=deployment_states)

    context.platform.call_function('iotcore-mqtt-dispatcher-' + context.config['index'], nuclio_sdk.Event(path='/publish', body={
        'topic': 'state',
        'payload': json.dumps(deployment_states)
    }), timeout=10)

    context.last_deployment_states = deployment_states


def init_context(context):

    # initialize in-cluster config
    kubernetes.config.load_incluster_config()

    # read current namespace
    current_namespace = open('/var/run/secrets/kubernetes.io/serviceaccount/namespace').read()

    # get current namespace
    setattr(context, 'namespace', current_namespace)
    setattr(context, 'last_deployment_states', '')

    # set configuration
    setattr(context, 'config', {
        'index': os.environ['STATE_UPDATER_INDEX'],
        'label_selector': os.environ['STATE_UPDATER_LABEL_SELECTOR']
    })


def _get_deployment_states(context, label_selector):
    deployments = kubernetes.client.AppsV1Api().list_namespaced_deployment(context.namespace, label_selector=label_selector)

    deployment_states = {}

    for deployment in deployments.items:
        deployment_states[deployment.metadata.name] = {
            'image': deployment.spec.template.spec.containers[0].image,
            'replicas': deployment.status.replicas,
            'readyReplicas': deployment.status.ready_replicas
        }

    context.logger.debug_with('Got states', states=deployment_states)

    return deployment_states
