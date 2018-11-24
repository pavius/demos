import json
import os
import promalyze
import time

import nuclio_sdk

# @nuclio.configure
#
# function.yaml:
#   apiVersion: "nuclio.io/v1"
#   kind: Function
#   spec:
#     build:
#       baseImage: python:3.6
#       commands:
#       - apt-get update && apt-get install -y git
#       - pip install git+https://github.com/pavius/promalyze.git@basic-auth-and-agg requests
#     triggers:
#       periodic:
#         kind: cron
#         attributes:
#           interval: 10s
# 


def handler(context, event):

    # get metric aggregates
    metric_aggregates = _get_metric_aggregates(context)

    context.logger.info_with('Sending metric aggregates', metric_aggregates=metric_aggregates)

    context.platform.call_function('iotcore-mqtt-dispatcher-' + context.config['index'], nuclio_sdk.Event(path='/publish', body={
        'topic': 'events',
        'payload': json.dumps(metric_aggregates)
    }), timeout=10)


def init_context(context):
    setattr(context, 'client', promalyze.Client('http://default-tenant-v3io-prometheus-server'))

    # set configuration
    setattr(context, 'config', {
        'index': os.environ['TELEMETRY_UPDATER_INDEX'],
    })


def _get_metric_aggregates(context):
    average_cpu = {}

    # get average over 10 seconds
    timeseries_set = context.client.range_query('avg_over_time(cpu_utilization[20s])', start=int(time.time()) - 20, step=10)
    for timeseries in timeseries_set.timeseries:
        average_cpu[timeseries.metadata['device_id']] = {
            'timestamp': timeseries.timestamps()[-1],
            'value': timeseries.values()[-1],
        }

    return average_cpu
