import datetime
import ssl
import threading
import queue
import functools
import os

import jwt
import paho.mqtt.client
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
#       - apt-get update && apt-get install -y curl
#       - pip install cryptography==2.3.1 pyjwt==1.6.4 paho-mqtt==1.3.1
#       - curl -L https://pki.google.com/roots.pem -o /tmp/ca_cert_path.pem

def handler(context, event):
    context.logger.debug_with('Called', path=event.path)
    if event.path == '/init':
        _init(context)
    elif event.path == '/publish':
        _publish(context, 
                 event.body['topic'], 
                 event.body.get('qos', 0), 
                 event.body['payload'])


def init_context(context):
    setattr(context, 'config', {
        'index': os.environ['IOTCORE_MQTT_DISPATCHER_INDEX'],
        'project_id': os.environ['IOTCORE_MQTT_DISPATCHER_PROJECT_ID'],
        'region_name': os.environ['IOTCORE_MQTT_DISPATCHER_REGION_NAME'],
        'registry_id': os.environ['IOTCORE_MQTT_DISPATCHER_REGISTRY_ID'],
        'device_id': os.environ['IOTCORE_MQTT_DISPATCHER_DEVICE_ID'],
        'algorithm': os.environ.get('IOTCORE_MQTT_DISPATCHER_ALGORITHM', 'RS256'),
        'private_key': os.environ['IOTCORE_MQTT_DISPATCHER_PRIVATE_KEY'],
        'ca_cert_path': os.environ.get('IOTCORE_MQTT_DISPATCHER_CA_CERT_PATH', '/tmp/ca_cert_path.pem'),
    })

    setattr(context, 'mqtt_client', None)


def _init(context):
    context.logger.debug_with('Initializing')

    # create client
    context.mqtt_client = Client(context.logger,
                                 context.config['project_id'],
                                 context.config['region_name'],
                                 context.config['registry_id'],
                                 context.config['device_id'],
                                 context.config['private_key'],
                                 context.config['algorithm'],
                                 context.config['ca_cert_path'])

    # start the client
    context.mqtt_client.start()

    # subscribe to configuration events
    context.mqtt_client.subscribe('/devices/{}/config'.format(context.config['device_id']), 1, functools.partial(_on_config_message, context))


def _publish(context, topic, qos, payload):
    context.logger.debug_with('Got publish request', topic=topic, payload=str(payload))

    # publish to client
    context.mqtt_client.publish(topic, qos, payload)


def _on_config_message(context, topic, payload):
    context.logger.debug_with('Got config message', payload=str(payload))

    # call config reader
    context.platform.call_function('config-reader-' + context.config['index'], nuclio_sdk.Event(body=payload))


class Client(object):

    def __init__(self,
                 logger,
                 project_id,
                 region_name,
                 registry_id,
                 device_id,
                 private_key_contents,
                 algorithm,
                 ca_cert_path,
                 jwt_expiration_seconds=3600,
                 num_workers=4):
        self._logger = logger
        self._project_id = project_id
        self._region_name = region_name
        self._registry_id = registry_id
        self._device_id = device_id
        self._private_key_contents = private_key_contents
        self._algorithm = algorithm
        self._ca_cert_path = ca_cert_path
        self._client = None
        self._stopped = True
        self._subscriptions = {}
        self._control_request_queue = queue.Queue()
        self._jwt_expiration_seconds = jwt_expiration_seconds

        # create a set of workers
        self._worker_pool = self._create_worker_pool(num_workers)

    def subscribe(self, topic, qos, callback):

        # shove to queue
        self._control_request_queue.put({
            'kind': '_subscribe',
            'args': (topic, qos, callback)
        })

    def publish(self, topic, qos, payload):

        # shove to queue
        self._control_request_queue.put({
            'kind': '_publish',
            'args': (topic, qos, payload)
        })

    def start(self):
        self._stopped = False

        # create an MQTT client
        self._create_client()

        # create a thread that
        threading.Thread(target=self._run_client_loop).start()

    def stop(self):
        self._stopped = True

    def _run_client_loop(self):

        while not self._stopped:
            error = self._client.loop(timeout=1.0)

            if error != paho.mqtt.client.MQTT_ERR_SUCCESS:
                self._logger.warn_with('Got error, trying to reconnect', error=error)

                time.sleep(1)

                # try to recreate the client
                self._create_client()

            self._logger.debug_with('Released', client_id=id(self._client))

            # try to get tasks from the control queue
            self._handle_control_requests()

            # check if jwt expired
            self._check_jwt_expiration()

    def _create_client(self):
        client_id = 'projects/{}/locations/{}/registries/{}/devices/{}'.format(self._project_id,
                                                                               self._region_name,
                                                                               self._registry_id,
                                                                               self._device_id)

        # create client object (doesn't connect)
        client = paho.mqtt.client.Client(client_id=client_id)

        # username is ignored, password is a JWT
        client.username_pw_set(username='unused', password=self._create_jwt())

        # use TLS
        client.tls_set(ca_certs=self._ca_cert_path, tls_version=ssl.PROTOCOL_TLSv1_2)

        client.on_connect = self._on_connect
        client.on_publish = self._on_publish
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message

        # do the connect
        client.connect('mqtt.googleapis.com', 8883)

        # set in member
        self._client = client

        # create subscriptions
        for topic, (handler, qos) in self._subscriptions.items():
            self._subscribe(topic, qos, handler)

        return client

    def _create_jwt(self):
        token = {
            # The time that the token was issued at
            'iat': datetime.datetime.utcnow(),

            # The time the token expires.
            'exp': datetime.datetime.utcnow() + datetime.timedelta(seconds=self._jwt_expiration_seconds),

            # The audience field should always be set to the GCP project id.
            'aud': self._project_id
        }

        self._last_jwt_creation_time = datetime.datetime.utcnow()

        return jwt.encode(token, self._private_key_contents, algorithm=self._algorithm)

    def _handle_control_requests(self):
        while True:
            if self._control_request_queue.empty():
                break

            # get an item from the queue
            item = self._control_request_queue.get(block=False)

            self._logger.debug_with('Dispatching control request', kind=item['kind'])

            # call the function
            getattr(self, item['kind'])(*item['args'])

    def _on_connect(self, client, userdata, flags, rc):
        self._logger.debug_with('Got connection event', result=paho.mqtt.client.connack_string(rc))

    def _on_disconnect(self, client, userdata, rc):
        self._logger.debug_with('Got disconnection event', result=paho.mqtt.client.connack_string(rc))

    def _on_publish(self, client, userdata, mid):
        self._logger.debug_with('Got publish event')

    def _on_message(self, client, userdata, message):
        (handler, _) = self._subscriptions.get(message.topic)

        self._logger.debug_with('Got message',
                                topic=message.topic,
                                payload=str(message.payload))

        if handler is not None:

            # post to worker pool
            self._worker_pool.put((handler, (message.topic, message.payload)))

    def _subscribe(self, topic, qos, callback):
        self._logger.debug_with('Subscribing', topic=topic, qos=qos)

        # save topic / callback
        self._subscriptions[topic] = (callback, qos)

        # subscribe at mqtt
        self._client.subscribe(topic, qos)

    def _publish(self, topic, qos, payload):
        self._logger.debug_with('Publishing', topic=topic, qos=qos, payload=payload)

        # publish to mqtt
        self._client.publish('/devices/{}/'.format(self._device_id) + topic, payload, qos=qos)

    def _check_jwt_expiration(self):

        # refresh the jwt at 80% of lifetime
        jwt_refresh_interval_seconds = 0.8 * self._jwt_expiration_seconds

        if (datetime.datetime.utcnow() - self._last_jwt_creation_time).seconds > jwt_refresh_interval_seconds:
            self._logger.debug_with('JWT expired, recreating client', refresh_interval=jwt_refresh_interval_seconds)

            # simply recreate the client
            self._create_client()

    def _create_worker_pool(self, num_workers):
        self._logger.debug_with('Creating workers', num_workers=num_workers)

        worker_queue = queue.Queue()

        # create a set of threads that listen on this queue
        for worker_idx in range(num_workers):
            threading.Thread(target=self._execute_work, args=(worker_idx, worker_queue, )).start()

        return worker_queue

    def _execute_work(self, worker_idx, worker_queue):
        self._logger.debug_with('Worker created', worker_idx=worker_idx)

        # read queue
        while True:
            try:
                (handler, args) = worker_queue.get()
                self._logger.debug_with('Got work', worker_idx=worker_idx)

                # call the handler with the args
                handler(*args)

            except Exception as e:
                self._logger.warn_with('Got exception while handling work', worker_idx=worker_idx, e=str(e))
