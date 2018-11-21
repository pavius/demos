import os
import paramiko
import yaml
import base64
import json
import requests
import uuid
import time
import sys
import io

import delegator
import urllib3
import nuclio_sdk
from google.oauth2 import service_account
from googleapiclient import discovery

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class DemoDeployer(object):

    def __init__(self,
                 logger,
                 datanode_external_ip,
                 appnode_external_ip,
                 project_id,
                 region_name,
                 registry_id,
                 device_id_format,
                 num_devices,
                 service_account_info,
                 source_code_base_url,
                 system_password,
                 ssh_password):

        self._logger = logger
        self._ssh_clients = {}
        self._appnode = {'external_ip': appnode_external_ip}
        self._datanode = {'external_ip': datanode_external_ip}
        self._username = None
        self._system_password = None
        self._ssh_password = None
        self._dashboard_cookies = None
        self._project_id = project_id
        self._region_name = region_name
        self._registry_id = registry_id
        self._device_id_format = device_id_format
        self._num_devices = num_devices
        self._service_account_info = service_account_info
        self._source_code_base_url = source_code_base_url
        self._system_password = system_password
        self._ssh_password = ssh_password

        self._iotcore_client = self._create_iotcore_client(service_account_info)

    def deploy(self):
        self._username = 'iguazio'

        # connect to the appnode
        self._ssh_clients['appnode'] = self._create_ssh_client(self._appnode['external_ip'],
                                                               self._username,
                                                               self._ssh_password)

        # get docker registry info
        docker_registry_info = self._get_docker_registry_info()
        self._logger.debug_with('Got Docker registry info', info=docker_registry_info)

        # patch roles on appnode to allow nuclio functions to do everything
        self._patch_roles()

        # create control plane session
        self._create_control_plane_session(self._username, self._system_password)

        # create iotcore resources
        device_infos = self._create_iotcore_resources()

        # delete all services, projects and functions in the iot core demo
        self._run_command('appnode', 'kubectl delete -n default-tenant deploy,function,project -l app=iotcoredemo',
                          raise_on_error=False)

        # create functions that exist once
        self._create_system_project()

        # for each device
        for device_info in device_infos:
            self._create_device_project(device_info['idx'], device_info, docker_registry_info)

        # close ssh client
        for ssh_client in self._ssh_clients.values():
            ssh_client.close()

    def _create_system_project(self):

        # create nuclio project
        project_name = self._create_nuclio_project('default-tenant', 'IoT Core System', name='iot-core-demo-system')

        # create sync-docker-image
        self._create_nuclio_function(f'sync-docker-image',
                                     'default-tenant',
                                     project_name,
                                     self._url_contents_to_base64(self._source_code_base_url + 'sync-docker-image.py'),
                                     'main:handler',
                                     'python:3.6',
                                     triggers={
                                         'http': {
                                             'kind': 'http',
                                             'maxWorkers': 10
                                         }
                                     })

        # create api
        self._create_nuclio_function(f'api',
                                     'default-tenant',
                                     project_name,
                                     self._url_contents_to_base64(self._source_code_base_url + 'api.py'),
                                     'main:handler',
                                     'python:3.6',
                                     base_image='python:3.6',
                                     triggers={
                                         'http': {
                                             'kind': 'http',
                                             'maxWorkers': 10
                                         }
                                     },
                                     build_commands=[
                                         'apt-get update && apt-get install build-essential',
                                         'pip install google-api-python-client==1.7.4 google-auth-httplib2==0.0.3 google-auth==1.5.1 google-cloud-pubsub==0.38.0'
                                     ],
                                     env={
                                         'DEMO_API_PROJECT_ID': self._project_id,
                                         'DEMO_API_REGION_NAME': self._region_name,
                                         'DEMO_API_REGISTRY_ID': self._registry_id,
                                         'DEMO_API_SERVICE_ACCOUNT': json.dumps(self._service_account_info)
                                     })

    def _create_device_project(self, device_idx, device_info, docker_registry_info):

        # create nuclio project
        project_name = self._create_nuclio_project('default-tenant', f'IoT Core Device #{device_idx}', name=f'iot-core-demo-device-{device_idx}')

        # create the device service
        self._create_service(device_idx, device_info['id'])

        # create config-reader function
        self._create_nuclio_function(f'config-reader-{device_idx}',
                                     'default-tenant',
                                     project_name,
                                     self._url_contents_to_base64(self._source_code_base_url + 'config-reader.py'),
                                     'main:handler',
                                     'python:3.6',
                                     env={
                                         'CONFIG_READER_INDEX': str(device_idx),
                                         'CONFIG_READER_LOCAL_REGISTRY_URL': docker_registry_info['url'],
                                         'CONFIG_READER_LOCAL_REGISTRY_USERNAME': docker_registry_info['username'],
                                         'CONFIG_READER_LOCAL_REGISTRY_PASSWORD': docker_registry_info['password']
                                     })

        # create state-updater function
        self._create_nuclio_function(f'state-updater-{device_idx}',
                                     'default-tenant',
                                     project_name,
                                     # self._url_contents_to_base64(self._source_code_base_url + 'state-updater.py'),
                                     self._file_contents_to_base64('./functions/state-updater.py'),
                                     'main:handler',
                                     'python:3.6',
                                     env={
                                         'STATE_UPDATER_INDEX': str(device_idx),
                                         'STATE_UPDATER_LABEL_SELECTOR': f'iguazio.com/index={device_idx},iguazio.com/monitor-state=true',
                                     })

        # create dispatcher function
        self._create_nuclio_function(f'iotcore-mqtt-dispatcher-{device_idx}',
                                     'default-tenant',
                                     project_name,
                                     self._url_contents_to_base64(
                                         self._source_code_base_url + 'iotcore-mqtt-dispatcher.py'),
                                     'main:handler',
                                     'python:3.6',
                                     env={
                                         'IOTCORE_MQTT_DISPATCHER_INDEX': str(device_idx),
                                         'IOTCORE_MQTT_DISPATCHER_PROJECT_ID': self._project_id,
                                         'IOTCORE_MQTT_DISPATCHER_REGION_NAME': self._region_name,
                                         'IOTCORE_MQTT_DISPATCHER_REGISTRY_ID': self._registry_id,
                                         'IOTCORE_MQTT_DISPATCHER_DEVICE_ID': device_info['id'],
                                         'IOTCORE_MQTT_DISPATCHER_PRIVATE_KEY': device_info['keys']['private_key'],
                                     })

    def _create_ssh_client(self, host, username, password):
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(host, 22, username, password)

        self._logger.debug_with('SSH client created', host=host, username=username)

        return ssh_client

    def _run_command(self, cluster, command, raise_on_error=True):
        self._logger.debug_with('Running command', cluster=cluster, command=command)
        _, out, err = self._ssh_clients[cluster].exec_command(command)

        # get exit status of command
        exit_status = out.channel.recv_exit_status()

        try:
            out = out.read().decode('utf-8')
        except:
            out = ''

        try:
            err = err.read().decode('utf-8')
        except:
            err = ''

        self._logger.debug_with('Command complete', exit_status=exit_status, out=out, err=err)

        if exit_status != 0 and raise_on_error:
            raise RuntimeError(f'Failed to run command ({exit_status})')

        return out, err

    def _get_docker_registry_info(self):

        # get the secret we created for nuclio
        out, _ = self._run_command('appnode',
                                   'kubectl get secret -n default-tenant default-tenant-nuclio-registry-credentials -o yaml')

        # parse the output
        docker_config = json.loads(base64.b64decode(yaml.load(out)['data']['.dockerconfigjson']))

        for auth_url, auth_info in docker_config['auths'].items():
            return {
                'url': auth_url,
                'username': auth_info['username'],
                'password': auth_info['password']
            }

    def _send_http_request(self, url, method, path, body, headers=None, return_raw_response=False):
        self._logger.debug_with(f'Sending HTTP request to {url}',
                                method=method,
                                path=path,
                                headers=headers,
                                body=body)

        response = getattr(requests, method)(url + path,
                                             json=body,
                                             cookies=self._dashboard_cookies,
                                             headers=headers,
                                             verify=False)

        self._logger.debug_with('Got response',
                                body=response.text,
                                status_code=response.status_code,
                                cookies=response.cookies)

        if response.status_code >= 400:
            raise RuntimeError(f'Failed to execute HTTP request {response.status_code}')

        if return_raw_response:
            return response

        try:
            return json.loads(response.text)
        except json.decoder.JSONDecodeError:
            return {}

    def _send_dashboard_request(self, method, path, body=None, headers=None, return_raw_response=False):
        return self._send_http_request('https://' + self._datanode['external_ip'],
                                       method,
                                       path,
                                       body,
                                       headers,
                                       return_raw_response)

    def _send_provazio_request(self, method, path, body=None, headers=None, return_raw_response=False):
        # 'http://localhost:18060'
        return self._send_http_request('http://dev.cloud.iguazio.com/api',
                                       method,
                                       path,
                                       body,
                                       headers,
                                       return_raw_response)

    def _create_control_plane_session(self, username, password):
        self._logger.info_with('Creating control plane session', username=username)

        # post a session
        response = self._send_dashboard_request('post', '/api/sessions', {
            'data': {
                'type': 'session',
                'attributes': {
                    'username': username,
                    'password': password,
                    'plane': 'control'
                }
            }
        }, return_raw_response=True)

        # save session, if any
        try:
            self._dashboard_cookies = response.cookies
        except KeyError:
            pass

    def _create_nuclio_project(self, namespace, display_name, name='', description=''):
        project_name = name or str(uuid.uuid4())

        self._logger.info_with('Creating Nuclio project',
                               namespace=namespace,
                               display_name=display_name)

        self._send_dashboard_request('post', '/api/projects', {
            'data': {
                'type': 'project',
                'id': project_name,
                'attributes': {
                    'metadata': {
                        'name': project_name,
                        'namespace': namespace,
                        'labels': {
                            'entries': [
                                {
                                    'key': 'app',
                                    'value': 'iotcoredemo'
                                }
                            ]
                        },
                    },
                    'spec': {
                        'display_name': display_name,
                        'description': description
                    }
                }
            }
        })

        return project_name

    def _create_nuclio_function(self,
                                name,
                                namespace,
                                project_name,
                                source_code,
                                handler,
                                runtime,
                                base_image=None,
                                build_commands=None,
                                env=None,
                                triggers=None):
        self._logger.info_with('Creating Nuclio function',
                               name=name,
                               namespace=namespace)

        env = env or {}
        triggers = triggers or {}

        headers = {
            'x-igz-nuclio-project-name': project_name,
        }

        body = {
            'data': {
                'type': 'function',
                'attributes': {
                    'metadata': {
                        'name': name,
                        'namespace': namespace,
                        'labels': {
                            'entries': [
                                {
                                    'key': 'nuclio.io/project-name',
                                    'value': project_name
                                },
                                {
                                    'key': 'app',
                                    'value': 'iotcoredemo'
                                }
                            ]
                        },
                        'annotations': {
                            'entries': []
                        }
                    },
                    'spec': {
                        'description': '',
                        'disable': False,
                        'triggers': {
                            'entries': [{'key': name, 'value': value} for name, value in triggers.items()]
                        },
                        'env': [{'name': name, 'value': value} for name, value in env.items()],
                        'handler': handler,
                        'runtime': runtime,
                        'build': {
                            'function_source_code': source_code,
                            'code_entry_type': 'sourceCode',
                            'commands': build_commands or []
                        },
                        'target_cpu': 75,
                        'min_replicas': 1,
                        'max_replicas': 1
                    }
                }
            }
        }

        if base_image is not None:
            body['data']['attributes']['spec']['build']['base_image'] = base_image

        self._send_dashboard_request('post', '/api/functions', body=body, headers=headers)

    def _patch_roles(self):
        self._logger.info_with('Patching roles')

        self._run_command('appnode', '''echo 'kind: Role
apiVersion: rbac.authorization.k8s.io/v1
metadata:
  namespace: default-tenant
  name: cluster-admin
rules:
- apiGroups: ["*"]
  resources: ["*"]
  verbs: ["*"]
---
kind: RoleBinding
apiVersion: rbac.authorization.k8s.io/v1
metadata:
  name: cluster-admin
  namespace: default-tenant
subjects:
- kind: ServiceAccount
  name: default
  namespace: default-tenant
- kind: ServiceAccount
  name: default-tenant-v3io-prometheus-server
  namespace: default-tenant
roleRef:
  kind: Role
  name: cluster-admin
  apiGroup: rbac.authorization.k8s.io' | kubectl apply -f -
''')

    def _create_service(self, device_idx, location):
        service_port = 31900 + device_idx
        self._logger.info_with('Creating tdemo app',
                               device_idx=device_idx,
                               service_port=service_port)

        self._run_command('appnode', f'''echo 'apiVersion: v1
kind: Service
metadata:
  name: tdemo-{device_idx}
  labels:
    app: iotcoredemo
spec:
  type: NodePort
  ports:
  - port: 8080
    protocol: TCP
    name: http
    nodePort: {service_port}
  selector:
    app: tdemo-{device_idx}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: tdemo-{device_idx}
  labels:
    app: iotcoredemo
    iguazio.com/index: "{device_idx}"
    iguazio.com/monitor-state: "true"
spec:
  selector:
    matchLabels:
      app: tdemo-{device_idx}
  replicas: 1
  template:
    metadata:
      labels:
        app: tdemo-{device_idx}
    spec:
      containers:
      - name: tdemo
        image: pavius/tdemo:0.0.1
        env:
        - name: TDEMO_LOCATION
          value: {location}
        ports:
        - containerPort: 8080
' | kubectl apply -n default-tenant -f -
''')

    def _url_contents_to_base64(self, url):
        response = self._send_http_request(url, 'get', '', None, return_raw_response=True)

        return base64.b64encode(response.text.encode('utf-8')).decode('utf-8')

    def _file_contents_to_base64(self, path):
        with open(path, "rb") as f:
            return base64.b64encode(f.read()).decode('utf-8')

    # us/colorado/denver -> [{topic: us, qos: 0}, {topic: us/colorado, qos: 0}, {topic: us/colorado/denver, qos: 0}]
    def _get_location_subscriptions(self, location):
        subscriptions = []

        location_segments = location.split('/')

        for location_segment_idx in range(len(location_segments)):
            subscriptions.append({
                'topic': '/'.join(location_segments[:location_segment_idx + 1]),
                'qos': 0
            })

        return subscriptions

    def _create_iotcore_resources(self):

        try:

            # delete device
            self._delete_registry_devices(self._project_id,
                                          self._region_name,
                                          self._registry_id)

            # delete registry
            self._delete_registry(self._project_id,
                                  self._region_name,
                                  self._registry_id)

            time.sleep(5)

        except:
            pass

        # create a registry
        registry_name = self._create_registry(self._project_id,
                                              self._region_name,
                                              self._registry_id)

        device_infos = []

        # create devices
        for device_idx in range(self._num_devices):
            # create a device
            device_info = self._create_device(registry_name, device_idx, self._device_id_format.format(device_idx))
            device_info['idx'] = device_idx

            # add to device infos
            device_infos.append(device_info)

        return device_infos

    def _create_registry(self, project_id, region_name, registry_id):
        self._logger.debug_with('Creating registry',
                                project_id=project_id,
                                region_name=region_name,
                                registry_id=registry_id)

        registry_parent = 'projects/{}/locations/{}'.format(project_id, region_name)
        body = {
            'id': registry_id
        }

        # create the registry
        self._iotcore_client. \
            projects(). \
            locations(). \
            registries(). \
            create(parent=registry_parent, body=body). \
            execute()

        return f'{registry_parent}/registries/{registry_id}'

    def _create_device(self, registry_name, device_idx, device_id):
        locations = [
            'us/colorado/boulder',
            'us/colorado/denver',
            'us/colorado/aspen',
            'us/arizona/phoenix',
            'us/arizona/scottsdale'
        ]

        device_location = locations[device_idx]

        self._logger.debug_with('Creating device',
                                registry_name=registry_name,
                                device_id=device_id,
                                location=device_location)

        # generate device keys
        device_keys = self._create_device_keypair(device_id)

        device_template = {
            'id': device_id,
            'credentials': [{
                'publicKey': {
                    'format': 'RSA_X509_PEM',
                    'key': device_keys['public_key']
                }
            }],
            'metadata': {
                'index': str(device_idx),
                'location': device_location
            }
        }

        devices = self._iotcore_client.projects().locations().registries().devices()

        # create the device
        devices.create(parent=registry_name,
                       body=device_template).execute()

        return {
            'id': device_id,
            'registry_name': registry_name,
            'keys': device_keys
        }

    def _create_device_keypair(self, device_id):
        private_key_file_path = f'/tmp/rsa-private-{device_id}.pem'
        public_key_file_path = f'/tmp/rsa-public-{device_id}.pem'

        delegator.run(
            f'openssl req -x509 -newkey rsa:2048 -keyout {private_key_file_path} -nodes -out {public_key_file_path} -subj "/CN=unused"')

        with io.open(private_key_file_path) as f:
            private_key_file_contents = f.read()

        with io.open(public_key_file_path) as f:
            public_key_file_contents = f.read()

        return {
            'private_key': private_key_file_contents,
            'public_key': public_key_file_contents,
        }

    def _create_iotcore_client(self, service_account_info):
        credentials = service_account.Credentials.from_service_account_info(service_account_info)

        scoped_credentials = credentials.with_scopes(['https://www.googleapis.com/auth/cloud-platform'])
        discovery_url = '{}?version={}'.format('https://cloudiot.googleapis.com/$discovery/rest', 'v1')

        return discovery.build(
            'cloudiotcore',
            'v1',
            discoveryServiceUrl=discovery_url,
            credentials=scoped_credentials)

    def _delete_registry(self, project_id, region_name, registry_id):
        registry_name = 'projects/{}/locations/{}/registries/{}'.format(project_id, region_name, registry_id)

        self._logger.debug_with('Deleting registry',
                                project_id=project_id,
                                region_name=region_name,
                                registry_id=registry_id)

        self._iotcore_client. \
            projects(). \
            locations(). \
            registries(). \
            delete(name=registry_name).execute()

    def _delete_registry_devices(self, project_id, region_name, registry_id):
        registry_name = 'projects/{}/locations/{}/registries/{}'.format(project_id, region_name, registry_id)

        self._logger.debug_with('Deleting devices',
                                project_id=project_id,
                                region_name=region_name,
                                registry_id=registry_id)

        devices = self._iotcore_client. \
            projects(). \
            locations(). \
            registries(). \
            devices(). \
            list(parent=registry_name). \
            execute(). \
            get('devices', [])

        for device in devices:
            # delete the device
            device_name = '{}/devices/{}'.format(registry_name, device.get('id'))
            self._logger.debug_with('Deleting device', device_name=device_name)

            # delete device
            self._iotcore_client. \
                projects(). \
                locations(). \
                registries(). \
                devices(). \
                delete(name=device_name). \
                execute()


if __name__ == '__main__':
    logger = nuclio_sdk.Logger('DEBUG')
    logger.set_handler('default', sys.stdout, nuclio_sdk.logger.HumanReadableFormatter())

    # read service account file info
    with open(os.environ['DEPLOYER_SERVICE_ACCOUNT_FILE_PATH'], "rb") as service_account_file:
        service_account_info = json.load(service_account_file)

    demo_deployer = DemoDeployer(logger,
                                 os.environ['DEPLOYER_DATANODE_IP'],
                                 os.environ['DEPLOYER_APPNODE_IP'],
                                 os.environ['DEPLOYER_PROJECT_ID'],
                                 os.environ['DEPLOYER_REGION_NAME'],
                                 os.environ['DEPLOYER_REGISTRY_ID'],
                                 os.environ['DEPLOYER_DEVICE_ID_FORMAT'],
                                 2,
                                 service_account_info,
                                 os.environ['DEPLOYER_SOURCE_CODE_BASE_URL'],
                                 os.environ['DEPLOYER_SYSTEM_PASSWORD'],
                                 os.environ['DEPLOYER_SSH_PASSWORD'])

    demo_deployer.deploy()
