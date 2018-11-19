import os
import delegator

# @nuclio.configure
#
# function.yaml:
#   apiVersion: "nuclio.io/v1"
#   kind: Function
#   spec:
#     build:
#       commands:
#       - wget https://download.docker.com/linux/static/stable/x86_64/docker-18.06.1-ce.tgz
#       - tar -xvzf docker-18.06.1-ce.tgz
#       - mv docker/docker /usr/bin/docker
#       - rm -rf docker
#       - pip install delegator.py
#     volumes:
#     - volume:
#         name: docker-sock
#         hostPath:
#           path: /var/run/docker.sock
#       volumeMount:
#         name: docker-sock
#         mountPath: /var/run/docker.sock

def handler(context, event):
    source = event.body.get('source', {})
    dest = event.body.get('dest', {})

    # sync the docker image
    _sync_docker_image(context,
                       source={
                           'url': source.get('url') or os.environ['SOURCE_URL'],
                           'image': source.get('image') or os.environ['SOURCE_IMAGE'],
                           'creds': {
                               'username': source.get('creds', {}).get('username') or os.environ.get('SOURCE_USERNAME'),
                               'password': source.get('creds', {}).get('password') or os.environ.get('SOURCE_PASSWORD')
                           }
                       },
                       dest={
                           'url': dest.get('url') or os.environ['DEST_URL'],
                           'image': dest.get('image') or os.environ.get('DEST_IMAGE'),
                           'creds': {
                               'username': dest.get('creds', {}).get('username') or os.environ.get('DEST_USERNAME'),
                               'password': dest.get('creds', {}).get('password') or os.environ.get('DEST_PASSWORD')
                           }
                       })


def _sync_docker_image(context, source, dest):

    # if user didn't pass dest image, use source image
    dest['image'] = dest['image'] or source['image']

    commands = [
        f'docker pull {source["url"]}/{source["image"]}',
        f'docker tag {source["url"]}/{source["image"]} {dest["url"]}/{dest["image"]}',
        f'docker push {dest["url"]}/{dest["image"]}'
    ]

    # if creds were passed, add a login command
    _add_login_command(context, commands, source['url'], source.get('creds'))
    _add_login_command(context, commands, dest['url'], dest.get('creds'))

    for command in commands:
        _run_command(context, command)


def _run_command(context, command):
    context.logger.debug_with('Running command', command=command)
    delegator_command = delegator.run(command)

    # get stuff from command
    command_stdout = delegator_command.out
    command_stderr = delegator_command.err
    command_return_code = delegator_command.return_code

    if command_return_code != 0:
        context.logger.warn_with('Failed to run command',
                                 command=command,
                                 rc=command_return_code,
                                 out=command_stdout,
                                 err=command_stderr)
        raise RuntimeError('Failed to run command')
    else:
        context.logger.debug_with('Command completed successfully',
                                  command=command,
                                  out=command_stdout,
                                  err=command_stderr)


def _add_login_command(context, commands, url, creds):
    if creds['username'] is not None and creds['password'] is not None:
        commands.insert(0, f'docker login -u {creds["username"]} -p {creds["password"]} {url}')
