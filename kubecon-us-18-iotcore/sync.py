import sys
import requests
import delegator
import threading
import queue

import nuclio_sdk


class RepoSyncer(object):

    def __init__(self, logger):
        self._logger = logger
        self._worker_queue = self._create_worker_queue(4)

    def sync_repos(self, version):

        while True:
            self._logger.debug_with('Getting artifact version manifest', version=version)

            artifact_version_manifest = requests.get(f'http://dev.cloud.iguazio.com/api/artifact_version_manifests/{version}').json()
            self._logger.info_with('Got manifest', artifact_version_manifest=artifact_version_manifest)

            if 'error' not in artifact_version_manifest:
                break

        # iterate images
        for image_name, image_info in artifact_version_manifest['docker']['iguaziodocker'].items():
            image_name = image_name + ':' + image_info['tag']

            self._logger.info_with('Queueing image', image_name=image_name)

            self._worker_queue.put((self._run_command, [f'docker pull iguaziodocker/{image_name}']))
            self._worker_queue.put((self._run_command, [f'docker tag iguaziodocker/{image_name} quay.io/iguazio/{image_name}']))
            self._worker_queue.put((self._run_command, [f'docker push quay.io/iguazio/{image_name}']))

        # wait for all pulls
        self._worker_queue.join()

    def _create_worker_queue(self, num_workers):
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

                worker_queue.task_done()

            except Exception as e:
                self._logger.warn_with('Got exception while handling work', worker_idx=worker_idx, e=str(e))

    def _run_command(self, command, raise_on_error=True):
        print('**** '+ command)

        self._logger.debug_with('Running command', command=command)

        # cmd = delegator.run(command)
        # cmd.block()
        #
        # self._logger.debug_with('Command executed',
        #                         command=command,
        #                         stdout=cmd.out,
        #                         stderr=cmd.err)
        #
        # if cmd.return_code != 0:
        #     raise RuntimeError(f'Error: out({cmd.out}) err({cmd.err})')


if __name__ == '__main__':
    logger = nuclio_sdk.Logger('DEBUG')
    logger.set_handler('default', sys.stdout, nuclio_sdk.logger.HumanReadableFormatter())

    RepoSyncer(logger).sync_repos('1.9_cloud_b47_20181126164646')
