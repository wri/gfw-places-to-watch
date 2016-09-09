import os
import subprocess
import time
from threading import Thread

from processing.utilities import file_utilities as util


class Job(object):
    """ A class to store information about jobs for each worker. Contains an executable, arguments, and potentiall
    more info
    :param executable: An executable that can be called via subprocess
    :param arguments A list of arguments to follow it
    :param input: the inputs required for the job to be executed
    :param output: the output file
    :return:
    """

    def __init__(self, executable, arguments=None, input=None, output=None):

        self.executable = executable
        self.arguments = arguments

        self._input = None
        self.input = input

        self.output = output

    # Validate input
    @property
    def input(self):
        return self._input

    @input.setter
    def input(self, i):
        if i:
            if not isinstance(i, (list, tuple)):
                i = [i]
        else:
            i = []
        self._input = i

    def inputs_ready(self, worker_id, debug):

        if self.input:

            input_ready_list = []

            for input_ras in self.input:
                # Used to indicate that the input tif has been written completely
                txt_file = os.path.splitext(input_ras)[0] + '.txt'
                input_ready_list.append(os.path.exists(txt_file))

            input_ready_list = list(set(input_ready_list))

            # If the inputs are ready, great
            if len(input_ready_list) == 1 and input_ready_list[0]:
                inputs_ready = True

            else:
                if debug:
                    print '{0}: Inputs not ready: {1}'.format(worker_id, self.input)

                inputs_ready = False

        # If not input required, task is ready
        else:
            inputs_ready = True

        return inputs_ready

    def run(self, worker_id, debug):

        cmd = [self.executable] + self.arguments

        if debug:
            print cmd

        if self.executable == 'python':
            msg_exec = os.path.basename(self.arguments[0])
        else:
            msg_exec = self.executable

        print '{0}: Starting {1}. Output: {2}'.format(worker_id, msg_exec, os.path.basename(self.output))

        # To execute on windows
        # if os.name == 'nt':
        #     subprocess.check_call(cmd, shell=True)
        #
        # else:
        #     subprocess.check_call(cmd)

        subprocess.check_call(cmd)

        # Leave a .txt file in the folder. This will signal to other workers that this
        # output raster is complete, and can be used as an input to other processes
        util.write_marker_txt_file(self.output)


def process_queue(num_threads, q, debug):
    # Create multiple workers to process the queue

    print 'Starting queue process now using {0} threads'.format(num_threads)
    for i in range(num_threads):
        worker = Thread(target=process_jobs, args=(i, q, debug))
        worker.setDaemon(True)
        worker.start()

    # Blocks the main thread of the program until the workers have finished the queue
    q.join()

    # Sleep for a second to finish logging all messages from the workers
    time.sleep(1)
    print 'Queued process complete'


def process_jobs(i, q, debug):
    """This is the worker thread function.
    It processes items in the queue one after
    another.  These daemon threads go into an
    infinite loop, and only exit when
    the main thread ends.
    """
    while True:
        j = q.get()

        if j.inputs_ready(i, debug):
            j.run(i, debug)
            q.task_done()

        # Otherwise put the task back in the queue
        else:
            time.sleep(1)
            q.task_done()
            q.put(j)
