# -*- coding: utf-8 -*-
"""
a PBS (portable batch system) parallel-computing job manager.
see also: README.md, example.ipynb

this submodule handles via a unified API the execution of jobs 
on different systems, such as: PBS cluster, local multi-CPU 
machine, or submission to a script file .

@author: Alon Diament, Tuller Lab
Created on Wed Mar 18 22:45:50 2015
"""
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
import os
import re
from subprocess import run, check_output

from .config import PBS_ID, PBS_suffix, PBS_queue, DefResource, JobDir, LogOut, LogErr, \
        running_on_cluster
from . import dal
from . import utils


class JobExecutor(object):
    """ dummy template for a JobExecutor. """
    def __init__(self):
        pass
    def submit(self, JobInfo, Spawn=False):
        """ submits the job to some executor, updates the following fields: 
            submit_id, subtime, state.
            must return submit_id or 'failed'. """
        pass
    def qstat(self):
        """ returns a dict with PBS_IDs as keys and [name, state in {'R','Q'}]
            as values. """
        pass
    def shutdown(self):
        pass


class ClusterJobExecutor(JobExecutor):
    pass


class PBSJobExecutor(ClusterJobExecutor):
    """ PBSJobExecutor is initiated with default params that can be
        overriden by jobs. """
    def __init__(self, queue=PBS_queue, resources=DefResource,
                 id_suffix=PBS_suffix):
        self.queue = queue
        self.resources = resources
        self.id_suffix = id_suffix

    def submit(self, JobInfo, Spawn=False):
        ErrDir = os.path.abspath(JobDir) + '/{}/logs/'.format(JobInfo['BatchID'])
        os.makedirs(ErrDir, exist_ok=True)
        OutDir = os.path.abspath(JobDir) + '/{}/logs/'.format(JobInfo['BatchID'])
        os.makedirs(OutDir, exist_ok=True)

        # build command
        Qsub = ['qsub', '-q', self.queue, '-e', ErrDir, '-o', OutDir, '-l']
        if 'resources' in JobInfo:
            this_res = JobInfo['resources']
        else:
            this_res = self.resources
        this_sub = Qsub + [','.join(['{}={}'.format(k, v)
                           for k, v in sorted(this_res.items())])]
        if 'queue' in JobInfo:
            this_sub[2] = JobInfo['queue']

        submit_id_raw = check_output(this_sub + [JobInfo['script']])\
                .decode('UTF-8').replace('\n', '')
        submit_id = submit_id_raw.replace(self.id_suffix, '')
        update_fields(JobInfo, submit_id, Spawn)

        return submit_id

    def qstat(self):
        Q = {}
        if not running_on_cluster:
            print('get_pbs_queue: not running on cluster.')
            return Q
        data = check_output(['qstat', '-u', os.environ['USER']],
                            universal_newlines=True)
        data = data.split('\n')
        job_parse = re.compile(r'(\d+).')
        line_parse = re.compile(r'\s+')
        for line in data:
            job = job_parse.match(line)  # power
            if job:
                line = line_parse.split(line)
                Q[job.group(1)] = [line[3], line[9]]
        return Q


class LocalJobExecutor(JobExecutor):
    """ returns a pool executer with a submit method.
        currently using ThreadPoolExecutor to start new subprocesses. """
    def __init__(self, max_workers=os.cpu_count()):
        self._pool = ThreadPoolExecutor(max_workers=max_workers)
        self._queue = OrderedDict()

    def submit(self, JobInfo, Spawn=False):
        if PBS_ID != 'pbsmgr':
            print('cannot submit from a subprocess. PBS_ID must be set to "pbsmgr".')
            return 'failed'

        ErrDir = os.path.abspath(JobDir) + '/{}/logs/'.format(JobInfo['BatchID'])
        os.makedirs(ErrDir, exist_ok=True)
        OutDir = os.path.abspath(JobDir) + '/{}/logs/'.format(JobInfo['BatchID'])
        os.makedirs(OutDir, exist_ok=True)

        submit_id = str(utils.get_id())
        update_fields(JobInfo, submit_id, Spawn)
        self._pool.submit(self.__run_local_job, JobInfo)
        self._queue[submit_id] = [f"{JobInfo['BatchID']}-{JobInfo['JobIndex']}", 'Q']

        return submit_id

    def qstat(self):
        return self._queue

    def shutdown(self, wait=True):
        self._pool.shutdown(wait=wait)

    def __run_local_job(self, JobInfo):
        # execute a job locally as a new subprocess
        env = os.environ.copy()
        env.update(PBS_JOBID=JobInfo['submit_id'])

        with open(JobInfo['stdout'][-1], 'w') as oid:
            with open(JobInfo['stderr'][-1], 'w') as eid:
                print(JobInfo['submit_id'], JobInfo['script'])
                self._queue[JobInfo['submit_id']][1] = 'R'
                job_res = run(JobInfo['script'], shell=True, env=env, stdout=oid, stderr=eid)
        del self._queue[JobInfo['submit_id']]

        return job_res.returncode


class FileJobExecutor(JobExecutor):
    """ writes calls to job scripts into a shell script. """
    def __init__(self, script_file):
        self.path = script_file
        self._queue = OrderedDict()
        os.makedirs(os.path.dirname(self.path))
        with open(self.path, 'w') as fid:
            # will delete any existing file
            fid.write('#!/bin/bash\n\n')
        os.chmod(self.path, 0o744)

    def submit(self, JobInfo, Spawn):
        submit_id = utils.get_id()
        with open(self.path, 'a') as fid:
            fid.write(JobInfo['script'] + '\n')
        update_fields(JobInfo, submit_id, Spawn)
        self._queue[submit_id] = [(JobInfo['BatchID'], JobInfo['JobIndex']), 'Q']

        return submit_id


def update_fields(JobInfo, submit_id, Spawn):
    JobInfo['submit_id'] = submit_id
    # submit_id used by LocalJobExecutor so must update here (even if not in DB)

    if not Spawn:
        JobInfo['subtime'] = utils.get_time()
        JobInfo['state'] = 'submit'

        utils.dict_append(JobInfo, 'stdout', LogOut.format(**JobInfo))
        utils.dict_append(JobInfo, 'stderr', LogErr.format(**JobInfo))
        if 'PBS_ID' in JobInfo:
            del JobInfo['PBS_ID']
        if 'qstat' in JobInfo:
            del JobInfo['qstat']

    elif JobInfo['state'] != 'spawn':
        JobInfo['state'] = 'spawn'

    dal.update_job(JobInfo, Release=True)
