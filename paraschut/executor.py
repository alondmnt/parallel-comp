# -*- coding: utf-8 -*-
"""
PARASCHUT: parallel job scheduling utils.
see also: README.md, example.ipynb

this submodule handles via a unified API the execution of jobs 
on different systems, such as: PBS cluster, local multi-CPU 
machine, or submission to a script file .

@author: Alon Diament, Tuller Lab
Created on Wed Mar 18 22:45:50 2015
"""
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
import os
import re
from subprocess import run, check_output, call
import time
from warnings import warn

import pandas as pd

from .config import DefQueue, DefResource, ServerHost, Hostname
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
        return 'failed'

    def delete(self, JobInfo):
        pass

    def qstat(self):
        """ returns a dict with ClusterIDs as keys and [name, state in {'R','Q'}]
            as values. """
        return {}

    def get_job_id(self):
        """ returns the job ID assigned by the cluster. """
        return None

    def get_job_summary(self, ClusterID=None):
        """ returns a dict with any fields describing the job state
            (time, resources, etc.). """
        return {}

    def isconnected(self):
        """ whether we're connected to the cluster and can submit. """
        return

    def shutdown(self):
        pass


class ClusterJobExecutor(JobExecutor):
    """ dummy subclass for server based clusters. """
    pass


class PBSJobExecutor(ClusterJobExecutor):
    """ PBSJobExecutor is initiated with default params that can be
        overriden by jobs. """
    def __init__(self, queue=DefQueue, resources=DefResource):
        self.queue = queue
        self.resources = resources

        if 'PBS_JOBID' in os.environ:
            self.job_id = os.environ['PBS_JOBID'].split('.')[0]
        else:
            self.job_id = None

        if (self.job_id is not None) or (ServerHost in Hostname):
            self.connected_to_cluster = True
        else:
            self.connected_to_cluster = False

    def submit(self, JobInfo, Spawn=False):
        OutFile, ErrFile = utils.get_log_paths(JobInfo)

        # build command
        Qsub = ['qsub']
        if 'queue' in JobInfo and JobInfo['queue'] is not None:
            Qsub += ['-q', JobInfo['queue']]
        elif self.queue is not None:
            Qsub += ['-q', self.queue]
        if ErrFile is not None:
            Qsub += ['-e', ErrFile]
        if OutFile is not None:
            Qsub += ['-o', OutFile]
        if 'resources' in JobInfo:
            this_res = JobInfo['resources']
        else:
            this_res = self.resources
        if this_res is not None:
            Qsub += ['-l'] + [','.join(['{}={}'.format(k, v)
                              for k, v in sorted(this_res.items())])]
        if 'vars' in JobInfo:
            Qsub += ['-v'] + [','.join(['{}={}'.format(k, repr(v))
                              for k, v in sorted(JobInfo['vars'].items())])]

        submit_id = check_output(Qsub + [JobInfo['script']])\
                .decode('UTF-8').replace('\n', '').split('.')[0]
        update_fields(JobInfo, submit_id, Spawn, OutFile, ErrFile)

        return submit_id

    def delete(self, JobInfo):
        for jid in dal.get_internal_ids(JobInfo):
           if not call(['qdel', jid]):
               dal.remove_internal_id(JobInfo, jid)
               JobInfo['state'] = 'init'

        dal.update_job(JobInfo)

    def qstat(self):
        Q = {}
        if not self.isconnected():
            print('qstat: not running on PBS cluster.')
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

    def get_job_id(self):
        return self.job_id

    def get_job_summary(self, ClusterID=None):
        if ClusterID is None:
            ClusterID = self.job_id
        if ClusterID is None:
            print('get_job_summary: not running on a cluster node.')
            return {}
        try:
            return self.__parse_qstat(check_output(['qstat', '-f', ClusterID]))
        except Exception as e:
            # sometimes this fails on cluster, not clear why (cluster does not recognize the BatchID)
            print(e)
            return None

    def isconnected(self):
        return self.connected_to_cluster

    def __parse_qstat(self, text):
        JobInfo = {}
        text = text.decode('utf-8')
        line_parse = re.compile(r'([\w.]*) = ([\w\s:_\-/]*)')
        for line in text.splitlines():
            hit = line_parse.match(line.strip())
            if hit is not None:
                JobInfo[hit.group(1)] = hit.group(2)
        return JobInfo


class SGEJobExecutor(ClusterJobExecutor):
    """ SGEJobExecutor is initiated with default params that can be
        overriden by jobs. """
    def __init__(self, queue=DefQueue, resources=DefResource):
        self.queue = queue
        self.resources = resources

        if 'JOB_ID' in os.environ:
            self.job_id = os.environ['JOB_ID'].split('.')[0]
        else:
            self.job_id = None

        if (self.job_id is not None) or (ServerHost in Hostname):
            self.connected_to_cluster = True
        else:
            self.connected_to_cluster = False

    def submit(self, JobInfo, Spawn=False):
        OutFile, ErrFile = utils.get_log_paths(JobInfo)

        # build command
        Qsub = ['qsub']
        if 'queue' in JobInfo and JobInfo['queue'] is not None:
            Qsub += ['-q', JobInfo['queue']]
        elif self.queue is not None:
            Qsub += ['-q', self.queue]
        if 'name' in JobInfo and JobInfo['name'] is not None:
            Qsub += ['-N', '_'.join(utils.make_iter(JobInfo['name']))]
        if ErrFile is not None:
            Qsub += ['-e', ErrFile]
        if OutFile is not None:
            Qsub += ['-o', OutFile]
        if 'resources' in JobInfo:
            this_res = JobInfo['resources']
        else:
            this_res = self.resources
        if this_res is not None:
            if 'smp' in this_res:
                Qsub += ['-pe', f'smp {this_res.pop("smp")}']
            Qsub += ['-l'] + [','.join(['{}={}'.format(k, v)
                              for k, v in sorted(this_res.items())])]
        if 'vars' in JobInfo:
            Qsub += ['-v'] + [','.join(['{}={}'.format(k, repr(v))
                              for k, v in sorted(JobInfo['vars'].items())])]

        submit_id_raw = check_output(Qsub + [JobInfo['script']])\
                .decode('UTF-8').replace('\n', '')
        submit_id = submit_id_raw.split(' ')[2].split('.')[0]
        update_fields(JobInfo, submit_id, Spawn, OutFile, ErrFile)

        return submit_id

    def delete(self, JobInfo):
        for jid in dal.get_internal_ids(JobInfo):
           if not call(['qdel', jid]):
               dal.remove_internal_id(JobInfo, jid)
               JobInfo['state'] = 'init'

        dal.update_job(JobInfo)

    def qstat(self):
        Q = {}
        if not self.isconnected():
            print('qstat: not running on SGE cluster.')
            return Q
        data = check_output(['qstat', '-u', os.environ['USER']],
                            universal_newlines=True)
        data = data.split('\n')
        line_parse = re.compile(r'\s+')
        for line in data:
            line = line_parse.split(line)
            if len(line) > 5 and line[1].isnumeric():
                Q[line[1]] = [line[3], line[5].replace('r', 'R').replace('q', 'Q')]
        return Q

    def get_job_id(self):
        return self.job_id

    def get_job_summary(self, ClusterID=None):
        if ClusterID is None:
            ClusterID = self.job_id
        if ClusterID is None:
            print('get_job_summary: not running on a cluster node.')
            return {}
        try:
            return self.__parse_qstat(check_output(['qstat', '-j', ClusterID]))
        except Exception as e:
            # sometimes this fails on cluster, not clear why (cluster does not recognize the BatchID)
            print(e)
            return None

    def isconnected(self):
        return self.connected_to_cluster

    def __parse_qstat(self, text):
        JobInfo = {}
        text = text.decode('utf-8')
        line_parse = re.compile(r'([\w.]*):(\s*)([\w\s:_\-/]*)')
        for line in text.splitlines():
            hit = line_parse.match(line.strip())
            if hit is not None:
                JobInfo[hit.group(1)] = hit.group(3)
        return JobInfo


class SlurmJobExecutor(ClusterJobExecutor):
    """ SlurmJobExecutor is initiated with default params that can be
        overriden by jobs. """
    def __init__(self, queue=DefQueue, resources=DefResource):
        self.queue = queue
        self.resources = resources

        if 'SLURM_JOBID' in os.environ:
            self.job_id = os.environ['SLURM_JOBID'].split('.')[0]
        else:
            self.job_id = None

        if (self.job_id is not None) or (ServerHost in Hostname):
            self.connected_to_cluster = True
        else:
            self.connected_to_cluster = False

    def submit(self, JobInfo, Spawn=False):
        OutFile, ErrFile = utils.get_log_paths(JobInfo)

        # build command
        Qsub = ['sbatch']
        if 'queue' in JobInfo and JobInfo['queue'] is not None:
            Qsub += ['-p', JobInfo['queue']]
        elif self.queue is not None:
            Qsub += ['-p', self.queue]
        if 'name' in JobInfo and JobInfo['name'] is not None:
            Qsub += ['--job-name=' + '_'.join(utils.make_iter(JobInfo['name']))]
        if ErrFile is not None:
            Qsub += ['-e', ErrFile]
        if OutFile is not None:
            Qsub += ['-o', OutFile]
        if 'resources' in JobInfo:
            this_res = JobInfo['resources']
        else:
            this_res = self.resources
        if this_res is not None:
            if 'walltime' in this_res:
                Qsub += ['--time', this_res.pop('walltime')]
            Qsub += ['--gres=' + this_res]
        if 'vars' in JobInfo:
            warn('environment variables cannot be set on Slurm clusters.')

        submit_id_raw = check_output(Qsub + [JobInfo['script']])\
                .decode('UTF-8').replace('\n', '')
        submit_id = submit_id_raw.split(' ')[3].split('.')[0]
        update_fields(JobInfo, submit_id, Spawn, OutFile, ErrFile)

        return submit_id

    def delete(self, JobInfo):
        for jid in dal.get_internal_ids(JobInfo):
           if not call(['scancel', jid]):
               dal.remove_internal_id(JobInfo, jid)
               JobInfo['state'] = 'init'

        dal.update_job(JobInfo)

    def qstat(self):
        Q = {}
        if not self.isconnected():
            print('qstat: not running on Slurm cluster.')
            return Q
        data = check_output(['squeue', '-u', os.environ['USER']],
                            universal_newlines=True)
        data = data.split('\n')
        line_parse = re.compile(r'\s+')
        for line in data:
            line = line_parse.split(line)
            if len(line) > 5 and line[1].isnumeric():
                Q[line[1]] = [line[3], line[5].replace('r', 'R').replace('q', 'Q')]
        return Q

    def get_job_id(self):
        return self.job_id

    def get_job_summary(self, ClusterID=None):
        if ClusterID is None:
            ClusterID = self.job_id
        if ClusterID is None:
            print('get_job_summary: not running on a cluster node.')
            return {}
        try:
            return self.__parse_qstat(check_output(['sstat', ClusterID]))
        except Exception as e:
            # sometimes this fails on cluster, not clear why (cluster does not recognize the BatchID)
            print(e)
            return None

    def isconnected(self):
        return self.connected_to_cluster

    def __parse_qstat(self, text):
        return pd.read_csv(BytesIO(text), sep=r'\s+').to_dict(orient='index')[1]


class LocalJobExecutor(JobExecutor):
    """ returns a pool executer with a submit method.
        currently using ThreadPoolExecutor to start new subprocesses. """
    def __init__(self, max_workers=os.cpu_count(), submitter=False, verbose=2):
        """ verbose level 1 is for muted exceptions, 2 is for warnings, 
            3 is for debugging logs. """
        self._pool = ThreadPoolExecutor(max_workers=max_workers)
        self._queue = OrderedDict()
        self.verbose = verbose

        if 'PBS_JOBID' in os.environ and not submitter:
            self.job_id = os.environ['PBS_JOBID'].split('.')[0]
        else:
            self.job_id = 'paraschut'

        self.connected_to_cluster = True

    def submit(self, JobInfo, Spawn=False):
        if self.job_id != 'paraschut':
            self.__print('cannot submit from a subprocess. ClusterID must be set to "paraschut".', 2)
            return 'failed'

        OutFile, ErrFile = utils.get_log_paths(JobInfo)
        submit_id = str(int(10**3*time.time() % 10**10))
        update_fields(JobInfo, submit_id, Spawn, OutFile, ErrFile)
        self._queue[submit_id] = [f"{JobInfo['BatchID']}-{JobInfo['JobIndex']}", 'Q',
                                  self._pool.submit(self.__run_local_job, JobInfo)]
        # we store a Future object with the result of the run in queue

        return submit_id

    def delete(self, JobInfo):
        try:
            for jid in dal.get_internal_ids(JobInfo):
                if jid in self._queue and self._queue[jid][2].cancel():
                    # if this mechanism ever fails or becomes cumbersome, 
                    # we can always mark a job for deletion and handle it 
                    # by ourselves in __run_local_job() (see previous commit)
                    del self._queue[jid]
                    dal.remove_internal_id(JobInfo, jid)
                    JobInfo['state'] = 'init'
                    self.__print(f"job '{jid}' ('{JobInfo['BatchID']}', '{JobInfo['JobIndex']}') was deleted", 3)
                else:
                    self.__print(f"job '{jid}' cannot be deleted", 2)

            dal.update_job(JobInfo)

        except Exception as err:
            self.__print(f'delete failed with {type(err)}: {err}', 1)

    def qstat(self):
        return self._queue

    def get_job_id(self):
        return self.job_id

    def isconnected(self):
        return self.connected_to_cluster

    def shutdown(self, wait=True):
        self._pool.shutdown(wait=wait)

    def __run_local_job(self, JobInfo):
        # execute a job locally as a new subprocess
        time.sleep(0.1)  # we need this delay to let self._queue update
        try:
            self.__validate_job(JobInfo)

            env = os.environ.copy()
            env.update(PBS_JOBID=JobInfo['submit_id'])
            if 'vars' in JobInfo:
                env.update(JobInfo['vars'])

            if JobInfo['state'] == 'spawn':
                # we need to get the right stdout/stderr
                JobInfo.update(dal.spawn_get_info(JobInfo['BatchID'],
                        JobInfo['JobIndex'], ClusterID=JobInfo['submit_id']))

            with open(JobInfo['stdout'][-1], 'w') as oid:
                with open(JobInfo['stderr'][-1], 'w') as eid:
                    self.__print(f"{JobInfo['submit_id']}: {JobInfo['script']}", 3)
                    self._queue[JobInfo['submit_id']][1] = 'R'
                    job_res = run(JobInfo['script'], shell=True, env=env, stdout=oid, stderr=eid)
            del self._queue[JobInfo['submit_id']]

        except Exception as err:
            self.__print(f"executing job ({JobInfo['BatchID']}, {JobInfo['JobIndex']}) failed with error: \n{err}", 1)
            del self._queue[JobInfo['submit_id']]

        return job_res.returncode

    def __validate_job(self, JobInfo):
        if 'submit_id' not in JobInfo:
            raise Exception(f"unknown submit_id for ({JobInfo['BatchID'], JobInfo['JobIndex']})")
        submit_id = JobInfo['submit_id']
        if submit_id not in self._queue:
            raise Exception(f"non-existing job {submit_id}")
        if len(self._queue[submit_id]) != 3:
            raise Exception(f"bad queue entry {submit_id}: {self._queue[submit_id]}")

    def __print(self, string, verbose_level=3):
        if self.verbose >= verbose_level:
            print(string)


class FileJobExecutor(JobExecutor):
    """ writes calls to job scripts into a shell script. """
    def __init__(self, script_file, submitter=False):
        self.path = script_file
        self._queue = OrderedDict()

        # init execution script
        if os.path.dirname(self.path):
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
        with open(self.path, 'w') as fid:
            # will delete any existing file
            fid.write('#!/bin/bash\n\n')
        os.chmod(self.path, 0o744)

        if 'PBS_JOBID' in os.environ and not submitter:
            self.job_id = os.environ['PBS_JOBID'].split('.')[0]
        else:
            self.job_id = 'paraschut'

        self.connected_to_cluster = True

    def submit(self, JobInfo, Spawn=False):
        if self.job_id != 'paraschut':
            print('cannot submit from within a job. ClusterID must be set to "paraschut".')
            return 'failed'

        submit_id = str(int(10**3*time.time() % 10**10))
        with open(self.path, 'a') as fid:
            # setting environmet variables
            fid.write(f'export PBS_JOBID={submit_id} ' +
                      ' '.join(['{}={}'.format(k, repr(v))
                                for k, v in sorted(JobInfo['vars'].items())]) + '\n')
            # running script
            fid.write(JobInfo['script'] +
                      f"  # ({JobInfo['BatchID']}, {JobInfo['JobIndex']})\n")
        update_fields(JobInfo, submit_id, Spawn, None, None)
        self._queue[submit_id] = [f"{JobInfo['BatchID']}, {JobInfo['JobIndex']}", 'Q']

        return submit_id

    def delete(self, JobInfo):
        """ re-write script with one job omitted. """
        tag = f"{JobInfo['BatchID']}, {JobInfo['JobIndex']}"
        with open(self.path + '.tmp', 'w') as wid:
            with open(self.path, 'r') as rid:
                for line in rid.readlines():
                    if not tag in line:
                        wid.write(line)
                    else:
                        JobInfo['state'] = 'init'

        os.remove(self.path)
        os.rename(self.path + '.tmp', self.path)
        os.chmod(self.path, 0o744)
        dal.remove_internal_id(JobInfo, [k for k, v in self._queue.items() if tag in v])
        self._queue = {k: v for k, v in self._queue.items() if tag not in v}
        dal.update_job(JobInfo)

    def qstat(self):
        return self._queue

    def get_job_id(self):
        return self.job_id

    def isconnected(self):
        return self.connected_to_cluster


def update_fields(JobInfo, submit_id, Spawn, OutFile, ErrFile):
    JobInfo['submit_id'] = submit_id
    # submit_id used by LocalJobExecutor so must update here (even if not in DB)

    if not Spawn:
        JobInfo['state'] = 'submit'
        utils.dict_append(JobInfo, 'stdout', OutFile)
        utils.dict_append(JobInfo, 'stderr', ErrFile)
        JobInfo.pop('ClusterID', None)
        JobInfo.pop('qstat', None)

    elif JobInfo['state'] != 'spawn':
        JobInfo['state'] = 'spawn'

    dal.update_job(JobInfo, Release=True)
