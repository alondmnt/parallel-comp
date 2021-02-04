# -*- coding: utf-8 -*-
"""
a PBS (portable batch system) parallel-computing job manager.
see also: README.md, example.ipynb

this submodule handles local execution of jobs on multiple processes.

@author: Alon Diament, Tuller Lab
Created on Wed Mar 18 22:45:50 2015
"""
from concurrent.futures import ThreadPoolExecutor
import os
from subprocess import run


def get_executor(max_workers=os.cpu_count()):
    # returns a pool executer with a submit_method
    # currently using ThreadPoolExecutor to start
    # new subprocesses
    return ThreadPoolExecutor(max_workers=max_workers)


def run_local_job(JobInfo):
    # execute a job locally as a new subprocess
    env = os.environ.copy()
    env.update(PBS_JOBID=JobInfo['submit_id'])

    os.makedirs(os.path.dirname(JobInfo['stdout'][-1]), exist_ok=True)
    os.makedirs(os.path.dirname(JobInfo['stderr'][-1]), exist_ok=True)

    with open(JobInfo['stdout'][-1], 'w') as oid:
        with open(JobInfo['stderr'][-1], 'w') as eid:
            print(JobInfo['script'])
            job_res = run(JobInfo['script'], shell=True, env=env, stdout=oid, stderr=eid)

    return job_res.returncode
