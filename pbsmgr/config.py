# -*- coding: utf-8 -*-
"""
a PBS (portable batch system) parallel-computing job manager.
see also: README.md, example.ipynb

this submodule contains all configurable variables.

@author: Alon Diament, Tuller Lab
Created on Wed Mar 18 22:45:50 2015
"""
import os

ServerHost = 'tau.ac.il'
QFile = '/tamir1/dalon/RP/Consistency/jobs/job_queue.db'  # '../jobs/job_queue.pkl'
JobDir = '../jobs/'
PBS_suffix = '.power8.tau.ac.il'
LogDir = JobDir + '{BatchID}/logs/{submit_id}' + PBS_suffix
LogOut = LogDir + '.OU'  # template
LogErr = LogDir + '.ER'
PBS_queue = 'tamir-nano4'
WriteTries = 20

# example for a job template
DefResource = {'mem': '1gb', 'pmem': '1gb', 'vmem': '3gb',
               'pvmem': '3gb', 'cput': '04:59:00'}
JobTemplate =   {'BatchID': None,
                 'JobIndex': None,
                 'priority': 1,
                 'name': ['human', 'genome_map'],
                 'data': None,
                 'script': 'my_template_script.sh',  # template sciprt, see generate_script()
                 'queue': PBS_queue,
                 'resources': DefResource,
                 'state': 'init'}

# automatically-set variables

if 'PBS_JOBID' in os.environ:
    # this also serves as a sign that we're running on cluster
    PBS_ID_raw = os.environ['PBS_JOBID']
    PBS_ID = PBS_ID_raw.replace(PBS_suffix, '')
else:
    PBS_ID_raw = None
    PBS_ID = None
if 'HOSTNAME' in os.environ:
    hostname = os.environ['HOSTNAME']
else:
    hostname = []
if (PBS_ID is not None) or (ServerHost in hostname):
    running_on_cluster = True
else:
    running_on_cluster = False
LocalPath = os.path.splitdrive(os.path.abspath('.'))[1]
