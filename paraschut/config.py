# -*- coding: utf-8 -*-
"""
PARASCHUT: parallel job scheduling utils.
see also: README.md, example.ipynb

this submodule contains all configurable variables.

@author: Alon Diament, Tuller Lab
Created on Wed Mar 18 22:45:50 2015
"""
import os
import socket

LocalRun = True  # instead of submitting to cluster, run on local machine
ServerHost = 'tau.ac.il'
QFile = 'job_queue.db'  # T:/dalon/RP/Consistency/jobs/
JobDir = 'jobs/'  # ../jobs/
PBS_suffix = '.power8.tau.ac.il'
LogDir = JobDir + '{BatchID}/logs/{submit_id}' + PBS_suffix
LogOut = LogDir + '.OU'  # template
LogErr = LogDir + '.ER'
TempFiles = ['*.genes.*', '*.len*.npz']
PBS_queue = 'tamir-nano4'
WriteTries = 20

# example for a job template
DefResource = {'mem': '1gb', 'pmem': '1gb', 'vmem': '3gb',
               'pvmem': '3gb', 'cput': '04:59:00'}
JobTemplate =   {'BatchID': None,
                 'JobIndex': None,
                 'priority': 1,
                 'name': ['human', 'genome_map'],
                 'data_type': 'foo',
                 'data': None,
                 'script': 'my_template_script.sh',  # template sciprt, see generate_script()
                 'queue': PBS_queue,
                 'resources': DefResource,
                 'state': 'init'}

# automatically-set variables

hostname = socket.gethostname()
LocalPath = os.path.splitdrive(os.path.abspath('.'))[1]
