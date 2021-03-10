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
QFile = 'example/job_queue.db'
JobDir = 'example/'
LogFormat = JobDir + '{BatchID}/logs/{name}_{JobIndex:04d}_{subtime}'  # template
LogOut = LogFormat + '.out'
LogErr = LogFormat + '.err'
TempFiles = ['*.genes.*', '*.len*.npz']
DefQueue = 'tamir-nano4'
WriteTries = 20 

# example for a job template
DefResource = {'mem': '1gb', 'pmem': '1gb', 'vmem': '3gb',
               'pvmem': '3gb', 'cput': '04:59:00'}
JobTemplate =   {'BatchID': None,
                 'JobIndex': None,
                 'priority': 1,
                 'name': 'most_excellent_job',
                 'batch_type': 'foo',
                 'data': None,
                 'script': 'my_template_script.sh',  # template sciprt, see generate_script()
                 'queue': DefQueue,
                 'resources': DefResource,
                 'state': 'init'}

# automatically-set variables

Hostname = socket.gethostname()
