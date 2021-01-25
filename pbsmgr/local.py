# -*- coding: utf-8 -*-
"""
a PBS (portable batch system) parallel-computing job manager.
see also: README.md, example.ipynb

this submodule handles local execution of jobs on multiple processes.

@author: Alon Diament, Tuller Lab
Created on Wed Mar 18 22:45:50 2015
"""
from multiprocessing import Pool

from .config import LocalRun


def run_local_job(BatchId, JobIndex):
    # this should be called from submit_one_job()
    pass


def local_submit():
    # this may call submit_jobs() repeatedly
    pass
