# -*- coding: utf-8 -*-
"""
PARASCHUT: parallel job scheduling utils.
see also: README.md, example.ipynb

this submodule hold various general functions.

@author: Alon Diament, Tuller Lab
Created on Wed Mar 18 22:45:50 2015
"""
import os
from datetime import datetime
import time

from .config import LogOut, LogErr


def get_time():
    """ readable timestamp in seconds. """
    return int(datetime.now().strftime('%Y%m%d%H%M%S'))


def get_id():
    time.sleep(1)  # precaution against non-unique IDs
    return get_time()


def make_iter(var):
    if type(var) in [str, dict]:
        var = [var]
    try:
        iter(var)
    except:
        var = [var]
    return var


def isiterable(p_object):
    try:
        iter(p_object)
    except TypeError:
        return False
    return True


def dict_append(dictionary, key, value):
    if key not in dictionary:
        dictionary[key] = []
    dictionary[key].append(value)


def get_log_paths(JobInfo, replace={'{submit_id}': '%j'}, prev_stdout=None):
    if prev_stdout is None:
        if 'stdout' in JobInfo:
            prev_stdout = JobInfo['stdout']
        else:
            prev_stdout = []

    OutFile = LogOut
    ErrFile = LogErr
    for k, v in replace.items():
        OutFile = OutFile.replace(k, v)
        ErrFile = ErrFile.replace(k, v)

    # ensuring a unique log file
    OutFile = OutFile.format(**JobInfo).split('.')
    refile = lambda x, i: '.'.join(x[:-1] + [str(i)] + x[-1:])
    i = 0
    while refile(OutFile, i) in prev_stdout:
        i += 1
    OutFile = refile(OutFile, i)
    ErrFile = refile(LogErr.format(**JobInfo).split('.'), i)

    os.makedirs(os.path.dirname(OutFile), exist_ok=True)
    os.makedirs(os.path.dirname(ErrFile), exist_ok=True)

    return OutFile, ErrFile
