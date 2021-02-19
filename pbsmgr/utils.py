# -*- coding: utf-8 -*-
"""
PARASCHUT: parallel job scheduling utils.
see also: README.md, example.ipynb

this submodule hold various general functions.

@author: Alon Diament, Tuller Lab
Created on Wed Mar 18 22:45:50 2015
"""
from datetime import datetime
import time


def get_time():
    """ readable timestamp in seconds. """
    return int(datetime.now().strftime('%Y%m%d%H%M%S'))


def get_id():
    time.sleep(1)  # precaution against non-unique IDs
    return get_time()


def make_iter(var):
    if type(var) == str:
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
