# -*- coding: utf-8 -*-
"""
a PBS (portable batch system) parallel-computing job manager.
see also: README.md, example.ipynb

this submodule hold various general functions.

@author: Alon Diament, Tuller Lab
Created on Wed Mar 18 22:45:50 2015
"""
from datetime import datetime


def get_time():
    """ readable timestamp in seconds. """
    return int(datetime.now().strftime('%Y%m%d%H%M%S'))


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
