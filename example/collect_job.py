# -*- coding: utf-8 -*-
"""
example job that prints the mean of the given data

@author: dalon
"""

import pickle as pkl
import sys
import os
from time import sleep
import numpy as np
sys.path.append(os.getcwd())
import paraschut as psu


def main(args):
    assert len(args) == 2 and args[0].isnumeric() and args[1].isnumeric(), \
        '2 arguments required for job: BatchID (integer), JobIndex (integer)'
    BatchID = int(args[0])
    JobIndex = int(args[1])

    jobinfo = psu.get_job_info(BatchID, JobIndex, SetID=True, ClusterID=psu.ClusterID)

    data = [psu.get_job_info(BatchID, j)['result'] for j in jobinfo['data']]
    res = np.mean(data)
    jobinfo['result'] = res
    print(res)

    psu.set_complete(JobInfo=jobinfo, Submit=True)


if __name__ == '__main__':
    main(sys.argv[1:])
