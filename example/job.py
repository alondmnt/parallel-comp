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
import pbsmgr as pbs

pbs.QFile = 'example/job_queue.pkl'
pbs.JobDir = 'example/'

def main(args):
    assert len(args) == 2 and args[0].isnumeric() and args[1].isnumeric(), \
        '2 arguments required for job: BatchID (integer), JobIndex (integer)'
    BatchID = int(args[0])
    JobIndex = int(args[1])

    jobinfo = pbs.get_job_info(BatchID, JobIndex, SetID=True)
    print(jobinfo)

    data = pkl.load(open(jobinfo['data'], 'rb'))
    print(np.mean(data))
    sleep(60)  # 1 minute

    jobinfo['qstat'] = pbs.get_qstat()
    jobinfo['status'] = 'complete'
    pbs.update_job(jobinfo)


if __name__ == '__main__':
    main(sys.argv[1:])
