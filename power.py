# -*- coding: utf-8 -*-
"""
Created on Wed Mar 18 22:45:50 2015

@author: dalon
"""

import re
import os
import subprocess
import pickle

if 'PBS_JOBID' in os.environ:
    # this also serves as a sign that we're running on power
    JobID = os.environ['PBS_JOBID']
else:
    JobID = None

if os.name == 'posix':
    DrivePath = '/tamir1/dalon/'
else:
    DrivePath = 'T:/'
QFile = '../jobs/job_queue.pickle'


def submit_jobs(MaxJobs=1, JobDir='../jobs/', PowerQ='tamirs1'):
    DefResource = {'mem': '6gb', 'pmem': '6gb', 'vmem': '12gb',
                   'pvmem': '12gb', 'cput': '04:59:00'}
    JobDir = os.path.abspath(JobDir) + '/'
    ErrDir = JobDir + 'logs/err'
    OutDir = JobDir + 'logs/out'

    Qsub = ['qsub', '-q', PowerQ, '-e', ErrDir, '-o', OutDir, '-l']
    Q = get_jobs_stat(JobDir)  # also updates global job queue file

    """
    JOB/PART PRIORITY RULES
    1. higher gets precedence - equal priority submitted in parallel.
    2. each part has a priority - determines precedence within job.
       (we will not start submitting part with priority X until all
       part with priority Y>X finished.)
    3. job priority - max of part-priorities. precedence between jobs
       is given only when priorities are above 100.
    """
    job_priority = {j: max([0] + [p['priority'] for p in Q[j]
                                  if p['status'] == 'init' or
                                  p['status'] == 'submit'])
                    for j in list(Q)}
    max_priority = max(job_priority.values())
    if max_priority >= 100:
        isGlobalPriority = True
    else:
        isGlobalPriority = False

    count = 0
    for JobID in sorted(list(Q)):
        if count >= MaxJobs:
            break
        if isGlobalPriority and job_priority[JobID] < max_priority:
            continue
        PartList = Q[JobID]
        if len(PartList) == 0:
            continue
        for p, part in enumerate(PartList):
            if part['priority'] < job_priority[JobID]:
                continue
            part = get_part_info(JobID, part['JobPart'])
            if (part['status'] == 'init'):
                print('submiting:\t{}'.format(part['script']))
                if 'resources' in part:
                    this_res = part['resources']
                else:
                    this_res = DefResource
                this_sub = Qsub + [','.join(['{}={}'.format(k, v)
                                   for k, v in sorted(this_res.items())])]
                subprocess.call(this_sub + [part['script']])
                part['status'] = 'submit'
                update_part(part)  # updates local job file
                PartList[p] = part
                count += 1
                if count >= MaxJobs:
                    break
        update_job(PartList)  # updates global job queue

    print('submitted {} jobs'.format(count))


def get_jobs_stat(JobDir='../jobs/', ReInit=False):
    """
    goes over QFile. if there's something new to update - update it.
    this function, along with generate_jobs and submit_jobs, are the only ones
    to write to QFile.
    """
    global QFile
    QFile = JobDir + 'job_queue.pickle'
    Q = pickle.load(open(QFile, 'rb'))
    for JobID in list(Q):
        if len(Q[JobID]) == 0:
            del(Q[JobID])
            continue
        Completed = sum([1 for part in Q[JobID]
                         if (part['status'] == 'complete') or
                         (part['status'] == 'collected')])
        Completed /= len(Q[JobID])
        if (Completed == 1):
            continue
        for p, part in enumerate(Q[JobID]):
            JobFile = part['jobfile']
            if os.path.isfile(JobFile):
                JobInfo = pickle.load(open(JobFile, 'rb'))
                if ReInit and JobInfo['status'] != 'complete':
                    JobInfo['status'] = 'init'
                    update_part(JobInfo)
                Q[JobID][p] = JobInfo
            else:
                Q[JobID][p] = []
        Q[JobID] = [p for p in Q[JobID] if len(p) > 0]
    pickle.dump(Q, open(QFile, 'wb'))
    return Q


def parse_qstat(text):
    JobInfo = {}
    text = text.decode('utf-8')
    print('\n')
    for line in text.splitlines():
        print(line)
        hit = re.match('([\w.]*) = ([\w\s:]*)', line.strip())
        if hit is not None:
            JobInfo[hit.group(1)] = hit.group(2)
    return JobInfo


def get_qstat():
    if JobID is None:
        print('not running on a power node')
        return {}
    return parse_qstat(subprocess.check_output(['qstat', '-f', JobID]))


def get_queue():
    # reads from global job queue file
    Q = pickle.load(open(QFile, 'rb'))
    for JobID in sorted(list(Q)):
        parts = Q[JobID]
        if len(parts) == 0:
            continue
        print('\n{}: {}/{}'.format(JobID, parts[0]['organism'],
                                   '-'.join(parts[0]['name'])))
        status = {}
        for p in parts:
            dict_append(status, p['status'], p['JobPart'])
        print(status)


def get_job_info(JobID):
    Q = pickle.load(open(QFile, 'rb'))
    return [pickle.load(open(Q[JobID][part['JobPart']]['jobfile'], 'rb'))
            for part in Q[JobID]]


def get_job_file(JobID, JobPart):
    Q = pickle.load(open(QFile, 'rb'))
    return Q[JobID][JobPart]['jobfile']


def get_part_info(JobID, JobPart):
    return pickle.load(open(get_job_file(JobID, JobPart), 'rb'))


def update_part(PartInfo):
    # updates the specific part's local job file (not global queue)
    pickle.dump(PartInfo, open(PartInfo['jobfile'], 'wb'))


def update_job(JobInfo):
    # updates the global job queue
    Q = pickle.load(open(QFile, 'rb'))
    Q[JobInfo[0]['JobID']] = JobInfo
    pickle.dump(Q, open(QFile, 'wb'))


def set_job_status(JobID, NewStatus):
    # updates the specific parts' local job files (not global queue)
    Q = pickle.load(open(QFile, 'rb'))
    for part in Q[JobID]:
        part = get_part_info(JobID, part['JobPart'])
        part['status'] = NewStatus
        update_part(part)


def dict_append(dictionary, key, value):
    if key not in dictionary:
        dictionary[key] = []
    dictionary[key].append(value)
