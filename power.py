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
    JobID = os.environ['PBS_JOBID'].split('.')[0]
else:
    JobID = None
if 'HOSTNAME' in os.environ:
    hostname = os.environ['HOSTNAME']
else:
    hostname = None
if (JobID is not None) or (hostname == 'power.tau.ac.il'):
    running_on_power = True
else:
    running_on_power = False

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
    Q = get_queue(Verbose=False)

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
        if len(Q[JobID]) == 0:
            continue
        for part in Q[JobID]:
            if part['priority'] < job_priority[JobID]:
                continue
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
                count += 1
                if count >= MaxJobs:
                    break

    print('submitted {} jobs'.format(count))


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


def get_power_queue():
    Q = {}
    if not running_on_power:
        return Q
    data = subprocess.check_output(['qstat', '-u', os.environ['USER']],
                                   universal_newlines=True)
    data = data.split('\n')
    for line in data:
        job = re.match('(\d+).power', line)
        if job:
            line = re.split('\s+', line)
            Q[job.group(1)] = line[3]
    return Q


def get_queue(Verbose=True):
    # reads from global job queue file
    Q = pickle.load(open(QFile, 'rb'))
    count_total = sum([len(j) for j in Q.values()])
    powQ = get_power_queue()
    count_run = 0
    for JobID in sorted(list(Q)):
        if len(Q[JobID]) == 0:
            continue

        status = {}
        for p, pfile in enumerate(Q[JobID]):
            pinfo = pickle.load(open(pfile, 'rb'))
            if 'PowerID' in pinfo:
                if pinfo['PowerID'] in powQ:
                    pinfo['JobPart'] = float(pinfo['JobPart'])
                    count_run += 1
            dict_append(status, pinfo['status'], pinfo['JobPart'])
            Q[JobID][p] = pinfo

        if Verbose:
            print('\n{}: {}/{}'.format(JobID, pinfo['organism'],
                                       '-'.join(pinfo['name'])))
            print(status)

    if Verbose:
        print('\ntotal jobs running on power: {}\n'.format(len(powQ)) +
              'known running jobs: {}/{}'.format(count_run, count_total))
    else:
        return Q


def get_job_info(JobID):
    Q = pickle.load(open(QFile, 'rb'))
    return [get_part_info(JobID, part) for part in range(len(Q[JobID]))]


def get_job_file(JobID, JobPart):
    Q = pickle.load(open(QFile, 'rb'))
    return Q[JobID][JobPart]


def get_part_info(JobID, JobPart):
    return pickle.load(open(get_job_file(JobID, JobPart), 'rb'))


def update_part(PartInfo):
    # updates the specific part's local job file (not global queue)
    pickle.dump(PartInfo, open(PartInfo['jobfile'], 'wb'))


def set_part_status(JobID, JobPart, NewStatus='init', Unless=''):
    try:
        test = iter(Unless)
    except:
        Unless = [Unless]
    part = get_part_info(JobID, JobPart)
    if part['status'] in Unless:
        return
    part['status'] = NewStatus
    update_part(part)


def set_job_status(JobID, NewStatus='init', Unless=''):
    # updates the specific parts' local job files (not global queue)
    try:
        test = iter(JobID)
    except:
        JobID = [JobID]
    Q = pickle.load(open(QFile, 'rb'))
    for j in JobID:
        for part in range(len(Q[j])):
            set_part_status(j, part, NewStatus, Unless)


def dict_append(dictionary, key, value):
    if key not in dictionary:
        dictionary[key] = []
    dictionary[key].append(value)
