# -*- coding: utf-8 -*-
"""
a PBS (portable batch system) parallel-computing job manager.
see also: README.md, example.ipynb

@author: Alon Diament, Tuller Lab
Created on Wed Mar 18 22:45:50 2015
"""

import re
import os
import subprocess
import time
import pickle
pickle.HIGHEST_PROTOCOL = 2  # for compatibility with python2
from datetime import datetime
import json
import hashlib
import shutil
import sqlite3
import warnings
import zlib
import pandas as pd
from collections import Counter
from copy import deepcopy


ServerPath = '/tamir1/'
ServerHost = 'tau.ac.il'
QFile = 'T:/dalon/RP/Consistency/jobs/job_queue.db'  # '../jobs/job_queue.pkl'
JobDir = '../jobs/'
PBS_queue = 'tamirs3'

if 'PBS_JOBID' in os.environ:
    # this also serves as a sign that we're running on cluster
    PBS_ID = os.environ['PBS_JOBID'].split('.')[0]
    LogOut = '{}.OU'.format(os.environ['PBS_JOBID'])
    LogErr = '{}.ER'.format(os.environ['PBS_JOBID'])
else:
    PBS_ID = None
    LogOut = None
    LogErr = None
if 'HOSTNAME' in os.environ:
    hostname = os.environ['HOSTNAME']
else:
    hostname = []
if (PBS_ID is not None) or (ServerHost in hostname):
    running_on_cluster = True
else:
    running_on_cluster = False
LocalPath = os.path.splitdrive(os.path.abspath('.'))[1]


# example for a job template
DefResource = {'mem': '1gb', 'pmem': '1gb', 'vmem': '3gb',
               'pvmem': '3gb', 'cput': '04:59:00'}
JobTemplate =   {'BatchID': None,
                 'JobIndex': None,
                 'priority': 1,
                 'name': ['human', 'genome_map'],
                 'data': None,
                 'script': 'my_template_script.sh',  # template sciprt, see generate_script()
                 'queue': PBS_queue,
                 'jobfile': None,
                 'resources': DefResource,
                 'status': 'init'}


def submit_jobs(MaxJobs=None, MinPrior=0, OutFile=None, ForceSubmit=False, **Filter):
    """ submits all next available jobs according to job priorities.
        MaxJobs is loaded from [JobDir]/maxjobs unless specified (default=1000).
        ForceSubmit ignores another process currently submitting jobs (or an
        abandoned lock file).
        Filter are fields that get_queue() accepts. """
    if not running_on_cluster:
        print('submit_jobs: not running on cluster.')
        return

    lock_file = JobDir + 'submitting'
    if os.path.isfile(lock_file) and OutFile is None and not ForceSubmit:
        sec_since_submit = time.time() - \
                os.path.getmtime(lock_file)
        if sec_since_submit > 300:
            os.remove(lock_file)
        else:
            print('already submitting jobs.')
            return
    flag_file = open(lock_file, 'w')
    if MaxJobs is None:
        try:
            with open(JobDir + 'maxjobs', 'r') as fid:
                MaxJobs = int(fid.readline())
        except:
            MaxJobs = 1000

    Q = get_queue(Verbose=False, **Filter)

    """
    BATCH/JOB PRIORITY RULES
    1. higher gets precedence - equal priority submitted in parallel.
    2. each job has a priority - determines precedence within job.
       (we will not start submitting job with priority X until all
       job with priority Y>X finished.)
    3. job priority - max of job-priorities. precedence between jobs
       is given only when priorities are above 100.
    """
    job_priority = {j: max([0] + [p['priority'] for p in Q[j]
                                  if p['status'] not in
                                  {'complete', 'collected'}])
                    for j in list(Q)}
    max_priority = max(job_priority.values())
    if max_priority < MinPrior:
        print('submit_jobs: no job satisfying given min-priority ({}).'.format(
              MinPrior))
        return
    if max_priority >= 100:
        isGlobalPriority = True
    else:
        isGlobalPriority = False

    if OutFile is not None:
        out_file = open(OutFile, 'w')
        out_file.write('#!/bin/bash\n')
    else:
        out_file = None
    count_in_queue = len(get_pbs_queue())
    count = count_in_queue
    for j in sorted(list(Q)):
        if count >= MaxJobs:
            break
        if job_priority[j] < MinPrior:
            continue
        if isGlobalPriority and job_priority[j] < max_priority:
            continue
        if len(Q[j]) == 0:
            continue
        count = submit_one_batch(j, count, MaxJobs, OutFile=out_file)

    flag_file.close()
    if OutFile is not None:
        out_file.close()
        os.chmod(OutFile, 0o744)
    os.remove(lock_file)
    print('max jobs: {}\nin queue: {}\nsubmitted: {}'.format(MaxJobs,
          count_in_queue,
          count - count_in_queue))


def make_iter(var):
    if type(var) == str:
        var = [var]
    try:
        test = iter(var)
    except:
        var = [var]
    return var


def submit_one_batch(BatchID, SubCount=0, MaxJobs=1e6, OutFile=None):
    for batch in make_iter(BatchID):
        BatchInfo = get_batch_info(batch)
        job_priority = max([0] + [j['priority'] for j in BatchInfo
                                  if j['status'] not in
                                  {'complete', 'collected'}])
        for job in BatchInfo:
            if job['priority'] < job_priority:
                continue
            if job['status'] == 'init':
                submit_one_job(batch, job['JobIndex'], OutFile=OutFile)
                SubCount += 1
                if SubCount >= MaxJobs:
                    break
    return SubCount


def submit_one_job(BatchID, JobIndex, Spawn=False, OutFile=None):
    global JobDir
    global PBS_queue

    ErrDir = os.path.abspath(JobDir) + '/{}/logs/'.format(BatchID)
    OutDir = os.path.abspath(JobDir) + '/{}/logs/'.format(BatchID)
    if not os.path.isdir(ErrDir):
        os.makedirs(ErrDir)

    Qsub = ['qsub', '-q', PBS_queue, '-e', ErrDir, '-o', OutDir, '-l']

    for j in make_iter(JobIndex):
        job = get_job_info(BatchID, j, HoldFile=True)
        print('submiting:\t{}'.format(job['script']))
        if job['status'] != 'init':
            warnings.warn('already submitted')
        if 'resources' in job:
            this_res = job['resources']
        else:
            this_res = DefResource
        this_sub = Qsub + [','.join(['{}={}'.format(k, v)
                           for k, v in sorted(this_res.items())])]
        if 'queue' in job:
            this_sub[2] = job['queue']
        if OutFile is None:
            job['submit_id'] = subprocess.check_output(this_sub + [job['script']]).decode('UTF-8').split('.')[0]
        else:
            OutFile.write(job['script'] + '\n')

        if not Spawn:
            job['status'] = 'submit'
            job['subtime'] = time.time()
            if 'PBS_ID' in job:
                del job['PBS_ID']
            if 'qstat' in job:
                del job['qstat']
        else:
            if job['status'] != 'spawn':
                # remove previous information
                job['status'] = 'spawn'
                job['PBS_ID'] = []
                job['hostname'] = []
                job['spawn_id'] = []
                job['spawn_complete'] = set()
                job['spawn_resub'] = set()

        update_job(job, Release=True)


def parse_qstat(text):
    JobInfo = {}
    text = text.decode('utf-8')
#    print('\n')
    for line in text.splitlines():
#        print(line)
        hit = re.match('([\w.]*) = ([\w\s:_\-/]*)', line.strip())
        if hit is not None:
            JobInfo[hit.group(1)] = hit.group(2)
    return JobInfo


def get_qstat(BatchID=PBS_ID):
    if BatchID is None:
        print('not running on a cluster node')
        return {}
    try:
        return parse_qstat(subprocess.check_output(['qstat', '-f', BatchID]))
    except subprocess.CalledProcessError as e:
        # sometimes this fails on cluster, not clear why (cluster does not recognize the BatchID)
        print(e)
        return None


def get_pbs_queue():
    Q = {}
    if not running_on_cluster:
        return Q
    data = subprocess.check_output(['qstat', '-u', os.environ['USER']],
                                   universal_newlines=True)
    data = data.split('\n')
    for line in data:
        job = re.match('(\d+).', line)  # power
        if job:
            line = re.split('\s+', line)
            Q[job.group(1)] = [line[3], line[9]]
    return Q


def get_queue(Verbose=True, ResetMissing=False, Display=None, **Filter):
    """ Verbose mode prints a job report to screen, otherwise a dictionary Q
        is returned with the metadata of all jobs.
        ResetMissing will set the state of jobs that failed while online.
        Display is an iterable of states that desired for display (other
        states will not be reported in Verbose mode).
        additional keyword arguments are used to filter batches by their
        fields.

        Example:
            get_queue(Display={'complete'})
            will display only completed jobs.

            get_queue(name='awesome')
            will display only the batch named awesome.
            for a differences Display filtering and Filter see the README,
            and look for [skip_flag] in the code. """

    # reads from global job queue file
    Q = get_sql_queue(QFile, Filter)
    curr_time = time.time()
    powQ = get_pbs_queue()

    missing = {}  # submitted but not running
    cnt = Counter()
    cnt['total'] = 0
    cnt['complete'] = 0
    cnt['online'] = 0
    for BatchID in sorted(list(Q)):
        status = {}
        for p, pinfo in enumerate(Q[BatchID]):
            cnt['total'] += 1
            job_index = pinfo['JobIndex']

            # checking job against current PBS queue
            # (for online and crashed jobs)
            if 'PBS_ID' in pinfo:
                for pid in make_iter(pinfo['PBS_ID']):
                    if pid in powQ:
                        if powQ[pid][1] == 'R':
                            job_index = str(job_index) + '*'
                            cnt['online'] += 1
                    elif pinfo['status'] in {'submit', 'spawn', 'run'} and \
                            'subtime' in pinfo:
                        if pinfo['subtime'] < curr_time:
                            dict_append(missing, BatchID, job_index)
            cnt[pinfo['status']] += 1
            dict_append(status, pinfo['status'], job_index)

        if len(Q[BatchID]) == 0:
            print('\nempty {}'.format(BatchID))
            del Q[BatchID]
            continue

        if Verbose:
            if Display is not None:
                status = {s: p for s, p in status.items() if s in Display}
            if len(status) > 0:
                print('\n{}: {}'.format(BatchID, '/'.join(pinfo['name'])))
                print(status)

    if ResetMissing:
        for BatchID in missing:
            for JobIndex in missing[BatchID]:
                jobDir = JobDir + '{}/{}'.format(BatchID, JobIndex)
                if os.path.isdir(jobDir):
                    shutil.rmtree(jobDir)
                set_job_field(BatchID, JobIndex, {'status': 'init'})

    if Verbose:
        print('\nmissing jobs: {}'.format(missing))
        cnt['complete'] += cnt['collected']
        print('\ntotal jobs on PBS queue: {}'.format(len(powQ)))
        try:
             print('running/complete/total: {online}/{complete}/{total}'.format(**cnt))
        except:
            pass
    else:
        return Q


def get_sql_queue(QFile, Filter={}):
    with sqlite3.connect(QFile) as conn:
        df = pd.read_sql_query('SELECT * FROM ' +
                               """(SELECT j.*,
                                          b.name,
                                          b.organism,
                                          b.data_type
                                   FROM batch b INNER JOIN job j 
                                   ON j.BatchID = b.BatchID) """ +
                               ('WHERE ' + ' AND '.join(
                                parse_filter(Filter))
                                if len(Filter) else ''), conn)
    return df.iloc[:, 1:].groupby('BatchID')\
        .apply(lambda df: df.sort_values('JobIndex')[['metadata', 'md5']]\
               .apply(unpack_job, axis=1)\
               .tolist()).to_dict()


def parse_filter(Filter):
    return ['(' + ' OR '.join([f'({k} = "{sub_v}")' if isinstance(sub_v, str)
                               else f'({k} = {sub_v})' for sub_v in make_iter(v)])
            + ')' for k, v in Filter.items()]


def qdel_batch(BatchID):
    """ this will run the PBS qdel command on the entire batch of jobs. """
    for job in get_batch_info(BatchID):
        qdel_job(JobInfo=job)


def qdel_job(BatchID=None, JobIndex=None, JobInfo=None):
    """ this will run the PBS qdel command on the given job. """
    pid_keys = ['PBS_ID', 'submit_id']

    if JobInfo is None:
        JobInfo = get_job_info(BatchID, JobIndex, HoldFile=True)
    if 'PBS_ID' not in JobInfo and 'submit_id' not in JobInfo:
        print('qdel_job: unknown PBS_ID for {BatchID},{JobIndex}'.format(**JobInfo))
        return

    pid_list = []
    for k in pid_keys:
        if k in JobInfo:
            JobInfo[k] = make_iter(JobInfo[k])
            pid_list += JobInfo[k]

    for pid in pid_list:
        if not subprocess.call(['qdel', pid]):  # success
            for k in pid_keys:
                if k in JobInfo:
                    try:
                        JobInfo[k].remove(pid)
                    except ValueError:
                        pass

    for k in pid_keys:
        if k in JobInfo and len(JobInfo[k]) == 1:  # back to single ID
            JobInfo[k] = JobInfo[k][0]
        if k in JobInfo and not len(JobInfo[k]):  # all deleted
            del JobInfo[k]
#            if k == 'submit_id':
            JobInfo['status'] = 'init'

    update_job(JobInfo, Release=True)


def get_batch_info(BatchID):
    with sqlite3.connect(QFile) as conn:
        batch_query = list(conn.execute(f"""SELECT metadata, md5 from job WHERE
                                            BatchID={BatchID}"""))

    return [unpack_job(job) for job in batch_query]


def get_job_info(BatchID, JobIndex, HoldFile=False, SetID=False):
    """ HoldFile is ignored but kept for backward compatibility. """
    with sqlite3.connect(QFile) as conn:
        job_query = list(conn.execute(f"""SELECT metadata, md5 from job WHERE
                                          BatchID={BatchID} AND
                                          JobIndex={JobIndex}"""))
        if len(job_query) != 1:
            raise Exception('job is not unique (%d)' % len(job_query))
        job_query = job_query[0]
    JobInfo = unpack_job(job_query)

    if not SetID:
        return JobInfo

    if JobInfo['status'] == 'spawn':
        JobInfo['PBS_ID'].append(PBS_ID)
        JobInfo['hostname'].append(hostname)
        # while the following IDs are set asynchronously, we can test if
        # were set correctly by comparing spawn_count to length of spawn_id
        next_id = 0
        while next_id in JobInfo['spawn_id']:  # find missing
            next_id += 1
        if next_id >= JobInfo['spawn_count']:
                raise Exception('bad spawn_id {} >= {}, too many submitted'.format(
                                next_id, JobInfo['spawn_count']))
        JobInfo['spawn_id'].append(next_id)
        print('spawn_id', next_id)
    else:
        JobInfo['PBS_ID'] = PBS_ID
        JobInfo['hostname'] = hostname
        JobInfo['status'] = 'run'

    dict_append(JobInfo, 'stdout', '{}/{}/logs/{}'.format(JobDir, BatchID, LogOut))
    dict_append(JobInfo, 'stderr', '{}/{}/logs/{}'.format(JobDir, BatchID, LogErr))
    update_job(JobInfo, Release=True)

    return JobInfo


def get_job_template(SetID=False):
    res = deepcopy(JobTemplate)
    if SetID:
        time.sleep(1)  # precaution against non-unique IDs
        res['BatchID'] = int(datetime.now().strftime('%Y%m%d%H%M%S'))
    return res


def update_job(JobInfo, Release=False):
    """ Release is ignored but kept for backward compatibility. """

    if JobInfo['jobfile'] is None:
        JobInfo['jobfile'] = JobDir + '{}/meta_{}.pkl'.format(
                JobInfo['BatchID'], JobInfo['JobIndex'])

    with sqlite3.connect(QFile) as conn:
        BatchID, JobIndex = JobInfo['BatchID'], JobInfo['JobIndex']
        md5 = list(conn.execute(f"""SELECT md5 FROM job WHERE
                                    BatchID={BatchID} AND
                                    JobIndex={JobIndex}"""))
        if len(md5) != 1:
            raise Exception('job is not unique (%d)' % len(md5))
        # here we're ensuring that JobInfo contains an updated version
        # of the data, that is consistent with the DB
        if md5[0][0] != JobInfo['md5']:
            raise Exception(f'job ({BatchID}, {JobIndex}) was overwritten by another process')

        metadata = pack_job(JobInfo)
        conn.execute(f"""UPDATE job
                         SET metadata=?, md5=?, status=?, priority=? WHERE
                         BatchID={BatchID} AND
                         JobIndex={JobIndex}""",
                         [metadata, JobInfo['md5'],
                          JobInfo['status'], JobInfo['priority']])


def pack_job(JobInfo):
    if 'md5' in JobInfo:
        del JobInfo['md5']
    metadata = zlib.compress(pickle.dumps(JobInfo))
    JobInfo['md5'] = hashlib.md5(metadata).hexdigest()
    return metadata


def unpack_job(job_query):
    assert job_query[-1] == hashlib.md5(job_query[-2]).hexdigest()
    JobInfo = pickle.loads(zlib.decompress(job_query[-2]))
    JobInfo['md5'] = job_query[-1]
    return JobInfo


def update_batch(batch):
    for job in batch:
        update_job(job)


def add_batch_to_queue(BatchID, Jobs):
    """ provide a list of job info dicts, will replace any existing
        batch. unlike add_job_to_queue it ensures that BatchID doe not
        exist in queue, and that all given jobs are from the same batch. """
    if not os.path.isfile(QFile):
        init_db(QFile)

    Jobs = make_iter(Jobs)
    if not all([BatchID == job['BatchID'] for job in Jobs]):
        raise Exception(f'some jobs do not match BatchID={BatchID}')

    if len(get_batch_info(BatchID)) > 0:
        raise Exception(f'BatchID={BatchID} already exists')

    add_job_to_queue(Jobs)
    print('\nbatch {} (size {:,d}) added to queue ({})'.format(BatchID,
          len(Jobs), QFile))


def add_job_to_queue(Jobs):
    """ job info or a list of job info dicts supported.
        automatically sets the JobIndex id in each JobInfo,
        and saves the metdata. """
    if not os.path.isfile(QFile):
        init_db(QFile)

    batch_index = {}
    with sqlite3.connect(QFile) as conn:
        for job in make_iter(Jobs):
            BatchID = job['BatchID']

            # setting JobIndex automatically
            if BatchID not in batch_index:
                batch_index[BatchID] = len(get_batch_info(BatchID))
            if batch_index[BatchID] == 0:
                # init new batch
                conn.execute("""INSERT INTO batch
                                VALUES (?,?,?,?)""",
                                [BatchID, '/'.join(job['name']),
                                 job['organism'], job['data_type']])

            job['JobIndex'] = batch_index[BatchID]
            batch_index[BatchID] += 1
            metadata = pack_job(job)
            conn.execute("""INSERT INTO job(JobIndex, BatchID, status,
                                            priority, metadata, md5)
                            VALUES (?,?,?,?,?,?)""",
                         [job['JobIndex'], BatchID, job['status'],
                          job['priority'], metadata, job['md5']])


def set_complete(BatchID, JobIndex):
    JobInfo = get_job_info(BatchID, JobIndex, HoldFile=True)
    JobInfo['qstat'] = get_qstat()
    JobInfo['status'] = 'complete'
    update_job(JobInfo, Release=True)


def set_job_field(BatchID, JobIndex, Fields={'status': 'init'},
                  Unless={'status': ['complete', 'collected']}):
    """ called with default args, this sets all jobs in progress
        (unless completed) to `init` state. """
    for j in make_iter(JobIndex):
        job = get_job_info(BatchID, j, HoldFile=False)

        skip_job = False
        for k in Unless:  # this tests whether the job is protected
            if k in job:
                if job[k] in make_iter(Unless[k]):
                    skip_job = True
        if skip_job:
            continue

        for k, v in Fields.items():
            job[k] = v
        update_job(job, Release=True)


def set_batch_field(BatchID, Fields={'status': 'init'},
                    Unless={'status': ['complete', 'collected']}):
    """ calls set_job_field on the batch. """
    for b in make_iter(BatchID):
        BatchInfo = get_batch_info(b)
        for job in BatchInfo:
            set_job_field(b, job['JobIndex'], Fields, Unless)


def remove_batch(BatchID):
    with sqlite3.connect(QFile) as conn:
        for batch in make_iter(BatchID):
            WorkDir = JobDir + str(batch)
            if os.path.isdir(WorkDir):
                shutil.rmtree(WorkDir)
            conn.execute(f"""DELETE FROM batch
                             WHERE BatchID={batch}""")
            conn.execute(f"""DELETE FROM job
                             WHERE BatchID={batch}""")


# TODO: archive_batch(BatchID=None, Time=None)
# up to Time or any list of BatchIDs

# TODO: load_archive(fname)


def remove_batch_by_state(state):
    """ can provide an itreable of states to consider the union of possible
        states. """
    Q = get_queue(Verbose=False)
    for BatchID in Q.keys():
        if all([True if p['status'] in state else False
                for p in Q[BatchID]]):
            remove_batch(BatchID)


def clean_temp_folders():
    """ clean folders from temp files, that are sometimes left
        when jobs get stuck. """
    Q = get_queue(Verbose=False)
    rmv_list = []
    for BatchID in Q.keys():
        for job in Q[BatchID]:
            tempdir = JobDir + '{}/{}'.format(BatchID, job['JobIndex'])
            if os.path.exists(tempdir):
                shutil.rmtree(tempdir)
                rmv_list.append(tempdir)
    print('removed {} temp dirs'.format(len(rmv_list)))
    return rmv_list


def dict_append(dictionary, key, value):
    if key not in dictionary:
        dictionary[key] = []
    dictionary[key].append(value)


def boost_batch_priority(BatchID, Booster=100):
    for batch in make_iter(BatchID):
        BatchInfo = get_batch_info(batch)
        for JobInfo in BatchInfo:
            JobInfo['priority'] += Booster
            update_job(JobInfo)


def spawn_submit(JobInfo, N):
    """ run the selected job multiple times in parallel. job needs to handle
        'spawn' status for correct logic: i.e., only last job to complete
        updates additional fields in JobInfo. """
    set_job_field(JobInfo['BatchID'], JobInfo['JobIndex'],
                   Fields={'spawn_count': N+1,
                           'stdout': [], 'stderr': []}, Unless={})
    for i in range(N):
        time.sleep(10)  # avoid collisions
        submit_one_job(JobInfo['BatchID'], JobInfo['JobIndex'], Spawn=True)

    # update current job with spawn IDs
    return get_job_info(JobInfo['BatchID'], JobInfo['JobIndex'], SetID=True)


def spawn_complete(JobInfo):
    """ signal that one spawn has ended successfully. only update done by
        current job to JobInfo, unless all spawns completed. """
    my_id = JobInfo['spawn_id'][-1]
    JobInfo = get_job_info(JobInfo['BatchID'], JobInfo['JobIndex'], HoldFile=True)
    if JobInfo['status'] != 'spawn':
        # must not leave function with an ambiguous status in JobInfo
        # e.g., if job has been re-submitted by some one/job
        # (unknown logic follows)
        raise Exception('unknown status [{}] encountered for spawned ' +
                        'job ({}, {})'.format(JobInfo['status'],
                                              JobInfo['BatchID'],
                                              JobInfo['JobIndex']))

    try:
        JobInfo['PBS_ID'].remove(PBS_ID)
        JobInfo['hostname'].remove(hostname)
    except:
        pass
    JobInfo['spawn_complete'].add(my_id)
    update_job(JobInfo, Release=True)

    if len(JobInfo['spawn_id']) != JobInfo['spawn_count']:
        # unexplained frequent problem, tried to work this out by delaying sub
        # cannot resubmit because jobs may still be queued at this point
        print('if queue is *empty*, consider using',
              'pbsmgr.spawn_resubmit({BatchID}, {JobIndex})'.format(**JobInfo))
    elif len(JobInfo['PBS_ID']) == 0:
        # no more running or *queued* spawns
        is_missing = [s for s in JobInfo['spawn_id']
                      if s not in JobInfo['spawn_complete']]
        if len(is_missing):
            # submit
            print('missing spawns')
            for m in is_missing:
                if m in JobInfo['spawn_resub']:
                    continue  # only once
                JobInfo['spawn_id'].remove(m)  # (by value)
                JobInfo['spawn_resub'].add(m)
            update_job(JobInfo)
            spawn_resubmit(JobInfo['BatchID'], JobInfo['JobIndex'])

        else:
            # reinstate job id and submit status (so it is recognized by get_queue())
            JobInfo['status'] = 'run'  # 'submit'
            JobInfo['hostname'] = hostname
            JobInfo['PBS_ID'] = PBS_ID
            update_job(JobInfo)
            # set but not update yet (delay post-completion submissions)
            JobInfo['status'] = 'complete'

    return JobInfo


def spawn_resubmit(BatchID, JobIndex):
    """ submit missing spawns. """
    JobInfo = get_job_info(BatchID, JobIndex)
    if JobInfo['status'] == 'spawn':
        for i in range(len(JobInfo['spawn_id']), JobInfo['spawn_count']):
            time.sleep(10)
            submit_one_job(JobInfo['BatchID'], JobInfo['JobIndex'], Spawn=True)


def isiterable(p_object):
    try:
        it = iter(p_object)
    except TypeError:
        return False
    return True


def print_log(BatchID, JobIndex, LogKey='stdout', LogIndex=-1):
    """ print job logs. """
    if isiterable(BatchID):
        [print_log(j, JobIndex, LogKey, LogIndex) for j in BatchID]
        return
    if isiterable(JobIndex):
        [print_log(BatchID, p, LogKey, LogIndex) for p in JobIndex]
        return
    if isiterable(LogIndex):
        [print_log(BatchID, JobIndex, LogKey, i) for i in LogIndex]
        return

    JobInfo = get_job_info(BatchID, JobIndex)
    if LogKey not in JobInfo:
        print('log unknown')
        return
    LogFile = JobInfo[LogKey]
    if LogIndex > 0 and len(LogFile) <= LogIndex:
        print('log index too large')
        return
    if LogIndex < 0 and len(LogFile) < abs(LogIndex):
        print('log index too small')
        return
    LogFile = LogFile[LogIndex]
    if not os.path.exists(LogFile):
        print('log is missing.\n{}'.format(LogFile))
        return

    print('\n\n[[[{} log for {}/{}/job_{}:]]]\n'.format(LogKey, BatchID,
          '/'.join(JobInfo['name']), JobIndex))
    with open(LogFile, 'r') as fid:
        for line in fid:
            print(line[:-1])


def generate_script(JobInfo, Template=None):
    """ will generate a script based on a template while replacing all fields
        appearing in the template (within curly brackets {fieldname}) according
        to the value of the field in JobInfo. """
    global JobDir
    if Template is None:
        Template = JobInfo['script']
    if JobDir[-1] != '/' and JobDir[-1] != '\\':
        JobDir = JobDir + '/'
    OutDir = JobDir + str(JobInfo['BatchID'])
    if not os.path.isdir(OutDir):
        os.makedirs(OutDir)
    tmp = os.path.basename(Template).split('.')
    ext = tmp[-1]
    tmp = tmp[0].split('_')[0]
    OutScript = '/{}_{BatchID}_{JobIndex}.{}'.format(tmp, ext, **JobInfo)
    OutScript = OutDir + OutScript
    with open(Template) as fid:
        with open(OutScript, 'w') as oid:
            for line in fid:
                oid.write(line.format(**JobInfo))
    os.chmod(OutScript, 0o744)
    JobInfo['script'] = OutScript


def generate_data(JobInfo, Data):
    """ will save data and update metadata with the file location.
        standard file naming / dump. """
    JobInfo['data'] = '{}/{}/data_{}.pkl'.format(JobDir, JobInfo['BatchID'],
                                                 JobInfo['JobIndex'])
    if not os.path.isdir(os.path.dirname(JobInfo['data'])):
        os.makedirs(os.path.dirname(JobInfo['data']))
    pickle.dump(Data, open(JobInfo['data'], 'wb'), protocol=pickle.HIGHEST_PROTOCOL)


def init_db(conn):
    """ creating tables for a given sqlite3 connection. """

    # keeping the main searchable fields here
    conn.execute("""CREATE TABLE batch(
                    BatchID     INT     PRIMARY KEY,
                    name        TEXT    NOT NULL,
                    organism    TEXT    NOT NULL,
                    data_type   TEXT    NOT NULL
                    );""")

    # keeping just the essentials as separate columns, rest in the metadata JSON
    conn.execute("""CREATE TABLE job(
                    JobID       INTEGER     PRIMARY KEY AUTOINCREMENT,
                    JobIndex    INT     NOT NULL,
                    BatchID     INT     NOT NULL,
                    status      TEXT    NOT NULL,
                    priority    INT     NOT NULL,
                    metadata    TEXT    NOT NULL,
                    md5         TEXT    NOT NULL
                    );""")
