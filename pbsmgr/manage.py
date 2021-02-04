# -*- coding: utf-8 -*-
"""
a PBS (portable batch system) parallel-computing job manager.
see also: README.md, example.ipynb

this submodule handles job submission and monitoring.

@author: Alon Diament, Tuller Lab
Created on Wed Mar 18 22:45:50 2015
"""
from collections import Counter
import os
import re
import shutil
import subprocess
import time
import warnings

import numpy as np
import pandas as pd

from .config import QFile, JobDir, LocalRun, PBS_suffix, PBS_queue, DefResource, \
        WriteTries, LogOut, LogErr, PBS_ID, running_on_cluster
from . import utils
from . import dal
from . import local


### QUEUE FUNCTIONS ###

def get_queue(Verbose=True, ResetMissing=False, ReportMissing=False,
              Display=None, Filter=''):
    """ Verbose mode prints a job report to screen, otherwise a dictionary Q
        is returned with the metadata of all jobs.
        ResetMissing will set the state of jobs that failed while online.
        Display is an iterable of states that desired for display (other
        states will not be reported in Verbose mode).
        Filter accepts SQLite conditions as a string.

        Example:
            get_queue(Display={'complete'})
            will display only completed jobs.

            get_queue(Filter='name LIKE "%most-excellent%"')
            will display only that their name contains the phrase 'most-excellent'.
            for a differences Display filtering and Filter see the README. """

    # reads from global job queue file
    Q = get_sql_queue(QFile, Filter)
    Q_server = get_pbs_queue()

    maybe_online = {'submit', 'spawn', 'run'}
    missing = {}  # submitted but not running
    cnt = Counter()
    cnt['total'] = 0
    cnt['complete'] = 0
    cnt['online'] = 0
    for BatchID in sorted(list(Q)):
        if len(Q[BatchID]) == 0:
            print('\nempty {}'.format(BatchID))
            del Q[BatchID]
            continue

        state = {}
        for job in Q[BatchID]:
            if job['state'] == 'spawn':
                job.update(dal.spawn_get_info(job['BatchID'], job['JobIndex']))

            cnt['total'] += 1
            job_index = job['JobIndex']
            if 'PBS_ID' not in job:
                cnt[job['state']] += 1
                utils.dict_append(state, job['state'], job_index)
                continue

            # checking job against current PBS queue
            # (for online and crashed jobs)
            found_id = False
            for pid in utils.make_iter(job['PBS_ID']):
                if pid in Q_server and Q_server[pid][1] == 'R':
                    job_index = str(job_index) + '*'
                    cnt['online'] += 1
                    found_id = True

            if not found_id and job['state'] in maybe_online:
                utils.dict_append(missing, BatchID, job_index)

            cnt[job['state']] += 1
            utils.dict_append(state, job['state'], job_index)

        if Verbose:
            if Display is not None:
                state = {s: p for s, p in state.items() if s in Display}
            if len(state) > 0:
                print('\n{}: {}'.format(BatchID, '/'.join(job['name'])))
                print(state)

    if ResetMissing:
        for BatchID in missing:
            for JobIndex in missing[BatchID]:
                job_dir = JobDir + '{}/{}'.format(BatchID, JobIndex)
                if os.path.isdir(job_dir):
                    shutil.rmtree(job_dir)
                dal.set_job_field(BatchID, JobIndex, {'state': 'init'})

    if not Verbose:
        return Q

    # Verbose=True
    print('\nmissing jobs: {}'.format(missing))
    cnt['complete'] += cnt['collected']
    print('\ntotal jobs on PBS queue: {}'.format(len(Q_server)))
    try:
         print('running/complete/total: {online}/{complete}/{total}'.format(**cnt))
    except:
        pass

    if ReportMissing and len(missing):
        print('\nerror logs for missing jobs:\n')
        for BatchID in missing:
            dal.print_log(BatchID, missing[BatchID], 'stderr')


def get_sql_queue(QFile, Filter=''):
    conn = dal.open_db()

    filter_str = ''
    if len(Filter):
        filter_str = 'WHERE ' + Filter

    df = pd.read_sql_query('SELECT * FROM ' +
                           """(SELECT j.*,
                                      b.name,
                                      b.data_type
                               FROM batch b INNER JOIN job j 
                               ON j.BatchID = b.BatchID) """ + filter_str,
                           conn)
    dal.close_db(conn)
    return df.iloc[:, 1:].groupby('BatchID')\
        .apply(lambda df: df.sort_values('JobIndex')[['metadata', 'md5']]\
               .apply(dal.unpack_job, axis=1)\
               .tolist()).to_dict()


def get_qstat(BatchID=PBS_ID):
    if BatchID is None:
        print('get_qstat: not running on a cluster node.')
        return {}
    try:
        return parse_qstat(subprocess.check_output(['qstat', '-f', BatchID]))
    except Exception as e:
        # sometimes this fails on cluster, not clear why (cluster does not recognize the BatchID)
        print(e)
        return None


def parse_qstat(text):
    JobInfo = {}
    text = text.decode('utf-8')
    line_parse = re.compile(r'([\w.]*) = ([\w\s:_\-/]*)')
    for line in text.splitlines():
        hit = line_parse.match(line.strip())
        if hit is not None:
            JobInfo[hit.group(1)] = hit.group(2)
    return JobInfo


def get_pbs_queue():
    Q = {}
    if not running_on_cluster:
        print('get_pbs_queue: not running on cluster.')
        return Q
    data = subprocess.check_output(['qstat', '-u', os.environ['USER']],
                                   universal_newlines=True)
    data = data.split('\n')
    job_parse = re.compile(r'(\d+).')
    line_parse = re.compile(r'\s+')
    for line in data:
        job = job_parse.match(line)  # power
        if job:
            line = line_parse.split(line)
            Q[job.group(1)] = [line[3], line[9]]
    return Q


### SUBMIT FUNCTIONS ###

def submit_jobs(MaxJobs=None, MinPrior=0, LocalRun=LocalRun, OutFile=None, ForceSubmit=False,
                Filter=''):
    """ submits all next available jobs according to job priorities.
        MaxJobs is loaded from [JobDir]/maxjobs unless specified (default=1000).
        ForceSubmit ignores another process currently submitting jobs (or an
        abandoned lock file).
        Filter are SQL query conditions that get_queue() accepts. """
    if not running_on_cluster and not LocalRun:
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

    Q = get_queue(Verbose=False, Filter=Filter)

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
                                  if p['state'] not in
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

    if LocalRun:
        if PBS_ID != 'pbsmgr':
            print('cannot submit from a subprocess.')
            os.remove(lock_file)
            return
        local_sub = local.get_executor()
    else:
        local_sub = None
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
        count = submit_one_batch(j, count, MaxJobs, OutFile=out_file, LocalSub=local_sub)

    flag_file.close()
    if OutFile is not None:
        out_file.close()
        os.chmod(OutFile, 0o744)
    os.remove(lock_file)
    print('max jobs: {}\nin queue: {}\nsubmitted: {}'.format(MaxJobs,
          count_in_queue,
          count - count_in_queue))


def submit_one_batch(BatchID, SubCount=0, MaxJobs=1e6, OutFile=None, LocalSub=None):
    """ despite its name, this function accepts also an iterable with
        multiple BatchIDs. """
    for batch in utils.make_iter(BatchID):
        BatchInfo = dal.get_batch_info(batch)
        job_priority = max([0] + [j['priority'] for j in BatchInfo
                                  if j['state'] not in
                                  {'complete', 'collected'}])
        for job in BatchInfo:
            if job['priority'] < job_priority:
                continue
            if job['state'] == 'init':
                submit_one_job(batch, job['JobIndex'], OutFile=OutFile, LocalSub=LocalSub)
                SubCount += 1
                if SubCount >= MaxJobs:
                    break
    return SubCount


def submit_one_job(BatchID, JobIndex, Spawn=False, SpawnCount=None,
                   OutFile=None, LocalSub=None):
    """ despite its name, this function accepts either an integer
        or an iterable of integers as JobIndex. """
    ErrDir = os.path.abspath(JobDir) + '/{}/logs/'.format(BatchID)
    OutDir = os.path.abspath(JobDir) + '/{}/logs/'.format(BatchID)
    if not os.path.isdir(ErrDir):
        os.makedirs(ErrDir)

    Qsub = ['qsub', '-q', PBS_queue, '-e', ErrDir, '-o', OutDir, '-l']

    for j in utils.make_iter(JobIndex):
        conn = dal.open_db()
        job = dal.get_job_info(BatchID, j, HoldFile=True, db_connection=conn)
        print('submiting:\t{}'.format(job['script']))
        if job['state'] != 'init':
            warnings.warn('already submitted')
        if 'resources' in job:
            this_res = job['resources']
        else:
            this_res = DefResource
        this_sub = Qsub + [','.join(['{}={}'.format(k, v)
                           for k, v in sorted(this_res.items())])]
        if 'queue' in job:
            this_sub[2] = job['queue']

        # where to submit to
        if LocalSub is not None:
            time.sleep(1)  # ensure that the assigned id is unique (and same as subtime)
            submit_id = str(utils.get_time())
        elif OutFile is None:
            submit_id_raw = subprocess.check_output(this_sub + [job['script']]).decode('UTF-8').replace('\n', '')
            submit_id = submit_id_raw.replace(PBS_suffix, '')
        else:
            OutFile.write(job['script'] + '\n')
            submit_id_raw = OutFile.name
            submit_id = OutFile.name

        if not Spawn:
            job['state'] = 'submit'
            job['submit_id'] = submit_id
            job['subtime'] = utils.get_time()
            utils.dict_append(job, 'stdout', LogOut.format(**job))
            utils.dict_append(job, 'stderr', LogErr.format(**job))
            if 'PBS_ID' in job:
                del job['PBS_ID']
            if 'qstat' in job:
                del job['qstat']

            dal.update_job(job, Release=True)
            dal.close_db(conn)

            if LocalSub is not None:
                LocalSub.submit(local.run_local_job, job)
            continue

        # Spawn==True
        if job['state'] != 'spawn':
            job['state'] = 'spawn'
            dal.update_job(job, Release=True)

        dal.spawn_add_to_db(BatchID, JobIndex, submit_id, SpawnCount=SpawnCount,
                            db_connection=conn)
        dal.close_db(conn)

        if LocalSub is not None:
            LocalSub.submit(local.run_local_job, job)


def spawn_submit(JobInfo, N):
    """ run the selected job multiple times in parallel. job needs to handle
        'spawn' state for correct logic: i.e., only last job to complete
        updates additional fields in JobInfo and sets it to 'complete'.
        this state-logic is handled by calling spawn_complete(). """
    print(f'submitting {N} spawn jobs')

    dal.spawn_del_from_db(JobInfo['BatchID'], JobInfo['JobIndex'])

    # adding self
    dal.spawn_add_to_db(JobInfo['BatchID'], JobInfo['JobIndex'], PBS_ID)

    for _ in range(N):
        submit_one_job(JobInfo['BatchID'], JobInfo['JobIndex'], Spawn=True)

    # update current job with spawn ID
    return dal.get_job_info(JobInfo['BatchID'], JobInfo['JobIndex'], SetID=True)


def spawn_resubmit(BatchID, JobIndex, SpawnCount=None):
    """ resubmits spawns that were left in 'submit' state, if they failed
        for some reason. the function will check against PBS queue for queued
        spawns and will not submit them again.
        SpawnCount is optional (usually inferred), and sets the total desired number
        of spawns. """
    conn = dal.open_db()
    if SpawnCount is None:
        SpawnCount = len(dal.spawn_get_info(BatchID, JobIndex,
                                            db_connection=conn)['SpawnID'])

    Q_server = str(tuple(get_pbs_queue())).replace(',)', ')')
    condition = f'spawn_state=="submit" AND PBS_ID NOT IN {Q_server}'
    dal.spawn_del_from_db(BatchID, JobIndex, Filter=condition, db_connection=conn)
    dal.close_db(conn)

    n_miss = SpawnCount - len(dal.spawn_get_info(BatchID, JobIndex)['SpawnID'])
    for _ in range(n_miss):
        submit_one_job(BatchID, JobIndex, Spawn=True, SpawnCount=SpawnCount)


def periodic_submitter(period=10, n=np.inf, **kwargs):
    """ looped calls to submit_jobs(**kwargs).
        'period' measured in minutes.
        'n' can be used to limit the number of iterations. """
    subs = 0
    while subs < n:
        submit_jobs(**kwargs)
        time.sleep(60*period)
        subs += 1


### QDEL FUNCTIONS ###

def qdel_batch(BatchID):
    """ this will run the PBS qdel command on the entire batch of jobs. """
    for batch in utils.make_iter(BatchID):
        for job in dal.get_batch_info(batch):
            qdel_job(JobInfo=job)


def qdel_job(BatchID=None, JobIndex=None, JobInfo=None):
    """ this will run the PBS qdel command on the given job. """
    pid_keys = ['PBS_ID', 'submit_id']

    if JobInfo is None:
        JobInfo = dal.get_job_info(BatchID, JobIndex, HoldFile=True)
    if 'PBS_ID' not in JobInfo and 'submit_id' not in JobInfo:
        print('qdel_job: unknown PBS_ID for {BatchID},{JobIndex}'.format(**JobInfo))
        return

    pid_list = []
    for k in pid_keys:
        if k in JobInfo:
            JobInfo[k] = utils.make_iter(JobInfo[k])
            pid_list += [j for j in JobInfo[k] if j not in pid_list]

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
            JobInfo['state'] = 'init'

    dal.update_job(JobInfo, Release=True)


### SET JOB STATES AND SUCH ###

def set_complete(BatchID=None, JobIndex=None, JobInfo=None, Submit=False):
    if JobInfo is None:
        JobInfo = dal.get_job_info(BatchID, JobIndex, HoldFile=True, ignore_spawn=True)
    JobInfo['qstat'] = get_qstat()
    JobInfo['state'] = 'complete'
    dal.update_job(JobInfo, Release=True)

    if Submit:
        submit_jobs()


def set_job_field(BatchID, JobIndex, Fields={'state': 'init'},
                  Unless={'state': ['complete', 'collected']},
                  db_connection=None):
    """ called with default args, this sets all jobs in progress
        (unless completed) to `init` state. """
    for j in utils.make_iter(JobIndex):
        job = dal.get_job_info(BatchID, j, HoldFile=False,
                               db_connection=db_connection, ignore_spawn=True)

        skip_job = False
        for k in Unless:  # this tests whether the job is protected
            if k in job:
                if job[k] in utils.make_iter(Unless[k]):
                    skip_job = True
        if skip_job:
            continue

        for k, v in Fields.items():
            job[k] = v
        dal.update_job(job, Release=True, db_connection=db_connection)


def set_batch_field(BatchID, Fields={'state': 'init'},
                    Unless={'state': ['complete', 'collected']}):
    """ calls set_job_field on the batch. """
    conn = dal.open_db()  # single connection for all updates

    for b in utils.make_iter(BatchID):
        JobList = dal.get_job_indices(b, db_connection=conn)
        for job in JobList:
            set_job_field(b, job, Fields, Unless,
                          db_connection=conn)

    dal.close_db(conn)


def spawn_complete(JobInfo, db_connection=None, tries=WriteTries):
    """ signal that one spawn has ended successfully by updating the spawn
        table. once all spawns completed, returns a 'complete' state to the
        calling function (which may then proceed to complete the parent job). """
    # ideally placed in dal.py, it is currently here to avoid a dependency
    # loop (dal-->manage-->dal due to the call to spawn_resubmit() below).

    for t in range(tries):
        try:
            conn = dal.open_db(db_connection)
            JobInfo = dal.get_job_info(JobInfo['BatchID'], JobInfo['JobIndex'],
                                       db_connection=conn)
            if JobInfo['state'] != 'spawn':
                # must not leave function with an ambiguous state in JobInfo
                # e.g., if job has been re-submitted by some one/job
                # (unknown logic follows)
                dal.close_db(conn)
                raise Exception(f"""unknown state "{JobInfo['state']}" encountered for spawned
                                    job ({JobInfo['BatchID']}, {JobInfo['JobIndex']})""")

            # first, set current spawn state to complete
            # (use same db_connection to make sure that no other job sees that I am done)
            conn.execute("""UPDATE spawn
                            SET spawn_state='complete' WHERE
                            BatchID=? AND JobIndex=? AND PBS_ID=?""",
                            [JobInfo['BatchID'], JobInfo['JobIndex'],
                             JobInfo['PBS_ID'][0]])

            # second, get all states of spawns
            spawn_dict = dal.spawn_get_info(JobInfo['BatchID'], JobInfo['JobIndex'],
                                            db_connection=conn)
            if not (pd.Series(spawn_dict['spawn_state']) == 'complete').all():
                dal.close_db(conn, db_connection)
                # we run the following just in case some job failed
                # I think this shouldn't cause any infinite loop
                spawn_resubmit(JobInfo['BatchID'], JobInfo['JobIndex'])
                break

            # all spawn jobs completed
            # update parent job, remove spawns from DB
            JobInfo.update(spawn_dict)
            JobInfo['state'] = 'run'
            dal.update_job(JobInfo, db_connection=conn)
            JobInfo['state'] = 'complete'
            # signal calling function that we're done, but defer broadcasting
            # to all other jobs yet

            dal.spawn_del_from_db(JobInfo['BatchID'], JobInfo['JobIndex'],
                              db_connection=conn)
            dal.close_db(conn, db_connection)
            break

        except Exception as err:
            dal.close_db(conn, db_connection)
            print(f'spawn_complete: try {t+1} failed with:\n{err}\n')
            time.sleep(1)
            if t == tries - 1:
                raise(err)

    return JobInfo


def boost_batch_priority(BatchID, Booster=100):
    for batch in utils.make_iter(BatchID):
        BatchInfo = dal.get_batch_info(batch)
        for JobInfo in BatchInfo:
            JobInfo['priority'] += Booster
            dal.update_job(JobInfo)
