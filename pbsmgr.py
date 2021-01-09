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
QFile = '/tamir1/dalon/RP/Consistency/jobs/job_queue.db'  # '../jobs/job_queue.pkl'
JobDir = '../jobs/'
PBS_suffix = '.power8.tau.ac.il'
LogDir = JobDir + '{BatchID}/logs/{submit_id}' + PBS_suffix
LogOut = LogDir + '.OU'  # template
LogErr = LogDir + '.ER'
PBS_queue = 'tamir-nano4'

if 'PBS_JOBID' in os.environ:
    # this also serves as a sign that we're running on cluster
    PBS_ID_raw = os.environ['PBS_JOBID']
    PBS_ID = PBS_ID_raw.replace(PBS_suffix, '')
else:
    PBS_ID_raw = None
    PBS_ID = None
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
                 'resources': DefResource,
                 'state': 'init'}


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
        iter(var)
    except:
        var = [var]
    return var


def submit_one_batch(BatchID, SubCount=0, MaxJobs=1e6, OutFile=None):
    for batch in make_iter(BatchID):
        BatchInfo = get_batch_info(batch)
        job_priority = max([0] + [j['priority'] for j in BatchInfo
                                  if j['state'] not in
                                  {'complete', 'collected'}])
        for job in BatchInfo:
            if job['priority'] < job_priority:
                continue
            if job['state'] == 'init':
                submit_one_job(batch, job['JobIndex'], OutFile=OutFile)
                SubCount += 1
                if SubCount >= MaxJobs:
                    break
    return SubCount


def submit_one_job(BatchID, JobIndex, Spawn=False, OutFile=None):
    """ accepts either an integer or an iterable of integers as JobIndex. """
    ErrDir = os.path.abspath(JobDir) + '/{}/logs/'.format(BatchID)
    OutDir = os.path.abspath(JobDir) + '/{}/logs/'.format(BatchID)
    if not os.path.isdir(ErrDir):
        os.makedirs(ErrDir)

    Qsub = ['qsub', '-q', PBS_queue, '-e', ErrDir, '-o', OutDir, '-l']

    for j in make_iter(JobIndex):
        conn = open_db()
        job = get_job_info(BatchID, j, HoldFile=True, db_connection=conn)
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
        if OutFile is None:
            submit_id_raw = subprocess.check_output(this_sub + [job['script']]).decode('UTF-8').replace('\n', '')
            submit_id = submit_id_raw.replace(PBS_suffix, '')
        else:
            OutFile.write(job['script'] + '\n')

        if not Spawn:
            job['state'] = 'submit'
            job['submit_id'] = submit_id
            job['subtime'] = get_time()
            dict_append(job, 'stdout', LogOut.format(**job))
            dict_append(job, 'stderr', LogErr.format(**job))
            if 'PBS_ID' in job:
                del job['PBS_ID']
            if 'qstat' in job:
                del job['qstat']

            update_job(job, Release=True)
            close_db(conn)
            return

        # Spawn==True
        if job['state'] != 'spawn':
            job['state'] = 'spawn'
            update_job(job, Release=True)

        spawn_add_to_db(BatchID, JobIndex, submit_id, db_connection=conn)
        close_db(conn)


def parse_qstat(text):
    JobInfo = {}
    text = text.decode('utf-8')
#    print('\n')
    for line in text.splitlines():
#        print(line)
        hit = re.match(r'([\w.]*) = ([\w\s:_\-/]*)', line.strip())
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
        job = re.match(r'(\d+).', line)  # power
        if job:
            line = re.split(r'\s+', line)
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
    curr_time = get_time()
    powQ = get_pbs_queue()

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
        for pinfo in Q[BatchID]:
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
                    elif pinfo['state'] in {'submit', 'spawn', 'run'} and \
                            'subtime' in pinfo:
                        if pinfo['subtime'] < curr_time:
                            dict_append(missing, BatchID, job_index)
            cnt[pinfo['state']] += 1
            dict_append(state, pinfo['state'], job_index)

        if Verbose:
            if Display is not None:
                state = {s: p for s, p in state.items() if s in Display}
            if len(state) > 0:
                print('\n{}: {}'.format(BatchID, '/'.join(pinfo['name'])))
                print(state)

    if ResetMissing:
        for BatchID in missing:
            for JobIndex in missing[BatchID]:
                jobDir = JobDir + '{}/{}'.format(BatchID, JobIndex)
                if os.path.isdir(jobDir):
                    shutil.rmtree(jobDir)
                set_job_field(BatchID, JobIndex, {'state': 'init'})

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
    with open_db() as conn:
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
    for batch in make_iter(BatchID):
        for job in get_batch_info(batch):
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
            JobInfo['state'] = 'init'

    update_job(JobInfo, Release=True)


def get_job_indices(BatchID, db_connection=None):
    conn = open_db(db_connection)
    batch_query = list(conn.execute(f"""SELECT JobIndex from job WHERE
                                        BatchID={BatchID}
                                        ORDER BY JobIndex"""))
    close_db(conn, db_connection)

    return [job[0] for job in batch_query]


def get_batch_info(BatchID, db_connection=None):
    conn = open_db(db_connection)
    batch_query = list(conn.execute(f"""SELECT metadata, md5 from job WHERE
                                        BatchID={BatchID}
                                        ORDER BY JobIndex"""))
    close_db(conn, db_connection)

    return [unpack_job(job) for job in batch_query]


def get_job_info(BatchID, JobIndex, HoldFile=False, SetID=False,
                 db_connection=None):
    """ HoldFile is ignored but kept for backward compatibility. """
    conn = open_db(db_connection)
    job_query = list(conn.execute(f"""SELECT metadata, md5 from job WHERE
                                      BatchID={BatchID} AND
                                      JobIndex={JobIndex}"""))
    if len(job_query) != 1:
        raise Exception('job is not unique (%d)' % len(job_query))
    job_query = job_query[0]
    JobInfo = unpack_job(job_query)

    close_db(conn, db_connection)

    if JobInfo['state'] == 'spawn':
        # no update made to job in DB
        JobInfo.update(spawn_get_info(BatchID, JobIndex, PBS_ID=PBS_ID,
                                      db_connection=db_connection))
        return JobInfo

    # not spawn
    if not SetID:
        return JobInfo

    # SetID==True
    JobInfo['PBS_ID'] = PBS_ID
    JobInfo['hostname'] = hostname
    JobInfo['state'] = 'run'

    update_job(JobInfo, Release=True, db_connection=db_connection)

    return JobInfo


def get_job_template(SetID=False):
    res = deepcopy(JobTemplate)
    if SetID:
        time.sleep(1)  # precaution against non-unique IDs
        res['BatchID'] = get_time()
    return res


def get_time():
    """ readable timestamp in seconds. """
    return int(datetime.now().strftime('%Y%m%d%H%M%S'))


def update_job(JobInfo, Release=False, db_connection=None, tries=3):
    """ Release is ignored but kept for backward compatibility. """

    for t in range(tries):
        try:
            conn = open_db(db_connection)
            BatchID, JobIndex = JobInfo['BatchID'], JobInfo['JobIndex']
            md5 = list(conn.execute(f"""SELECT md5, idx FROM job WHERE
                                        BatchID={BatchID} AND
                                        JobIndex={JobIndex}"""))
            if len(md5) != 1:
                raise Exception('job is not unique (%d)' % len(md5))
            # here we're ensuring that JobInfo contains an updated version
            # of the data, that is consistent with the DB
            if md5[0][0] != JobInfo['md5']:
                raise Exception(f'job ({BatchID}, {JobIndex}) was overwritten by another process')

            metadata = pack_job(JobInfo)
            # we INSERT, then DELETE the old entry, to catch events where two jobs
            # somehow managed to write (almost) concurrently - the second will fail
            conn.execute("""INSERT INTO job(JobIndex, BatchID, state,
                                            priority, metadata, md5)
                            VALUES (?,?,?,?,?,?)""",
                         [JobInfo['JobIndex'], JobInfo['BatchID'],
                          JobInfo['state'], JobInfo['priority'],
                          metadata, JobInfo['md5']])

            conn.execute("""DELETE FROM job
                            WHERE idx=? AND BatchID=? AND JobIndex=?""",
                         [md5[0][1], JobInfo['BatchID'], JobInfo['JobIndex']])
            if conn.total_changes < 2:
                raise Exception('failed to update job' +
                                '(probably trying to delete an already deleted entry)')

        #        conn.execute(f"""UPDATE job
        #                         SET metadata=?, md5=?, state=?, priority=? WHERE
        #                         BatchID={BatchID} AND
        #                         JobIndex={JobIndex}""",
        #                         [metadata, JobInfo['md5'],
        #                          JobInfo['state'], JobInfo['priority']])

            close_db(conn, db_connection)
            break

        except Exception as err:
            print(f'update_job: try {t+1} failed')
            if t == tries - 1:
                raise(err)


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


def update_batch(batch, db_connection=None):
    conn = open_db(db_connection)

    for job in batch:
        update_job(job, db_connection=conn)

    close_db(conn, db_connection)


def add_batch_to_queue(BatchID, Jobs):
    """ provide a list of job info dicts, will replace any existing
        batch. unlike add_job_to_queue it ensures that BatchID doe not
        exist in queue, and that all given jobs are from the same batch. """
    if not os.path.isfile(QFile):
        init_db(QFile)

    Jobs = make_iter(Jobs)
    if not all([BatchID == job['BatchID'] for job in Jobs]):
        raise Exception(f'some jobs do not match BatchID={BatchID}')

    if len(get_job_indices(BatchID)) > 0:
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
    with open_db() as conn:
        for job in make_iter(Jobs):
            BatchID = job['BatchID']

            # setting JobIndex automatically
            if BatchID not in batch_index:
                batch_index[BatchID] = len(get_job_indices(BatchID))
            if batch_index[BatchID] == 0:
                # init new batch
                conn.execute("""INSERT INTO batch
                                VALUES (?,?,?,?)""",
                                [BatchID, '/'.join(job['name']),
                                 job['organism'], job['data_type']])

            job['JobIndex'] = batch_index[BatchID]
            batch_index[BatchID] += 1
            metadata = pack_job(job)
            conn.execute("""INSERT INTO job(JobIndex, BatchID, state,
                                            priority, metadata, md5)
                            VALUES (?,?,?,?,?,?)""",
                         [job['JobIndex'], BatchID, job['state'],
                          job['priority'], metadata, job['md5']])


def set_complete(BatchID, JobIndex):
    JobInfo = get_job_info(BatchID, JobIndex, HoldFile=True)
    JobInfo['qstat'] = get_qstat()
    JobInfo['state'] = 'complete'
    update_job(JobInfo, Release=True)


def set_job_field(BatchID, JobIndex, Fields={'state': 'init'},
                  Unless={'state': ['complete', 'collected']},
                  db_connection=None):
    """ called with default args, this sets all jobs in progress
        (unless completed) to `init` state. """
    for j in make_iter(JobIndex):
        job = get_job_info(BatchID, j, HoldFile=False,
                           db_connection=db_connection)

        skip_job = False
        for k in Unless:  # this tests whether the job is protected
            if k in job:
                if job[k] in make_iter(Unless[k]):
                    skip_job = True
        if skip_job:
            continue

        for k, v in Fields.items():
            job[k] = v
        update_job(job, Release=True, db_connection=db_connection)


def set_batch_field(BatchID, Fields={'state': 'init'},
                    Unless={'state': ['complete', 'collected']}):
    """ calls set_job_field on the batch. """
    conn = open_db()  # single connection for all updates

    for b in make_iter(BatchID):
        JobList = get_job_indices(b, db_connection=conn)
        for job in JobList:
            set_job_field(b, job, Fields, Unless,
                          db_connection=conn)

    close_db(conn)


def remove_batch(BatchID):
    with open_db() as conn:
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
        if all([True if p['state'] in state else False
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
            if os.path.isdir(tempdir):
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
        'spawn' state for correct logic: i.e., only last job to complete
        updates additional fields in JobInfo and sets it to 'complete'. """

    spawn_del_from_db(JobInfo['BatchID'], JobInfo['JobIndex'])

    for _ in range(N):
        submit_one_job(JobInfo['BatchID'], JobInfo['JobIndex'], Spawn=True)

    # adding self
    spawn_add_to_db(JobInfo['BatchID'], JobInfo['JobIndex'], PBS_ID)

    # update current job with spawn ID
    return get_job_info(JobInfo['BatchID'], JobInfo['JobIndex'], SetID=True)


def spawn_add_to_db(BatchID, JobIndex, PBS_ID, db_connection=None):
    conn = open_db(db_connection)
    # auto-numbering of spawns
    # TODO: if we ever delete some row, auto-numbering will fail and may
    #       generate already existing IDs
    SpawnID = len(list(
            conn.execute(f"""SELECT SpawnID FROM spawn
                             WHERE BatchID={BatchID} AND
                                   JobIndex={JobIndex}""")))
    conn.execute("""INSERT INTO spawn(BatchID, JobIndex, SpawnID, PBS_ID,
                                      spawn_state, stdout, stderr)
                    VALUES (?,?,?,?,?,?,?)""",
                 [BatchID, JobIndex, SpawnID, PBS_ID,
                  'submit', LogOut.format(BatchID=BatchID, submit_id=PBS_ID),
                  LogErr.format(BatchID=BatchID, submit_id=PBS_ID)])
    close_db(conn, db_connection)


def spawn_del_from_db(BatchID, JobIndex, db_connection=None):
    conn = open_db(db_connection)
    conn.execute("""DELETE FROM spawn WHERE
                    BatchID=? AND JobIndex=?""",
                    [BatchID, JobIndex])
    close_db(conn, db_connection)


def spawn_get_info(BatchID, JobIndex, PBS_ID=None, db_connection=None):
    fields = ['SpawnID', 'PBS_ID', 'spawn_state', 'stdout', 'stderr']
    condition = f'BatchID={BatchID} AND JobIndex={JobIndex}'
    max_query_size = None
    if (PBS_ID is not None) and (len(PBS_ID) > 0):
        condition += f' AND PBS_ID="{PBS_ID}"'
        max_query_size = 1

    conn = open_db(db_connection)
    spawn_query = pd.read_sql(f"""SELECT {', '.join(fields)}
                                  FROM spawn
                                  WHERE {condition}""", conn)
    close_db(conn, db_connection)

    if len(spawn_query) == 0:
        raise Exception(f'no corresponding spawn job for {BatchID, JobIndex, PBS_ID}')
    elif (max_query_size is not None) and (len(spawn_query) > max_query_size):
        raise Exception(f'too many spawns for PBS_ID={PBS_ID}')

    return spawn_query.to_dict(orient='list')  # return a dict


def spawn_complete(JobInfo, db_connection=None):
    """ signal that one spawn has ended successfully. only update done by
        current job to JobInfo, unless all spawns completed. """

    conn = open_db(db_connection)
    JobInfo = get_job_info(JobInfo['BatchID'], JobInfo['JobIndex'],
                           db_connection=conn)
    if JobInfo['state'] != 'spawn':
        # must not leave function with an ambiguous state in JobInfo
        # e.g., if job has been re-submitted by some one/job
        # (unknown logic follows)
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
    spawn_dict = spawn_get_info(JobInfo['BatchID'], JobInfo['JobIndex'],
                                db_connection=conn)
    if (pd.Series(spawn_dict['spawn_state']) == 'complete').all():
        # update parent job, remove spawns from DB
        JobInfo.update(spawn_dict)
        JobInfo['state'] = 'complete'
        update_job(JobInfo, db_connection=conn)

        spawn_del_from_db(JobInfo['BatchID'], JobInfo['JobIndex'],
                          db_connection=conn)

    close_db(conn, db_connection)

    return JobInfo


def spawn_resubmit(BatchID, JobIndex):
    """ submit missing spawns. """
    JobInfo = get_job_info(BatchID, JobIndex)
    if JobInfo['state'] != 'spawn':
        return

    for i in range(len(JobInfo['spawn_id']), JobInfo['spawn_count']):
        # time.sleep(1)
        submit_one_job(JobInfo['BatchID'], JobInfo['JobIndex'], Spawn=True)


def spawn_fix_ghosts(BatchID, JobIndex):
    """ run this after spawn_resubmit fails when NO SPAWNS ARE RUNNING to
        remove spawns still existing (that failed) from the 
        `PBS_ID` and `spawn_id` lists, and resubmit.
        this is the spawn-eqeuivalent of resetting missing jobs. """
    JobInfo = get_job_info(BatchID, JobIndex, HoldFile=True)
    if JobInfo['state'] != 'spawn':
        return

    print('{} ghosts in PBS_ID'.format(len(JobInfo['PBS_ID'])))
    JobInfo['PBS_ID'] = []
    JobInfo['hostname'] = []

    print('{} ghosts in spawn_id'.format(len(JobInfo['spawn_id']) -
                                         len(JobInfo['spawn_complete'])))
    JobInfo['spawn_id'] = list(JobInfo['spawn_complete'])

    update_job(JobInfo, Release=True)
    spawn_resubmit(BatchID, JobIndex)


def isiterable(p_object):
    try:
        iter(p_object)
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
    if not os.path.isfile(LogFile):
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
    """ creating tables for a given sqlite3 connection.
        job hierarchy: batch > job > spawn.
        a batch contains multiple jobs/steps.
        a job may go for extra parallelization by launching spawns. """

    # keeping the main searchable fields here
    conn.execute("""CREATE TABLE batch(
                    BatchID     INT     PRIMARY KEY,
                    name        TEXT    NOT NULL,
                    organism    TEXT    NOT NULL,
                    data_type   TEXT    NOT NULL
                    );""")

    # keeping just the essentials as separate columns, rest in the metadata JSON
    conn.execute("""CREATE TABLE job(
                    idx         INTEGER     PRIMARY KEY AUTOINCREMENT,
                    BatchID     INT     NOT NULL,
                    JobIndex    INT     NOT NULL,
                    state       TEXT    NOT NULL,
                    priority    INT     NOT NULL,
                    metadata    TEXT    NOT NULL,
                    md5         TEXT    NOT NULL
                    );""")

    # no metadata for spawn jobs (what is needed is a copy of the metadata
    # in the job table + a unique SpawnID)
    conn.execute("""CREATE TABLE spawn(
                    idx         INTEGER     PRIMARY KEY AUTOINCREMENT,
                    BatchID     INT     NOT NULL,
                    JobIndex    INT     NOT NULL,
                    SpawnID     INT     NOT NULL,
                    PBS_ID      TEXT    NOT NULL,
                    stdout      TEXT    NOT NULL,
                    stderr      TEXT    NOT NULL,
                    spawn_state       TEXT    NOT NULL
                    );""")


def open_db(db_connection=None):
    """ returns a connection to SQL DB whether you provide an
        existing one or not. """
    if db_connection is None:
        return sqlite3.connect(QFile)
    else:
        return db_connection


def close_db(conn, db_connection=None):
    """ either close connection to SQL DB (if opened) or do nothing
        (if connection previously provided). """
    if db_connection is not None:
        return
    conn.commit()
    conn.close()
