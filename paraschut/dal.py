# -*- coding: utf-8 -*-
"""
PARASCHUT: parallel job scheduling utils.
see also: README.md, example.ipynb

this submodule handles database access.

@author: Alon Diament, Tuller Lab
Created on Wed Mar 18 22:45:50 2015
"""
import hashlib
import os
import pickle
pickle.HIGHEST_PROTOCOL = 2  # for compatibility with python2
import re
import sqlite3
import time
import warnings
import zlib

import pandas as pd

from .config import QFile, WriteTries, LogOut, LogErr, hostname
from . import utils


def open_db(db_connection=None):
    """ returns a connection to SQL DB whether you provide an
        existing one or not. will setup the DB at the configed path
        if it does not exist.  """
    if db_connection is not None:
        return db_connection

    if os.path.isfile(QFile):
        return sqlite3.connect(QFile)

    # DB does not exist
    conn = sqlite3.connect(QFile)
    init_db(conn)
    return conn        


def close_db(conn, db_connection=None):
    """ either close connection to SQL DB (if opened) or do nothing
        (if connection previously provided). """
    if db_connection is not None:
        return
    conn.commit()
    conn.close()


def init_db(conn):
    """ creating tables for a given sqlite3 connection.
        job hierarchy: batch > job > spawn.
        a batch contains multiple jobs/steps.
        a job may go for extra parallelization by launching spawns. """

    print(f'initializing db at: {QFile}')

    # keeping the main searchable fields here
    conn.execute("""CREATE TABLE batch(
                    BatchID     INT     PRIMARY KEY,
                    name        TEXT    NOT NULL,
                    batch_type  TEXT    NOT NULL
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
                    ClusterID   TEXT    NOT NULL,
                    stdout      TEXT    NOT NULL,
                    stderr      TEXT    NOT NULL,
                    spawn_state TEXT    NOT NULL
                    );""")


def pack_job(JobInfo):
    JobInfo.pop('md5', None)
    metadata = zlib.compress(pickle.dumps(JobInfo))
    JobInfo['md5'] = hashlib.md5(metadata).hexdigest()
    return metadata


def unpack_job(job_query):
    assert job_query[-1] == hashlib.md5(job_query[-2]).hexdigest()
    JobInfo = pickle.loads(zlib.decompress(job_query[-2]))
    JobInfo['md5'] = job_query[-1]
    return JobInfo


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


def get_job_info(BatchID, JobIndex, SetID=False, ClusterID=None,
                 db_connection=None, enforce_unique_job=True, ignore_spawn=False,
                 HoldFile=False):
    """ HoldFile is ignored but kept for backward compatibility.
        ClusterID is a required argument when SetID=True or when handling
        a spawn job, but otherwise it's not necessary. """
    conn = open_db(db_connection)
    job_query = list(conn.execute(f"""SELECT metadata, md5 from job WHERE
                                      BatchID={BatchID} AND
                                      JobIndex={JobIndex}"""))
    try:
        JobInfo = unpack_job(validate_query(BatchID, JobIndex, job_query, enforce_unique_job, conn))
    except Exception as err:
        close_db(conn)
        raise(err)

    close_db(conn, db_connection)

    if JobInfo['state'] == 'spawn':
        if ignore_spawn:
            # do not leave old data just lying around
            JobInfo.pop('spawn_state', None)
            JobInfo.pop('SpawnID', None)
        else:
            # no update made to job in DB
            JobInfo.update(spawn_get_info(BatchID, JobIndex, ClusterID=ClusterID,
                                          db_connection=db_connection))
        return JobInfo

    # not spawn
    if not SetID:
        return JobInfo

    # SetID==True
    if ClusterID != JobInfo['submit_id']:
        raise Exception(f"current ClusterID ({ClusterID}) != submit_id ({JobInfo['submit_id']})")
    JobInfo['ClusterID'] = ClusterID
    JobInfo['hostname'] = hostname
    JobInfo['state'] = 'run'

    update_job(JobInfo, Release=True, db_connection=db_connection)

    return JobInfo


def validate_query(BatchID, JobIndex, job_query, enforce_unique_job, conn):
    if len(job_query) == 0:
        raise Exception(f'job ({BatchID}, {JobIndex}) does not exist.')
    elif len(job_query) > 1:
        if enforce_unique_job:
            warnings.warn(f'job ({BatchID, JobIndex}) is not unique (n={len(job_query)}). running make_job_unique()')
            make_job_unique(BatchID, JobIndex, db_connection=conn)
            job_query = job_query[-1:]
        else:
            raise Exception(f'job ({BatchID}, {JobIndex}) is not unique (n={len(job_query)}). run make_job_unique()')

    return job_query[0]

def spawn_get_info(BatchID, JobIndex, ClusterID=None, db_connection=None,
                   tries=WriteTries):
    fields = ['SpawnID', 'ClusterID', 'spawn_state', 'stdout', 'stderr']
    condition = f'BatchID={BatchID} AND JobIndex={JobIndex}'
    max_query_size = None
    if (ClusterID is not None) and (ClusterID != 'paraschut') and (len(ClusterID) > 0):
        condition += f' AND ClusterID="{ClusterID}"'
        max_query_size = 1

    # why would we need tries for reading? because it's possible that
    # the spawn table in the DB hasn't been updated yet
    for t in range(tries):
        conn = open_db(db_connection)
        spawn_query = pd.read_sql(f"""SELECT {', '.join(fields)}
                                    FROM spawn
                                    WHERE {condition}""", conn)
        close_db(conn, db_connection)
        try:
            if len(spawn_query) == 0:
                raise Exception(f'no corresponding spawn job for {BatchID, JobIndex, ClusterID}')
            elif (max_query_size is not None) and (len(spawn_query) > max_query_size):
                raise Exception(f'too many spawns for ClusterID={ClusterID}')

        except Exception as err:
            print(f'spawn_get_info: try {t+1}/{tries} failed with:\n{err}\n')
            time.sleep(1)
            if t == tries - 1:
                raise(err)

    return spawn_query.to_dict(orient='list')  # return a dict


def update_job(JobInfo, Release=False, db_connection=None, tries=WriteTries,
               enforce_unique_job=True):
    """ Release is ignored but kept for backward compatibility. """
    BatchID, JobIndex = JobInfo['BatchID'], JobInfo['JobIndex']
    PID = JobInfo['ClusterID'] if 'ClusterID' in JobInfo else 'unkown_ClusterID'
    md5_init = JobInfo['md5']

    for t in range(tries):
        try:
            conn = open_db(db_connection)
            n_change = conn.total_changes
            md5 = list(conn.execute(f"""SELECT idx, metadata, md5
                                        FROM job WHERE
                                        BatchID={BatchID} AND
                                        JobIndex={JobIndex}"""))
            md5 = validate_query(BatchID, JobIndex, md5, enforce_unique_job, conn)
            # here we're ensuring that JobInfo contains an updated version
            # of the data, that is consistent with the DB
            if md5[-1] != JobInfo['md5']:
                other_id = unpack_job(md5)
                if 'ClusterID' in other_id:
                    other_id = other_id['ClusterID']
                else:
                    other_id = 'unknown'
                raise Exception(f'job ({BatchID}, {JobIndex}, {PID}) was overwritten by another process ({other_id})')

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
                         [md5[0], JobInfo['BatchID'], JobInfo['JobIndex']])
            if (conn.total_changes - n_change) < 2:
                raise Exception('failed to update job' +
                                '(probably trying to delete an already deleted entry)')

            close_db(conn, db_connection)
            break

        except Exception as err:
            close_db(conn, db_connection)
            print(f'update_job: try {t+1}/{tries} failed with:\n{err}\n')
            JobInfo['md5'] = md5_init  # revert to previous md5 to pass tests
            time.sleep(1)
            if t == tries - 1:
                raise(err)


def update_batch(batch, db_connection=None):
    conn = open_db(db_connection)

    for job in batch:
        update_job(job, db_connection=conn)

    close_db(conn, db_connection)


def make_job_unique(BatchID, JobIndex, db_connection=None):
    """ this shouldn't really happen unless something went seriously wrong.
        will leave one entry (most recent one) in the DB and delete all others. """
    conn = open_db(db_connection)

    job_query = list(conn.execute(f"""SELECT idx FROM job WHERE 
                                      BatchID={BatchID} AND 
                                      JobIndex={JobIndex}"""))
    for job in job_query[:-1]:
        conn.execute(f"""DELETE FROM job WHERE
                         idx={job[0]}""")

    close_db(conn, db_connection)


def spawn_add_to_db(JobInfo, ClusterID, SpawnCount=None,
                    spawn_state='submit',
                    db_connection=None, tries=WriteTries):
    # we use JobInfo here in order to have all fields when generating log paths
    BatchID, JobIndex = JobInfo['BatchID'], JobInfo['JobIndex']
    JobInfo = JobInfo.copy()
    JobInfo['submit_id'] = ClusterID

    for t in range(tries):
        try:
            conn = open_db(db_connection)

            # auto-numbering of spawns
            spawn_query = pd.read_sql(f"""SELECT SpawnID, stdout FROM spawn
                                       WHERE BatchID={BatchID} AND
                                       JobIndex={JobIndex}""", conn)
            SpawnIDs = spawn_query['SpawnID']
            if SpawnCount is None:
                SpawnID = len(SpawnIDs)
            else:  # total number of spawns is known
                SpawnID = [i for i in range(SpawnCount) if i not in SpawnIDs]
                if len(SpawnID) == 0:
                    raise Exception(f'nothing to submit for SpawnCount={SpawnCount}')
                SpawnID = SpawnID[0]

            OutFile, ErrFile = utils.get_log_paths(JobInfo, prev_stdout=spawn_query['stdout'].tolist())

            print(f'new spawn with id={SpawnID}')
            conn.execute("""INSERT INTO spawn(BatchID, JobIndex, SpawnID, ClusterID,
                                              spawn_state, stdout, stderr)
                            VALUES (?,?,?,?,?,?,?)""",
                         [BatchID, JobIndex, SpawnID, ClusterID, spawn_state,
                          OutFile, ErrFile])
            close_db(conn, db_connection)
            break

        except Exception as err:
            close_db(conn, db_connection)
            print(f'spawn_add_to_db: try {t+1}/{tries} failed with:\n{err}\n')
            time.sleep(1)
            if t == tries - 1:
                raise(err)


def spawn_del_from_db(BatchID, JobIndex, db_connection=None, tries=WriteTries,
                      Filter=''):
    filter_str = ''
    if len(Filter):
        filter_str = ' AND ' + Filter

    for t in range(tries):
        try:
            conn = open_db(db_connection)
            conn.execute("""DELETE FROM spawn WHERE
                            BatchID=? AND JobIndex=?""" + filter_str,
                            [BatchID, JobIndex])
            close_db(conn, db_connection)
            break

        except Exception as err:
            close_db(conn, db_connection)
            print(f'spawn_del_from_db: try {t+1}/{tries} failed with:\n{err}\n')
            time.sleep(1)
            if t == tries - 1:
                raise(err)


def print_log(BatchID, JobIndex, LogKey='stdout', LogIndex=-1, Lines=None, RegEx=None):
    """ print job logs. """
    if utils.isiterable(BatchID):
        [print_log(j, JobIndex, LogKey, LogIndex, Lines, RegEx) for j in BatchID]
        return
    if utils.isiterable(JobIndex):
        [print_log(BatchID, p, LogKey, LogIndex, Lines, RegEx) for p in JobIndex]
        return
    if utils.isiterable(LogIndex):
        [print_log(BatchID, JobIndex, LogKey, i, Lines, RegEx) for i in LogIndex]
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
          '/'.join(utils.make_iter(JobInfo['name'])), JobIndex))
    with open(LogFile, 'r') as fid:
        if type(RegEx) is str:
            RegEx = re.compile(RegEx)
        for i, line in enumerate(fid):
            if Lines is not None and i not in Lines:
                continue
            if RegEx is not None and RegEx.search(line) is None:
                continue
            print(line[:-1])


def get_internal_ids(JobInfo, fields=['submit_id', 'ClusterID']):
    # handling running/submitted jobs/spawns by using both fields
    job_list = []
    for f in fields:
        if f in JobInfo:
            job_list += utils.make_iter(JobInfo[f])

    return list(set(job_list))  # unique


def remove_internal_id(JobInfo, jid, fields=['submit_id', 'ClusterID']):
    for f in fields:
        if f not in JobInfo:
            continue

        for j in utils.make_iter(jid):
            if JobInfo[f] == j:
                del JobInfo[f]
            elif type(JobInfo[f]) is list and j in JobInfo[f]:
                JobInfo[f].remove(jid)

        if f in JobInfo and not len(JobInfo[f]):
            del JobInfo[f]
