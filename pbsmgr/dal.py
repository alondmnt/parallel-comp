# -*- coding: utf-8 -*-
"""
a PBS (portable batch system) parallel-computing job manager.
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
import warnings
import zlib

import pandas as pd

from .config import QFile, WriteTries, LogOut, LogErr, PBS_ID, hostname
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
                    spawn_state TEXT    NOT NULL
                    );""")


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
                 db_connection=None, enforce_unique_job=True, ignore_spawn=False):
    """ HoldFile is ignored but kept for backward compatibility. """
    conn = open_db(db_connection)
    job_query = list(conn.execute(f"""SELECT metadata, md5 from job WHERE
                                      BatchID={BatchID} AND
                                      JobIndex={JobIndex}"""))
    if len(job_query) != 1:
        if enforce_unique_job:
            warnings.warn(f'job ({BatchID, JobIndex}) is not unique ({len(job_query)}). running make_job_unique()')
            make_job_unique(BatchID, JobIndex, db_connection=conn)
            job_query = job_query[-1:]
        else:
            close_db(conn)
            raise Exception(f'job ({BatchID}, {JobIndex}) is not unique ({len(job_query)}). run make_job_unique()')
    job_query = job_query[0]
    JobInfo = unpack_job(job_query)

    close_db(conn, db_connection)

    if not ignore_spawn and JobInfo['state'] == 'spawn':
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


def update_job(JobInfo, Release=False, db_connection=None, tries=WriteTries,
               enforce_unique_job=True):
    """ Release is ignored but kept for backward compatibility. """
    BatchID, JobIndex = JobInfo['BatchID'], JobInfo['JobIndex']
    PID = JobInfo['PBS_ID'] if 'PBS_ID' in JobInfo else 'unkown_PBS_ID'

    for t in range(tries):
        try:
            conn = open_db(db_connection)
            n_change = conn.total_changes
            md5 = list(conn.execute(f"""SELECT idx, metadata, md5
                                        FROM job WHERE
                                        BatchID={BatchID} AND
                                        JobIndex={JobIndex}"""))
            if len(md5) != 1:
                if enforce_unique_job:
                    warnings.warn(f'job ({BatchID, JobIndex}) is not unique ({len(md5)}). running make_job_unique()')
                    make_job_unique(BatchID, JobIndex, db_connection=conn)
                    md5 = md5[-1:]
                else:
                    raise Exception(f'job ({BatchID, JobIndex}) is not unique ({len(md5)}). run make_job_unique()')
            # here we're ensuring that JobInfo contains an updated version
            # of the data, that is consistent with the DB
            if md5[0][-1] != JobInfo['md5']:
                other_id = unpack_job(md5[0])['PBS_ID']
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
                         [md5[0][0], JobInfo['BatchID'], JobInfo['JobIndex']])
            if (conn.total_changes - n_change) < 2:
                raise Exception('failed to update job' +
                                '(probably trying to delete an already deleted entry)')

            close_db(conn, db_connection)
            break

        except Exception as err:
            close_db(conn, db_connection)
            print(f'update_job: try {t+1} failed with:\n{err}\n')
            JobInfo['md5'] = md5[0][-1]  # revert to previous md5 to pass tests
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


def spawn_add_to_db(BatchID, JobIndex, PBS_ID, SpawnCount=None,
                    db_connection=None, tries=WriteTries):
    for t in range(tries):
        try:
            conn = open_db(db_connection)

            # auto-numbering of spawns
            SpawnIDs = pd.read_sql(f"""SELECT SpawnID FROM spawn
                                       WHERE BatchID={BatchID} AND
                                       JobIndex={JobIndex}""", conn)['SpawnID']
            if SpawnCount is None:
                SpawnID = len(SpawnIDs)
            else:  # total number of spawns is known
                SpawnID = [i for i in range(SpawnCount) if i not in SpawnIDs]
                if len(SpawnID) == 0:
                    raise Exception(f'nothing to submit for SpawnCount={SpawnCount}')
                SpawnID = SpawnID[0]

            print(f'new spawn with id={SpawnID}')
            conn.execute("""INSERT INTO spawn(BatchID, JobIndex, SpawnID, PBS_ID,
                                              spawn_state, stdout, stderr)
                            VALUES (?,?,?,?,?,?,?)""",
                         [BatchID, JobIndex, SpawnID, PBS_ID,
                          'submit', LogOut.format(BatchID=BatchID, submit_id=PBS_ID),
                          LogErr.format(BatchID=BatchID, submit_id=PBS_ID)])
            close_db(conn, db_connection)
            break

        except Exception as err:
            close_db(conn, db_connection)
            print(f'spawn_add_to_db: try {t+1} failed with:\n{err}\n')
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
            print(f'spawn_del_from_db: try {t+1} failed with:\n{err}\n')
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
          '/'.join(JobInfo['name']), JobIndex))
    with open(LogFile, 'r') as fid:
        if type(RegEx) is str:
            RegEx = re.compile(RegEx)
        for i, line in enumerate(fid):
            if Lines is not None and i not in Lines:
                continue
            if RegEx is not None and RegEx.search(line) is None:
                continue
            print(line[:-1])
