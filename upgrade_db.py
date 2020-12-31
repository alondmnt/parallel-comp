# -*- coding: utf-8 -*-
"""
functions for upgrading to the database version 2.

Created on Wed Dec 23 21:33:03 2020

@author: Alon Diament, Tuller Lab
"""

import hashlib
import json
import os
import pickle
import sqlite3
import zlib

import pandas as pd

from pbsmgr import QFile, init_db, pack_job, unpack_job, get_sql_queue


def upgrade_db(QFile=QFile):
    with connect(QFile, exists_OK=False) as conn:
        init_db(conn)
    populate_db(QFile)


def connect(QFile, exists_OK=True, **kwargs):
    db_file = '.'.join(QFile.split('.')[:-1] + ['db'])
    if os.path.isfile(db_file) and not exists_OK:
        raise Exception(f'database "{db_file}" already exist')
    return sqlite3.connect(db_file, **kwargs)


def populate_db(QFile=QFile):
    """ add all currently queued jobs to new DB. """

    conn = connect(QFile, exists_OK=True)
    Q = get_queue(QFile=QFile)

    for batch_id, batch in Q.items():
        for job in batch:
            upgrade_job(job)            
            metadata = pack_job(job)
            conn.execute("""INSERT INTO job(JobIndex, BatchID, state,
                                            priority, metadata, md5)
                            VALUES (?,?,?,?,?,?)""",
                         [job['JobIndex'], batch_id, job['state'],
                          job['priority'], metadata, job['md5']])

        conn.execute("""INSERT INTO batch
                        VALUES (?,?,?,?)""",
                     [batch_id, '/'.join(batch[0]['name']),
                      batch[0]['organism'], batch[0]['data_type']])

    conn.commit()
    conn.close()

    # test new DB
    reconstructed_Q = get_sql_queue('.'.join(QFile.split('.')[:-1] + ['db']))
    assert all([json.dumps(reconstructed_Q[b], default=set_default) == 
                json.dumps(Q[b], default=set_default)
                for b in Q])


def upgrade_job(JobInfo):
    if 'organism' in JobInfo:
        JobInfo['name'] = [JobInfo['organism']] + JobInfo['name']
    if 'status' in JobInfo:
        JobInfo['state'] = JobInfo['status']
        del JobInfo['status']


def get_queue(QFile):
    """ legacy get_queue for databse version 1. """

    # reads from global job queue file
    Q = pickle.load(open(QFile, 'rb'))

    for BatchID in sorted(list(Q)):
        processed = []
        for p, pfile in enumerate(Q[BatchID]):
            try:
                pinfo = pickle.load(open(pfile, 'rb'))
            except Exception as err:
                print(f'pickle error: {err}')
                continue

            pinfo = rename_key(pinfo, 'JobID', 'BatchID')
            pinfo = rename_key(pinfo, 'JobPart', 'JobIndex')
            Q[BatchID][p] = pinfo  # replace `pfile` with data
            processed.append(p)

        Q[BatchID] = [Q[BatchID][p] for p in processed]

        if len(Q[BatchID]) == 0:
            del Q[BatchID]
            continue

    return Q


def rename_key(d, old_key, new_key):
    return {new_key if k == old_key else k:v for k,v in d.items()}


def set_default(obj):
    if isinstance(obj, set):
        return list(obj)
    raise TypeError
