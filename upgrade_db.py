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

from pbsmgr import QFile


def upgrade_db(QFile=QFile):
    init_db(QFile)
    populate_db(QFile)


def connect(QFile, exists_OK=True, **kwargs):
    db_file = '.'.join(QFile.split('.')[:-1] + ['db'])
    if os.path.isfile(db_file) and not exists_OK:
        raise Exception(f'database "{db_file}" already exist')
    return sqlite3.connect(db_file, **kwargs)


def init_db(QFile=QFile):
    """ creating tables """

    conn = connect(QFile, exists_OK=False)

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

    conn.commit()
    conn.close()


def populate_db(QFile=QFile):
    """ add all currently queued jobs to new DB. """

    conn = connect(QFile, exists_OK=True)
    Q = get_queue(QFile=QFile)

    for batch_id, batch in Q.items():
        conn.execute("""INSERT INTO batch
                        VALUES (?,?,?,?)""",
                     [batch_id, '/'.join(batch[0]['name']),
                      batch[0]['organism'], batch[0]['data_type']])
        for job in batch:
            metadata = zlib.compress(bytes(json.dumps(job, default=set_default),
                                           'UTF-8'))
            md5 = hashlib.md5(metadata).hexdigest()
            conn.execute("""INSERT INTO job(JobIndex, BatchID, status,
                                            priority, metadata, md5)
                            VALUES (?,?,?,?,?,?)""",
                         [job['JobIndex'], batch_id, job['status'],
                          job['priority'],
                          metadata, md5])

    conn.commit()

    # test new DB
    reconstructed_Q = get_sql_queue(conn)
    assert all([json.dumps(reconstructed_Q[b]) == 
                json.dumps(Q[b], default=set_default)
                for b in Q])

    conn.close()


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


def get_sql_queue(conn):
    df = pd.read_sql_query("""SELECT j.*,
                                     b.name,
                                     b.organism,
                                     b.data_type
                               FROM batch b INNER JOIN job j 
                               ON j.BatchID = b.BatchID""", conn)
    return df.iloc[:, 1:].groupby('BatchID')\
        .apply(lambda df: df.sort_values('JobIndex')['metadata']\
               .apply(lambda x: json.loads(zlib.decompress(x).decode()))\
               .tolist()).to_dict()


def make_iter(obj):
    if isinstance(obj, str):
        return [obj]
    try:
        return list(obj)
    except:
        return [obj]


def rename_key(d, old_key, new_key):
    return {new_key if k == old_key else k:v for k,v in d.items()}


def set_default(obj):
    if isinstance(obj, set):
        return list(obj)
    raise TypeError
