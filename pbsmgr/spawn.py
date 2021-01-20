# -*- coding: utf-8 -*-
"""
a PBS (portable batch system) parallel-computing job manager.
see also: README.md, example.ipynb

this submodule handles spawn jobs.

@author: Alon Diament, Tuller Lab
Created on Wed Mar 18 22:45:50 2015
"""
import pandas as pd

from .config import WriteTries, LogOut, LogErr
from . import dal


def spawn_add_to_db(BatchID, JobIndex, PBS_ID, SpawnCount=None,
                    db_connection=None, tries=WriteTries):
    for t in range(tries):
        try:
            conn = dal.open_db(db_connection)

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
            dal.close_db(conn, db_connection)
            break

        except Exception as err:
            dal.close_db(conn, db_connection)
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
            conn = dal.open_db(db_connection)
            conn.execute("""DELETE FROM spawn WHERE
                            BatchID=? AND JobIndex=?""" + filter_str,
                            [BatchID, JobIndex])
            dal.close_db(conn, db_connection)
            break

        except Exception as err:
            dal.close_db(conn, db_connection)
            print(f'spawn_del_from_db: try {t+1} failed with:\n{err}\n')
            if t == tries - 1:
                raise(err)


def spawn_complete(JobInfo, db_connection=None, tries=WriteTries):
    """ signal that one spawn has ended successfully. only update done by
        current job to JobInfo, unless all spawns completed. """

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
                # # we run the following just in case some job failed
                # # I think this shouldn't cause any infinite loop
                # spawn_resubmit(JobInfo['BatchID'], JobInfo['JobIndex'])
                break

            # all spawn jobs completed
            # update parent job, remove spawns from DB
            JobInfo.update(spawn_dict)
            JobInfo['state'] = 'run'
            dal.update_job(JobInfo, db_connection=conn)
            JobInfo['state'] = 'complete'
            # signal calling function that we're done, but defer broadcasting
            # to all other jobs yet

            spawn_del_from_db(JobInfo['BatchID'], JobInfo['JobIndex'],
                              db_connection=conn)
            dal.close_db(conn, db_connection)
            break

        except Exception as err:
            dal.close_db(conn, db_connection)
            print(f'spawn_complete: try {t+1} failed with:\n{err}\n')
            if t == tries - 1:
                raise(err)

    return JobInfo
