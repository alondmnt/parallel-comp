# -*- coding: utf-8 -*-
"""
PARASCHUT: parallel job scheduling utils.
see also: README.md, example.ipynb

this submodule handles the generation of new jobs (and their removal).

@author: Alon Diament, Tuller Lab
Created on Wed Mar 18 22:45:50 2015
"""
from collections import Counter
from copy import deepcopy
from glob import iglob
import os
import pickle
import shutil

from .config import JobTemplate, JobDir, TempFiles
from . import dal
from . import manage
from . import utils


def get_job_template(SetID=False):
    res = deepcopy(JobTemplate)
    if SetID:
        res['BatchID'] = utils.get_id()
    return res


def generate_script(JobInfo, Template=None):
    """ will generate a script based on a template while replacing all fields
        appearing in the template (within curly brackets {fieldname}) according
        to the value of the field in JobInfo.

        example:
        JobInfo = generate_script(JobInfo) """
    global JobDir
    if Template is None:
        Template = JobInfo['script']
    # case: command line
    if not os.path.isfile(Template):
        JobInfo['script'] = Template.format(**JobInfo)
        return JobInfo

    # case: script
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

    return JobInfo


def generate_data(JobInfo, Data):
    """ will save data and update metadata with the file location.
        standard file naming / dump.

        example:
        vec = np.ones(10**6)
        JobInfo = generate_data(JobInfo, vec) """
    JobInfo['data'] = '{}/{}/data_{}.pkl'.format(JobDir, JobInfo['BatchID'],
                                                 JobInfo['JobIndex'])
    if not os.path.isdir(os.path.dirname(JobInfo['data'])):
        os.makedirs(os.path.dirname(JobInfo['data']))
    pickle.dump(Data, open(JobInfo['data'], 'wb'), protocol=pickle.HIGHEST_PROTOCOL)

    return JobInfo


def add_batch_to_queue(BatchID, Jobs, set_index=False, build_script=False):
    """ provide a list of job info dicts, will replace any existing
        batch. unlike add_job_to_queue it ensures that BatchID doe not
        exist in queue, and that all given jobs are from the same batch. """

    Jobs = utils.make_iter(Jobs)
    if not all([BatchID == job['BatchID'] for job in Jobs]):
        raise Exception(f'some jobs do not match BatchID={BatchID}')

    if len(dal.get_job_indices(BatchID)) > 0:
        raise Exception(f'BatchID={BatchID} already exists')

    add_job_to_queue(Jobs, set_index=set_index, build_script=build_script)
    print('\nbatch {} (size {:,d}) added to queue ({})'.format(BatchID,
          len(Jobs), dal.QFile))


def add_job_to_queue(job_list, set_index=False, build_script=False):
    """ job info or a list of job info dicts supported.
        automatically sets the JobIndex id in each JobInfo,
        and saves the metdata. """
    conn = dal.open_db()
    batch_index = {}
    for job in utils.make_iter(job_list):
        BatchID = job['BatchID']

        if BatchID not in batch_index:
            batch_index[BatchID] = len(dal.get_job_indices(BatchID))
        if batch_index[BatchID] == 0:
            # init new batch
            conn.execute("""INSERT INTO batch
                            VALUES (?,?,?)""",
                            [BatchID, '/'.join(job['name']),
                             job['data_type']])

        if set_index:
            job['JobIndex'] = batch_index[BatchID]
        if build_script:
            job = generate_script(job)
        batch_index[BatchID] += 1
        metadata = dal.pack_job(job)
        conn.execute("""INSERT INTO job(JobIndex, BatchID, state,
                                        priority, metadata, md5)
                        VALUES (?,?,?,?,?,?)""",
                     [job['JobIndex'], BatchID, job['state'],
                      job['priority'], metadata, job['md5']])

    dal.close_db(conn)


def remove_batch(BatchID):
    conn = dal.open_db()

    for batch in utils.make_iter(BatchID):
        WorkDir = JobDir + str(batch)
        if os.path.isdir(WorkDir):
            shutil.rmtree(WorkDir)
        conn.execute(f"""DELETE FROM batch
                         WHERE BatchID={batch}""")
        conn.execute(f"""DELETE FROM job
                         WHERE BatchID={batch}""")

    dal.close_db(conn)


def remove_batch_by_state(state):
    """ can provide an itreable of states to consider the union of possible
        states. jobs in corresponding the batches will be removed from queue. """
    Q = manage.get_queue(Verbose=False)
    for BatchID in Q.keys():
        if all([True if p['state'] in state else False
                for p in Q[BatchID]]):
            remove_batch(BatchID)


def clean_temp_folders(batch_list=None):
    """ clean folders from temp files, that are sometimes left
        when jobs get stuck. """
    if batch_list is None:
        batch_list = manage.get_queue(Verbose=False).keys()

    rmv_list = []
    for BatchID in utils.make_iter(batch_list):
        for job in dal.get_batch_info(BatchID):
            tempdir = JobDir + '{}/{}'.format(BatchID, job['JobIndex'])
            if os.path.isdir(tempdir):
                shutil.rmtree(tempdir)
                rmv_list.append(tempdir)
    print('removed {} temp dirs'.format(len(rmv_list)))
    return rmv_list


def clean_temp_files(dir_str=TempFiles, batch_list=None):
    """ clean files based on [dir_str]. e.g., '*.genes'. """
    if batch_list is None:
        batch_list = manage.get_queue(Verbose=False).keys()

    rmv_dict = Counter()
    for BatchID in utils.make_iter(batch_list):
        for pattern in utils.make_iter(dir_str):
            rmv_dict[BatchID] += len([os.remove(f) for f in
                    iglob(f'{JobDir}{BatchID}/{pattern}')])

    print(f'files removed:\n{rmv_dict}')
