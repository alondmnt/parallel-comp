# -*- coding: utf-8 -*-
"""
Created on Wed Mar 18 22:45:50 2015

@author: dalon
"""

import re
import os
import subprocess
import sys
import time
import pickle
pickle.HIGHEST_PROTOCOL = 2
import shutil
import warnings
from numpy.random import randint
from collections import Counter
from copy import deepcopy

if 'PBS_JOBID' in os.environ:
    # this also serves as a sign that we're running on power
    PowerID = os.environ['PBS_JOBID'].split('.')[0]
    LogOut = '{}.OU'.format(os.environ['PBS_JOBID'])
    LogErr = '{}.ER'.format(os.environ['PBS_JOBID'])
else:
    PowerID = None
    LogOut = None
    LogErr = None
if 'HOSTNAME' in os.environ:
    hostname = os.environ['HOSTNAME']
else:
    hostname = []
if (PowerID is not None) or ('tau.ac.il' in hostname):
    running_on_power = True
else:
    running_on_power = False

if os.name == 'posix':
    DrivePath = '/tamir1/dalon/'
    if not running_on_power:
        DrivePath = '/media' + DrivePath
else:
    DrivePath = 'T:/dalon/'
QFile = '../jobs/job_queue.pkl'
JobDir = '../jobs/'
PowerQ = 'tamirs3'


def submit_jobs(MaxJobs=None, MinPrior=0, **Filter):
    if not running_on_power:
        print('submit_jobs: not running on power.')
        return
    if PowerID is not None and 'power5' not in hostname:
        if get_qstat()['queue'] == 'nano4':
            print('submit_jobs: cannot submit from nano4.')
            return
    busy_flag = JobDir + 'submitting'
    if os.path.isfile(busy_flag):
        sec_since_submit = time.time() - \
                os.path.getmtime(busy_flag)  #
        if sec_since_submit > 300:
            os.remove(busy_flag)
        else:
            print('already submitting jobs.')
            return
    flag_file = open(busy_flag, 'w')
    if MaxJobs is None:
        try:
            with open(JobDir + 'maxjobs', 'r') as fid:
                MaxJobs = int(fid.readline())
        except:
            pass

    Q = get_queue(Verbose=False, **Filter)

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

    count_in_queue = len(get_power_queue())
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
        count = submit_one_job(j, count, MaxJobs)

    flag_file.close()
    os.remove(busy_flag)
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


def submit_one_job(JobID, SubCount=0, MaxJobs=1e6):
    for j in make_iter(JobID):
        JobInfo = get_job_info(j)
        job_priority = max([0] + [p['priority'] for p in JobInfo
                                  if p['status'] not in
                                  {'complete', 'collected'}])
        for part in JobInfo:
            if part['priority'] < job_priority:
                continue
            if part['status'] == 'init':
                submit_one_part(j, part['JobPart'])
                SubCount += 1
                if SubCount >= MaxJobs:
                    break
    return SubCount


def submit_one_part(JobID, JobPart, Spawn=False):
    DefResource = {'mem': '6gb', 'pmem': '6gb', 'vmem': '12gb',
                   'pvmem': '12gb', 'cput': '04:59:00'}

    global JobDir
    global PowerQ

    ErrDir = os.path.abspath(JobDir) + '/{}/logs/'.format(JobID)
    OutDir = os.path.abspath(JobDir) + '/{}/logs/'.format(JobID)
    if not os.path.isdir(ErrDir):
        os.makedirs(ErrDir)

    Qsub = ['qsub', '-q', PowerQ, '-e', ErrDir, '-o', OutDir, '-l']

    for p in make_iter(JobPart):
        part = get_part_info(JobID, p, HoldFile=True)
        print('submiting:\t{}'.format(part['script']))
        if part['status'] != 'init':
            warnings.warn('already submitted')
        if 'resources' in part:
            this_res = part['resources']
        else:
            this_res = DefResource
        this_sub = Qsub + [','.join(['{}={}'.format(k, v)
                           for k, v in sorted(this_res.items())])]
        if 'queue' in part:
            this_sub[2] = part['queue']
        subprocess.call(this_sub + [part['script']])
#        print(this_sub + [part['script']])
        if not Spawn:
            part['status'] = 'submit'
            part['subtime'] = time.time()
            if 'PowerID' in part:
                del part['PowerID']
            if 'qstat' in part:
                del part['qstat']
        else:
            if part['status'] != 'spawn':
                # remove previous information
                part['status'] = 'spawn'
                part['PowerID'] = []
                part['hostname'] = []
                part['spawn_id'] = []
                part['spawn_complete'] = set()
                part['spawn_resub'] = set()

        update_part(part, Release=True)


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


def get_qstat(JobID=PowerID):
    if JobID is None:
        print('not running on a power node')
        return {}
    try:
        return parse_qstat(subprocess.check_output(['qstat', '-f', JobID]))
    except subprocess.CalledProcessError as e:
        # sometimes this fails on power, not clear why (power does not recognize the jobid)
        print(e)
        return None


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
            Q[job.group(1)] = [line[3], line[9]]
    return Q


def get_queue(Verbose=True, ResetMissing=False, Display=None, **Filter):
    # reads from global job queue file
    Q = pickle.load(open(QFile, 'rb'))
    curr_time = time.time()
    powQ = get_power_queue()
#    powQ = {j: status[1] for j, status in powQ.items() if status[1] == 'R'}

    Qout = deepcopy(Q)
    missing = {}  # submitted but not running
    cnt = Counter()
    for JobID in sorted(list(Q)):
        status = {}
        processed_part = []
        skip_flag = False
        for p, pfile in enumerate(Q[JobID]):
            if not os.path.isfile(pfile):
                continue
            try:
                pinfo = pickle.load(open(pfile, 'rb'))
            except:
                dict_append(status, 'error', p)
                continue
            if len(Filter):
                skip_flag = False  # skip unless all filters matched (AND)
                for k, v in Filter.items():
                    if k not in pinfo:
                        continue
                    for filt in make_iter(v):
                        one_match = False
                        for n in make_iter(pinfo[k]):
                            if filt in n:
                                one_match = True
                        if not one_match:
                            skip_flag = True
                if skip_flag:
                    del Qout[JobID]  # skip entire job if part has a match
                    break
            cnt['total'] += 1
            if 'PowerID' in pinfo:
                for pid in make_iter(pinfo['PowerID']):
                    if pid in powQ:
                        if powQ[pid][1] == 'R':
                            pinfo['JobPart'] = str(pinfo['JobPart']) + '*'
                            cnt['run'] += 1
                    elif pinfo['status'] in {'submit', 'spawn'} and \
                            'subtime' in pinfo:
                        if pinfo['subtime'] < curr_time:
                            dict_append(missing, JobID, pinfo['JobPart'])
            cnt[pinfo['status']] += 1
            dict_append(status, pinfo['status'], pinfo['JobPart'])
            Qout[JobID][p] = pinfo
            processed_part.append(p)

        if skip_flag:
            continue

        Q[JobID] = [Q[JobID][p] for p in processed_part]
        Qout[JobID] = [Qout[JobID][p] for p in processed_part]

        if len(Q[JobID]) == 0:
            print('\nempty {}'.format(JobID))
            del Q[JobID]
            del Qout[JobID]
            continue

        if Verbose:
            if Display is not None:
                status = {s: p for s, p in status.items() if s in Display}
            if len(status) > 0:
                print('\n{}: {}/{}'.format(JobID, pinfo['organism'],
                                           '-'.join(pinfo['name'])))
                print(status)

    if ResetMissing:
        for JobID in missing:
            for JobPart in missing[JobID]:
                PartDir = JobDir + '{}/{}'.format(JobID, JobPart)
                if os.path.isdir(PartDir):
                    shutil.rmtree(PartDir)
                set_part_field(JobID, JobPart, {'status': 'init'})

    if Verbose:
        print('\nmissing jobs: {}'.format(missing))
        cnt['complete'] += cnt['collected']
        print('\ntotal jobs on power queue: {}'.format(len(powQ)))
        try:
             print('running/complete/total: {run}/{complete}/{total}'.format(**cnt))
        except:
            pass
    else:
        return Qout


def get_job_info(JobID):
    Q = pickle.load(open(QFile, 'rb'))
    return [get_part_info(JobID, part) for part in range(len(Q[JobID]))]


def get_job_file(JobID, JobPart):
    Q = pickle.load(open(QFile, 'rb'))
    return Q[JobID][JobPart].replace('/tamir1/dalon/', DrivePath)


def get_part_info(JobID, JobPart, HoldFile=False, SetID=False):
    if SetID:
        HoldFile = True
    JobInfo = pickle.load(open(get_job_file(JobID, JobPart), 'rb'))
    if 'updating_info' not in JobInfo:
        JobInfo['updating_info'] = False
    if HoldFile:
        tries = 1
        while JobInfo['updating_info']:
            time.sleep(randint(1, 10))
            JobInfo = pickle.load(open(get_job_file(JobID, JobPart), 'rb'))
            tries += 1
            if tries > 10:
                raise Exception('cannot update job info')
        JobInfo['updating_info'] = True
        update_part(JobInfo)
    if SetID:
        if JobInfo['status'] == 'spawn':
            JobInfo['PowerID'].append(PowerID)
            JobInfo['hostname'].append(hostname)
            # while the following IDs are set asynchronously, we can test if
            # were set correctly by comparing spawn_count to length of spawn_id
            next_id = 0
            while next_id in JobInfo['spawn_id']:  # find missing
                next_id += 1
            JobInfo['spawn_id'].append(next_id)
        else:
            JobInfo['PowerID'] = PowerID
            JobInfo['hostname'] = hostname
        dict_append(JobInfo, 'stdout', '../jobs/{}/logs/{}'.format(JobID, LogOut))
        dict_append(JobInfo, 'stderr', '../jobs/{}/logs/{}'.format(JobID, LogErr))
        update_part(JobInfo, Release=True)
    return JobInfo


def update_part(PartInfo, Release=False):
    # updates the specific part's local job file (not global queue)
    if Release:
        PartInfo['updating_info'] = False
    if PartInfo['jobfile'] is None:
        PartInfo['jobfile'] = JobDir + '{}/info_{}.pkl'.format(
                PartInfo['JobID'], PartInfo['JobPart'])
    pickle.dump(PartInfo, open(PartInfo['jobfile'], 'wb'), protocol=pickle.HIGHEST_PROTOCOL)


def update_job(JobInfo):
    for p in JobInfo:
        update_part(p)


def add_job_to_queue(JobID, JobParts, QFile=QFile):
    """ provide a list of job info files. """
    if os.path.exists(QFile):
        Q = pickle.load(open(QFile, 'rb'))
    else:
        Q = {}  # init new queue file
    Q[JobID] = JobParts
    pickle.dump(Q, open(QFile, 'wb'), protocol=pickle.HIGHEST_PROTOCOL)
    print('\njob {} (size {:,d}) added to queue ({})'.format(JobID,
          len(JobParts), QFile))


def add_part_to_queue(JobInfo, QFile=QFile):
    """ NOTE: this can turn problematic and recursive (how to define jobfile
        before JobPart is known).
        job info or a list of job info dicts supported.
        automatically sets the JobPart id in each JobInfo (in place). """
    if os.path.exists(QFile):
        Q = pickle.load(open(QFile, 'rb'))
    else:
        Q = {}  # init new queue file
    for part in make_iter(JobInfo):
        JobID = JobInfo['JobID']
        if JobInfo[JobID] not in Q:
            # add a new job
            Q[JobID] = []
        JobInfo['JobPart'] = len(Q[JobID])
        Q[JobID].append(JobInfo['jobfile'])
    pickle.dump(Q, open(QFile, 'wb'), protocol=pickle.HIGHEST_PROTOCOL)


def set_complete(JobID, JobPart):
    set_part_field(JobID, JobPart, Fields={'status': 'complete'}, Unless={})


def set_part_field(JobID, JobPart, Fields={'status': 'init'},
                   Unless={'status': ['complete', 'collected'],
                           'func': 'build_genome',
                           'updating_info': True}):
    for p in make_iter(JobPart):
        part = get_part_info(JobID, p, HoldFile=False)

        skip_part = False
        for k in Unless:  # this tests whether the part is protected
            if k in part:
                if part[k] in make_iter(Unless[k]):
                    skip_part = True
        if skip_part:
            continue

        for k, v in Fields.items():
#            if k in Unless:  # OLD: this tests whether the value being changed is protected
#                if part[k] in make_iter(Unless[k]):
#                    continue
            part[k] = v
        update_part(part, Release=True)


def set_job_field(JobID, Fields={'status': 'init'},
                  Unless={'status': ['complete', 'collected'],
                          'func': 'build_genome',
                          'updating_info': True}):
    # updates the specific parts' local job files (not global queue)
    Q = pickle.load(open(QFile, 'rb'))
    for j in make_iter(JobID):
        for part in range(len(Q[j])):
            set_part_field(j, part, Fields, Unless)


def recover_queue():
    Q = {}
    for dirpath, dirnames, filenames in os.walk(JobDir):
        jid = os.path.basename(dirpath)
        if not jid.isnumeric():
            continue
        jid = int(jid)
        for f in filenames:
            if f.find('info_') >= 0:
                dict_append(Q, jid, dirpath + '/' + f)
    pickle.dump(Q, open(QFile, 'wb'), protocol=pickle.HIGHEST_PROTOCOL)


def remove_job(JobID):
    for j in make_iter(JobID):
        WorkDir = JobDir + str(j)
        if os.path.isdir(WorkDir):
            shutil.rmtree(WorkDir)
        Q = pickle.load(open(QFile, 'rb'))
        if j in Q:
            del Q[j]
        Q = pickle.dump(Q, open(QFile, 'wb'), protocol=pickle.HIGHEST_PROTOCOL)


# TODO: archive_job(JobID=None, Time=None)
# up to Time or any list of JobIDs

# TODO: load_archive(fname)


def clear_collected():
    Q = get_queue(Verbose=False)
    for JobID in Q.keys():
        if all([True if p['status'] == 'collected' else False
                for p in Q[JobID]]):
            remove_job(JobID)


def clean_temp_folders():
    """ clean RP mapping folders from temp files, that are sometimes left
        when jobs get stuck. """
    Q = get_queue(Verbose=False)
    rmv_list = []
    for JobID in Q.keys():
        for part in Q[JobID]:
            tempdir = JobDir + '{}/{}'.format(JobID, part['JobPart'])
            if os.path.exists(tempdir):
                shutil.rmtree(tempdir)
                rmv_list.append(tempdir)
    print('removed {} temp dirs'.format(len(rmv_list)))
    return rmv_list


def dict_append(dictionary, key, value):
    if key not in dictionary:
        dictionary[key] = []
    dictionary[key].append(value)


def boost_job_priority(JobID, Booster=100):
    Q = pickle.load(open(QFile, 'rb'))
    for j in make_iter(JobID):
        for p in Q[j]:
            part = pickle.load(open(p, 'rb'))
            part['priority'] += Booster
            update_part(part)


def spawn_submit(JobInfo, N):
    """ run the selected job multiple times in parallel. job needs to handle
        'spawn' status for correct logic: i.e., only last job to complete
        updates additional fields in JobInfo. """
    set_part_field(JobInfo['JobID'], JobInfo['JobPart'],
                   Fields={'spawn_count': N+1,
                           'stdout': [], 'stderr': []}, Unless={})
    for i in range(N):
        time.sleep(10)  # avoid collisions
        submit_one_part(JobInfo['JobID'], JobInfo['JobPart'], Spawn=True)

    # update current job with spawn IDs
    return get_part_info(JobInfo['JobID'], JobInfo['JobPart'], SetID=True)


def spawn_complete(JobInfo):
    """ signal that one spawn has ended successfully. only update done by
        current job to JobInfo, unless all spawns completed. """
    my_id = JobInfo['spawn_id'][-1]
    JobInfo = get_part_info(JobInfo['JobID'], JobInfo['JobPart'], HoldFile=True)
    if JobInfo['status'] != 'spawn':
        # must not leave function with an ambiguous status in JobInfo
        # e.g., if job has been re-submitted by some one/job
        # (unknown logic follows)
        raise Exception('unknown status [{}] encountered for spawned ' +
                        'job ({}, {})'.format(JobInfo['status'],
                                              JobInfo['JobID'],
                                              JobInfo['JobPart']))

    try:
        JobInfo['PowerID'].remove(PowerID)
        JobInfo['hostname'].remove(hostname)
    except:
        pass
    JobInfo['spawn_complete'].add(my_id)
    update_part(JobInfo, Release=True)

    if len(JobInfo['spawn_id']) != JobInfo['spawn_count']:
        # unexplained frequent problem, tried to work this out by delaying sub
        # cannot resubmit because jobs may still be queued at this point
        print('if queue is *empty*, consider using',
              'power.spawn_resubmit({JobID}, {JobPart})'.format(**JobInfo))
    elif len(JobInfo['PowerID']) == 0:
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
            update_part(JobInfo)
            spawn_resubmit(JobInfo['JobID'], JobInfo['JobPart'])

        else:
            # reinstate job id and submit status (so it is recognized by get_queue())
            JobInfo['status'] = 'submit'
            JobInfo['hostname'] = hostname
            JobInfo['PowerID'] = PowerID
            update_part(JobInfo)
            # set but not update yet (delay post-completion submissions)
            JobInfo['status'] = 'complete'

    return JobInfo


def spawn_resubmit(JobID, JobPart):
    """ submit missing spawns. """
    JobInfo = get_part_info(JobID, JobPart)
    if JobInfo['status'] == 'spawn':
        for i in range(len(JobInfo['spawn_id']), JobInfo['spawn_count']):
            time.sleep(10)
            submit_one_part(JobInfo['JobID'], JobInfo['JobPart'], Spawn=True)


def isiterable(p_object):
    try:
        it = iter(p_object)
    except TypeError:
        return False
    return True


def print_log(JobID, JobPart, LogKey='stdout', LogIndex=-1):
    """ print job logs. """
    if isiterable(JobID):
        [print_log(j, JobPart, LogKey, LogIndex) for j in JobID]
        return
    if isiterable(JobPart):
        [print_log(JobID, p, LogKey, LogIndex) for p in JobPart]
        return
    if isiterable(LogIndex):
        [print_log(JobID, JobPart, LogKey, i) for i in LogIndex]
        return

    JobInfo = get_part_info(JobID, JobPart)
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

    print('\n\n[[[{} log for {}/{}/part_{}:]]]\n'.format(LogKey, JobID,
          '/'.join(JobInfo['name']), JobPart))
    with open(LogFile, 'r') as fid:
        for line in fid:
            print(line[:-1])


def generate_script(JobInfo, Template=None, JobDir=JobDir):
    """ originally from RP module. """
    if Template is None:
        Template = JobInfo['script']
    if JobDir[-1] != '/' and JobDir[-1] != '\\':
        JobDir = JobDir + '/'
    OutDir = JobDir + str(JobInfo['JobID'])
    if not os.path.isdir(OutDir):
        os.makedirs(OutDir)
    tmp = os.path.basename(Template).split('.')
    ext = tmp[-1]
    tmp = tmp[0].split('_')[0]
    OutScript = '/{}_{JobID}_{JobPart}.{}'.format(tmp, ext, **JobInfo)
    OutScript = OutDir + OutScript
    with open(Template) as fid:
        with open(OutScript, 'w') as oid:
            for line in fid:
                oid.write(line.format(**JobInfo))
    os.chmod(OutScript, 0o744)
    JobInfo['script'] = OutScript


def generate_data(JobInfo, Data, JobDir=JobDir):
    """ standard file naming / dump. """
    JobInfo['data'] = '{}/{}/data_{}.pkl'.format(JobDir, JobInfo['JobID'],
                                                 JobInfo['JobPart'])
    if not os.path.isdir(os.path.dirname(JobInfo['data'])):
        os.makedirs(os.path.dirname(JobInfo['data']))
    pickle.dump(Data, open(JobInfo['data'], 'wb'), protocol=pickle.HIGHEST_PROTOCOL)
