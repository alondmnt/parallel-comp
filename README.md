# pbsmgr

`pbsmgr` is a job management package for PBS (portable batch system) and can be easily adjusted to similar HPC cluster systems (such as SGE, Torque, Slurm, condor, etc.). it is used in conjnuction with the cluster's batch manager to submit and monitor jobs. it is de-centralized (does not require a dedicated node running as a server), based on SQLite, controls the execution flow of inter-dependent jobs, handle job errors, and manipulates job parameters at scale.

see also the Jupyter notebook [example.ipynb](example.ipynb).

## general concepts

jobs are organized in batches that may contain any number of jobs. usually, a batch is comprised of jobs with the same objective, so that when all jobs in the batch end - the pipeline is complete. this manager contains utilities for generating such batches, controlling their execution and monitoring them for failure/success.

in practice, when a job is submitted on the cluster it goes through this typical flow: (1) when the job is created via pbsmgr (data and metadata are prepared) its state is set to 'init'. (2) when it is submitted via pbsmgr (to a queue on the cluster) its state is set to 'submit'. (3) during execution, a script specific to job is run. (4) the user-defined function asks pbsmgr for the job metadata. (5) while doing so, the job state is updated to 'run' (for monitoring). (6) when the job is complete, the state is updated once again to 'complete'. (7) if the output data is processed post-completion the job state may be updated once again to indicate that (e.g., using 'collected').

an index of all batches and jobs and their metadata is stored in a single SQLite DB. batches are organized in directories (one for each batch), and often have additional input and output data (this is up to the user to decide). jobs that completed are still kept in this index until they are explicitly removed, which may help in post-mortem / post-completion analysis, and when the pipeline is meant to run multiple times. job parameters may also be manipulated before re-run.

job metadata is kept in a dictionary, such as the one in the following example. the example shows the minimal set of required fields, but additional ones may be added by the user.

        >> pbsmgr.get_job_template()
        {'BatchID': None,
         'JobIndex': None,
         'data': None,
         'jobfile': None,
         'name': ['human', 'genome_map'],
         'priority': 1,
         'queue': 'tamirs3',
         'resources': {'cput': '04:59:00',
          'mem': '1gb',
          'pmem': '1gb',
          'pvmem': '3gb',
          'vmem': '3gb'},
         'script': 'my_template_script.sh',
         'status': 'init'}

note the 'priority' field and its score. jobs are submitted in priority groups in descending order. first, all jobs within a batch with the highest priority score will be submitted. the next-ranked priority group will be submitted only once they have completed, and so on. this allows one, for example, to separate the execution of a Map step and a Reduce step when implementing a MapReduce workflow. the absolute scores assigned as priorities can be arbitrary as long as they are ordered, with one exception: jobs with priorities that exceed 100 are globally prioritized - across all batches (the top ones are submitted before any other job in any other batch is). future versions may implement more elaborate dependencies between jobs.

note, that some system-specific definitions appear in `config.py`.

## user API

### job monitoring

**job status**

the most compact display of all job states is using the get\_queue() function. it can be run locally, but has some additional functionality that is only accessible when running on the cluster. to get a full report, run:

        pbsmgr.get_queue()

for each job, along with a title containing the batch-id and name, `get_queue` will tediously display the status of all its jobs in a compact output, sorted by the following types of status:

* init: set when generated / reset. only type available for submission.

* submit: after submission.

* run: one a job is oneline. will be denoted by an asterisk (*) when the job is recognized as currently online/active based on the cluster's `qstat` command.

* spawn: a state where a job duplicates and resubmits itself multiple times spontaneaously, when necessary. will be denoted by multiple asterisks (according to the number of spawned jobs). meant for example, for handling subsets of the data independently (when the subset structure is not known a priori) in bottleneck stages of the pipeline.

* complete: after a job terminates successfully (with a valid result).

* collected: after result has been successfully handled by a downstream job and its output data is no longer needed (and intermediate data has usually been deleted). for example, this job may be part of a Map step, and its output has been recently processed by a Reduce step in a MapReduce workflow.

NOTE, that additional job states may be arbitrarily defined by the user (simply by updating the metadata accordingly during the run, see example/job.py).

`get_queue` can limit the the display of jobs to selected states, using the following syntax:

        pbsmgr.get_queue(Display={'submit', 'run', 'spawn})

e.g., for displaying only a compact report of currently supposed-to-be running jobs.

`get_queue` can also filter jobs according any meta-data field, such as:

        pbsmgr.get_queue(Filter='name LIKE "%Ingolia_2009%" AND BatchID > 20210101000000 AND data_type=="ribo"')

e.g., for displaying only jobs from the Ingolia-2009 experiment queued after January 1st 2021. Filter accepts any number of SQLite conditions.

_missing jobs_ will be reported by `get_queue` according to the following criterion: a job that has begun running but disappeared from queue before completing. this can be a useful tool to identify jobs that need resubmitting / debugging. you may ask `get_queue` to reset their state, or print their logs automatically.

at the bottom of the report, a summary will display the total number of jobs the user currently has on PBS queues, the number of recognized running jobs, the number of completed jobs out of those that matched the filtering conditions.

**logs**

after a job has closed, you can print its stdout / stderr, using `print_log()`, as follows: (last 2 args are optional)

        pbsmgr.print_log(<batch_id>, <job_idx>, <opt:'stdout'/'stderr'>, <opt: history_idx>, <opt: lines to print>, <opt: regex>)

for example, to display the last recorded error log:

        pbsmgr.print_log(1482002879, 0, 'stderr')

**job meta-data**

if you need lower level access to job properties, the following function retrieves everything known to us about the job.

        pbsmgr.get_job_info(<batch_id>, <job_idx>)

this includes, for instance, the last qstat data recorded before the job shut down, the data files used as input/output, its PBS_JOBID, hostname, etc. in most cases, it is unnecessary to access this data.

### job submission

to submit all jobs according to priority rules (see [general concepts](#general-concepts) above), use the following command:

        pbsmgr.submit_jobs()

the same filters (such as job names, etc.) that were described above for `get_queue` can be used here as well.

**submitting specific jobs**

        pbsmgr.submit_one_batch(BatchID)
        pbsmgr.submit_one_job(BatchID, JobIndex)

**resubmitting jobs**

when the jobs you want to resubmit are recognized as 'missing' (see section on job status), you can use `get_queue` to reset their status to 'init' automatically:

        pbsmgr.get_queue(ResetMissing=True)

if a job was stuck in 'spawn' status, you may try the following: (before resorting to initializing status)

        pbsmgr.spawn_resubmit(<batch_id>, <job_idx>)

more generally, you may:

* get BatchIDs, e.g., all jobs with the name Yang-2015 can be retrieved by calling get-queue with the flag Verbose=False (instead of printing to screen, it will generate a new variable containing all relevant jobs):

        B = pbsmgr.get_queue(Filter='name LIKE "%Yang_2015%"', Verbose=False)

* set job status back to 'init'

        pbsmgr.set_batch_field(B)

by default, this function sets the status back to init _unless_ the job already completed (thus, it allows you to submit only failed jobs). to reset the job completely (regardless of job status), use:

        pbsmgr.set_batch_field(B, Unless={})

multiple JobIndexes can be provided as a list, i.e. [1, 2, ...]. similarly, you can set the status of specific jobs within the batch using:

        pbsmgr.set_job_field(B, J)

* submit again (can also wait for automatic submission to take place, if there are other jobs running)

        pbsmgr.submit_jobs()

**deleting jobs from queue**

* you may delete jobs from the PBS queue using:

        pbsmgr.qdel_batch(B)
        pbsmgr.qdel_job(B, J)

**deleting jobs from DB**

* you may remove jobs from the job DB using:

        pbsmgr.remove_batch(B)
        pbsmgr.remove_batch_by_state(state)

## program API

the following functions are useful to create and use jobs from within scripts and programs.

### job preparation

        pbsmgr.get_job_template(SetID=True)  # will also set BatchID to current time in seconds
        pbsmgr.generate_script(JobInfo)  # will generate a job-specific execution script from a template
        pbsmgr.generate_data(JobInfo, Data)  # will save data and update metadata with the file location
        pbsmgr.add_batch_to_queue(BatchID, Jobs)
        pbsmgr.add_job_to_queue(JobInfo)

### job runtime

        pbsmgr.get_job_info(SetID=True)  # will update the hostname and PBS_ID of the running job
        pbsmgr.get_qstat()  # can be used to report on metadata from the PBS system
        pbsmgr.set_complete()  # set state when job is done
        pbsmgr.update_job(JobInfo)  # for more elaborate updates to job metadata
        pbsmgr.spawn_submit()
        pbsmgr.spawn_complete()
        pbsmgr.spawn_resubmit()  # for re-running missing spawn jobs
        pbsmgr.spawn_get_info()  # gets the status of all spawns associated with this job
        pbsmgr.submit_jobs()  # can be used after completion to submit the next priority group
