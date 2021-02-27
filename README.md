# PARASCHUT: PARAllel SCHeduling UTils

`paraschut` is a job management package. it is used to submit and monitor jobs, locally or on a server. currently, it supports PBS (portable batch system) and SGE (Sun Grid Engine), and can be easily [extended](#extending-to-other-job-schedulers) to other HPC cluster systems (such as Torque, Slurm, condor, etc.). conveniently, it can also turn your local machine into a small cluster by managing multi-processses via the same API. it is de-centralized (does not require a dedicated node running as a server), based on SQLite, controls the execution flow of inter-dependent jobs, handles job errors, and manipulates job parameters at scale.

see also the Jupyter notebook [example.ipynb](example.ipynb).

## typical workflow

jobs are organized in batches that may contain any number of jobs. usually, a batch is comprised of jobs with the same objective, so that when all jobs in the batch end - the pipeline is complete. this manager contains utilities for generating such batches, controlling their execution and monitoring them for failure/success.

in practice, when a job is submitted on the cluster it goes through this typical flow: (1) when the job is created via `paraschut` (data and metadata are prepared) its state is set to 'init'. (2) when it is submitted via `paraschut` (to a queue on the cluster) its state is set to 'submit'. (3) during execution, a script specific to job is run. (4) the user-defined function asks `paraschut` for the job metadata. (5) while doing so, the job state is updated to 'run' (for monitoring). (6) when the job is complete, the state is updated once again to 'complete'. usually, the job will also attempt to submit any new valid jobs that were not submitted before. (7) if the output data is processed post-completion the job state may be updated once again to indicate that (e.g., using 'collected').

an index of all batches and jobs and their metadata is stored in a single SQLite DB. batches are organized in directories (one for each batch), and often have additional input and output data (this is up to the user to decide). jobs that completed are still kept in this index until they are explicitly removed, which may help in post-mortem / post-completion analysis, and when the pipeline is meant to run multiple times. job parameters may also be manipulated before re-run.

job metadata is kept in a dictionary, such as the one in the following example. the example shows the minimal set of required fields, but additional ones may be added by the user.

        >> import paraschut as psu
        >> psu.get_job_template()
        {'BatchID': None,
         'JobIndex': None,
         'priority': 1,
         'name': ['human', 'genome_map'],
         'data_type': 'foo',
         'data': None,
         'script': 'my_template_script.sh',
         'queue': 'tamirs3',
         'resources': {'cput': '04:59:00',
          'mem': '1gb',
          'pmem': '1gb',
          'pvmem': '3gb',
          'vmem': '3gb'},
         'state': 'init'}

note the 'priority' field and its score. jobs are submitted in priority groups in descending order. first, all jobs within a batch with the highest priority score will be submitted. the next-ranked priority group will be submitted only once they have successfully completed, and so on. this allows one, for example, to separate the execution of a Map step and a Reduce step when implementing a MapReduce workflow. the absolute scores assigned as priorities can be arbitrary as long as they are ordered, with one exception: jobs with priorities that exceed 100 are globally prioritized - across all batches (the top ones are submitted before any other job in any other batch is). future versions may implement more elaborate dependencies between jobs.

an additional field - 'vars' - may be added to the metadata, containing a dict of variable/value pairs that will be set as environment variables for the specific job. this can be used as a method for passing arguments to scripts.

NOTE, that some system-specific definitions appear in `config.py`. for example, in order to use your machine as local cluster, set LocalRun there to True.

## user API

### job monitoring

**job states**

a dashboard of all job states may be printed using the `get_queue()` function. it can be run locally (where there is access to the DB file), but has some additional functionality that is only accessible when running on the cluster. to get a full report, run:

        get_queue()

this will display a summary of the state for each job, along with a title containing the BatchID and its name, aggregated by the following types of states:

* init: set when generated / reset. only type available for submission.

* submit: after submission.

* run: one a job is oneline. will be denoted by an asterisk (*) when the job is recognized as currently online/active based on the cluster's `qstat` command.

* spawn: a state where a job duplicates and resubmits itself multiple times spontaneaously, when necessary. will be denoted by multiple asterisks (according to the number of spawned jobs). meant for example, for handling subsets of the data independently (when the subset structure is not known a priori) in bottleneck stages of the pipeline.

* complete: after a job terminates successfully (with a valid result).

* collected: after result were successfully handled by a downstream job and its output data is no longer needed (and intermediate data were usually deleted). for example, this job may be part of a Map step, and its output has been recently processed by a Reduce step in a MapReduce workflow.

NOTE, that additional job states may be arbitrarily defined by the user (simply by updating the metadata accordingly during the run, see example/job.py).

`get_queue` can limit the the display of jobs to selected states, by using an additional argument:

        get_queue(Display={'submit', 'run', 'spawn})

e.g., for displaying only a compact report of currently supposed-to-be running jobs.

`get_queue` can also filter jobs according to fields, such as:

        get_queue(Filter='name LIKE "%Ingolia_2009%" AND BatchID > 20210101000000 AND data_type=="ribo"')

e.g., for displaying only jobs from the Ingolia-2009 experiment queued after January 1st 2021. Filter accepts any number of SQLite conditions.

missing jobs will be reported by `get_queue` according to the following criterion: a job that started running but has disappeared from queue before completing. this can be a useful tool for identifying jobs that need resubmitting / debugging. you may also use `get_queue` to automatically reset their state, or print their logs.

at the bottom of the report, a summary will display the total number of jobs the user currently has on PBS queues, the number of recognized running jobs, the number of completed jobs out of those that matched the filtering conditions.

**logs**

after a job shuts down, you can print its stdout / stderr, using `print_log()`, as follows: (last 2 args are optional)

        print_log(<batch_id>, <job_idx>, <opt:'stdout'/'stderr'>, <opt: history_idx>, <opt: lines to print>, <opt: regex>)

for example, to display the last recorded error log:

        print_log(1482002879, 0, 'stderr')

**job meta-data**

if you need lower level access to job parameters, the following function retrieves all its metadata. 

        get_job_info(<batch_id>, <job_idx>)

this includes, for instance, the last qstat data recorded before the job shut down, the data files used as input/output, its PBS_JOBID, hostname, etc. usually it is unnecessary to access this data. this function is usually used by jobs to get their parameter set.

### job submission

in order to submit all jobs according to priority rules (see [typical workflow](#typical-workflow) above), use the following command:

        submit_jobs()

the same filters (such as job names, etc.) that were described above for `get_queue` can be used here as well. it is also useful to have jobs run this function upon completion to get the next priority group submitted automatically.

when running jobs locally, jobs cannot submit other jobs (you may still call the function in your code, and the submission will be ignored). in this case, it is better to call `submit_jobs` occasionally from the main process. this can be done automatically using the following command:

        periodic_submitter()

**submitting specific jobs**

        submit_one_batch(BatchID)
        submit_one_job(BatchID, JobIndex)

**resubmitting jobs**

when the jobs you want to resubmit are recognized as 'missing' (see section on job status), you may use `get_queue` to reset their status back to 'init' automatically:

        get_queue(ResetMissing=True)

if a job was stuck in 'spawn' status, you may try the following: (before resorting to initializing status)

        spawn_resubmit(<batch_id>, <job_idx>)

more generally, you may:

* get BatchIDs, e.g., all jobs with the name Yang-2015 can be retrieved by calling `get_queue` with the flag Verbose=False (instead of printing to screen, it will generate a new variable containing all relevant jobs):

        B = get_queue(Filter='name LIKE "%Yang_2015%"', Verbose=False)

* set job status back to 'init'

        set_batch_field(B)

by default, this function sets the status back to init _unless_ the job already completed (thus, it allows you to submit only failed jobs). to reset the job completely (regardless of job status), use:

        set_batch_field(B, Unless={})

multiple JobIndices can be provided as a list, i.e. [1, 2, ...]. similarly, you can set the status of specific jobs within the batch using:

        set_job_field(B, J)

* submit again (can also wait for automatic submission to take place, if there are other jobs running)

        submit_jobs()

**deleting jobs from queue**

* you may delete jobs from the PBS queue using:

        qdel_batch(B)
        qdel_job(B, J)

**deleting jobs from DB**

* you may remove jobs from the job DB using:

        remove_batch(B)
        remove_batch_by_state(state)

## program API

the following functions are useful to create and use jobs from within scripts and programs.

### job preparation

        get_job_template(SetID=True)  # will also set BatchID to current time in seconds
        generate_script(JobInfo)  # will generate a job-specific execution script from a template
        generate_data(JobInfo, Data)  # will save data and update metadata with the file location
        add_batch_to_queue(BatchID, JobInfoList)
        add_job_to_queue(JobInfo)

### job runtime

        get_job_info(SetID=True)  # will update the hostname and PBS_ID of the running job
        get_qstat()  # can be used to report on metadata from the PBS system
        set_complete(Submit=True)  # set state when job is done  and submit the next set of valid jobs
        update_job(JobInfo)  # for more elaborate updates to job metadata
        spawn_submit()  # for multiplying the current job
        spawn_complete()  # for signaling that one spawn completed, and receiving that all spawns completed
        spawn_resubmit()  # for re-running missing spawn jobs
        spawn_get_info()  # gets the status of all spawns associated with this job
        submit_jobs()  # can be used after completion to submit the next priority group

## extending to other job schedulers

`paraschut` uses a generalized JobExecutor class for managing jobs. currently 4 JobExecutors are implemented: PBSJobExecutor, SGEJobExecutor, LocalJobExecutor, and FileJobExecutor. PBSJobExecutor and SGEJobExecutor, for example, are two simple wrappers for the `qsub`, `qstat`, and `qdel` commands in PBS and SGE, respectively. potentially, a wrapper for any other job scheduler can be added as a new subclass and given as an argument to `submit_jobs`, `get_queue`, and other relevant functions.
