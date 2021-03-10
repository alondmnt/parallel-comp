# PARASCHUT changelog

## v0.6.0

- feature: `SlurmJobExecutor` is a new class for managing jobs on a Slurm cluster.  you may define it as the DefaultJobExecutor in `manage.py`.
- improved: default stdout/stderr logs now define specfic file paths that are compatible across cluster types.
- fixed: `get_batch_info` supports spawn jobs. as a result, other dependent functions such as `qdel_batch` now handle spawn jobs properly.
- changed: `config.py` parameter PBS_queue was renamed to DefQueue, and parameter PBS_suffix was removed.
- improved: misc stability and error handling issues.

## v0.5.0

this release adds support for the SGE (Sun Grid Engine) cluster.

- feature: `SGEJobExecutor` is a new class for managing jobs on a SGE cluster. you may define it as the DefaultJobExecutor in `manage.py`.
- feature: setting environment variables for jobs. can be defined by adding a `vars` field with a dict of variable/value pairs.
- improved: misc stability and error handling issues.
- changed: renamed PBS_ID-->ClusterID, data_type-->batch_type.

## v0.4.0

this release focuses on locally executing jobs using multi-processing, and making the job manager generalized and extendable via a set of `JobExecutor` classes.

- feature: local execution of jobs using the same API (there is no need to update the jobs themselves, or manage them differently). this is activated by setting LocalRun=True in the config file. this effectively turns the local machine into a small cluster. the only difference is that jobs cannot submit other jobs when running locally (submission will be ignored), and `submit_jobs` should be called periodically from the main process instead.
- feature: `periodic_submitter` for running `submit_jobs` every X minutes, and/or Y times.
- feature: `JobExecutor` classes. these are wrappers that allow extending the package to other types of clusters while preserving the API. currently 3 JobExecutors are implemented:
	- `PBSJobExecutor`: for submitting jobs on a PBS cluster.
	- `LocalJobExecutor`: for submitting jobs as subprocesses on a local machine.
	- `FileJobExecutor`: for "submitting" jobs to a shell script that can be exectued sequentially.
- changed: `get_job_info` should be called with a given `ClusterID` if `SetID=True` or the job is a spawn job.

## v0.3.0

this release focuses on improved performance and a database update.

- SQLite job DB
	- should be more robust/safer, and sometimes faster
	- filter displayed jobs in queue using SQL conditions (see `get_queue(Filter='condition OR condition')`)
- all parameters are now defined in `config.py`
- script for migrating existing DB to SQL
	- after setting up `config.py` with the location of the new DB (with the extension '.db') and JobDir, run: `upgrade_db.run()`
- monitoring
	- can print logs even for failed jobs that never went online
	- additional advanced log printing args, e.g. `print_log(..., Lines=range(10), RegEx='failed')`
	- automatically print the error logs of all missing jobs when calling `get_queue(ReportMissing=True)`
- management
	- feature: `clean_temp_files()` for cleaning job dirs before resubmission (if needed).
- spawn jobs
	- feature: `spawn_get_info` for a status of all spawns
	- improved: more robust internal spawn logic
	- improved: `spawn_resubmit()` can be run while jobs are online (but rarely needed)
- improved: new BatchIDs are based on humanized date/time in sec
- naming convention: status renamed to state
- refactoring: job distributed between `manage.py`, `dal.py`, `job_factory` and `utils.py`

## v0.2.0

- feature: `qdel_batch()`, `qdel_job()` for job deletion from PBS queue
- feature: `remove_batch_by_state()` for easy cleaning of job DB based on state (e.g., 'collected')
- feature: new job status 'run' automatically set by `get_job_info(..., SetID=True)`.
- improved: `set_complete()` also automatically saves a copy of PBS’s `qstat` output.

## v0.1.0

first public release.
