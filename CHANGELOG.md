#  pbsmgr changelog

## 0.3.0

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

## 0.2.0

- feature: `qdel_batch()`, `qdel_job()` for job deletion from PBS queue
- feature: `remove_batch_by_state()` for easy cleaning of job DB based on state (e.g., 'collected')
- feature: new job status 'run' automatically set by `get_job_info(..., SetID=True)`.
- improved: `set_complete()` also automatically saves a copy of PBS’s `qstat` output.

## 0.1.0

first public release.
