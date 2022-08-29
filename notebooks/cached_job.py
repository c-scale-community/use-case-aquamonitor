from datetime import datetime, timedelta
from json import dump, load, JSONDecodeError
import logging
from pathlib import Path
from time import sleep, time
from typing import Any, Dict, Optional

from openeo.rest import OpenEoClientException, OpenEoApiError
from openeo.rest.connection import Connection
from openeo.rest.job import BatchJob, JobFailedException

# Initiate logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
fh = logging.FileHandler('batch.log', mode="a")
ch = logging.StreamHandler()
fh.setLevel(logging.INFO)
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(ch)

class CachedJob(BatchJob):
    """
    Wrapper around RESTJob that keeps a local cache of the remote jobs, so that we do not have to
    calculate jobs multiple times. Works with job_title being unique per job. Changing job title
    will cause recalulation.
    """
    def __init__(
        self,
        job_title: str,
        local_cache_file: Path,
        connection: Connection,
        job_id: Optional[str] = None,
        flat_graph: Optional[Dict[str, Any]] = None,
        recalculate: bool = False
    ):
        self._job_title = job_title
        self._local_cache_file = local_cache_file
        self._flat_graph = flat_graph
        # try load cache file
        try:
            with open(self._local_cache_file) as f:
                self._job_cache: Dict[str, str] = load(f)
        except (FileNotFoundError, JSONDecodeError, TypeError):  # not the best error catching, should refine
            # assume the file does not exist:
            logger.info(f"logging file not found, creates a local log file at {str(local_cache_file)} when saving.")
            self._job_cache: Dict[str, str] = {}
        # check if job was cached
        if job_title in self._job_cache.keys() and not recalculate:
            super().__init__(self._job_cache[job_title], connection)
            self._is_cached = True
        else:
            self._is_cached = False
            # if we know the job id, we represent an existing job on the backend
            if job_id:
                # check if job id exists
                matching_jobs: filter = filter(
                    lambda job: job.get("id") == job_id, connection.list_jobs()
                )
                if any(matching_jobs):
                    return super().__init__(job_id, connection)
                else:
                    raise AttributeError("job id not found in backend.")
                
            # else we create the job on the backend
            elif flat_graph:
                logger.info(f"cached job not found in backend, creating a job '{job_title}' in the backend.")
                job: BatchJob = connection.create_job(process_graph=flat_graph, title=job_title)
                return super().__init__(job.job_id, connection)
            else:
                raise AttributeError("CachedJob not existing yet on the backend requires either" +
                 "a job_id, or a process_graph")
    
    @property
    def flat_graph(self) -> bool:
        return self._flat_graph

    @flat_graph.setter
    def flat_grapth(self, b):
        self._flat_graph = b

    @property
    def is_cached(self) -> bool:
        return self._is_cached

    @is_cached.setter
    def is_cached(self, b):
        self._is_cached = b
    
    @property
    def job_cache(self) -> str:
        return self._job_cache

    @property
    def job_title(self) -> str:
        return self._job_title
    
    @job_title.setter
    def job_title(self, title: str):
        self._job_title = title
    
    @property
    def local_cache_file(self) -> str:
        return self._job_title
    
    @local_cache_file.setter
    def local_cache_file(self, file_path: Path):
        self._local_cache_file = file_path

    def save(self):
        self.job_cache[self._job_title] = self.job_id
        with open(self._local_cache_file, 'w+') as outfile:
            dump(self.job_cache, outfile)
    
    def start_and_wait(self):
        """
        starts the job and, polls the job progress.
        """
        super().start_and_wait(print=logger.info)
        if self._local_cache_file:
            self.save()
        
        return self
