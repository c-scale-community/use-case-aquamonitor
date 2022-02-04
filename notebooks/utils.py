from datetime import datetime, timedelta
from dateutil.parser import parse
from functools import reduce
import logging
from pathlib import Path
import re
from requests import ConnectionError
from time import sleep, time
from typing import Callable, Dict, List, Optional

from openeo.internal.jupyter import VisualList
from openeo.rest import OpenEoApiError, JobFailedException
from openeo.rest.datacube import DataCube
from openeo.rest.job import RESTJob, JobResults


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


def get_latest_completed_job(jobs: VisualList, title: str) -> Optional[Dict[str, str]]:
        if not any(jobs):  # if not jobs exist yet in this backend
            return None
            
        match_jobs: filter = filter(lambda job: job.get("title") == title and job.get("status") == "finished", jobs)
        m = next(match_jobs, None)  # Check if filter object emtpy
        if not m:
            return m
        return reduce(lambda j1, j2: j1 if j1["updated"] > j2["updated"] else j2, match_jobs, m)

def start_and_wait(
    dc: DataCube,
    job_name: str = "aquamonitor",
    result_format: str = "NetCDF"
) -> RESTJob:
    """
    Creates an asynchronous job for a datacube, polls the job progress and return the result assets.
    args:
        dc (DataCube): DataCube that needs to be resolved.
        job_name (str): name of the job in the openeo backend.
        result_format (str): format of the result saves in the OpenEO backend.
    
    return:
        (JobResults) results object of the job
    """
    
    def wait(
        max_poll_interval: int = 60,
        connection_retry_interval: int = 30,
        soft_error_max: int = 10,
        start_time: [Optional[datetime]] = None
    ) -> RESTJob:
        """
        Poll the job until it is completed. Also use the refresh token if needed.
        
        args:
            max_poll_interval (int): number of seconds that the poll uses at maximum.
            connection_retry_interval (int): number of seconds to wait after a soft error.
            soft_error_max (int): maximum number of soft errors to tolerate before timing out.
            start_time (datetime): time at which the polling starts.
        """
        if start_time is None:
            start_time: datetime = time()
        # Dirty copy-pasta from python client source code
        def elapsed() -> str:
            return str(timedelta(seconds=time() - start_time)).rsplit(".")[0]

        def print_status(msg: str):
            logger.info("{t} Job {i!r}: {m}".format(t=elapsed(), i=job.job_id, m=msg))

        # Start with fast polling.
        poll_interval = min(5, max_poll_interval)
        status = None
        _soft_error_count = 0

        def soft_error(message: str):
            """Non breaking error (unless we had too much of them)"""
            nonlocal _soft_error_count
            _soft_error_count += 1
            if _soft_error_count > soft_error_max:
                raise OpenEoClientException("Excessive soft errors")
            print_status(message)
            sleep(connection_retry_interval)

        while True:
            # TODO: also allow a hard time limit on this infinite poll loop?
            try:
                job_info = job.describe_job()
            except ConnectionError as e:
                soft_error("Connection error while polling job status: {e}".format(e=e))
                continue
            except OpenEoApiError as e:
                if e.http_status_code == 503:
                    soft_error("Service availability error while polling job status: {e}".format(e=e))
                    continue
                elif e.http_status_code == 403:
                    dc._connection.authenticate_oidc()  # Make sure we do not timeout during the wait
                else:
                    raise

            status = job_info.get("status", "N/A")
            progress = '{p}%'.format(p=job_info["progress"]) if "progress" in job_info else "N/A"
            print_status("{s} (progress {p})".format(s=status, p=progress))
            if status not in ('submitted', 'created', 'queued', 'running'):
                break

            # Sleep for next poll (and adaptively make polling less frequent)
            sleep(poll_interval)
            poll_interval = min(1.25 * poll_interval, max_poll_interval)

        if status != "finished":
            raise JobFailedException("Batch job {i} didn't finish properly. Status: {s} (after {t}).".format(
                i=job.job_id, s=status, t=elapsed()
            ), job=job)

    dc: DataCube = dc.save_result(format=result_format)  # Add save result step to the end of the graph
    
    job: RESTJob = dc._connection.create_job(process_graph=dc.flat_graph(), title=job_name)
    start_time: datetime = time()
    job.start_job()
    wait(start_time=start_time, soft_error_max=50)
    
    return job

def get_or_create_results(dc: DataCube, job_name: str, recalculate: bool, result_format: str) -> JobResults:
    """
    args:
        dc (DataCube): DataCube that needs to be resolved.
        job_name (str): name of the job in the openeo backend.
        recalculate (bool): whether to search for previous results or recalculate.
    
    returns:
        JobResults: results of the cached or created job.
    """
    
    results = None
    if not recalculate:
        job: Optional[Dict[str, str]] = get_latest_completed_job(dc._connection.list_jobs(), job_name)
        if job:
            results: JobResults = RESTJob(job["id"], dc._connection).get_results()
    if not results:
        results: JobResults = start_and_wait(dc, job_name, result_format).get_results()
        
    return results
    

def get_files_from_dc(
    dc: DataCube,
    out_directory: Path,
    job_name: str = "aquamonitor",
    result_format: str = "NetCDF",
    recalculate: bool = True
) -> List[Path]:
    """
    Creates an asynchronous job for a datacube, polls the job progress and returns a list of paths
    to the results once finished. 
    
    args:
        dc (DataCube): DataCube that needs to be resolved.
        out_directory (Path): directory where the files will be stored.
        job_name (str): name of the job in the openeo backend.
        result_format (str): format of the result saves in the OpenEO backend.
        recalculate (bool): whether to search for previous results or recalculate.
    
    returns:
        List[Paths]: list of paths which point to the results.
    """
    
    results: JobResults = get_or_create_results(dc, job_name, recalculate, result_format)
    
    files: List[Path] = []
    for asset in results.get_assets():
        meta_type: str = asset.metadata["type"]
        if meta_type.startswith("application/x-netcdf") or meta_type.startswith("image/tiff"):
            file: Path = asset.download(out_directory / asset.name, chunk_size=2 * 1024 * 1024)
            files.append(file)
    return files

def get_urls_from_dc(
    dc: DataCube,
    job_name: str = "aquamonitor",
    result_format: str = "NetCDF",
    recalculate: bool = True
) -> List[str]:
    """
    Creates an asynchronous job for a datacube, polls the job progress and returns a list of
    download urls once the job has finished.
    
    args:
        dc (DataCube): DataCube that needs to be resolved.
        job_name (str): name of the job in the openeo backend.
        result_format (str): format of the result saves in the OpenEO backend.
        recalculate (bool): whether to search for previous results or recalculate.
    
    returns:
        List[str]: list of download_urls which point to the results.
    """
    
    results: JobResults = get_or_create_results(dc, job_name, recalculate, result_format)
    
    return [asset.href for asset in results.get_assets()]

def get_cache(dc: DataCube, title: str) -> Optional[DataCube]:
    """
    Get cached result from backend.
    
    args:
        dc (DataCube): DataCube that needs to be checked.
        title (str): Title of the job that will be assesed.
        
    returns:
        Optional[DataCube]: DataCube if there is any matching the job.
    """

    # If we have a job with a result already, return
    jobs: VisualList = dc._connection.list_jobs()
    job: Optional[Dict[str, str]] = get_latest_completed_job(jobs, title)
    if job:
        return dc._connection.load_result(job["id"])
    return None
    
def get_or_create_cache(
    dc: DataCube,
    job_name: str = "aquamonitor",
    result_format: str = "NetCDF"
) -> DataCube:
    """
    Get or create a cache from a DataCube. Data is cached at the current backend and is identified by the
    job title and getting the latest successful job.
    """
    cache: Optional[DataCube] = get_cache(dc, job_name)
    if cache:
        return cache
    else:
        job: RESTJob = start_and_wait(dc, job_name, result_format)
        return dc._connection.load_result(re.sub(r'vito', r'', job.job_id))  # Temp workaround for broken backend
        