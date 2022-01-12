from datetime import datetime, timedelta
import logging
from pathlib import Path
from requests import ConnectionError
from time import sleep, time
from typing import Callable, List, Optional

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


def get_results_from_dc(
    dc: DataCube,
    job_name: str = "aquamonitor",
    format: str = "NetCDF"
) -> JobResults:
    """
    Creates an asynchronous job for a datacube, polls the job progress and return the result assets.
    args:
        dc (DataCube): DataCube that needs to be resolved.
        job_name (str): name of the job in the openeo backend.
        format (str): format of the result saves in the OpenEO backend.
    
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
           
    dc: DataCube = dc.save_result(format=format)  # Add save result step to the end of the graph
    job: RESTJob = dc._connection.create_job(process_graph=dc.flat_graph(), title=job_name)
    start_time: datetime = time()
    job.start_job()
    wait(start_time=start_time, soft_error_max=50)
    
    return job.get_results()

def get_files_from_dc(
    dc: DataCube,
    out_directory: Path,
    job_name: str = "aquamonitor",
    format: str = "NetCDF"
) -> List[Path]:
    """
    Creates an asynchronous job for a datacube, polls the job progress and returns a list of paths
    to the results once finished. 
    
    args:
        dc (DataCube): DataCube that needs to be resolved.
        out_directory (Path): directory where the files will be stored.
        job_name (str): name of the job in the openeo backend.
        format (str): format of the result saves in the OpenEO backend.
    
    returns:
        List[Paths]: list of paths which point to the results.
    """
    
    files: List[Path] = []
    
    results: JobResults = get_results_from_dc(dc, job_name, format)

    for asset in results.get_assets():
        if asset.metadata["type"].startswith("application/x-netcdf"):
            file: Path = asset.download(out_directory / asset.name, chunk_size=2 * 1024 * 1024)
            files.append(file)
    return files

def get_urls_from_dc(
    dc: DataCube,
    job_name: str = "aquamonitor",
    format: str = "NetCDF",
) -> List[str]:
    """
    Creates an asynchronous job for a datacube, polls the job progress and returns a list of
    download urls once the job has finished.
    
    args:
        dc (DataCube): DataCube that needs to be resolved.
        job_name (str): name of the job in the openeo backend.
        format (str): format of the result saves in the OpenEO backend.
    
    returns:
        List[str]: list of download_urls which point to the results.
    """
    
    results: JobResults = get_results_from_dc(dc, job_name, format)
    
    return [asset.href for asset in results.get_assets()]
