from datetime import datetime, timedelta
from pathlib import Path
from requests import ConnectionError
from time import sleep, time
from typing import Callable, List, Optional

from openeo.rest import OpenEoApiError, JobFailedException
from openeo.rest.datacube import DataCube
from openeo.rest.job import RESTJob


def get_files_from_dc(dc: DataCube, out_directory: Path, name: str = "aquamonitor") -> List[Path]:
    def wait(
        print: Callable = print,
        max_poll_interval: int = 60,
        connection_retry_interval: int = 30,
        soft_error_max: int = 10,
        start_time: [Optional[datetime]] = None
    ) -> RESTJob:
        if start_time is None:
            start_time: datetime = time()
        # Dirty copy-pasta from python client source code
        def elapsed() -> str:
            return str(timedelta(seconds=time() - start_time)).rsplit(".")[0]

        def print_status(msg: str):
            print("{t} Job {i!r}: {m}".format(t=elapsed(), i=job.job_id, m=msg))

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
           
    dc: DataCube = dc.save_result(format="NetCDF")  # Add save result step to the end of the graph
    job: RESTJob = dc._connection.create_job(process_graph=dc.flat_graph(), title=name)
    start_time: datetime = time()
    job.start_job()
    wait(start_time=start_time, soft_error_max=50)
    
    files: List[Path] = []

    results: JobResults = job.get_results()
    for asset in results.get_assets():
        if asset.metadata["type"].startswith("application/x-netcdf"):
            file: Path = asset.download(out_directory / asset.name, chunk_size=2 * 1024 * 1024)
            files.append(file)
    return files