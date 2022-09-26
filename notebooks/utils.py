from copy import deepcopy
from functools import reduce
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from openeo.internal.jupyter import VisualList
from openeo.metadata import Band, CollectionMetadata
from openeo.rest.datacube import DataCube
from openeo.rest.job import JobResults

from cached_job import CachedJob

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

def get_or_create_results(
    dc: DataCube,
    job_name: str,
    recalculate: bool,
    result_format: str,
    local_cache_file: Optional[Path] = None
) -> JobResults:
    """
    args:
        dc (DataCube): DataCube that needs to be resolved.
        job_name (str): name of the job in the openeo backend.
        recalculate (bool): whether to search for previous results or recalculate.
        local_cache_file (Optional[Path]): file where jobs are cached.
    
    returns:
        JobResults: results of the cached or created job.
    """

    if not local_cache_file and not recalculate:
        raise RuntimeError("must specify either recalculate=True or local_cache_file")
    
    results: Optional[JobResults] = None
    dc: DataCube = dc.save_result(format=result_format)  # add save result to backend
    job: CachedJob = CachedJob(job_name, local_cache_file, connection=dc._connection, flat_graph=dc.flat_graph(), recalculate=recalculate)
    if not recalculate:
        if job.status() == "finished":
            results: JobResults = job.get_results()
    if not results:
        results: JobResults = job.start_and_wait().get_results()
        
    return results
    

def get_files_from_dc(
    dc: DataCube,
    out_directory: Path,
    job_name: str = "aquamonitor",
    result_format: str = "NetCDF",
    recalculate: bool = False,
    local_cache_file: Optional[Path] = None
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
    
    results: JobResults = get_or_create_results(dc, job_name, recalculate, result_format, local_cache_file)
    
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
    recalculate: bool = True,
    local_cache_file: Optional[Path] = None
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
    
    results: JobResults = get_or_create_results(dc, job_name, recalculate, result_format, local_cache_file)
    
    return [asset.href for asset in results.get_assets()]

def get_cache(
    cached_cube: DataCube,
    job: CachedJob,
    temporal_extent: Tuple[str, str],
    spatial_extent: Dict[str, Tuple[float]],
    reference_system: int = 4326
) -> Optional[DataCube]:
    """
    Get cached result from backend.
    
    args:
        cached_cube (DataCube): cached DataCube containing DAG and old metadata.
        job (CachedJob): job cached locally.
        temporal_extent (List): list containing start date and end date (or None if present) of
            cached_cube.
        spatial_extent (Dict): dictionary containing extents of x and y coordinates.
        
    returns:
        Optional[DataCube]: DataCube if there is any matching the job.
    """

    loaded_cube: DataCube = cached_cube._connection.load_result(
        # id=re.sub(r"vito", r"", job.job_id)  # Temp workaround for broken backend
        id=job.job_id
    )

    # Set metadata based on previous metadata with matching spatial and temporal extents
    bands: List[Band] = deepcopy(cached_cube.metadata).band_dimension.bands

    loaded_cube = loaded_cube \
        .add_dimension("spectral", "some_label", type="bands") \
        .rename_labels("spectral", list(map(lambda band: band.name, bands)))

    def get_band_meta_dict(b: Band):
        return { 
            "name": b.name,
            "common_name": b.common_name,
            "center_wavelength": b.wavelength_um,
            "gsd": b.gsd
        }

    m: CollectionMetadata = CollectionMetadata({
        "cube:dimensions": {
            "x": {
                "type": "spatial",
                "extent": spatial_extent["x"],
                "reference_system": reference_system
            },
            "y": {
                "type": "spatial",
                "extent": spatial_extent["y"],
                "reference_system": reference_system
            },
            "t": {
                "type": "temporal",
                "extent": temporal_extent
            },
            "spectral": {
                "type": "bands",
                "values": list(map(lambda band: band.name, bands))
            }
        },
        "summaries": {
            "eo:bands": list(map(lambda band: get_band_meta_dict(band), bands))
        }
    })

    # # Set metadata based on previous metadata with matching spatial and temporal extents
    # m: CollectionMetadata = deepcopy(cached_cube.metadata)

    # # Set temporal extent
    # t_dim: TemporalDimension = TemporalDimension("t", temporal_extent)
    # print(f"t_dim type: {t_dim.type}")
    # # band dimension already updated in cube
    # band_dim: BandDimension = m.band_dimension
    # print(f"band_dim type: {band_dim.type}")
    # # get x dimension and update extent
    # x_dim_old: SpatialDimension = filter(lambda dim: dim.name == "x", m.spatial_dimensions).__next__()
    # x_dim: SpatialDimension = SpatialDimension("x", spatial_extent["x"], x_dim_old.crs, x_dim_old.step)
    # print(f"x_dim type: {x_dim.type}")
    # # get y dimension and update extent
    # y_dim_old: SpatialDimension = filter(lambda dim: dim.name == "y", m.spatial_dimensions).__next__()
    # y_dim: SpatialDimension = SpatialDimension("y", spatial_extent["y"], y_dim_old.crs, y_dim_old.step)
    
    # loaded_cube_m: CollectionMetadata = CollectionMetadata(m, dimensions=[t_dim, band_dim, x_dim, y_dim])
    loaded_cube.metadata = m
    return loaded_cube
    
def get_or_create_cached_cube(
    dc: DataCube,
    local_cache_file: Path,
    temporal_extent: List[str],
    spatial_extent: Dict[str, List[float]],
    job_name: str = "aquamonitor",
    result_format: str = "NetCDF"
) -> DataCube:
    """
    Get or create a cache from a DataCube. Data is cached at the current backend and is identified by the
    job title and getting the latest successful job.
    """
    dc: DataCube = dc.save_result(format=result_format)
    job: CachedJob = CachedJob(job_name, local_cache_file, dc._connection, flat_graph=dc.flat_graph())
    if not job.is_cached:
        job.start_and_wait()

    return get_cache(dc, job, temporal_extent, spatial_extent)
