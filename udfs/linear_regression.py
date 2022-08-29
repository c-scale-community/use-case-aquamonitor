from numpy import float64, timedelta64
from openeo.udf import XarrayDataCube
from xarray import DataArray, Dataset

def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    """Linear regression of a time-series DataCube.
    
    This function assumes a DataCube with Dimension 't' as an input. This dimension is removed from the result.
    
    Args:
        cube (XarrayDataCube): datacube to apply the udf to.
        context (dict): key-value arguments.
    
    Examples:
        >>> dc = con.load_connection("LANDSAT8_L1C")
        >>> def load_udf(udf_path: Union[str, Path]):
        ...     with open(udf_path, "r+") as f:
        ...     return f.read()
        ...
        >>> linear_regression_udf: str = load_udf(percentile_udf_path)
        >>> dc.apply_dimension(code = linear_regression_udf, runtime="Python")
    
    """
    array: DataArray = cube.get_array()
    from_ns: float64 = (array.t[1].values - array.t[0].values)  / timedelta64(1, "ns")
    fit: Dataset = array.polyfit(dim="t", deg=1) * from_ns
    
    slope: Dataset = fit.sel(degree=1)

    return XarrayDataCube(
        array=DataArray(slope["polyfit_coefficients"]) # , dims=slope.dims, coords=slope.coords)
    )