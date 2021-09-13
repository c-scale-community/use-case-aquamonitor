from xarray import DataArray
import xarray
from typing import List

from openeo.udf import XarrayDataCube
from xarray.core.coordinates import DataArrayCoordinates


def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    """Take the percentile value of a time-series DataCube.
    
    This function assumes a DataCube with Dimension 't' as an input. This dimension is removed from the result.

    context_options:
        value [float]: percentile value. Defaults to float 20
        interpolation [string]: type of interpolation, taken from http://xarray.pydata.org/en/stable/generated/xarray.DataArray.quantile.html.
            defaults to "linear".
    """
    if not "value" in context:
        context["value"] = 20.
    
    if not "interpolation" in context:
        context["interpolation"] = "linear"
        
    if not "bands" in context:
        context["bands"] = ["green", "nir", "swir"]
    
    array: DataArray = cube.get_array()
    
    quantile: DataArray = array.quantile(q=context["value"]/100, dim=["t"], interpolation=context["interpolation"], keep_attrs=True, skipna=True)
    
    return XarrayDataCube(
        array=DataArray(quantile, dims=quantile.dims, coords=quantile.coords)
    )
