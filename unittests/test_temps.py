import pandas as pd
import pytest
from shapely.geometry import Point, Polygon
import geopandas as gpd
import xarray as xr
import sys
sys.path.append('../')
# Assuming the main code is in a script or module named 'climate_analysis'
from src.create_climate import calculate_hdd_cdd, sample

def test_calculate_hdd_cdd():
    # Create a sample DataFrame
    data = {'tas': [16, 17, 18.5, 20, 14, 15]}
    df = pd.DataFrame(data)

    # Apply the calculate_hdd_cdd function
    df[['HDD', 'CDD']] = df.apply(calculate_hdd_cdd, axis=1)

    # Expected HDD and CDD values
    expected_hdd = pd.Series([0, 0, 0, 0, 1.5, 0.5], dtype=float, name='HDD')
    expected_cdd = pd.Series([0, 0, 0.5, 2.0, 0, 0], dtype=float, name ='CDD')
    print(expected_hdd)
    print(df['HDD'])
    # Check if calculated HDD and CDD are as expected
    pd.testing.assert_series_equal(df['HDD'], expected_hdd, check_dtype=True)
    pd.testing.assert_series_equal(df['CDD'], expected_cdd, check_dtype=True)

@pytest.fixture
def mock_data():
    # Creating a mock GeoDataFrame
    crs = "EPSG:27700"  # British National Grid
    geometry = [Polygon([(0, 0), (1, 0), (1, 1)])]
    gdf = gpd.GeoDataFrame(geometry=geometry, crs=crs)
    
    # Creating a mock xarray Dataset
    tas = xr.DataArray([10, 20, 15], dims=["projection_x_coordinate"], coords={"projection_x_coordinate": [0, 1, 2]})
    xds = xr.Dataset({"tas": tas})

    return gdf, xds

def test_clip_and_sample(mock_data):
    gdf, xds = mock_data

    # Assuming clipping and sampling methods exist and are correctly defined
    clipped = xds.sel(projection_x_coordinate=xr.DataArray([0.4], dims="points"), method="nearest")
    print(clipped['tas'].values)
    # Check if clipping and nearest sampling are done correctly
    assert clipped['tas'].values == [10]  # Expected nearest value

# More tests can be added as needed for other parts of the processing
def test_temperature_at_thresholds():
    # Data exactly at the thresholds
    data = {'tas': [15.5, 18.0]}
    df = pd.DataFrame(data)
    df[['HDD', 'CDD']] = df.apply(calculate_hdd_cdd, axis=1)

    # Expected values when temperature is exactly at the threshold
    expected_hdd = pd.Series([0.0, 0.0], name='HDD')
    expected_cdd = pd.Series([0.0, 0.0], name='CDD')

    pd.testing.assert_series_equal(df['HDD'], expected_hdd, check_dtype=True)
    pd.testing.assert_series_equal(df['CDD'], expected_cdd, check_dtype=True)


def test_non_numeric_and_missing_values():
    data = {'tas': [None, 11, 20]}
    df = pd.DataFrame(data)

    # Apply function and handle exceptions or use fillna for expected behavior
    try:
        df[['HDD', 'CDD']] = df.apply(calculate_hdd_cdd, axis=1)
    except Exception as e:
        assert isinstance(e, ValueError)  # Check if the error is due to non-numeric input

    # Alternatively, handle non-numeric internally in calculate_hdd_cdd function
    # Then test for expected output, e.g., filling NaN for invalid computations
    expected_hdd = pd.Series([None, 4.5, 0.0], name='HDD')
    expected_cdd = pd.Series([None, 0.0, 2.0], name='CDD')
    pd.testing.assert_series_equal(df['HDD'], expected_hdd)
    pd.testing.assert_series_equal(df['CDD'], expected_cdd)


def test_empty_geodataframe_and_dataset():
    # Empty GeoDataFrame
    empty_gdf = gpd.GeoDataFrame(geometry=[], crs="EPSG:27700")
    empty_xds = xr.Dataset()

    # Function to test clipping and sampling
    try:
        # Assuming this function will raise an error or handle it silently
        sampled_values = sample(empty_gdf, empty_xds)
        assert sampled_values.empty, "The result should be empty for an empty input"
    except Exception as e:
        assert isinstance(e, ValueError)  # Expecting an error or handle depending on function design
    
