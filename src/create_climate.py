"""
Temperature data processing library for calculating Heating and Cooling Degree Days (HDD/CDD).

Processes NetCDF temperature data and UK postcode shapefiles to calculate HDD/CDD values 
for use in energy modeling. Handles both annual and seasonal (summer/winter) calculations
with validation. Samples using nearest neighbor interpolation and checks for consistency

Processes postcodes in their sub folders from the edina postcode shapefiles

Temp data from HAD-Uk, downloaded from CEDA Archive, HADUK data is British National grid coords, which is CRS EPSG:27700
    - https://epsg.io/27700
    - https://www.metoffice.gov.uk/research/climate/maps-and-data/data/haduk-grid/datasets

Base temperatures:
   Heating: 15.5°C
   Cooling: 18.0°C

Creator: Grace Colverd
Date: 06/2024
"""
import os 
import pandas as pd 
import xarray as xr
import netCDF4 as nc
import geopandas as gpd 
import glob
from pathlib import Path
import rioxarray as rxr 

from .logging_config import get_logger
logger = get_logger(__name__)

base_temp_heating = 15.5  # Base temperature for heating
base_temp_cooling = 18.0  # Base temperature for cooling

def validate_temperature(temp):
    """Validate temperature is within reasonable range (-50°C to 50°C)"""
    if pd.isna(temp):
        return  
    if not -50 <= temp <= 50:
        raise ValueError(f"Temperature {temp}°C outside valid range")


def load_nc_file(path):
    """Load and preprocess NetCDF file into xarray Dataset, interpolating along x spatial dimensions."""
    nc_dataset = nc.Dataset(path)
    xds = xr.open_dataset(xr.backends.NetCDF4DataStore(nc_dataset))
    # xds = xr.open_dataset(xr.backends.NetCDF4DataStore(nc_dataset), decode_times=False)
    xds.rio.set_spatial_dims(x_dim='projection_x_coordinate', y_dim='projection_y_coordinate', inplace=True)
    xds = xds.interpolate_na(dim='projection_x_coordinate')
    return xds 



def calculate_hdd_cdd(row):
    """
    Calculate HDD/CDD from temperature using base temperatures.
    Returns Series with [hdd, cdd] values.
    """
    temp = row['tas']
    validate_temperature(temp)
    hdd = max(base_temp_heating - temp, 0)
    cdd = max(temp - base_temp_cooling, 0)
    if hdd is None or cdd is None:
        raise ValueError("Invalid temperature value")
    return pd.Series([hdd, cdd])


def sample(pc, xds):
    """
    Sample xarray Dataset values at GeoDataFrame centroids using nearest neighbor.
    Returns sampled values.
    """
    # print(xds.dims)
    # print(xds.coords)
    
    if pc.empty:
        raise ValueError("GeoDataFrame is empty")
    centroids = pc.geometry.centroid 
    
    # Convert centroids to suitable format if not already in xarray format
    x_coords = xr.DataArray(centroids.x, dims="points")
    y_coords = xr.DataArray(centroids.y, dims="points")

    # Sample the dataset using nearest neighbor interpolation
    sampled_values = xds.sel(
        x=x_coords,
        y=y_coords,
        method="nearest"
    )
    return sampled_values 



def calc_HDD_CDD_pc(pc, xds, tolerance=0.001):
    """
    Calculate Heating Degree Days (HDD) and Cooling Degree Days (CDD) for each point in a GeoDataFrame
    using the nearest temperature data from an xarray Dataset. Checks if the sum of seasonal data is within
    a specified tolerance of the annual totals.

    Parameters:
    - pc: GeoDataFrame with points and their geometries.
    - xds: xarray Dataset containing temperature data.
    - tolerance: float, maximum allowed difference between the sum of seasonal values and the annual total.

    Returns:
    - result: DataFrame with annual, summer, and winter HDD and CDD.
    """

    # Check CRS consistency

    # xds.rio.write_crs(pc.crs, inplace=True) 
    

    if pc.crs != xds.rio.crs:
        logger.info(f"CRS mismatch between GeoDataFrame and xarray Dataset: {pc.crs} vs {xds.rio.crs}")
        raise ValueError("CRS mismatch between GeoDataFrame and xarray Dataset")
    
    sampled_values = sample(pc, xds)  

    # Convert sampled DataArray to DataFrame
    sampled_df = sampled_values.to_dataframe().reset_index() 
    sampled_df = sampled_df[sampled_df['bnds'] == 1].reset_index(drop=True) 

    # Apply the function to calculate HDD and CDD
    sampled_df[['HDD', 'CDD']] = sampled_df.apply(calculate_hdd_cdd, axis=1)

    # Summarize data by points
    annual = sampled_df.groupby('points')[['HDD', 'CDD']].sum()

    # Define month indices for summer (April to September) and winter (October to March)
    summer_months = [4, 5, 6, 7, 8, 9]
    winter_months = [10, 11, 12, 1, 2, 3]

    # Filter data based on month for seasonal calculations
    sampled_df['month'] = sampled_df['time'].dt.month  # assuming 'time' is the time coordinate
    summer_data = sampled_df[sampled_df['month'].isin(summer_months)]
    winter_data = sampled_df[sampled_df['month'].isin(winter_months)]

    # Sum HDD and CDD for summer and winter
    summer = summer_data.groupby('points')[['HDD', 'CDD']].sum()
    winter = winter_data.groupby('points')[['HDD', 'CDD']].sum()

    # Merge results
    result = annual.join([summer.rename(columns=lambda x: x + '_summer'),
                          winter.rename(columns=lambda x: x + '_winter')])

    # Check if the sum of seasonal data is within the specified tolerance
    for season in ['HDD', 'CDD']:
        total_check = abs(result[f'{season}_summer'] + result[f'{season}_winter'] - result[season])
        if any(total_check > tolerance):
            raise ValueError(f"Seasonal totals for {season} exceed the tolerance threshold.")   
    result = result.join(pc.reset_index(), on='points')
    result.drop(columns=['UPP', 'PC_AREA', 'geometry'], inplace=True)
    return result


def save_pc_file(res, output_path):
    res.to_csv(output_path, index=False)
    


def run_all_pc_shps(output_path: Path, pc_base_path: Path, temp_file: Path):
    """
    Process all postcode shapefiles to calculate HDD/CDD values.
    Creates CSV output files in specified directory.
    
    """
    os.makedirs(output_path, exist_ok=True)
    pc_shps1 = glob.glob(f'{pc_base_path}/one_letter_pc_code/*/*.shp') 
    pc_shps2 = glob.glob(f'{pc_base_path}/two_letter_pc_code/*.shp')

    if not pc_shps1 and not pc_shps2:
        raise ValueError("No postcode shapefiles found.")

    xds = load_nc_file(temp_file )   
    print('xds : ')
    print(xds)
    xds = xds.rename({
        'projection_y_coordinate': 'y',
        'projection_x_coordinate': 'x'
    })


    xds.rio.set_crs('EPSG:27700', inplace=True)
    logger.info('Temperature data loaded successfully')
    #  check xds is loaded correctly
    if xds is None:
        raise ValueError("Temperature data not loaded correctly")


    for pc in pc_shps1 + pc_shps2:
        pc_name = os.path.basename(pc).split('.')[0]
        output_file = os.path.join(output_path, f'{pc_name}.csv')
        # check if output already exists
        if os.path.exists(output_file):
            logger.info(f"Temperature Output file {output_file} already exists. Skipping...")
            continue
        logger.info(f"For temperature, processing postcode {pc_name}...")
        pc_df = gpd.read_file(pc)
        res = calc_HDD_CDD_pc(pc_df, xds)
        save_pc_file(res, output_file)


def unify_temp():
    if os.path.exists('intermediate_data/unified_temp_data.csv'):
        logger.info('Unified temperature data already exists, skipping concatenation')        
    else:
        list_files=[]
        fold = 'intermediate_data/temp_data/*.csv'
        for f in glob.glob(fold):
            d = pd.read_csv(f)
            list_files.append(d)
        temp = pd.concat(list_files)
        temp.to_csv('intermediate_data/unified_temp_data.csv', index=False)
        logger.info('Temperature calculation complete, output saved to intermediate_data/unified_temp_data.csv')



def main( pc_base_path, temp_1km_path ):
    """Entry point to run HDD/CDD calculations for all postcodes."""
    output_path='intermediate_data/temp_data'
    logger.info('Starting temperature calculation for all postcodes')
    run_all_pc_shps(output_path, pc_base_path, temp_1km_path)
    logger.info('Temp calc intermediate complete, starting concatenation.')
    unify_temp()

    