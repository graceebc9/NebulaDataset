import numpy as np
import pandas as pd
import os
import glob
from src.pre_process_buildings import *
from src.postcode_utils import   join_pc_map_three_pc, join_three_pcds


from src.load_data import load_from_log, load_proc_dir_log_file, load_pc_to_output_area_mapping, load_postcode_geometry_data, load_rural_uran, load_centroids
from src.validations import call_validations
from src.logging_config import get_logger
from src.post_process_buildings_stock import post_proc_new_fuel 
from src.columns import all_cols_list
 

logger = get_logger(__name__)




######################### Post process type ######################### 

def validate_and_calculate_percentages_type(df):
    """
    Validates building type data and calculates percentages.
    
    Args:
        df: DataFrame with building type columns and 'len_res' total
        
    Returns:
        DataFrame with percentage columns added
        
    Raises:
        ValueError: If validation fails
    """
    
    # Get building type columns first
    df = df.copy() 
    excluded_cols = ['postcode', 'len_res', 'region', 'Unknown', 'None_type']
    building_types = df.columns.difference(excluded_cols)
    
    # Fill NA values only for building_types columns
    df[building_types] = df[building_types].fillna(0)
    
    # Also fill specific columns that are used in calculations
    df['len_res'] = df['len_res'].fillna(0)
    df['Unknown'] = df['Unknown'].fillna(0)  
    df['None_type'] = df['None_type'].fillna(0)
    
    # Calculate all_unknown if needed (currently unused)
    df['all_unknown'] = df['Unknown'] + df['None_type']
    if (df['all_unknown'] == 0).all():
        print('all unknown col is all 0') 
    required_cols = building_types.tolist() + ['all_unknown']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
 
    df['sum_buildings'] = df[required_cols].sum(axis=1)
    
    # Validation 1: Check if sums match len_res
    mismatch_mask = df['sum_buildings'] != df['len_res']
    if mismatch_mask.any():
        problematic_rows = df[mismatch_mask][['sum_buildings', 'len_res']]
        logger.warning(f"Sum mismatch found in {mismatch_mask.sum()} rows:\n{problematic_rows}")
        raise ValueError("Sum of building types does not match 'len_res' for some rows")
    
    # Validation 2: Check for zero len_res (would cause division by zero)
    zero_len_res = df['len_res'] == 0
    if zero_len_res.any():
        logger.warning(f"Found {zero_len_res.sum()} rows with len_res = 0")
        # Option 1: Raise error
        # raise ValueError("Cannot calculate percentages when len_res is 0")
        
        # Option 2: Handle gracefully by setting percentages to 0
        logger.info("Setting percentages to 0 for rows where len_res = 0")
    
    # Calculate percentages
    for column in building_types:
        # Handle division by zero case
        df[f'{column}_pct'] = df[column].div(df['len_res']).fillna(0) * 100
    
    df['all_unknown_typology_pct' ] = df['all_unknown'] .div(df['len_res']).fillna(0) * 100
    
    # Clean up temporary column
    df.drop(columns=['sum_buildings'], inplace=True)
    
    return df


def check_percentage_ranges(df):
    """
    Validates that all percentage columns are within 0-100 range.
    
    Args:
        df: DataFrame with percentage columns
        
    Raises:
        ValueError: If any percentages are outside valid range
    """
    percentage_cols = [col for col in df.columns if col.endswith('_pct')]
    
    if not percentage_cols:
        logger.warning("No percentage columns found to validate")
        return
    
    for col in percentage_cols:
        # Check for invalid values (including inf, -inf, NaN)
        valid_mask = df[col].between(0, 100, inclusive='both') & df[col].notna() & np.isfinite(df[col])
        invalid_mask = ~valid_mask
        
        if invalid_mask.any():
            problematic_entries = df[invalid_mask][[col, 'len_res']].head(10)  # Show first 10
            logger.error(f"Found {invalid_mask.sum()} invalid entries in {col}:\n{problematic_entries}")
            raise ValueError(f"Values in column '{col}' are outside valid range [0, 100] or contain invalid values")
    
    logger.info(f"All {len(percentage_cols)} percentage columns are within acceptable range")


def call_type_checks(df):
    """
    Main function to run all building type validations and calculations.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with validated data and percentage columns
    """
    try:
        logger.info("Starting building type validation and percentage calculation")
        
        # Validate input
        if df.empty:
            raise ValueError("Input DataFrame is empty")
            
        required_cols = ['len_res']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
        
        # Run validations
        df_processed = validate_and_calculate_percentages_type(df)
        check_percentage_ranges(df_processed)
        
        logger.info("Building type validation completed successfully")
        return df_processed
        
    except Exception as e:
        logger.error(f"Building type validation failed: {str(e)}")
        raise


######################### Post process age ######################### 

def validate_and_calculate_percentages_age(df):
    """
    Validate and calculate age-based percentages for building data.
    
    Args:
        df: DataFrame with building age data
        
    Returns:
        DataFrame with calculated age percentages
    """
 	
 
    # Get age type columns (exclude metadata columns)
    age_types = df.columns.difference(['postcode', 'len_res', 'region'])
    logger.debug(f'Age types: {age_types}')
    
    # Fill missing values and calculate totals
    df['len_res'] = df['len_res'].fillna(0)
    df['sum_buildings'] = df[age_types].fillna(0).sum(axis=1)
    
    # Remove existing None_age column and recalculate
    df.drop(columns=['None_age'], inplace=True)
    df['None_age'] = df['len_res'] - df['sum_buildings']
    
    # Calculate percentages for each age type
    for column in age_types.tolist() + ['None_age']:
        df[f'{column}_pct'] = (df[column] / df['len_res']) * 100
							
    # Clean up temporary columns
    df.drop(columns=['sum_buildings'], inplace=True)
    
    return df.fillna(0)


def check_age_percentage_ranges(df):
    """
    Validate that all percentage columns contain values between 0 and 100.
    
    Args:
        df: DataFrame with percentage columns
        
    Raises:
        ValueError: If any percentage values are outside the valid range
    """
    df = df.fillna(0)
    percentage_cols = [col for col in df.columns if '_pct' in col]
    
    for col in percentage_cols:
        if not df[col].between(0, 100.9).all():
            problematic_entries = df[~df[col].between(0, 100)]
            logger.warning(f"Problematic entries in column {col}:\n{problematic_entries}")
            raise ValueError(f"Values in column {col} are outside the range 0 to 100")


def call_age_checks(df):
    """
    Execute complete age validation and percentage calculation workflow.
    
    Args:
        df: DataFrame with building age data
        
    Returns:
        DataFrame with validated age percentages
    """
	

    df = validate_and_calculate_percentages_age(df)
    check_age_percentage_ranges(df)
    return df

######################### Post process fuel ######################### 

def test_data(df):
    
    logger.info('Starting tests')
    assert_larger(df, 'total_gas', 'avg_gas')
    assert_larger(df, 'total_elec', 'avg_elec')

    if df['postcode'].duplicated().sum() > 0: 
        raise Exception('Duplicated postcodes found')
    logger.info('Tests passed')


def call_post_process_fuel(intermed_dir, output_dir):
    os.makedirs(os.path.join(output_dir, 'attribute_logs'), exist_ok=True)
    op = os.path.join(intermed_dir, 'fuel')
    log= load_proc_dir_log_file( op)  
    log.to_csv(os.path.join(output_dir, 'attribute_logs/fuel_log_file.csv') ) 
    df = load_from_log(log)

    logger.info("Loaded data from logs.")
    df = post_proc_new_fuel(df)

    test_data(df)   
    return df 


def call_post_process_age(intermed_dir, output_dir):
    os.makedirs(os.path.join(output_dir, 'attribute_logs'), exist_ok=True)
    op = os.path.join(intermed_dir, 'age')
    log= load_proc_dir_log_file(op)  
    log.to_csv(os.path.join(output_dir, 'attribute_logs/age_log_file.csv') ) 

    df = load_from_log(log)
    logger.info("Loaded data from logs.")
    data = call_age_checks(df)
    return data


def call_post_process_type(intermed_dir, output_dir):
    os.makedirs(os.path.join(output_dir, 'attribute_logs'), exist_ok=True)
    op = os.path.join(intermed_dir, 'type')
    log= load_proc_dir_log_file(op)  
    log.to_csv(os.path.join(output_dir, 'attribute_logs/type_log_file.csv') ) 

    df = load_from_log(log)
    logger.info("Loaded data from logs.")
    data = call_type_checks(df)
    return data



######################### Unify post processing steps for buildings ######################### 

def generate_derived_cols(data):
        
    data['postcode_density'] = data['all_types_premise_area_total'] / data['postcode_area']
    data['postcode_density'] = np.where(data['postcode_density']> 1, 1, data['postcode_density'])
    data['log_pc_area'] = np.log(data.postcode_area)
  
    return data 
    

def merge_fuel_age_type(fuel, typed_data, age, temp  ):
    data = fuel.merge(typed_data, on=['postcode'])
    data = data.merge(age, on=['postcode'])
    data = data.merge(temp, left_on='postcode', right_on='POSTCODE')
    # data = fuel.merge(temp, left_on='postcode', right_on='POSTCODE')
    return data 


def postprocess_buildings(intermed_dir, output_dir):
    fuel_df = call_post_process_fuel(intermed_dir, output_dir)
    if 'percent_residential' not in fuel_df.columns:
        raise Exception('perc res missin')
        
    age_df = call_post_process_age(intermed_dir, output_dir)
    type_df = call_post_process_type(intermed_dir, output_dir)
    return fuel_df, age_df, type_df

def load_other_data(input_data_sources_location, intermediate_location = 'intermediate_data/'):
    
    if os.path.exists( os.path.join(intermediate_location, 'unified_temp_data.csv')):
        temp_data = pd.read_csv( os.path.join(intermediate_location, 'unified_temp_data.csv'))
    else:
        raise Exception('Temp data not found, re run stage create_climate in main.py')
    try:
        urbanisation_df = load_postcode_geometry_data(input_data_sources_location)
    except:
        raise Exception('Postcode geometry data not found, check postcode_areas.csv is in correct location in input data sources')
    try:
        pc_mapping = load_pc_to_output_area_mapping(input_data_sources_location)
    except:
        raise Exception('Error loading postcode mapping. Check lookups/PCD_OA_LSOA_MSOA_LAD_MAY22_UK_LU.csv is in correct location in input data sources')
    try:
        census_data = pd.read_csv( os.path.join(intermediate_location, 'unified_census_data.csv'))
    except:
        raise Exception('Error loading census data. Re run stage create_census in main.py and then check all files in src.post_process.unify_census are present in input data folder ' ) 
    return temp_data, urbanisation_df, pc_mapping, census_data

def unify_dataset(input_data_sources_location):
    logger.info('Starting post processing of buildings')
    os.makedirs('final_dataset', exist_ok=True)
    fuel_df, age_df, type_df = postprocess_buildings('intermediate_data', 'final_dataset')
    
    check_data_empty([fuel_df, age_df, type_df], ['fuel', 'age', 'type'])
    logger.info('Loaded fuel, age and type data. Loading other data')

    temp_data, urbanisation_df, pc_mapping, census_data = load_other_data(input_data_sources_location)
 
     
    check_data_empty([temp_data, urbanisation_df, pc_mapping, census_data], ['temp', 'urbanisation', 'pc_mapping', 'census_data'])
    logger.info('All data loaded. starting merge')
    data = merge_fuel_age_type(fuel_df, type_df, age_df, temp_data)
    
    if 'percent_residential' not in data.columns:
        raise Exception('perc res missin')
    
    logger.info('Data merged fuel age temp type successfully')
    check_data_empty([data], ['merged data'])
    logger.info('Starting to merge postcode mapping and urbanisation data') 
    data = join_pc_map_three_pc(data, 'postcode', pc_mapping )
    logger.info('Data merged postcode mapping successfully')
    check_data_empty([data], ['postcode mapping'])
    logger.info('Starting to merge urbanisation data')
    data = data.merge(urbanisation_df, on='POSTCODE')
    logger.info('Data merged urbanisation successfully')    
    check_data_empty([data], ['urbanisation'])
    logger.info('Starting to generate derived columns')
    data = generate_derived_cols(data)
    logger.info('Starting to merge census data')
 
    data = data.merge(census_data, left_on = 'oa21cd', right_on ='OA21CD')
    check_data_empty([data], ['census data'])
    logger.info('Data merged successfully')
    # merge rural urban 
    rur = load_rural_uran()

    
    data = data.merge(rur[['OA21CD', 'RUC21CD',  'RUC21NM', 'Urban_rural_flag' ]], on ='OA21CD', how='inner')
    centroid = load_centroids()
    data = join_three_pcds(data, 'postcode', centroid, ['PCD', 'PCD2', 'PCDS'])
    data = final_clean(data)
    data = final_rename(data)

    # check vals
    call_validations()
    
    return data


def check_data_empty(list_dfs, names ):
    for df, n  in zip(list_dfs, names):
        if df.empty:
            raise Exception(f'Data {n} is empty, check the data loading and processing steps')
        return df


def final_clean(new_df):
    cols_to_drop = ['index', 'region_y','region_x',  'len_res_x','len_res_y', 
    'POSTCODE',
    'pcd7',
    'pcd8',
    'pcds',
 'derived_unknown_res',
 'dointr',
 'doterm',
 'usertype',
     ]
    new_df['count_unclassified_domestic']= new_df['all_types_total_buildings'] - (new_df['clean_res_total_buildings'] + new_df['outb_res_total_buildings'])
    new_df.drop(cols_to_drop, axis=1, inplace=True)
    new_df.drop_duplicates(inplace=True)

    return new_df


######################### Filter to get final NEBULA sample ######################### 
 
def apply_filters(data, UPRN_THRESHOLD=40):
    """
    Apply multiple filters to a DataFrame containing residential energy usage data.
    
    Parameters:
    -----------
    data : pandas.DataFrame
        Input DataFrame containing residential and energy usage data
    UPRN_THRESHOLD : int, default=40
        Maximum allowed difference between gas meters and residential UPRNs
    gas_eui_threshold : float, default=500
        Maximum allowed gas energy usage intensity
    elec_eui_threshold : float, default=150
        Maximum allowed electricity energy usage intensity
        
    Returns:
    --------
    pandas.DataFrame
        Filtered DataFrame meeting all specified conditions
    """
 
 
	
    filters = {
        'residential_filter': lambda x: x['percent_residential'] == 100,
        'total_gas' : lambda x: x['total_gas'] > 0, 
        'total_elec': lambda x: x['total_elec'] > 0,
        'gas_meters_filter': lambda x: x['perc_diff_uprns_to_gas_meters'] <= UPRN_THRESHOLD,
        'gas_usage_range': lambda x: (x['gas_EUI_GIA'] <= 650) & (x['gas_EUI_GIA'] > 5),
        'electricity_usage': lambda x:  (x['elec_EUI_GIA'] <= 150) & (x['elec_EUI_GIA'] > 5),
        'building_count_range': lambda x: (x['count_all_types_total_buildings'].between(1, 200)),
        'heated_volume_range': lambda x: (x['all_buildings_GEA'].between(50, 20000)),
        'unknown_residential_types' : lambda x: x['percentage_unknown_res_buildings'] <= 25,
        'residential_premise_area_total': lambda x: x['residential_GEA_total'] >= x['residential_premise_area_total'],
        'outbuilding_GEA_total': lambda x: x['residential_GEA_total'] >= x['outbuilding_GEA_total'],
        'HDD' : lambda x: x['HDD'] > 0,
        'gas_per_meter': lambda x: x['avg_gas'] < 65000,
        'postcode_area' : lambda x: x['postcode_area'].between(10,  1.292852e+05),
        'bigtotal_gas' : lambda x: x['total_gas'] < 1000000,
    }


 
      
    # Apply all filters at once using numpy's logical AND
    mask = pd.Series(True, index=data.index)
    for filter_name, filter_func in filters.items():
        mask &= filter_func(data)
    
    # Create filtered DataFrame
    filtered_df = data.loc[mask].copy()
    for filter_name, filter_func in filters.items():
        rows_removed = len(data) - len(data[filter_func(data)])
        print(f"Filter '{filter_name}' removed {rows_removed} rows.")
    # Log filtering results if logger is available
    try:
        logger = get_logger(__name__)
        logger.info(f"Original rows: {len(data)}, Filtered/domestic rows: {len(filtered_df)}")
        for filter_name, filter_func in filters.items():
            rows_removed = len(data) - len(data[filter_func(data)])
            logger.debug(f"{filter_name}: removed {rows_removed} rows")
    except NameError:
        pass
    
    return filtered_df


def final_rename(data):
 
    renames = {      
         'diff_gas_meters_uprns_res': 'perc_diff_uprns_to_gas_meters',
     'all_types_total_fl_area_H_total': 'all_buildings_GEA',
     'all_unknown': 'all_unknown_typology',
    'None_age': 'all_none_age',
    'None_age_pct': 'all_none_age_pct',
    'unknown_alltypes': 'unknown_alltypes_count',
 'perc_clean_res': 'percentage_clean_res_buildings', 
 'perc_unknown_res': 'percentage_unknown_res_buildings',
 'perc_cl_res_basement' : 'percentage_clean_res_basement_builds',
 'perc_all_res_listed': 'percentage_all_res_listed_builds',
   'all_types_total_buildings': 'count_all_types_total_buildings',
    'clean_res_total_buildings': 'residential_total_buildings',
    'clean_res_premise_area_total' : 'residential_premise_area_total',
    'clean_res_total_fl_area_meta_total': 'residential_GEA_total',
    'clean_res_scaled_fl_area_meta_res_total': 'residential_GIA_total',
    'clean_res_base_floor_total': 'residential_basement_counts',
    'clean_res_basement_heated_vol_total': 'residential_basement_GEA_total',
    'clean_res_listed_bool_total': 'count_residential_listed_buildings',
    'clean_res_uprn_count_total': 'count_residential_uprns',
    'clean_res_premise_area_null_count': 'count_residential_premise_area_null',
    'outb_res_total_buildings': 'count_outbuildings',
    'outb_res_premise_area_total': 'outbuilding_premise_area_total',
    'outb_res_total_fl_area_H_total' : 'outbuilding_GEA_total',
    'confidence_floor_area': 'confidence_floor_area',
    'count_unclassified_domestic' : 'count_unclassified_domestic',
    'gas_eui_GIA_MR': 'gas_EUI_GIA',
 'elec_eui_GIA_MR' : 'elec_EUI_GIA',
 
}
    data.rename(columns=renames, inplace=True)
    data = data[all_cols_list]
    return data 