import numpy as np
import pandas as pd
import os
import glob
from src.pre_process_buildings import *
from src.postcode_utils import load_ids_from_file,  check_merge_files, join_pc_map_three_pc


from src.load_data import load_from_log, load_proc_dir_log_file, load_pc_to_output_area_mapping, load_postcode_geometry_data
from src.validations import call_validations
from src.logging_config import get_logger
from src.post_process_buildings_stock import post_proc_new_fuel 
logger = get_logger(__name__)




######################### Post process type ######################### 

def validate_and_calculate_percentages_type(df):
    df ['all_unknown'] = df.Unknown.fillna(0) + df.None_type.fillna(0)
    building_types = df.columns.difference(['postcode', 'len_res', 'region', 'Unknown','None_type' ])
    df['len_res'] = df['len_res'].fillna(0)
    df['sum_buildings'] = df[building_types].fillna(0).sum(axis=1)

    if not (df['sum_buildings'] == df['len_res']).all():
        logger.info(df[df['sum_buildings'] != df['len_res']][['sum_buildings', 'len_res']])
        raise ValueError("Sum of building types does not match 'len_res' for some rows")

    for column in building_types:
        df[f'{column}_pct'] = (df[column] / df['len_res']) * 100

    df.drop(columns=['sum_buildings'], inplace=True)
    return df

def check_percentage_ranges(df):
    df = df.fillna(0)
    percentage_cols = [col for col in df.columns if '_pct' in col]

    for col in percentage_cols:
        if not df[col].between(0, 100).all():
            problematic_entries = df[~df[col].between(0, 100)]
            logger.warning(f"Problematic entries in column {col}:\n{problematic_entries}")
            raise ValueError(f"Values in column {col} are outside the range 0 to 100")

    logger.debug("All percentages are within the acceptable range.")

def call_type_checks(df):
    df = validate_and_calculate_percentages_type(df)
    check_percentage_ranges(df)
    return df


######################### Post process age ######################### 

def validate_and_calculate_percentages_age(df):
    age_types = df.columns.difference(['postcode', 'len_res', 'region'])
    logger.debug(f'Age types: {age_types}')
    df['len_res'] = df['len_res'].fillna(0)
    df['sum_buildings'] = df[age_types].fillna(0).sum(axis=1)
    df.drop(columns=['None_age'], inplace=True)
    df['None_age'] = df['len_res'] - df['sum_buildings']

    for column in age_types:
        df[f'{column}_pct'] = (df[column] / df['len_res']) * 100

    df.drop(columns=['sum_buildings'], inplace=True)
    return df

def check_age_percentage_ranges(df):
    df = df.fillna(0)
    percentage_cols = [col for col in df.columns if '_pct' in col]

    for col in percentage_cols:
        if not df[col].between(0, 100).all():
            problematic_entries = df[~df[col].between(0, 100)]
            logger.warning(f"Problematic entries in column {col}:\n{problematic_entries}")
            raise ValueError(f"Values in column {col} are outside the range 0 to 100")


def call_age_checks(df):
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
    # remove some dups from oa to oa 2021-221 mappping
    census_data = census_data.drop_duplicates(subset=['OA21CD', 'RUC11CD'])
    check_data_empty([temp_data, urbanisation_df, pc_mapping, census_data], ['temp', 'urbanisation', 'pc_mapping', 'census_data'])
    logger.info('All data loaded. starting merge')
    data = merge_fuel_age_type(fuel_df, type_df, age_df, temp_data)
    

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
    
    data = final_clean(data)

    # check vals
    call_validations()
    
    return data


def check_data_empty(list_dfs, names ):
    for df, n  in zip(list_dfs, names):
        if df.empty:
            raise Exception(f'Data {n} is empty, check the data loading and processing steps')
        return df


def final_clean(new_df):
    cols_to_drop = ['index','ObjectId', 'region_y','region_x',  'len_res_x','len_res_y', 'Unknown', 'None_type', 
    'POSTCODE',
    'pcd7',
    'pcd8',
    'pcds',
 'derived_unknown_res',
 'dointr',
 'doterm',
 'usertype',
     ]
    new_df.drop(cols_to_drop, axis=1, inplace=True)
    cols_rename =  {'all_unknown_pct': 'all_unknown_typology_pct',
    'all_unknown': 'all_unknown_typology',
    'None_age': 'all_none_age',
    'None_age_pct': 'all_none_age_pct',
    'unknown_alltypes': 'unknown_alltypes_count',
 'total_res_total_buildings': 'Welcom' , 
 'perc_clean_res': 'percentage_clean_res_buildings', 
 'perc_unknown_res': 'percentage_unknown_res_buildings',
 'perc_cl_res_basement' : 'percentage_clean_res_basement_builds',
 'perc_all_res_listed': 'percentage_all_res_listed_builds',
    }
    new_df.rename(columns=cols_rename, inplace=True)
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
        'total_gas' : lambda x: x['total_gas'] > 0,
        'total_elec': lambda x: x['total_elec'] > 0,
        'residential_filter': lambda x: x['percent_residential'] == 100,
        'gas_meters_filter': lambda x: x['diff_gas_meters_uprns_res'] <= UPRN_THRESHOLD,
        'gas_usage_range': lambda x: (x['gas_EUI_H'] <= 500) & (x['gas_EUI_H'] > 5),
        'electricity_usage': lambda x: x['elec_EUI_H'] <= 150,
        'building_count_range': lambda x: (x['all_types_total_buildings'].between(1, 200)),
        'heated_volume_range': lambda x: (x['all_types_total_fl_area_H_total'].between(50, 20000)),
        'unknown_residential_types' : lambda x: x['percentage_unknown_res_buildings'] <= 25,
        'premise_area_total_fl_area': lambda x: x['clean_res_total_fl_area_H_total'] >= x['clean_res_premise_area_total'],
        'outb_res_total_fl_area_total': lambda x: x['clean_res_total_fl_area_H_total'] >= x['outb_res_total_fl_area_H_total'],
    }
 
      
    # Apply all filters at once using numpy's logical AND
    mask = pd.Series(True, index=data.index)
    for filter_name, filter_func in filters.items():
        mask &= filter_func(data)
    
    # Create filtered DataFrame
    filtered_df = data.loc[mask].copy()
    
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