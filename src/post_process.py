import numpy as np
import pandas as pd
import os
import glob
from src.pre_process_buildings import *
from src.postcode_utils import load_ids_from_file,  check_merge_files

from src.logging_config import get_logger
logger = get_logger(__name__)

# PERC_RANGE_METERS_UPRN = 20
PERC_UNKNOWN_RES_ALLOWED = 10

pc_excl_ovrlap = load_ids_from_file('src/overlapping_pcs.txt')
pc_excl_ovrlap = [x.strip() for x in pc_excl_ovrlap]

######################### Load from downloaded data ######################### 

def load_proc_dir_log_file(path):
    logger.info('Starting to load proc dir')
    folder = glob.glob(os.path.join(path, '*/*csv'))
    full_dict = []
    
    for file_path in folder:
        
        df = pd.read_csv(file_path)
        df.drop_duplicates(inplace=True)
        
        region = file_path.split('/')[-2]
        batch = os.path.basename(file_path).split('_')[0]
        data_len = len(df)
        
        full_dict.append({
            'path': file_path,
            'region': region,
            'batch': batch,
            'len': data_len,
            'memory': 'norm'
        })
    
    return pd.DataFrame(full_dict)

def load_from_log(log):
    fin = []
    
    for file_path, region in zip(log.path, log.region):
        df = pd.read_csv(file_path)
        df['region'] = region
        df.drop_duplicates(inplace=True)
        
        if df.groupby('postcode').size().max() > 1:
            logger.warning(f"Duplicate postcodes found in {file_path}")
        
        fin.append(df)
    
    fin_df = pd.concat(fin)
    return fin_df[~fin_df['postcode'].isin(pc_excl_ovrlap)].copy()

######################### Post process type ######################### 

# def validate_and_calculate_percentages_type(df):
#     df ['all_unknown'] = df.Unknown.fillna(0) + df.None_type.fillna(0)
#     building_types = df.columns.difference(['postcode', 'len_res', 'region', 'Unknown','None_type' ])
#     df['len_res'] = df['len_res'].fillna(0)
#     df['sum_buildings'] = df[building_types].fillna(0).sum(axis=1)

#     if not (df['sum_buildings'] == df['len_res']).all():
#         logger.info(df[df['sum_buildings'] != df['len_res']][['sum_buildings', 'len_res']])
#         raise ValueError("Sum of building types does not match 'len_res' for some rows")

#     for column in building_types:
#         df[f'{column}_pct'] = (df[column] / df['len_res']) * 100

#     df.drop(columns=['sum_buildings'], inplace=True)
#     return df

# def check_percentage_ranges(df):
#     df = df.fillna(0)
#     percentage_cols = [col for col in df.columns if '_pct' in col]

#     for col in percentage_cols:
#         if not df[col].between(0, 100).all():
#             problematic_entries = df[~df[col].between(0, 100)]
#             logger.warning(f"Problematic entries in column {col}:\n{problematic_entries}")
#             raise ValueError(f"Values in column {col} are outside the range 0 to 100")

#     logger.debug("All percentages are within the acceptable range.")

# def call_type_checks(df):
#     df = validate_and_calculate_percentages_type(df)
#     check_percentage_ranges(df)
#     return df


# ######################### Post process age ######################### 

# def validate_and_calculate_percentages_age(df):
#     age_types = df.columns.difference(['postcode', 'len_res', 'region'])
#     logger.debug(f'Age types: ' {age_types})
#     df['len_res'] = df['len_res'].fillna(0)
#     df['sum_buildings'] = df[age_types].fillna(0).sum(axis=1)

#     if not (df['sum_buildings'] == df['len_res']).all():
#         logger.warning(df[df['sum_buildings'] != df['len_res']][['sum_buildings', 'len_res']])
#         raise ValueError("Sum of building ages does not match 'len_res' for some rows")

#     for column in age_types:
#         df[f'{column}_pct'] = (df[column] / df['len_res']) * 100

#     df.drop(columns=['sum_buildings'], inplace=True)
#     return df

# def check_age_percentage_ranges(df):
#     df = df.fillna(0)
#     percentage_cols = [col for col in df.columns if '_pct' in col]

#     for col in percentage_cols:
#         if not df[col].between(0, 100).all():
#             problematic_entries = df[~df[col].between(0, 100)]
#             logger.warning(f"Problematic entries in column {col}:\n{problematic_entries}")
#             raise ValueError(f"Values in column {col} are outside the range 0 to 100")


# def call_age_checks(df):
#     df = validate_and_calculate_percentages_age(df)
#     check_age_percentage_ranges(df)
#     return df

######################### Post process fuel ######################### 

def test_data(df):
    logger.info('Starting tests')

    assert_larger(df, 'all_res_heated_vol_fc_total', 'clean_res_heated_vol_fc_total')
    
    assert_larger(df, 'all_res_heated_vol_fc_total', 'outb_res_heated_vol_fc_total')
    assert_larger(df, 'total_gas', 'avg_gas')
    assert_larger(df, 'total_elec', 'avg_elec')
    assert_larger(df, 'clean_res_heated_vol_fc_total', 'outb_res_heated_vol_fc_total')
    if 'all_res_gross_area_total' not in df.columns.tolist():
        None
    else:
        assert_larger(df, 'all_res_gross_area_total', 'all_res_premise_area_total')
    if 'clean_res_gross_area_total' not in df.columns.tolist():
        None
    else:
        assert_larger(df, 'clean_res_gross_area_total', 'clean_res_premise_area_total')
    
    if not df[(df['clean_res_total_buildings'] == df['all_res_total_buildings']) & 
              (df['all_res_premise_area_total'] != df['clean_res_premise_area_total'])].empty:
        raise Exception('Error in sum of residential buildings - clean and all not matching when building count same')

    logger.info('Tests passed')

def validate_vol_per_uprn(df):
    excl = df[(df['max_vol_per_uprn'] < 100) & (df['diff_gas_meters_uprns_res'] > 6)]
    return df[~df.index.isin(excl.index)]

def post_proc_new_fuel(df):
    df['outcode'] = df['postcode'].apply(lambda x: str(x).split(' ')[0])
    df['tot'] = (df['all_res_total_buildings'].fillna(0) + df['comm_alltypes_count'].fillna(0) + 
                 df['mixed_total_buildings'].fillna(0) + df['unknown_alltypes'].fillna(0))

    if not df[df['tot'] != df['all_types_total_buildings'].fillna(0)][['tot', 'all_types_total_buildings']].empty:
        logger.warning('Error - count of buildings not adding up')
        raise Exception('Error - count of buildings not adding up')
        
    df['percent_residential'] = df['all_res_total_buildings'] / df['all_types_total_buildings']
    df['perc_all_res'] = df['all_res_total_buildings'] / df['all_types_total_buildings']
    df['perc_clean_res'] = df['clean_res_total_buildings'] / df['all_types_total_buildings']
    
    ob_cols = [x for x in df.columns if x.startswith('outb')]

    for col in ob_cols:
        df[col] = df[col].fillna(0)
    
    df['perc_all_res_basement'] = df['clean_res_base_floor_total'] / df['all_types_total_buildings']
    df['perc_all_res_listed'] = df['all_res_listed_bool_total'] / df['all_types_total_buildings']

    df['diff_gas_meters_uprns_res'] = (np.abs(df['num_meters_gas'] - df['all_res_uprn_count_total']) / 
                                       df['num_meters_gas']) * 100
    
    df['gas_EUI'] = df['total_gas'] / df['clean_res_heated_vol_h_total']
    df['elec_EUI'] = df['total_elec'] / df['clean_res_heated_vol_h_total']

    
    return df

def deal_unknown_res(data):
    """
    Remove those with more than threshold value of unknwon residentails builds
    remove those with more volume of outbiilding than res (should only be a few rows )
    """
    og_len = len(data)
    logger.info(f'Length of data before removing unknownd: {og_len}')
    data['unkn_res'] = data['all_res_total_buildings'] - data['clean_res_total_buildings'] - data['outb_res_total_buildings']
    data['perc_unk_res'] = data['unkn_res'] / data['all_res_total_buildings'] * 100 
    data['perc_unk_res'] = data['perc_unk_res'].fillna(0)
    
    data= data[data['clean_res_heated_vol_fc_total'] > data['outb_res_heated_vol_fc_total'].fillna(0) ] 
    if len(data) / og_len * 100  < 0.9:
        raise Exception('More than 10% filtered out')
    return  data[data['perc_unk_res']< PERC_UNKNOWN_RES_ALLOWED ]


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
    # Define all filter conditions in a dictionary for clarity and maintainability
    filters = {
        'residential_filter': lambda x: x['percent_residential'] == 1,
        'gas_meters_filter': lambda x: x['diff_gas_meters_uprns_res'] <= UPRN_THRESHOLD,
        'gas_usage_range': lambda x: (x['h_av_gas'] <= 500) & (x['h_av_gas'] > 5),
        'electricity_usage': lambda x: x['h_av_elec'] <= 150,
        'building_count_range': lambda x: (x['all_types_total_buildings'].between(1, 200)),
        'heated_volume_range': lambda x: (x['all_res_heated_vol_h_total'].between(50, 200000))
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
        logger.info(f"Original rows: {len(data)}, Filtered rows: {len(filtered_df)}")
        for filter_name, filter_func in filters.items():
            rows_removed = len(data) - len(data[filter_func(data)])
            logger.debug(f"{filter_name}: removed {rows_removed} rows")
    except NameError:
        pass
    
    return filtered_df



def call_post_process(OUTPUT_DIR):
    
    op = os.path.join(OUTPUT_DIR, 'proc_dir/fuel')
    log= load_proc_dir_log_file( op)  
    log.to_csv('fuel_log_file.csv')

    df = load_from_log(log)
    logger.info("Loaded data from logs.")
    df = post_proc_new_fuel(df)
    data=deal_unknown_res(df)
    test_data(data)
    
    return data 



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
    # Define all filter conditions in a dictionary for clarity and maintainability
    filters = {
        'residential_filter': lambda x: x['percent_residential'] == 1,
        'gas_meters_filter': lambda x: x['diff_gas_meters_uprns_res'] <= UPRN_THRESHOLD,
        'gas_usage_range': lambda x: (x['h_av_gas'] <= 500) & (x['h_av_gas'] > 5),
        'electricity_usage': lambda x: x['h_av_elec'] <= 150,
        'building_count_range': lambda x: (x['all_types_total_buildings'].between(1, 200)),
        'heated_volume_range': lambda x: (x['all_res_heated_vol_h_total'].between(50, 200000))
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
        logger.info(f"Original rows: {len(data)}, Filtered rows: {len(filtered_df)}")
        for filter_name, filter_func in filters.items():
            rows_removed = len(data) - len(data[filter_func(data)])
            logger.debug(f"{filter_name}: removed {rows_removed} rows")
    except NameError:
        pass
    
    return filtered_df