import numpy as np
import pandas as pd
import os
import glob
from src.pre_process_buildings import *
from src.postcode_utils import load_ids_from_file,  check_merge_files, join_pc_map_three_pc

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

    # check for duplicated postcodes 
    
    if df['postcode'].duplicated().sum() > 0: 
        raise Exception('Duplicated postcodes found')
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

    data['unkn_res'] = data['all_res_total_buildings'] - data['clean_res_total_buildings'] - data['outb_res_total_buildings']
    data['perc_unk_res'] = data['unkn_res'] / data['all_res_total_buildings'] * 100 
    data['perc_unk_res'] = data['perc_unk_res'].fillna(0)
    return df

def deal_unknown_res(data):
    """
    Remove those with more than threshold value of unknwon residentails builds
    remove those with more volume of outbiilding than res (should only be a few rows )
    """
    print('Starting to deal with unknown res')
    print('OG legnth of data', len(data))
    og_len = len(data)
    logger.info(f'Length of data before removing unknownd: {og_len}')
    data= data[data['clean_res_heated_vol_fc_total'] > data['outb_res_heated_vol_fc_total'].fillna(0) ] 
    print('Len after ob filter ' , len(data))
    if len(data) / og_len * 100  < 0.95:
        raise Exception('More than 5% filtered out because ooutbuilding volume greater ')
    cl_data =   data[data['perc_unk_res']< PERC_UNKNOWN_RES_ALLOWED ]
    print('Len after removing unknown residential types')
    if cl_data[cl_data['all_res_gross_area_total'] < cl_data['all_res_premise_area_total']].shape[0] < 10 :
        cl_data= cl_data[cl_data['all_res_gross_area_total'] >= cl_data['all_res_premise_area_total']]
    else:
        raise Exception('More than 10 rows have gross area less than premise area (gross = total build ,premise = footprint)')

    return cl_data     


def call_post_process_fuel(intermed_dir, output_dir):
    os.makedirs(os.path.join(output_dir, 'attribute_logs'), exist_ok=True)
    op = os.path.join(intermed_dir, 'fuel')
    log= load_proc_dir_log_file( op)  
    log.to_csv(os.path.join(output_dir, 'attribute_logs/fuel_log_file.csv') ) 
    df = load_from_log(log)
    try:
        logger.info("Loaded data from logs.")
        df = post_proc_new_fuel(df)
        # cl_data=deal_unknown_res(df)
        test_data(df)
    except: 
        print('failed take df for validation ' ) 
        return df 
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


######################### load other data ######################### 

def load_pc_to_output_area_mapping(input_data_sources_location):
    """ Load the postcode to output area mappings
    These are for 2021 census 
    """
    pc_oa_mapping = pd.read_csv(os.path.join(input_data_sources_location , 'lookups/PCD_OA21_LSOA21_MSOA21_LAD_MAY23_UK_LU.csv') , encoding='latin1') 
    return pc_oa_mapping

    
def load_postcode_geometry_data(input_data_sources_location):
    data = pd.read_csv(os.path.join(input_data_sources_location, 'postcode_areas/postcode_areas.csv' ) ) 
    return data 

def generate_derived_cols(data):
    data['postcode_density'] = data['all_res_premise_area_total'] / data['postcode_area']
    data['postcode_density'] = np.where(data['postcode_density']> 1, 1, data['postcode_density'])
    data['log_pc_area'] = np.log(data.postcode_area)

    data['gas_eui'] = data['total_gas'] / data['clean_res_heated_vol_h_total']
    data['elec_eui'] = data['total_elec'] / data['clean_res_heated_vol_h_total']
    return data 

# def unify_census(input_data_sources_location):
#     # census and urban rural, inc mapping from 2011 to 2021 OAs
#     census  = glob.glob('intermediate_data/census_attrs/*csv')
#     ll=pd.DataFrame()
#     i = 0
#     for f in census:
#         df = pd.read_csv(f)
#         if i ==0:
#             res = df.copy()  
#             i+=1    
#         else:
#             res = df.merge(res, on='Output Areas Code', how='outer')

#     oa_lk = pd.read_csv(os.path.join(input_data_sources_location, 'lookups/Output_Areas_(2011)_to_Output_Areas_(2021)_to_Local_Authority_District_(2022)_Lookup_in_England_and_Wales_(Version_2).csv') ) 
#     u_r =  pd.read_csv(os.path.join(input_data_sources_location,  'urbal_rural_2011/RUC11_OA11_EW.csv')) 

#     cen_lk = res.merge(oa_lk, left_on = 'Output Areas Code', right_on ='OA21CD')
#     cen_ur = cen_lk.merge(u_r[['OA11CD', 'RUC11CD', 'RUC11']], on = 'OA11CD', how='inner')
#     return cen_ur 
######################### Unify post processing steps for buildings ######################### 

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
    
    if os.path.exists( os.path_join(intermediate_location, 'unified_temp_data.csv')):
        temp_data = pd.read_csv( os.path_join(intermediate_location, 'unified_temp_data.csv'))
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
        census_data = pd.read_csv( os.path_join(intermediate_location, 'unified_census_data.csv'))
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
    check_data_empty([data], ['merged data'])
    data = join_pc_map_three_pc(data, 'postcode', pc_mapping )
    check_data_empty([data], ['postcode mapping'])
    data = data.merge(urbanisation_df, on='POSTCODE')
    check_data_empty([data], ['urbanisation'])
    
    data = generate_derived_cols(data)
    data = data.merge(census_data, left_on = 'oa21cd', right_on ='OA21CD')
    check_data_empty([data], ['census data'])
    logger.info('Data merged successfully')
    
    return data


def check_data_empty(list_dfs, names ):
    for df, n  in zip(list_dfs, names):
        if df.empty:
            raise Exception(f'Data {n} is empty, check the data loading and processing steps')
        return df




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
        'residential_filter': lambda x: x['percent_residential'] == 1,
        'gas_meters_filter': lambda x: x['diff_gas_meters_uprns_res'] <= UPRN_THRESHOLD,
        'gas_usage_range': lambda x: (x['gas_eui'] <= 500) & (x['gas_eui'] > 5),
        'electricity_usage': lambda x: x['elec_eui'] <= 150,
        'building_count_range': lambda x: (x['all_types_total_buildings'].between(1, 200)),
        'heated_volume_range': lambda x: (x['all_res_heated_vol_h_total'].between(50, 200000)),
        'unknown_residential_types' : lambda x: x['perc_unk_res'] <= 10,
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