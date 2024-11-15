
import pandas as pd 
import os 
import numpy as np
from .logging_config import get_logger

logger = get_logger(__name__)


os.makedirs(f'{data_dir}/census_attrs', exist_ok=True)
census_loc = '/Volumes/T9/2024_Data_downloads/2024_11_nebula_paper_data/2021_UK_census'

def create_simple_census_perc(df, code_col, val_col, attr):
    total_households  = pd.pivot_table(df, index = 'Output Areas Code', values = 'Observation', aggfunc='sum') 
    h = pd.pivot_table(df, index = 'Output Areas Code', values = 'Observation', columns = code_col, aggfunc='sum') 
    perc_df = h.join(total_households)
    
    mapping_dict = {key: value for key, value in zip(df[code_col], df[val_col])}

    for k ,v in mapping_dict.items():
        perc_df[f'{attr}_perc_{v}'] = perc_df[k] / perc_df['Observation']

    column_sum = perc_df[[f'{attr}_perc_{v}' for v in mapping_dict.values() ]].sum(axis=1)
    is_sum_equal_to_1 = np.isclose(column_sum, 1)
    if len (perc_df[~is_sum_equal_to_1]) != 0:
        raise ValueError('The conversion to percentages has failed.' ) 

    perc_df[[f'{attr}_perc_{v}' for v in mapping_dict.values() ]].reset_index().to_csv(f'intermediate_data/census_attrs/{attr}.csv', index = False)
    logger.info(f'{attr} saved, length ', len(perc_df))
    

def create_complex_census_attr(df, code_col, val_col, code_col2, val_col2, attr):
    mapping_dict = {key: value for key, value in zip(df[code_col], df[val_col])}
    mapping_dict2 = {key: value for key, value in zip(df[code_col2], df[val_col2])}

    total_households = pd.pivot_table(df, index = 'Output Areas Code', values = 'Observation', aggfunc='sum')
    h = pd.pivot_table(df, index = 'Output Areas Code', values = 'Observation', columns = [code_col, code_col2], aggfunc='sum')
    perc_df = h.join(total_households)
    

    for k ,v in mapping_dict.items():
        for k2, v2 in mapping_dict2.items():
            perc_df[f'{attr}_perc_{v}_{v2}'] = perc_df[k, k2] / perc_df['Observation']
    
    column_sum = perc_df[[f'{attr}_perc_{v}_{v2}' for v in mapping_dict.values() for v2 in mapping_dict2.values() ]].sum(axis=1)
    is_sum_equal_to_1 = np.isclose(column_sum, 1)
    if len (perc_df[~is_sum_equal_to_1]) != 0:
        raise ValueError('The conversion to percentages has failed.' ) 

    perc_df[[f'{attr}_perc_{v}_{v2}' for v in mapping_dict.values() for v2 in mapping_dict2.values() ]].reset_index().to_csv(f'intermediate_data/census_attrs/{attr}.csv', index = False)
    logger.info(f'{attr} saved, length ', len(perc_df))



def main():

    logger.info('Starting to generate census attributes')

    df = pd.read_csv(os.path.join(census_loc, 'occupation/TS063-2021-5-filtered-2024-03-04T15_38_25Z.csv'))
    code_col = 'Occupation (current) (10 categories) Code'
    val_col = 'Occupation (current) (10 categories)'
    attr = 'occupation'
    create_simple_census_perc(df, code_col, val_col, attr)


    df= pd.read_csv(os.path.join(census_loc, 'economic_activity/TS066-2021-6-filtered-2024-03-04T15_29_15Z.csv') )  
    code_col = 'Economic activity status (20 categories) Code'
    val_col = 'Economic activity status (20 categories)'
    attr = 'economic_activity'
    create_simple_census_perc(df, code_col, val_col, attr)  

    df = pd.read_csv(os.path.join(census_loc, 'household_size/TS017-2021-3-filtered-2024-03-04T16_36_34Z.csv'))
    create_simple_census_perc(df,'Household size (9 categories) Code', 'Household size (9 categories)', 'household_siz_perc' )

    df=pd.read_csv(os.path.join(census_loc, 'ethnic_group/TS021-2021-3-filtered-2024-03-05T10_06_33Z.csv'))
    create_simple_census_perc(df, 'Ethnic group (20 categories) Code', 'Ethnic group (20 categories)', 'ethnic_group')

    df= pd.read_csv(os.path.join(census_loc, 'sex_by_age/RM121-2021-1-filtered-2024-03-05T10_10_26Z.csv') )
    create_simple_census_perc(df, 'Sex (2 categories) Code', 'Sex (2 categories)', 'sex')


    df = pd.read_csv(os.path.join(census_loc, 'household_bedroom_number/RM059-2021-3-filtered-2024-03-04T15_28_43Z.csv') )
    create_complex_census_attr(df, 'Household composition (6 categories) Code', 'Household composition (6 categories)', 'Number of Bedrooms (5 categories) Code', 'Number of Bedrooms (5 categories)',  'household_comp_by_bedroom' )

    df = pd.read_csv(os.path.join(census_loc, 'occupancy_rating/TS052-2021-5-filtered-2024-04-18T10_01_16Z.csv'))
    create_simple_census_perc(df, 'Occupancy rating for bedrooms (6 categories) Code', 'Occupancy rating for bedrooms (6 categories)', 'occupancy_rating')

    df = pd.read_csv(os.path.join(census_loc, 'central_heating/TS046-2021-4-filtered-2024-04-18T10_02_28Z.csv'))
    create_simple_census_perc(df, 'Type of central heating in household (13 categories) Code', 'Type of central heating in household (13 categories)', 'central_heating')

    df = pd.read_csv(os.path.join(census_loc, 'highest_qualification/TS067-2021-3-filtered-2024-04-18T09_58_11Z.csv'))
    create_simple_census_perc(df, 'Highest level of qualification (8 categories) Code', 'Highest level of qualification (8 categories)', 'highest_qual')

    df=pd.read_csv(os.path.join(census_loc, 'level_SE/TS062-2021-5-filtered-2024-04-18T10_23_30Z.csv'))
    create_simple_census_perc(df, 'National Statistics Socio-economic Classification (NS-SeC) (10 categories) Code', 'National Statistics Socio-economic Classification (NS-SeC) (10 categories)', 'socio_class')

    df = pd.read_csv(os.path.join(census_loc, 'household_deprivation/TS011-2021-6-filtered-2024-03-04T15_28_01Z.csv'))
    create_simple_census_perc(df, 'Household deprivation (6 categories) Code', 'Household deprivation (6 categories)', 'deprivation')
    logger.info('Finished generating census attributes')    


if __name__ == '__main__':
    main()