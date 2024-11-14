from src.pre_process_buildings import pre_process_building_data 
from src.postcode_utils import check_duplicate_primary_key, find_data_pc_joint
import numpy as np 
import pandas as pd
import sys 
import numpy as np  

def calc_filtered_percentage_of_building_age(df, age_types):
    """
    Function to create percentage of different building ages, filtered by specified types,
    and return it in a dictionary.
    
    Parameters:
    df : DataFrame containing one postcode's worth of building information.
    age_types : List of building ages to include in the output.
    """
    # Calculate the count of each age and normalize to get percentages
    df['premise_age_bucketed'] = np.where(df['premise_age'].isin(['Pre 1837', '1837-1869', '1870-1918']), 'Pre 1919', df['premise_age'])

    df = df[df['premise_use'] == 'Residential']
    all_building_ages = df['premise_age_bucketed'].value_counts()
    nn = df[(df['premise_age_bucketed'].isna()) | (df['premise_age_bucketed'] == 'Unknown date')    ]
    # Filter to keep only the specified building ages
    filtered_building_ages = all_building_ages[all_building_ages.index.isin(age_types)]
    fa = filtered_building_ages.to_dict()
    fa.update({'None_age': len(nn)})

    return fa

def calc_res_clean_counts(df):
    res = df[df['premise_use'] == 'Residential']
    return len(res)

def process_postcode_building_age(pc, onsud_data, INPUT_GPK, overlap=False, batch_dir=None, path_to_pcshp=None):
    """Process one postcode, deriving building attributes and electricity and fuel info.
    
    Inputs: 
    
    pc: postcode 
    onsud_data: output of find_postcode_for_ONSUD_file, tuples of data, pc_shp 
    INPUT_GPK: building file verisk 
    overlap: bool, is this for the overlapping postcodes? 
    batch_dir = needed for overlap - where are the batches stored?
    path_to_pcshp: path to postcode shapefiles location, needed for overlap 
    """
    age_types =  [  'Pre 1919',
                        '1919-1944',
                        '1945-1959', 
                        '1960-1979',
                        '1980-1989',
                        '1990-1999',
                        'Post 1999' 
                        ]

    pc = pc.strip()

    uprn_match = find_data_pc_joint(pc, onsud_data, input_gpk=INPUT_GPK)
    dc_full = {'postcode': pc}

    for val in age_types:
        dc_full[val] = np.nan
    dc_full['len_res'] = np.nan 
    dc_full['None_age'] = np.nan

    if uprn_match is None:
        print('Empty uprn match')
    else:
        df = pre_process_building_data(uprn_match)
        if len(df) != len(uprn_match):
            raise Exception('Error in pre-process - some cols dropped?')
        dc = calc_filtered_percentage_of_building_age(df, age_types)
        if df is not None:
            if check_duplicate_primary_key(df, 'upn'):
                print('Duplicate primary key found for upn')
                sys.exit()

        dc_res = {'len_res': calc_res_clean_counts(uprn_match)}
        dc_full.update(dc_res)
        dc_full.update(dc)

    return dc_full
