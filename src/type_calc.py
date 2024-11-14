

from .pre_process_buildings import pre_process_building_data 
from .postcode_utils import check_duplicate_primary_key , find_data_pc_joint
import numpy as np
import pandas as pd
import sys 
import numpy as np  




def calc_filtered_percentage_of_premise_type(df, prem_types):
    """
    Function to create percentage of different building types, filtered by specified types,
    and return it in a dictionary.
    
    Parameters:
    df : DataFrame containing one postcode's worth of building information.
    prem_types : List of building types to include in the output.
    """
    # Calculate the count of each type and normalize to get percentages
    df=df[df['premise_use']=='Residential']
    all_premise_types = df['premise_type'].value_counts()
    nn =  df[df['premise_type'].isna()]
    # Filter to keep only the specified premise types
    filtered_premise_types = all_premise_types[all_premise_types.index.isin(prem_types)]
    fp = filtered_premise_types.to_dict()
    fp.update({'None_type' : len(nn) } )

    return  fp 

def calc_res_clean_counts(df):
    res = df[df['premise_use']=='Residential']
    return len(res)




    
def process_postcode_buildtype(pc, onsud_data,  INPUT_GPK, overlap = False, batch_dir=None, path_to_pcshp=None  ):
    """Process one postcode, deriving building attributes and electricity and fuel info.
    
    Inputs: 
    
    pc: postcode 
    onsud_data: output of find_postcode_for_ONSUD_file, tuples of data, pc_shp 
    gas_df: gas uk gov data
    elec_df: uk goc elec data 
    INPUT_GPK: building file verisk 
    overlap: bool, is this for the overlapping postcodes? 
    batch_dir = needed for overlap - where are the batche stored?
    path_to_schp: path to postcode shapefiles location , needed for overlap 
    """
    prem_types = ['Medium height flats 5-6 storeys',
    'Small low terraces',
    '3-4 storey and smaller flats',
    'Tall terraces 3-4 storeys',
    'Large semi detached',
    'Standard size detached',
    'Standard size semi detached',
    '2 storeys terraces with t rear extension',
    'Semi type house in multiples',
    'Tall flats 6-15 storeys',
    'Large detached',
    'Very tall point block flats',
    'Very large detached',
    'Planned balanced mixed estates',
    'Linked and step linked premises',
    'Domestic outbuilding',
    'Unknown'
    ]


    pc = pc.strip() 
    uprn_match= find_data_pc_joint(pc, onsud_data, input_gpk=INPUT_GPK)
    dc_full = {'postcode': pc  }

    for val in prem_types:
        dc_full[val] = np.nan
    dc_full['len_res'] = np.nan 
    dc_full['None_type'] = np.nan

    if  uprn_match is None:
        print('Empty uprn match')
        
    else:
        df  = pre_process_building_data(uprn_match)    
        if len(df)!=len(uprn_match):
            raise Exception('Error in pre process - some cols dropped? ')
        dc = calc_filtered_percentage_of_premise_type(df, prem_types)
        if df is not None:
            if check_duplicate_primary_key(df, 'upn'):
                print('Duplicate primary key found for upn')
                sys.exit()
    

        dc_res = {'len_res' : calc_res_clean_counts(uprn_match) }
        dc_full.update(dc_res)
        dc_full.update(dc)
    
    return dc_full
