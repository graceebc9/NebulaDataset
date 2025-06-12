# store the results 

import glob 
import pandas as pd 

from src.postcode_utils import load_onsud_data, load_ids_from_file
import os
import sys 
import pandas as pd
import sys 
import numpy as np

import pandas as pd
import numpy as np
from typing import Dict, Optional, List
sys.path.append('/Users/gracecolverd/NebulaDataset')
# load verisk samples 

from src.pre_process_buildings import pre_process_building_data 
from src.postcode_utils import check_duplicate_primary_key, find_data_pc_joint

import os
import pandas as pd
from tqdm import tqdm

def get_ratio(proc, epc):
   

    dfm = proc[proc['uprn'].notna()]
    df = dfm.merge(epc, right_on='UPRN', left_on='uprn', how='inner')

    df['epc_form']= df['BUILT_FORM'].str.lower().str.replace('-', ' ')

    match_houses= df[df.apply(lambda row: str(row['epc_form']) in str(row['premise_type']), axis=1) & (df['PROPERTY_TYPE']!='Flat')]


    match_houses['gross_epc'] = match_houses['gross_area'] / match_houses['TOTAL_FLOOR_AREA'] * 100 
    match_houses['meta_res'] = match_houses['total_fl_area_meta'] / match_houses['TOTAL_FLOOR_AREA'] * 100
    return match_houses[['gross_epc', 'TOTAL_FLOOR_AREA', 'meta_res', 'uprn','gross_area' , 'total_fl_area_meta', 'total_fl_area_meta_source',  'BUILT_FORM', 'premise_type', 'PROPERTY_TYPE']]



def process_region(region_code, location_input_data_folder, BUILDING_PATH, PC_SHP_PATH):
    print('Starting region', region_code)
    print('loading ONSUD')
    onsud_path_base = os.path.join(location_input_data_folder, 'ONS_UPRN_database/ONSUD_DEC_2022/Data')
    onsud_path = os.path.join(onsud_path_base, f'ONSUD_DEC_2022_{region_code}.csv')    
    onsud_data = load_onsud_data(onsud_path, PC_SHP_PATH)
    pc_list = onsud_data[0].PCDS.unique().tolist()() 
    lcds = onsud_data[0].LAD21CD.unique().tolist() 
    epcl = [] 
    print('loading epc data')
    for ld in lcds:
        f = os.path.join( '/Volumes/T9/2024_Data_downloads/2025_epc_database/*/', f'*{ld}*', 'certificates*' )
        file = glob.glob( f)
        ep = pd.read_csv(file[0])
        epcl.append(ep)
    
    epc = pd.concat(epcl, ignore_index=True)
    print(f"Loaded {len(epc)} EPC records for region {region_code}")
    res = [] 
    error_list=[] 
    with tqdm(total=len(pc_list), desc="Processing PCs") as pbar:
        for pc in pc_list:
            try:
                uprn_match = find_data_pc_joint(pc, onsud_data, input_gpk=BUILDING_PATH)
                proc = pre_process_building_data(uprn_match) 
                match_df = get_ratio(proc, epc) 
                match_df['postcode'] = pc

                res.append(match_df)
            except Exception as e:
                tqdm.write(f"Error processing {pc}: {str(e)}")
                error_list.append((pc, str(e)))
    return pd.concat(res, ignore_index=True)

    
if __name__ == "__main__":
    region_code ='SW'
    op_path = '/Volumes/T9/01_2025_EPC_POSTCODES/gea_comparisons/esults'
    BUILDING_PATH = '/Volumes/T9/2024_Data_downloads/Versik_building_data/2024_03_22_updated_data/UKBuildings_Edition_15_new_format_upn.gpkg'

    PC_SHP_PATH = '/Volumes/T9/2024_Data_downloads/codepoint_polygons_edina/Download_all_postcodes_2378998/codepoint-poly_5267291' 
    location_input_data_folder = '/Volumes/T9/2024_Data_downloads/2024_11_nebula_paper_data/'

    result_df = process_region(region_code, location_input_data_folder, BUILDING_PATH, PC_SHP_PATH)
    output_path = os.path.join(op_path, f'processed_region_{region_code}.csv')