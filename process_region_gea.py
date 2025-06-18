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
import pickle
def get_ratio(proc, epc):
    dfm = proc[proc['uprn'].notna()]
    df = dfm.merge(epc, right_on='UPRN', left_on='uprn', how='inner')

    df['epc_form']= df['BUILT_FORM'].str.lower().str.replace('-', ' ')

    # match_houses= df[df.apply(lambda row: str(row['epc_form']) in str(row['premise_type']), axis=1) & (df['PROPERTY_TYPE']!='Flat')]

    df['gross_epc'] = df['gross_area'] / df['TOTAL_FLOOR_AREA'] * 100 
    df['meta_res'] = df['total_fl_area_meta'] / df['TOTAL_FLOOR_AREA'] * 100
    return df[['gross_epc', 'premise_age_bucketed','TOTAL_FLOOR_AREA', 'meta_res', 'PROPERTY_TYPE', 'uprn','gross_area' , 'total_fl_area_meta', 'total_fl_area_meta_source', 'epc_form', 'BUILT_FORM', 'TENURE', 'CONSTRUCTION_AGE_BAND', 'premise_age','premise_type', 'PROPERTY_TYPE']]

def save_checkpoint(data, checkpoint_path, processed_count, total_count):
    """Save checkpoint with current progress"""
    checkpoint_data = {
        'results': data,
        'processed_count': processed_count,
        'total_count': total_count
    }
    with open(checkpoint_path, 'wb') as f:
        pickle.dump(checkpoint_data, f)
    print(f"Checkpoint saved: {processed_count}/{total_count} postcodes processed")

def load_checkpoint(checkpoint_path):
    """Load checkpoint if it exists"""
    if os.path.exists(checkpoint_path):
        with open(checkpoint_path, 'rb') as f:
            checkpoint_data = pickle.load(f)
        print(f"Resuming from checkpoint: {checkpoint_data['processed_count']}/{checkpoint_data['total_count']} postcodes already processed")
        return checkpoint_data['results'], checkpoint_data['processed_count']
    return [], 0

def process_region(region_code, location_input_data_folder, BUILDING_PATH, PC_SHP_PATH, epc_base_path, checkpoint_interval=50):
    print('Starting region', region_code)
    
    # Set up checkpoint file path
    checkpoint_path = f'checkpoint_{region_code}.pkl'
    
    # Load existing checkpoint if available
    res, start_index = load_checkpoint(checkpoint_path)
    
    onsud_path_base = os.path.join(location_input_data_folder, '')
    onsud_path = os.path.join(onsud_path_base, f'ONSUD_DEC_2022_{region_code}.csv')    
    onsud_data = load_onsud_data(onsud_path, PC_SHP_PATH)
    pc_list = onsud_data[0].PCDS.unique().tolist()
    lcds = onsud_data[0].LAD21CD.unique().tolist() 
    epcl = [] 
    
    for ld in lcds:
        f = os.path.join(epc_base_path, '*/', f'*{ld}*', 'certificates*' )
        file = glob.glob( f)
        ep = pd.read_csv(file[0])
        epcl.append(ep)
    
    epc = pd.concat(epcl, ignore_index=True)
    epc['INSPECTION_DATE'] = pd.to_datetime(epc['INSPECTION_DATE'])

    # Keep only the most recent record for each UPRN
    epc = epc.loc[epc.groupby('UPRN')['INSPECTION_DATE'].idxmax()]
    print(f"Loaded {len(epc)} EPC records for region {region_code}")
    
    error_list=[]
    total_postcodes = len(pc_list)
    
    # Resume from checkpoint or start fresh
    pc_list_to_process = pc_list[start_index:]
    
    with tqdm(total=total_postcodes, initial=start_index, desc="Processing PCs") as pbar:
        for i, pc in enumerate(pc_list_to_process, start=start_index):
            try:
                uprn_match = find_data_pc_joint(pc, onsud_data, input_gpk=BUILDING_PATH)
                proc = pre_process_building_data(uprn_match) 
                match_df = get_ratio(proc, epc) 
                match_df['postcode'] = pc
                res.append(match_df)
                
                # Save checkpoint every checkpoint_interval postcodes
                if (i + 1) % checkpoint_interval == 0:
                    save_checkpoint(res, checkpoint_path, i + 1, total_postcodes)
                
            except Exception as e:
                tqdm.write(f"Error processing {pc}: {str(e)}")
                error_list.append((pc, str(e)))
            
            pbar.update(1)
    
    # Save final checkpoint
    save_checkpoint(res, checkpoint_path, total_postcodes, total_postcodes)
    
    # Clean up checkpoint file after successful completion
    if os.path.exists(checkpoint_path):
        os.remove(checkpoint_path)
        print("Processing completed. Checkpoint file removed.")
    
    return pd.concat(res, ignore_index=True)

    
if __name__ == "__main__":
    
    region_code = os.getenv('REGION')
    

    hpc=True 
    
    if hpc:
        epc_base_path = '/rds/user/gb669/hpc-work/energy_map/data/epc_database'
        op_path = '/rds/user/gb669/hpc-work/energy_map/data/epc_database/gea_comparisons_all'
        os.makedirs(op_path, exist_ok=True)
        location_input_data_folder='/home/gb669/rds/hpc-work/energy_map/data/onsud_files/Data'
        
        BUILDING_PATH='/home/gb669/rds/hpc-work/energy_map/data/building_files/UKBuildings_Edition_15_new_format_upn.gpkg'
        PC_SHP_PATH='/home/gb669/rds/hpc-work/energy_map/data/postcode_polygons/codepoint-poly_5267291'
    else:
        epc_base_path = '/Volumes/T9/2024_Data_downloads/2025_epc_database'
        op_path = '/Volumes/T9/01_2025_EPC_POSTCODES/gea_comparisons/esults'
        location_input_data_folder = '/Volumes/T9/2024_Data_downloads/2024_11_nebula_paper_data/'
    
        BUILDING_PATH = '/Volumes/T9/2024_Data_downloads/Versik_building_data/2024_03_22_updated_data/UKBuildings_Edition_15_new_format_upn.gpkg'

        PC_SHP_PATH = '/Volumes/T9/2024_Data_downloads/codepoint_polygons_edina/Download_all_postcodes_2378998/codepoint-poly_5267291' 
    

    result_df = process_region(region_code, location_input_data_folder, BUILDING_PATH, PC_SHP_PATH, epc_base_path)
    output_path = os.path.join(op_path, f'processed_region_{region_code}.csv')
    result_df.to_csv(output_path, index=False)