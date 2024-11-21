import os 
import pandas as pd
import glob 
import geopandas as gpd
from src.postcode_utils import load_ids_from_file,  check_merge_files, join_pc_map_three_pc
from src.logging_config import get_logger
logger = get_logger(__name__)
######################### Load from downloaded data ######################### 

pc_excl_ovrlap = load_ids_from_file('src/overlapping_pcs.txt')
pc_excl_ovrlap = [x.strip() for x in pc_excl_ovrlap]


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

