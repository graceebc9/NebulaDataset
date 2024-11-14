"""
Module: main.py
Description: Run the data processing pipeline to generate NEBULA dataset. This works in two stages:
1- Batch the ONSUD files / postcodes. 
    We split the Regional files into batches of 10k postcodes, and find the associated UPRNs associated with them. 
    Batches stored in dataset/batches/
2- Run the fuel calculations for each batch of postcodes. This involves finding all the buildings associated, calculating the per building metrics, and pulling in the gas and electricity data. 
3 - unify the results from the log fiels 
    To protect against timeout we log results in dataset/proc_dir/fuel
    this stage extracts all results, stores a processing log and processes the final dataset

Key features:
 - you can run logging with DEBUG to see more detailed logs 
 - batches were to enable multi processing on a HPC 

Outputs:
v0_postcodes_dataset: whole dataset with no filters, includes mixed use and domestic postcodes 
v1_postcodes_dataset: filtered to wholly residential and applies thresholds / filters (UPRN to gas match and thresholds for gas and elec EUI etc.)
fuel_log_file.csv: details the count of postcodes for each region/batch combo. If runnning for subset of dataset can check here to see counts align with batch size. If counts are missing, re-run stage 2

Author: Grace Colverd
Created: 05/2024
Modified: 2024
"""

#########################################   Data paths to update   ########################################################################### 

onsud_path_base = '/Volumes/T9/2024_Data_downloads/ONS_UPRN_database/ONSUD_DEC_2022/Data'
PC_SHP_PATH = '/Volumes/T9/2024_Data_downloads/codepoint_polygons_edina/Download_all_postcodes_2378998/codepoint-poly_5267291'
BUILDING_PATH = '/Volumes/T9/2024_Data_downloads/Versik_building_data/2024_03_22_updated_data/UKBuildings_Edition_15_new_format_upn.gpkg'
GAS_PATH = '/Volumes/T9/2024_Data_downloads/UKGOV_Gas_elec/Postcode_level_gas_2022.csv'
ELEC_PATH = '/Volumes/T9/2024_Data_downloads/UKGOV_Gas_elec/Postcode_level_all_meters_electricity_2022.csv'
OUTPUT_DIR = 'Dataset'
# OUTPUT_DIR = 'tests'

#########################################   Regions to run, YOU CAN UPDATE   ###################################################################### 

# Run generation locally (True) or on HPC (False)
running_locally = True 

# Regions to run
if running_locally:
    region_list = [ 'NW']
else:
    regions_list = os.getenv('REGION_LIST')

#########################################  Stages to run YOU CAN UPDATE TO RUN SUBSET OF PIPELINE   #################################################

STAGE1_split_onsud = False 
STAGE2_run_fuel_calc= True
STAGE3_post_process_data = True 

#########################################  Set variables, no need to update   ################################################################# 

batch_size = 10000
log_size = 1000
UPRN_TO_GAS_THRESHOLD = 40


#########################################    Script      ###################################################################################### 

from src.split_onsud_file import split_onsud_and_postcodes
from src.postcode_utils import load_ids_from_file
from src.pc_main import postcode_main , run_fuel_process
from src.post_process import call_post_process 
import os
import logging 

from src.logging_config import get_logger, setup_logging

setup_logging()  
logger = get_logger(__name__)


def main():
    log_file = os.path.join(OUTPUT_DIR, 'processing.log')
    logger.info("Starting data processing pipeline")
    logger.debug(f"Using output directory: {OUTPUT_DIR}")

    # Validate input paths
    required_paths = {
        'ONSUD base path': onsud_path_base,
        'Postcode shapefile path': PC_SHP_PATH,
        'Building data path': BUILDING_PATH,
        'Gas data path': GAS_PATH,
        'Electricity data path': ELEC_PATH
    }

    for name, path in required_paths.items():
        if not os.path.exists(path):
            logger.error(f"{name} not found at: {path}")
            raise FileNotFoundError(f"{name} not found at: {path}")
        logger.debug(f"Verified {name} at: {path}")

    # Split ONSUD data if required
    if STAGE1_split_onsud:
        logger.info("Starting ONSUD splitting process")
        
        for region in region_list:
            logger.info(f"Processing region: {region}")
            onsud_path = os.path.join(onsud_path_base, f'ONSUD_DEC_2022_{region}.csv')
            
            try:
                split_onsud_and_postcodes(onsud_path, PC_SHP_PATH, batch_size)
                logger.info(f"Successfully split ONSUD data for region {region}")
            except Exception as e:
                logger.error(f"Error splitting ONSUD data for region {region}: {str(e)}")
                raise
    else:
        logger.info("ONSUD splitting disabled, proceeding to postcode calculations")

    # Run fuel calculations
    overlap_outcode= None 
    overlap = 'No'
    
    if STAGE2_run_fuel_calc:
        batch_paths = list(set(load_ids_from_file('batch_paths.txt')))
        logger.info(f"Found {len(batch_paths)} unique batch paths to process")
        
        for i, batch_path in enumerate(batch_paths, 1):
            logger.info(f"Processing batch {i}/{len(batch_paths)}: {batch_path}")
            
            label = batch_path.split('/')[-2]
            batch_id = batch_path.split('/')[-1].split('.')[0].split('_')[-1]
            onsud_path = os.path.join(os.path.dirname(batch_path), f'onsud_{batch_id}.csv') 
            postcode_main(batch_path = batch_path, data_dir = OUTPUT_DIR, path_to_onsud_file = onsud_path, path_to_pcshp = PC_SHP_PATH, INPUT_GPK=BUILDING_PATH, region_label=label, 
                    batch_label=batch_id, attr_lab='fuel', process_function=run_fuel_process, gas_path=GAS_PATH, elec_path=ELEC_PATH,
                    overlap_outcode=overlap_outcode, overlap=overlap, log_size=log_size)
            logger.info(f"Successfully processed batch: {batch_path}")

            
    # Unify the results from the log files
    if STAGE3_post_process_data:
        data = call_post_process(OUTPUT_DIR)
        res_df = data[data['percent_residential']==1].copy()
        res_df = res_df[res_df['diff_gas_meters_uprns_res']<= UPRN_TO_GAS_THRESHOLD ]
        data.to_csv('All_domestic.csv')
        res_df.to_csv("NEBULA_main_filtered.csv")

    logger.info("Data processing pipeline completed")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"Fatal error in main program: {str(e)}", exc_info=True)
        raise
