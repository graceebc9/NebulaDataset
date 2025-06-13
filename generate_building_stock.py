"""
Copyright (c) 2024 Grace Colverd
This work is licensed under CC BY-NC-SA 4.0
To view a copy of this license, visit https://creativecommons.org/licenses/by-nc-sa/4.0/

For commercial licensing options, contact: gb669@cam.ac.uk
""" 

import os
import logging
from src.split_onsud_file import split_onsud_and_postcodes
from src.postcode_utils import load_ids_from_file
from src.pc_main import postcode_main, run_fuel_process, run_age_process, run_type_process
from src.post_process import apply_filters, unify_dataset
from src.logging_config import get_logger, setup_logging
import argparse 
import sys

# Initialize logging
setup_logging()
logger = get_logger(__name__)


def determine_process_settings():
    """Determine processing settings based on environment"""
    stages = {
        'STAGE0_split_onsud': False,
        'STAGE1_generate_census': False,
        'STAGE1_generate_climate': False,
        'STAGE1_generate_buildings_energy': os.getenv('ENERGY', 'no').lower() == 'yes',
        'STAGE1_generate_building_age': os.getenv('AGE', 'no').lower() == 'yes',
        'STAGE1_generate_building_typology': os.getenv('TYPE', 'no').lower() == 'yes',
        'STAGE3_post_process_data': False,
    }
    return stages

def main():
    print('Creating parser')
    # Create argument parser
    parser = argparse.ArgumentParser(description='Process building stock data')
    parser.add_argument('batch_path', type=str, help='Path to the batch file')
    parser.add_argument('--log', 
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                       default='INFO',
                       help='Set the logging level')
    parser.add_argument('--log-size', type=int, default=1000,
                       help='Size of logging batches')
    parser.add_argument('--onsud-path', type=str,
                       help='Override ONSUD path from environment')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Setup logging with argument-provided level
    
    print(f'Processing batch path: {args.batch_path}')
    batch_path = args.batch_path
    print('Loading stages')
    stages = determine_process_settings()
    print(stages)
    
    
    # Get paths from environment variables
    ONSUD_BASE = os.getenv('ONSUD_BASE')
    PC_SHP_PATH = os.getenv('PC_SHP_PATH')
    BUILDING_PATH = os.getenv('BUILDING_PATH')
    GAS_PATH = os.getenv('GAS_PATH')
    ELEC_PATH = os.getenv('ELEC_PATH')
    
    # Validate input paths
    required_paths = {
        'ONSUD base path': ONSUD_BASE,
        'Postcode shapefile path': PC_SHP_PATH,
        'Building data path': BUILDING_PATH,
        'Gas data path': GAS_PATH,
        'Electricity data path': ELEC_PATH
    }

    for name, path in required_paths.items():
        if not path or not os.path.exists(path):
            
            raise FileNotFoundError(f"{name} not found at: {path}")
        

    # Process batches
    print('Processing batch')
    print(batch_path)
    label = batch_path.split('/')[-2]
    print('label:', label)  
    batch_id = batch_path.split('/')[-1].split('.')[0].split('_')[-1]
    print('batch_id:', batch_id)
    onsud_path = os.path.join(os.path.dirname(batch_path), f'onsud_{batch_id}.csv') 
    # Run fuel calculations
    if stages['STAGE1_generate_buildings_energy']:
        print('starting fuel')  
        postcode_main(
            batch_path=batch_path,
            data_dir='intermediate_data',
            path_to_onsud_file=onsud_path,
            path_to_pcshp=PC_SHP_PATH,
            INPUT_GPK=BUILDING_PATH,
            region_label=label,
            batch_label=batch_id,
            attr_lab='fuel',
            process_function=run_fuel_process,
            gas_path=GAS_PATH,
            elec_path=ELEC_PATH,
            overlap_outcode=None,
            overlap='No',
            log_size=args.log_size
        )

    # Run age calculations
    if stages['STAGE1_generate_building_age']:
        
        postcode_main(
            batch_path=batch_path,
            data_dir='intermediate_data',
            path_to_onsud_file=onsud_path,
            path_to_pcshp=PC_SHP_PATH,
            INPUT_GPK=BUILDING_PATH,
            region_label=label,
            batch_label=batch_id,
            attr_lab='age',
            process_function=run_age_process,
            log_size=args.log_size
        )

    # Run typology calculations
    if stages['STAGE1_generate_building_typology']:
        
        postcode_main(
            batch_path=batch_path,
            data_dir='intermediate_data',
            path_to_onsud_file=onsud_path,
            path_to_pcshp=PC_SHP_PATH,
            INPUT_GPK=BUILDING_PATH,
            region_label=label,
            batch_label=batch_id,
            attr_lab='type',
            process_function=run_type_process,
            log_size=args.log_size
        )



if __name__ == "__main__":
    main()