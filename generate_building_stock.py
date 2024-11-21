import os
import logging
from src.split_onsud_file import split_onsud_and_postcodes
from src.postcode_utils import load_ids_from_file
from src.pc_main import postcode_main, run_fuel_process, run_age_process, run_type_process
from src.post_process import apply_filters, unify_dataset
from src.logging_config import get_logger, setup_logging

setup_logging()
logger = get_logger(__name__)


import sys

def determine_process_settings():
    """Determine processing settings based on environment"""
        
    # Get stage settings from environment variables with defaults
    stages = {
        'STAGE0_split_onsud': False ,
        'STAGE1_generate_census': False,
        'STAGE1_generate_climate': False,
        'STAGE1_generate_buildings_energy': os.getenv('ENERGY', 'no').lower() == 'yes',
        'STAGE1_generate_building_age': os.getenv('AGE', 'no').lower() == 'yes',
        'STAGE1_generate_building_typology': os.getenv('TYPE', 'no').lower() == 'yes',
        'STAGE3_post_process_data': False,
    }
    
    logger.info(f"Stage settings: {stages}")
    return stages


def main():
    if len(sys.argv) != 2:
        print("Usage: python generate_building_stock.py <batch_path>")
        sys.exit(1)
    
    batch_path = sys.argv[1]
    stages = determine_process_settings()
    # Setup logging
    logger.info("Starting data processing pipeline")
    
    
    onsud_path_base = os.getenv('ONSUD_PATH')
    PC_SHP_PATH = os.getenv('PC_SHP_PATH')
    BUILDING_PATH = os.getenv('BUILDING_PATH')
    GAS_PATH = os.getenv('GAS_PATH')
    ELEC_PATH = os.getenv('ELEC_PATH')

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



    # Process batches
    logger.info(f"Processing batch: {batch_path}")
    label = batch_path.split('/')[-2]
    batch_id = batch_path.split('/')[-1].split('.')[0].split('_')[-1]

    # Run fuel calculations
    if stages['STAGE1_generate_buildings_energy']:
        logger.info("Running fuel calculations")
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
            log_size=log_size
        )

    # Run age calculations
    if stages['STAGE1_generate_building_age']:
        logger.info("Running age calculations")
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
            log_size=log_size
        )

    # Run typology calculations
    if stages['STAGE1_generate_building_typology']:
        logger.info("Running typology calculations")
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
            log_size=log_size
        )

logger.info("Completed processing pipeline")

if __name__ == "__main__":
    main()