import os
import logging
from src.split_onsud_file import split_onsud_and_postcodes
from src.postcode_utils import load_ids_from_file
from src.pc_main import postcode_main, run_fuel_process, run_age_process, run_type_process
from src.post_process import apply_filters, unify_dataset
from src.logging_config import get_logger, setup_logging

setup_logging()
logger = get_logger(__name__)

def determine_process_settings():
    """Determine processing settings based on environment"""
    # Check if running on HPC
    running_locally = os.getenv('SLURM_ARRAY_TASK_ID') is None
    
    if running_locally:
        region_list = ['NW']  # Default for local testing
        process_type = 'all'  # Run all processes locally
    else:
        region_list = [os.getenv('REGION_LIST')]
        process_type = os.getenv('PROCESS_TYPE', 'all').lower()
        
    # Set processing stages based on process type
    stages = {
        'STAGE0_split_onsud': False,  
        'STAGE1_generate_census': False,
        'STAGE1_generate_climate': False,
        'STAGE1_generate_buildings_energy': process_type in ['all', 'fuel'],
        'STAGE1_generate_building_age': process_type in ['all', 'age'],
        'STAGE1_generate_building_typology': process_type in ['all', 'type'],
        'STAGE3_post_process_data': False  # Not needed for individual batches
    }
    
    return running_locally, region_list, stages

def get_batch_info(running_locally):
    """Get batch information based on running mode"""
    try:
        # Load all batch paths
        batch_paths = list(set(load_ids_from_file('batch_paths.txt')))
        
        if running_locally:
            return batch_paths
        else:
            # Get array index from environment
            array_idx = int(os.getenv('SLURM_ARRAY_TASK_ID', 0))
            
            if array_idx >= len(batch_paths):
                raise ValueError(f"Array index {array_idx} exceeds number of available batches ({len(batch_paths)})")
            
            # Select specific batch based on array index
            selected_batch = batch_paths[array_idx]
            logger.info(f"HPC mode: Processing batch {array_idx + 1}/{len(batch_paths)}: {selected_batch}")
            
            # Get corresponding ONSUD path
            batch_dir = os.path.dirname(selected_batch)
            batch_id = selected_batch.split('/')[-1].split('.')[0].split('_')[-1]
            onsud_path = os.path.join(batch_dir, f'onsud_{batch_id}.csv')
            
            if not os.path.exists(onsud_path):
                raise FileNotFoundError(f"ONSUD file not found: {onsud_path}")
                
            return [(selected_batch, onsud_path)]
            
    except FileNotFoundError:
        logger.error("batch_paths.txt not found")
        raise
    except Exception as e:
        logger.error(f"Error processing batch information: {e}")
        raise


def main():
    # Setup logging
    logger.info("Starting data processing pipeline")
    
    # Determine processing settings
    running_locally, region_list, stages = determine_process_settings()
    logger.info(f"Running {'locally' if running_locally else 'on HPC'}")
    logger.info(f"Processing regions: {region_list}")
    
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

    # Get batch information
    try:
        batch_info = get_batch_info(running_locally)
        logger.info(f"Processing {len(batch_info)} {'batches' if running_locally else 'batch'}")
    except Exception as e:
        logger.error(f"Failed to get batch information: {e}")
        raise

    # Process batches
    for batch_path, onsud_path in batch_info:
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