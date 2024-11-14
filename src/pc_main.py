"""
Module: pc_main.py
Description: Framework for creating postcode level attributres in repeatable manner. 
Calls in processing functiosn defeind in seperate files 

Key features
 - functions defined for fuel calc, age calc and typology calc to run at postcode level
 - uses batches, logging and sub batches, batches were to enable multi processing on a HPC 

Author: Grace Colverd
Created: 05/2024
Modified: 2024
"""

import os
import pandas as pd
from src.postcode_utils import load_onsud_data, load_ids_from_file
from src.fuel_proc import run_fuel_calc_main, load_fuel_data
from src.age_perc_proc import run_age_calc
from src.type_proc import run_type_calc
# from src.orientation_proc import run_orient_calc
import logging 


def gen_batch_ids(batch_ids: list, log_file: str, logger: logging.Logger) -> list:
    """Generate batch IDs, excluding already processed ones."""
    if os.path.exists(log_file):
        logger.info('Found existing log file, removing already processed IDs')
        logger.debug(f'Original batch size: {len(batch_ids)}')
        
        log = pd.read_csv(log_file)
        proc_id = log.postcode.unique().tolist()
        batch_ids = [x for x in batch_ids if x not in proc_id]
        
        logger.info(f'Reduced batch size after removing processed IDs: {len(batch_ids)}')
        return batch_ids
    else:
        logger.info('No existing log file found, processing all IDs')
        return batch_ids

def postcode_main(batch_path, data_dir, path_to_onsud_file, path_to_pcshp, INPUT_GPK, 
         region_label, batch_label, attr_lab, process_function, gas_path=None, 
         elec_path=None, overlap=None, batch_dir=None, overlap_outcode=None):
    """Main processing function."""
    
    # Setup logging
    proc_dir = os.path.join(data_dir, 'proc_dir', attr_lab, region_label)
    os.makedirs(proc_dir, exist_ok=True)
    logger = logging.getLogger(__name__)
    
    logger.info(f'Starting processing for region: {region_label}')
    logger.debug(f'Processing batch: {batch_path}')
    
    # Setup log file
    log_file = os.path.join(proc_dir, f'{batch_label}_log_file.csv')
    logger.debug(f'Using log file: {log_file}')
    
    # Load ONSUD data
    logger.debug('Loading ONSUD data')
    onsud_data = load_onsud_data(path_to_onsud_file, path_to_pcshp)
    logger.debug('ONSUD data loaded successfully')

    
    # Load and filter batch IDs
    batch_ids = load_ids_from_file(batch_path)
    batch_ids = gen_batch_ids(batch_ids, log_file, logger)

    
    # Log processing parameters
    logger.debug('Processing parameters:')
    parameters = {
        'Batch size': len(batch_ids),
        'Input GPK': INPUT_GPK,
        'Batch size limit': 100,
        'Batch label': batch_label,
        'Gas path': gas_path,
        'Electricity path': elec_path,
        'Overlap enabled': overlap,
        'Batch directory': batch_dir,
        'Output dir:' : data_dir, 
    }
    for param, value in parameters.items():
        logger.debug(f'{param}: {value}')
    
    process_function(
        batch_ids=batch_ids,
        onsud_data=onsud_data,
        INPUT_GPK=INPUT_GPK,
        subbatch_size=100,
        batch_label=batch_label,
        log_file=log_file,
        gas_path=gas_path,
        elec_path=elec_path,
        overlap=overlap,
        batch_dir=batch_dir,
        path_to_pcshp=path_to_pcshp
    )
    logger.info('Batch processing completed successfully')



def run_fuel_process(batch_ids, onsud_data, INPUT_GPK, subbatch_size, batch_label, 
                    log_file, gas_path, elec_path, overlap, batch_dir, path_to_pcshp):
    """Process fuel data."""
    logger = logging.getLogger('fuel_processor')
    
    gas_df, elec_df = load_fuel_data(gas_path, elec_path)
    
    run_fuel_calc_main(
        batch_ids, onsud_data, INPUT_GPK=INPUT_GPK,
        subbatch_size=subbatch_size, batch_label=batch_label,
        log_file=log_file, gas_df=gas_df, elec_df=elec_df
    )

def run_age_process(batch_ids, onsud_data, INPUT_GPK, batch_size, batch_label,
                   log_file, gas_path=None, elec_path=None, overlap=None,
                   batch_dir=None, path_to_pcshp=None):
    """Process age data."""
    logger = logging.getLogger('age_processor')
    logger.info('Starting age calculation process')
    
    run_age_calc(batch_ids, onsud_data, INPUT_GPK, batch_size,
                 batch_label, log_file, overlap)

def run_type_process(batch_ids, onsud_data, INPUT_GPK, batch_size, batch_label,
                    log_file, gas_path=None, elec_path=None, overlap=None,
                    batch_dir=None, path_to_pcshp=None):
    """Process type data."""
    logger = logging.getLogger('type_processor')
    logger.info('Starting type calculation process')
    
    run_type_calc(batch_ids, onsud_data, INPUT_GPK, batch_size,
                  batch_label, log_file)
