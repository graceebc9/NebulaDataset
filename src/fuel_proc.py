import concurrent.futures
import pandas as pd
import tempfile
import os
import logging
from src.fuel_calc import process_postcode_fuel
import threading
import geopandas as gpd

from .logging_config import get_logger
logger = get_logger(__name__)


def load_fuel_data(gas_path, elec_path):
    """Load gas and electricity data from CSV files."""
    try:
        logger.debug(f"Loading gas data from {gas_path}")
        gas_df = pd.read_csv(gas_path)
        
        logger.debug(f"Loading electricity data from {elec_path}")
        elec_df = pd.read_csv(elec_path)
        
        logger.debug("Successfully loaded fuel data")
        return gas_df, elec_df
    except Exception as e:
        logger.error(f"Failed to load fuel data: {str(e)}")
        raise

def process_fuel_batch_main(pc_batch, data, gas_df, elec_df, INPUT_GPK, 
                          process_batch_name, log_file):
    """Process a batch of postcodes for fuel calculation."""
    process_fuel_batch_base(
        process_postcode_fuel, pc_batch, data, gas_df, elec_df,
        INPUT_GPK, process_batch_name, log_file
    )

def run_fuel_calc_main(pcs_list, onsud_data, INPUT_GPK, subbatch_size, 
                      batch_label, log_file, gas_df, elec_df):
    """Main function to run fuel calculations for a list of postcodes."""
    logger.info(f"Starting fuel calculations for {len(pcs_list)} postcodes")
    logger.debug(f"Batch size: {subbatch_size}, Batch label: {batch_label}")
    
    for i in range(0, len(pcs_list), subbatch_size):
        batch = pcs_list[i:i+subbatch_size]
        logger.info(f"Processing sub-batch {i//subbatch_size + 1}, postcodes {i} to {min(i+subbatch_size, len(pcs_list))} for batch {batch_label}")
        process_fuel_batch_main(
            batch, onsud_data, gas_df, elec_df,
            INPUT_GPK, batch_label, log_file
        )

def process_fuel_batch_base(process_fn, pc_batch, data, gas_df, elec_df, 
                          INPUT_GPK, process_batch_name, log_file, 
                          overlap=None, batch_dir=None, path_to_pcshp=None):
    """Base function for processing a batch of postcodes."""
    logger.debug(f'Starting batch base function for batch of pcs: {len(pc_batch)}')
    
    # Initialize results list
    results = []
    for pc in pc_batch:
        try:
            pc_result = process_fn(
                pc, data, gas_df, elec_df, INPUT_GPK,)
            if pc_result is not None:
                results.append(pc_result)
            else:
                logger.warning(f"No results generated for postcode {pc}")
        except Exception as e:
            logger.error(f"Error processing postcode {pc}: {str(e)}")
            raise
    
    
    
    # Process results if we have any
    if results:
        try:
            df = pd.DataFrame(results)
            
            # Check for duplicate postcodes
            duplicates = df.groupby('postcode').size()
            if duplicates.max() > 1:
                duplicate_pcs = duplicates[duplicates > 1].index.tolist()
                logger.error(f"Duplicate postcodes found: {duplicate_pcs}")
                raise ValueError('Duplicate postcodes found in the batch')

            logger.debug('Saving results to log file...')
            
            # Handle file creation or appending
            if not os.path.exists(log_file):
                logger.debug('Creating new log file')
                df.to_csv(log_file, index=False)
            else:
                logger.info('Checking file structure compatibility...')
                # Validate file structure
                with open(log_file, 'r') as file:
                    existing_header = file.readline().strip().split(',')
                    if len(df.columns) != len(existing_header):
                        logger.error(f"Column count mismatch. Expected {len(existing_header)}, got {len(df.columns)}")
                        raise Exception('Results DataFrame has incorrect number of columns')
                    
                    # Reorder columns to match existing file
                    df = df[existing_header]
                    
                    # Verify column names match
                    if existing_header != list(df.columns):
                        mismatched_cols = set(existing_header) ^ set(df.columns)
                        logger.error(f"Column name mismatch. Differing columns: {mismatched_cols}")
                        raise ValueError('Header mismatch between DataFrame and existing CSV file')
                
                
                df.to_csv(log_file, mode='a', header=False, index=False)

            logger.info(f'Successfully saved batch {process_batch_name} to log file')
            
        except Exception as e:
            logger.error(f"Error saving results: {str(e)}")
            raise
    else:
        logger.warning(f"No results to save for batch {process_batch_name}")

