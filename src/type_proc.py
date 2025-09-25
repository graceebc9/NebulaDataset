import pandas as pd 
import os 
from src.type_calc import process_postcode_buildtype
from .logging_config import get_logger
logger = get_logger(__name__)

def process_type_batch(pc_batch, data, INPUT_GPK, process_batch_name, log_file):
    logger.debug('Starting batch processing for typology...')
    # Initialize an empty list to collect results
    results = []
    for pc in pc_batch:
        logger.debug('Processing postcode:', pc)
        pc_result = process_postcode_buildtype(pc, data, INPUT_GPK)
        if pc_result is not None:
            results.append(pc_result)
    
    logger.debug(f'Number of processed results: {len(results)}')
    
    # Only proceed if we have results
    if results:
        df = pd.DataFrame(results)
        logger.debug('Saving results to log file...')
        if df.groupby('postcode').size().max() > 1:
            logger.warning('Duplicate postcodes found in the batch')
            raise ValueError('Duplicate postcodes found in the batch')
        
        # Check if the log file already exists
        if not os.path.exists(log_file):
            logger.debug('Creating Log file')
            # If the file does not exist, write with header
            df.to_csv(log_file, index=False)
        else:
            # If the file exists, append without writing the header
            logger.debug('File already exists - append')
            df.to_csv(log_file, mode='a', header=False, index=False)

        logger.info(f'Log file saved for batch: {process_batch_name}')


def run_type_calc(pcs_list, data, INPUT_GPK, batch_size, batch_label, log_file):
    for i in range(0, len(pcs_list) , batch_size):
        batch = pcs_list[i:i+batch_size]
        process_type_batch(batch, data, INPUT_GPK, batch_label, log_file)
