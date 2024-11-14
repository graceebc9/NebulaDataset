import pandas as pd 
import os 
from src.type_calc import process_postcode_buildtype

def process_type_batch(pc_batch, data, INPUT_GPK, process_batch_name, log_file):
    print('Starting batch processing...')
    
    print('Loading global average data...')

    # Initialize an empty list to collect results
    results = []
    for pc in pc_batch:
        print('Processing postcode:', pc)
        pc_result = process_postcode_buildtype(pc, data, INPUT_GPK)
        if pc_result is not None:
            results.append(pc_result)
    
    print(f'Number of processed results: {len(results)}')
    
    # Only proceed if we have results
    if results:
        df = pd.DataFrame(results)
        print('Saving results to log file...')
        if df.groupby('postcode').size().max() > 1:
            print('Duplicate postcodes found in the batch')
            raise ValueError('Duplicate postcodes found in the batch')
        
        # Check if the log file already exists
        if not os.path.exists(log_file):
            print('Creating Log file')
            # If the file does not exist, write with header
            df.to_csv(log_file, index=False)
        else:
            # If the file exists, append without writing the header
            print('File already exists - append')
            df.to_csv(log_file, mode='a', header=False, index=False)

        print(f'Log file saved for batch: {process_batch_name}')


def run_type_calc(pcs_list, data, INPUT_GPK, batch_size, batch_label, log_file):
    
    for i in range(0, len(pcs_list) , batch_size):
        batch = pcs_list[i:i+batch_size]
        process_type_batch(batch, data, INPUT_GPK, batch_label, log_file)
