
# import geopandas as gpd
# import glob 
# import pandas as pd
# import os
# import concurrent.futures
# from osgeo import ogr
# import pandas as pd
# import os
# import shutil 
# import tempfile
# import threading

# # Ensure thread_local is defined at the global level
# thread_local = threading.local()

# # def generate_batch_list(full_list, log, col_name ): 
# #     print('Start generate list')
# #     if os.path.exists(log):
# #         log = pd.read_csv(log)
# #         complete = log[col_name].unique().tolist() 
# #         batch_list = [x for x in full_list if str(x) not in complete] 
    
# #     else:
# #         batch_list = full_list
# #     print('num to process ',   len(batch_list) ) 
# #     return batch_list 

# def generate_batch_list(full_list, log_path, col_name):
#     print('Start generate list')
#     if os.path.exists(log_path):
#         # Read only the specific column needed to improve read efficiency
#         log_df = pd.read_csv(log_path, usecols=[col_name])
        
#         # Convert the DataFrame column to a set for O(1) lookup times
#         completed_set = set(log_df[col_name].dropna().astype(str))
        
#         # Use set difference to efficiently filter out completed items
#         # Ensure full_list is converted to a set of strings for accurate comparison
#         full_set = set(map(str, full_list))
#         batch_list = list(full_set - completed_set)
#     else:
#         batch_list = full_list
    
#     print('num to process', len(batch_list))
#     return batch_list



# def run_batching(whole_batch_list, batch_fn, data, result_cols, log_file, batch_size, max_workers, pc_area_bool=False, merge_fuel = None ):
#     if pc_area_bool ==True:
#         whole_batch_list , pc_area_list = zip(*whole_batch_list)
#     with concurrent.futures.ThreadPoolExecutor(max_workers = max_workers ) as executor:
#         # Store futures if you need to wait for them or check for exceptions
#         futures = []
#         for i in range(0, len(whole_batch_list), batch_size):
#             batch = whole_batch_list[i:i+batch_size]
#             if pc_area_bool is True:
#                 pc_area = pc_area_list[i:i+batch_size]
#                 future = executor.submit(process_batch, batch, batch_fn, data,  result_cols, log_file, pc_area, merge_fuel)
#             else:
#                 print('starting execute')
#                 future= executor.submit(process_batch, batch, batch_fn, data,  result_cols, log_file, None , merge_fuel)
#             futures.append(future)
#         # Optionaly wailt for alutures if needel fd
#         concurrent.futures.wait(futures)

# def process_batch(batch_list, batch_fn, data, results_cols, log_file, pc_area, merge_fuel ):
#     results = []
#     if pc_area is not None :
#         print('area')
#         for item , area in zip(batch_list, pc_area):
#             item_results = batch_fn(item, data, area)
#             results.append(item_results)
#     elif merge_fuel is not None:
#         for item in batch_list:
#             item_results = batch_fn(item, data, merge_fuel)
#     else:
#         print('other')
#         for item in batch_list:
#             item_results = batch_fn(item, data)
#             results.append(item_results)
#     print('len results ' , len(results))
#     if len(results) != len(batch_list):
#         raise ValueError('Results length does not match batch length')
#     df = pd.DataFrame(results, columns=results_cols )
#     # Get the temp file path and whether it's the first write operation
#     temp_file_path, is_first_write = get_thread_temp_file(log_file)
#     print('temp file path is ' , temp_file_path )
#     # Open the file with 'a' mode to append and ensure headers are written correctly
#     with open(temp_file_path, 'a') as f:
#         df.to_csv(f, header=is_first_write, index=False)
    
#     # Update the flag to indicate the header should not be written next time
#     if is_first_write:
#         thread_local.temp_file_first_write = False

#     print('temp file saved for batch')



# def get_thread_temp_file(log_file):
#     if not hasattr(thread_local, 'temp_file'):
#         temp_dir = os.path.dirname(log_file)

#         temp_file = tempfile.NamedTemporaryFile(delete=False, mode='w+', suffix='.csv', prefix='temp_log_', dir=temp_dir)        

#         thread_local.temp_file = temp_file.name
#         thread_local.temp_file_first_write = True
#     return thread_local.temp_file, thread_local.temp_file_first_write



# # def merge_temp_logs_to_main(log_file ):
# #     """
# #     Merge all temporary log files created by threads into the main log file.

# #     Parameters:
# #     - log_file: str, the path to the main log file.
# #     """
# #     # Assume temp files are in the same directory as the main log file
# #     temp_files = [f for f in os.listdir(os.path.dirname(log_file)) if f.startswith('temp_log_') and f.endswith('.csv')]
# #     print('Num of temp files found ', len(temp_files) ) 

# #     # Create or append to the main log file
# #     if len(temp_files) != 0:
# #         for temp_file in temp_files:
# #             temp_file_path = os.path.join(os.path.dirname(log_file), temp_file)
# #             try:
# #                 df_temp = pd.read_csv(temp_file_path )
# #             except:
# #                 print('Error reading temp file ', temp_file_path )
# #                 continue
            
# #             if os.path.exists(log_file):
# #                 df_temp.to_csv(log_file, mode='a', header=False, index=False)
# #             else:
# #                 df_temp.to_csv(log_file, mode='w', header=True, index=False)
    
# #     cleanup_temp_files( temp_dir = os.path.dirname(log_file), temp_file_prefix="temp_log_", )

        

# def merge_temp_logs_to_main(log_file):
#     """
#     Merge all temporary log files created by threads into the main log file, ensuring they have the correct number of columns.

#     Parameters:
#     - log_file: str, the path to the main log file.
#     """
#     temp_files = [f for f in os.listdir(os.path.dirname(log_file)) if f.startswith('temp_log_') and f.endswith('.csv')]
#     print('Num of temp files found:', len(temp_files))

#     # Determine the expected number of columns
#     expected_columns = None
#     if os.path.exists(log_file):
#         # If main log exists, read the number of columns from it
#         with open(log_file, 'r') as f:
#             header = f.readline().strip()
#             expected_columns = len(header.split(','))
#     else:
#         # Define expected number of columns if main log doesn't exist
#         # This should be set based on your knowledge of the data
#         expected_columns = 45  # Example, replace with actual expected number

#     for temp_file in temp_files:
#         temp_file_path = os.path.join(os.path.dirname(log_file), temp_file)
#         try:
#             df_temp = pd.read_csv(temp_file_path)
            
#             if os.path.exists(log_file):
#                 df_temp.to_csv(log_file, mode='a', header=False, index=False)
#             else:
#                 df_temp.to_csv(log_file, mode='w', header=True, index=False)
            
            
#         except Exception as e:
#             print(f'Error reading temp file {temp_file_path}: {e}')
        
#     cleanup_temp_files(temp_dir=os.path.dirname(log_file), temp_file_prefix="temp_log_")

    


# def cleanup_temp_files(temp_dir, temp_file_prefix="temp_log_", ):
#     print('Starting cleanup')
#     for filename in os.listdir(temp_dir):
#         if filename.startswith(temp_file_prefix) and filename.endswith('.csv'):
#             os.remove(os.path.join(temp_dir, filename))
#             print(f"Deleted temporary file: {filename}") 
    