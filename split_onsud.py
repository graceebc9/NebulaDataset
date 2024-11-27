from src.split_onsud_file import split_onsud_and_postcodes
import os 
# Update paths as required
onsud_path_base = '/home/gb669/rds/hpc-work/energy_map/data/onsud_files/Data'
PC_SHP_PATH ='/rds/user/gb669/hpc-work/energy_map/data/postcode_polygons/codepoint-poly_5267291'

# Defauly batch size of 10k 
batch_size=10000
# Run for all regions 
region_list = ['EM', 'WM', 'LN', 'SE', 'SW', 'NE', 'NW', 'YH', 'EE', 'WA' ] 
    
for region in region_list:
    print('starting region: ', region)  
    onsud_path = os.path.join(onsud_path_base, f'ONSUD_DEC_2022_{region}.csv')            
    split_onsud_and_postcodes(onsud_path, PC_SHP_PATH, batch_size)
    print(f"Successfully split ONSUD data for region {region}")
    

    