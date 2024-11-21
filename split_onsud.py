from src.postcode_utils import split_onsud_and_postcodes

onsud_path_base = '/home/gb669/rds/hpc-work/energy_map/data/onsud_files/Data'
PC_SHP_PATH ='/rds/user/gb669/hpc-work/energy_map/data/postcode_polygons/codepoint-poly_5267291'
batch_size=10000
# Split ONSUD data if required
region_list = ['EM', 'WM', 'LN', 'SE', 'SW', 'NE', 'NW', 'YH', 'EE', 'WA' ] 
    
for region in region_list:
    print('starting region: ', region)  
    onsud_path = os.path.join(onsud_path_base, f'ONSUD_DEC_2022_{region}.csv')            
    split_onsud_and_postcodes(onsud_path, PC_SHP_PATH, batch_size)
    print(f"Successfully split ONSUD data for region {region}")
    

    