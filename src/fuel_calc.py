import pandas as pd
import sys 
import numpy as np
from .pre_process_buildings import pre_process_building_data 
from .postcode_utils import check_duplicate_primary_key, find_data_pc_joint

from .logging_config import get_logger
logger = get_logger(__name__)


COLS = ['premise_area', 'heated_vol_fc', 'heated_vol_h', 'base_floor', 
        'basement_heated_vol_max', 'listed_bool', 'uprn_count']
PREFIXES = ['all_res_', 'clean_res_']

def calc_df_sum_attribute(df, cols, prefix=''):
    """Takes input df with only one postcode and calcs attributes based on summing the building columns."""
    logger.debug(f"Calculating attributes with prefix: {prefix}")
    
    attr_dict = {}
    attr_dict[prefix + 'total_buildings'] = len(df)
    for col in cols:
        attr_dict[prefix + col + '_total'] = df[col].sum(min_count=1)
    return attr_dict

def gen_nulls():
    """Generate null values for all attributes."""
    logger.debug("Generating null attributes dictionary")
    
    dc = generate_null_attributes_full(PREFIXES, COLS)
    dc.update({'all_types_total_buildings': np.nan})
    dc.update({'all_types_uprn_count_total': np.nan})
    dc.update({'comm_alltypes_count': np.nan})
    dc.update({'unknown_alltypes': np.nan})
    
    return dc

def generate_null_attributes(prefix, cols):
    """Generate a dictionary with all column names prefixed as specified, with np.nan values."""
    logger.debug(f"Generating null attributes for prefix: {prefix}")
    
    null_attributes = {f'{prefix}total_buildings': np.nan}
    for col in cols:
        null_attributes[f'{prefix}{col}_total'] = np.nan
    
    return null_attributes

def generate_null_attributes_full(prefix, cols):
    """Generate a dictionary with all column names for all prefixes, with np.nan values."""
    logger.debug(f"Generating full null attributes for {len(prefix)} prefixes")
    
    null_attributes = {}
    for p in prefix:
        null_attributes[f'{p}total_buildings'] = np.nan
        for col in cols:
            null_attributes[f'{p}{col}_total'] = np.nan
    
    return null_attributes


# Constants
RES_USE_TYPES = [
    'Medium height flats 5-6 storeys', 'Small low terraces',
    '3-4 storey and smaller flats', 'Tall terraces 3-4 storeys',
    'Large semi detached', 'Standard size detached',
    'Standard size semi detached', '2 storeys terraces with t rear extension',
    'Semi type house in multiples', 'Tall flats 6-15 storeys',
    'Large detached', 'Very tall point block flats',
    'Very large detached', 'Planned balanced mixed estates',
    'Linked and step linked premises'
]
EXCL_RES_TYPES = ['Domestic outbuilding', None]

def handle_null_dataframe(prefixes, cols):
    """Handle the case where input DataFrame is None."""
    logger.warning("Input DataFrame is None, returning null attributes")
    dc = generate_null_attributes_full(prefixes, cols)
    dc.update({
        'mixed_alltypes_count': np.nan,
        'comm_alltypes_count': np.nan
    })
    return dc

def create_use_type_masks(df):
    """Create masks for different building use types."""
    return {
        'mixed': df['map_simple_use'] == 'Mixed Use',
        'commercial': df['map_simple_use'] == 'Commercial',
        'residential': df['map_simple_use'] == 'Residential',
        'unknown': df['map_simple_use'] == 'Non Residential'
    }

def process_mixed_use(df, masks, cols):
    """Process mixed use buildings."""
    mixed_df = df[masks['mixed']]
    return (calc_df_sum_attribute(mixed_df, cols, 'mixed_') 
            if not mixed_df.empty 
            else generate_null_attributes('mixed_', cols))

def process_commercial_unknown(df, masks):
    """Process commercial and unknown buildings."""
    return {
        'comm_alltypes_count': len(df[masks['commercial']]),
        'unknown_alltypes': len(df[masks['unknown']])
    }

def process_residential(df, masks, cols):
    """Process residential buildings."""
    res_df = df[masks['residential']]
    if res_df.empty:
        return {
            **generate_null_attributes('all_res_', cols),
            **generate_null_attributes('clean_res_', cols),
            **generate_null_attributes('outb_res_', cols)
        }

    res_type_masks = {
        'clean': res_df['premise_type'].isin(RES_USE_TYPES),
        'outbuilding': res_df['premise_type'] == 'Domestic outbuilding'
    }

    validate_residential_types(res_df)

    return {
        **calc_df_sum_attribute(res_df, cols, 'all_res_'),
        **calc_df_sum_attribute(res_df[res_type_masks['clean']], cols, 'clean_res_'),
        **calc_df_sum_attribute(res_df[res_type_masks['outbuilding']], cols, 'outb_res_')
    }

def validate_residential_types(res_df):
    """Validate that all residential types are expected."""
    unexpected_res_types = res_df[~res_df['premise_type'].isin(EXCL_RES_TYPES + RES_USE_TYPES)]
    if not unexpected_res_types.empty:
        unexpected_types = res_df['premise_type'].unique()
        logger.error(f"Unexpected residential types found: {unexpected_types}")
        raise ValueError("Unexpected residential types found")


def calculate_postcode_attr_with_null_case(df):
    """Calculate postcode attributes, handling null cases appropriately."""
    if df is None:
        return handle_null_dataframe(PREFIXES, COLS)

    logger.debug(f"Processing DataFrame with {len(df)} records")
    
    # Create masks for different use types
    use_type_masks = create_use_type_masks(df)
    
    # Calculate attributes for each category
    results = {
        **calc_df_sum_attribute(df, COLS, 'all_types_'),
        **process_mixed_use(df, use_type_masks, COLS),
        **process_commercial_unknown(df, use_type_masks),
        **process_residential(df, use_type_masks, COLS)
    }
    
    return results

def get_fuel_vars(pc, f, fuel_df):
    """Get fuel variables for a specific postcode."""
    logger.debug(f"Getting {f} fuel variables for postcode {pc}")
    
    dc_fuel = {}
    pc_fuel = fuel_df[fuel_df['Postcode'] == pc].copy()
    
    if len(pc_fuel) == 0:
        logger.debug(f"No {f} fuel data found for postcode {pc}")
        dc_fuel = {
            f'total_{f}': np.nan,
            f'avg_{f}': np.nan,
            f'median_{f}': np.nan,
            f'num_meters_{f}': np.nan
        }
    else:
        dc_fuel[f'num_meters_{f}'] = pc_fuel['Num_meters'].values[0]
        dc_fuel[f'total_{f}'] = pc_fuel['Total_cons_kwh'].values[0]
        dc_fuel[f'avg_{f}'] = pc_fuel['Mean_cons_kwh'].values[0]
        dc_fuel[f'median_{f}'] = pc_fuel['Median_cons_kwh'].values[0]
    
    return dc_fuel

def process_postcode_fuel(pc, onsud_data, gas_df, elec_df, INPUT_GPK, 
                         overlap=False, batch_dir=None, path_to_pcshp=None):
    """Process one postcode, deriving building attributes and electricity and fuel info."""
    pc = pc.strip()
    logger.debug(f'Starting to process postcode fuel for {pc}')
    uprn_match = find_data_pc_joint(pc, onsud_data, input_gpk=INPUT_GPK)
    dc_full = {'postcode': pc}
    
    if uprn_match.empty or uprn_match is None:
        logger.debug(f"No UPRN matches found for postcode {pc}")
        dc = gen_nulls()
        logger.debug(f"Generated {len(dc)} null attributes")
    else:
        df = pre_process_building_data(uprn_match)
        
        if len(df) != len(uprn_match):
            logger.error("Data loss during pre-processing")
            raise Exception("Error in pre-process - some columns dropped")
            
        dc = calculate_postcode_attr_with_null_case(df)
        
        if df is not None and check_duplicate_primary_key(df, 'upn'):
            logger.error("Duplicate primary key found for UPN")
            raise Exception("Duplicate primary key found for UPN")
    
    dc_full.update(dc)
    
    # Get fuel data
    logger.debug(f'Loading fuel vars for pc {pc}')
    dc_gas = get_fuel_vars(pc, 'gas', gas_df)
    dc_elec = get_fuel_vars(pc, 'elec', elec_df)
    dc_full.update(dc_gas)
    dc_full.update(dc_elec)
    
    logger.debug(f"Processed {len(dc_full)} attributes for postcode {pc}")
    return dc_full
    



# def calculate_postcode_attr_with_null_case(df):
#     """Calculate postcode attributes, handling null cases appropriately."""
#     res_use_types = [
#         'Medium height flats 5-6 storeys', 'Small low terraces',
#         '3-4 storey and smaller flats', 'Tall terraces 3-4 storeys',
#         'Large semi detached', 'Standard size detached',
#         'Standard size semi detached', '2 storeys terraces with t rear extension',
#         'Semi type house in multiples', 'Tall flats 6-15 storeys',
#         'Large detached', 'Very tall point block flats',
#         'Very large detached', 'Planned balanced mixed estates',
#         'Linked and step linked premises'
#     ]
#     excl_res_types = ['Domestic outbuilding', None]

#     if df is None:
#         logger.warning("Input DataFrame is None, returning null attributes")
#         dc = generate_null_attributes_full(PREFIXES, COLS)
#         dc.update({'mixed_alltypes_count': np.nan})
#         dc.update({'comm_alltypes_count': np.nan})
#         return dc

#     logger.debug(f"Processing DataFrame with {len(df)} records")
    
#     # Create masks once for each category
#     use_type_masks = {
#         'mixed': df['map_simple_use'] == 'Mixed Use',
#         'commercial': df['map_simple_use'] == 'Commercial',
#         'residential': df['map_simple_use'] == 'Residential',
#         'unknown': df['map_simple_use'] == 'Non Residential'
#     }

#     # Generate attributes for all types
#     dc = calc_df_sum_attribute(df, COLS, 'all_types_')
    
#     # Process mixed use buildings
#     mixed_df = df[use_type_masks['mixed']]
#     dc_mixed = (calc_df_sum_attribute(mixed_df, COLS, 'mixed_') 
#                if not mixed_df.empty 
#                else generate_null_attributes('mixed_', COLS))
    
#     # Process commercial buildings
#     comm_use = df[df['map_simple_use'] == 'Commercial']
#     dc_cm = {'comm_alltypes_count': len(comm_use)}

#     # Process unknown buildings
#     unknowns = df[df['map_simple_use'] == 'Non Residential']
#     dc_unk = {'unknown_alltypes': len(unknowns)}
    
#     # Process residential buildings
#     res_df = df[use_type_masks['residential']]
#     if not res_df.empty:
#         # Create residential type masks
#         res_type_masks = {
#             'clean': res_df['premise_type'].isin(res_use_types),
#             'outbuilding': res_df['premise_type'] == 'Domestic outbuilding'
#         }
#         # Calculate residential attributes
#         dc_res = calc_df_sum_attribute(res_df, COLS, 'all_res_')
#         dc_res_clean = calc_df_sum_attribute(res_df[res_type_masks['clean']], 
#                                            COLS, 'clean_res_')
#         dc_res_ob = calc_df_sum_attribute(res_df[res_type_masks['outbuilding']], 
#                                         COLS, 'outb_res_')
#     else:
#         dc_res = generate_null_attributes('all_res_', COLS)
#         dc_res_clean = generate_null_attributes('clean_res_', COLS)
#         dc_res_ob = generate_null_attributes('outb_res_', COLS)


#     # Validate residential types
#     unexpected_res_types = res_df[~res_df['premise_type'].isin(excl_res_types + res_use_types)]
#     if not unexpected_res_types.empty:
#         unexpected_types = res_df['premise_type'].unique()
#         logger.error(f"Unexpected residential types found: {unexpected_types}")
#         raise ValueError("Unexpected residential types found")
 

#     # Combine all dictionaries
#     dc.update(dc_cm)
#     dc.update(dc_unk)
#     dc.update(dc_res)
#     dc.update(dc_res_clean)
#     dc.update(dc_res_ob)
#     dc.update(dc_mixed)
    
#     return dc