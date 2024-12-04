import pandas as pd
import sys 
import numpy as np
from .pre_process_buildings import pre_process_building_data 
from .postcode_utils import check_duplicate_primary_key, find_data_pc_joint

import pandas as pd
import numpy as np
from typing import Dict, Optional, List

# Configuration constants
COLS = ['premise_area', 'total_fl_area_H', 'total_fl_area_FC',
 'base_floor', 'basement_heated_vol', 'listed_bool', 'uprn_count']

COLS_OB = ['premise_area', 'total_fl_area_H', 'total_fl_area_FC', 'uprn_count']
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



def generate_nulls(cols: List[str], prefix: str = '') -> Dict:
    """Generate null values for given columns with optional prefix."""
    return {
        f'{prefix}total_buildings': np.nan,
        **{f'{prefix}{col}_total': np.nan for col in cols}
    }

# def calc_df_sum_attribute(df: pd.DataFrame, cols: List[str], prefix: str = '') -> Dict:
#     """Calculate sum attributes for given DataFrame and columns."""
#     if df.empty:
#         return generate_nulls(cols, prefix)
    
#     return {
#         f'{prefix}total_buildings': len(df),
#         **{f'{prefix}{col}_total': df[col].sum(min_count=1) for col in cols}
#     }


def calc_df_sum_attribute(df: pd.DataFrame, cols: List[str], prefix: str = '') -> Dict:
    """Calculate sum attributes for given DataFrame and columns."""
    if df.empty:
        return generate_nulls(cols, prefix)
    
    result = {
        f'{prefix}total_buildings': len(df),
        **{f'{prefix}{col}_total': df[col].sum(min_count=1) for col in cols}
    }
    
    # Add null counts for specific columns directly in the calculation
    area_cols = ['premise_area', 'total_fl_area_H', 'total_fl_area_FC']
    for col in cols:
        if col in area_cols:
            result[f'{prefix}{col}_null_count'] = df[col].isna().sum()
    
    return result


def process_buildings(df: Optional[pd.DataFrame]) -> Dict:
    """Process building data, handling null cases centrally."""
    if df is None or df.empty:
        return {
            **generate_nulls(COLS, 'clean_res_'),
            **generate_nulls(COLS_OB, 'outb_res_'),
            **generate_nulls(COLS_OB, 'all_types_'),
            'mixed_alltypes_count': np.nan,
            'comm_alltypes_count': np.nan,
            'unknown_alltypes_count': np.nan
        }
    
    masks = {
        'mixed': df['map_simple_use'] == 'Mixed Use',
        'commercial': df['map_simple_use'] == 'Commercial',
        'residential': df['map_simple_use'] == 'Residential',
        'unknown': df['map_simple_use'] == 'Non Residential'
    }
    
    res_df = df[masks['residential']]
    if not res_df.empty:
        res_masks = {
            'clean': res_df['premise_type'].isin(RES_USE_TYPES),
            'outbuilding': res_df['premise_type'] == 'Domestic outbuilding',
            'all_res' : res_df['map_simple_use'] == 'Residential'
        }
        unexpected_types = set(res_df['premise_type']) - set(RES_USE_TYPES + EXCL_RES_TYPES)
        if unexpected_types:
            raise ValueError(f"Unexpected residential types: {unexpected_types}")
    
    return {
        **calc_df_sum_attribute(df, COLS_OB, 'all_types_'),
    
        'mixed_alltypes_count': len(df[masks['mixed']]),
        'comm_alltypes_count': len(df[masks['commercial']]),
        'unknown_alltypes_count': len(df[masks['unknown']]),
        'all_residential_types_count' : len(res_df),

        **calc_df_sum_attribute(
            res_df[res_df['premise_type'].isin(RES_USE_TYPES)] if not res_df.empty else pd.DataFrame(), 
            COLS, 'clean_res_'
        ),
        **calc_df_sum_attribute(
            res_df[res_df['premise_type'] == 'Domestic outbuilding'] if not res_df.empty else pd.DataFrame(), 
            COLS_OB, 'outb_res_'
        )
    }

def process_postcode_fuel(pc: str, onsud_data: pd.DataFrame, 
                         gas_df: pd.DataFrame, elec_df: pd.DataFrame, 
                         input_gpk: str, **kwargs) -> Dict:
    """Process postcode fuel data with centralized null handling."""
    pc = pc.strip()
    uprn_match = find_data_pc_joint(pc, onsud_data, input_gpk=input_gpk)
    
    building_data = (None if uprn_match is None or uprn_match.empty 
                    else pre_process_building_data(uprn_match))
    
    if building_data is not None and len(building_data) != len(uprn_match):
        raise ValueError("Data loss during pre-processing")
    
    def get_fuel_vars(fuel_type: str, fuel_df: pd.DataFrame) -> Dict:
        pc_fuel = fuel_df[fuel_df['Postcode'] == pc]
        if pc_fuel.empty:
            return {
                f'total_{fuel_type}': np.nan,
                f'avg_{fuel_type}': np.nan,
                f'median_{fuel_type}': np.nan,
                f'num_meters_{fuel_type}': np.nan
            }
        row = pc_fuel.iloc[0]
        return {
            f'num_meters_{fuel_type}': row['Num_meters'],
            f'total_{fuel_type}': row['Total_cons_kwh'],
            f'avg_{fuel_type}': row['Mean_cons_kwh'],
            f'median_{fuel_type}': row['Median_cons_kwh']
        }
    
    return {
        'postcode': pc,
        **process_buildings(building_data),
        **get_fuel_vars('gas', gas_df),
        **get_fuel_vars('elec', elec_df)
    }
