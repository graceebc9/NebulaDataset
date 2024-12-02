import numpy as np
import pandas as pd
from typing import List, Dict
from src.confidence_floor_area import calculate_floor_area_confidence 

def process_residential_counts(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process all residential building counts including direct and derived counts.
    Handles both basic building counts and derived residential calculations.
    """
    df = df.copy()
    
    # Basic building count columns
    building_cols = ['clean_res_total_buildings', 'unknown_res_total_buildings', 'outb_res_total_buildings']
       
 
    # Extended columns including commercial and mixed types
    extended_cols = building_cols + [
        'comm_alltypes_count',
        'mixed_alltypes_count',
        'unknown_alltypes_count'
    ]
    # check all cols are there 
    if not all([x in df.columns for x in extended_cols]):
        raise ValueError(f"Missing columns in DataFrame: {extended_cols}")
    
    # Fill NaNs where at least one value exists
    mask = df[extended_cols + ['all_types_total_buildings']].notna().any(axis=1)
    df.loc[mask, extended_cols] = df.loc[mask, extended_cols].fillna(0)
    
    df['derived_unknown_res'] = df['all_types_total_buildings'].sub(
        df[extended_cols].sum(axis=1)
    ).clip(lower=0)
    res_cols = ['clean_res_total_buildings', 'derived_unknown_res', 'outb_res_total_buildings']
    
    # Calculate total residential buildings from direct counts
    df['total_res_total_buildings'] = df[res_cols].sum(axis=1)

    
    # Calculate final residential percentage
    df['percent_residential'] = df['total_res_total_buildings'].div(
        df['all_types_total_buildings']
    ) * 100
    
    return df

def process_outbuildings_and_unknown(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process outbuilding and unknown residential columns.
    """
    df = df.copy()
    ob_cols = [x for x in df.columns if x.startswith('outb')]
    unknown_cols = [x for x in df.columns if x.startswith('unknown_res')]
    
    # Fill NaN with 0 only for outbuilding and unknown columns
    df[ob_cols + unknown_cols] = df[ob_cols + unknown_cols].fillna(0)
    
    # Extract outcode from postcode
    df['outcode'] = df['postcode'].str.split(' ').str[0]
    return df

def calculate_percentages(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate various percentage-based metrics.
    """
    df = df.copy()
    
    # Calculate residential percentages
    df['perc_clean_res'] = df['clean_res_total_buildings'].div(df['all_types_total_buildings'])
    df['perc_unknown_res'] = (df['derived_unknown_res'].div(df['total_res_total_buildings']) * 100).fillna(0)
    
    # Calculate basement and listed building percentages
    df['perc_cl_res_basement'] = df['clean_res_base_floor_total'].div(df['all_types_total_buildings'])
    df['perc_all_res_listed'] = (df['clean_res_listed_bool_total'].add(
        df['unknown_res_listed_bool_total'])).div(df['all_types_total_buildings'])
    
    return df

def process_uprn_and_meters(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process UPRN counts and meter differences.
    """
    df = df.copy()
    
    # Sum UPRN counts
    df['all_res_uprns'] = df[[
        'clean_res_uprn_count_total',
        'outb_res_uprn_count_total',
        'unknown_res_uprn_count_total'
    ]].sum(axis=1)
    
    # Calculate meter differences
    df['diff_gas_meters_uprns_res'] = (
        np.abs(df['num_meters_gas'] - df['all_res_uprns']).div(df['num_meters_gas']) * 100
    )
    
    return df

def calculate_energy_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate energy usage intensity metrics.
    """
    df = df.copy()
    
    df['gas_EUI_H'] = df['total_gas'].div(df['clean_res_total_fl_area_H_total'])
    df['elec_EUI_H'] = df['total_elec'].div(df['clean_res_total_fl_area_H_total'])
    
    return df

def process_floor_areas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process floor areas for different building types.
    """
    df = df.copy()
    
    floor_area_cols = {
        'H': ['clean_res_total_fl_area_H_total', 'outb_res_total_fl_area_H_total', 'unknown_res_total_fl_area_H_total'],
        'FC': ['clean_res_total_fl_area_FC_total', 'outb_res_total_fl_area_FC_total', 'unknown_res_total_fl_area_FC_total']
    }
    
    for suffix, cols in floor_area_cols.items():
        mask = df[cols].notna().any(axis=1)
        df.loc[mask, cols] = df.loc[mask, cols].fillna(0)
        df[f'all_res_total_fl_area_{suffix}_total'] = df[cols].sum(axis=1)
    
    return df

def round_specified_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Round specified columns to 2 decimal places.
    """
    df = df.copy()
    round_cols = [
        'outb_res_total_fl_area_H_total', 
        'clean_res_total_fl_area_H_total',
        'clean_res_premise_area_total',
        'perc_clean_res',
        'perc_cl_res_basement',
        'perc_all_res_listed',
        'perc_unknown_res'
    ]
    df[round_cols] = df[round_cols].round(2)
    return df

def post_proc_new_fuel(df: pd.DataFrame) -> pd.DataFrame:
    """
    Main function to post-process fuel data with improved NaN handling.
    Orchestrates the sequence of data processing steps.
    """
    # Process steps in logical order
    # print(df.columns.tolist() )
    df = process_outbuildings_and_unknown(df)
    df = process_residential_counts(df)  # Now handles all residential counting in one place
    df = calculate_percentages(df)
    df = process_uprn_and_meters(df)
    df = calculate_energy_metrics(df)
    df = process_floor_areas(df)
    df = round_specified_columns(df)
    
    # Calculate floor area confidence (assuming this function exists)
    df = calculate_floor_area_confidence(
        df, 
        'clean_res_total_fl_area_H_total',
        'clean_res_total_fl_area_FC_total'
    )
    
    return df