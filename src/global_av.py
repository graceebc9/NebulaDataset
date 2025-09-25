
from scipy.stats import mode
from .pre_process_buildings import create_age_buckets, create_height_bucket_cols

import geopandas as gpd
import pandas as pd
from typing import List, Tuple
import logging
from pathlib import Path


from src.logging_config import get_logger
logger = get_logger(__name__)

def validate_input_data(subset: gpd.GeoDataFrame) -> bool:
    """
    Validate the required columns exist in the input data.
    
    Args:
        subset: GeoDataFrame to validate
        
    Returns:
        bool: True if valid, False otherwise
    """
    required_columns = ['premise_floor_count', 'height', 'map_simple_use']
    return all(col in subset.columns for col in required_columns)

def process_single_bbox(
    bbox: Tuple[float, float, float, float],
    input_gpk: Path,
    height_range: Tuple[float, float] = (2.5, 5.5)
) -> pd.DataFrame:
    """
    Process a single bounding box and return statistics.
    
    Args:
        bbox: Tuple of bounding box coordinates (minx, miny, maxx, maxy)
        input_gpk: Path to input geopackage file
        height_range: Tuple of min and max average floor heights to consider
        
    Returns:
        DataFrame containing processed statistics
    """
    logging.info(f'Processing bounding box: {bbox}')
    
    # Read and validate data
    subset = gpd.read_file(input_gpk, bbox=bbox)
    if subset.empty:
        logging.warning(f'Empty subset for bounding box: {bbox}')
        return pd.DataFrame()
    
    if not validate_input_data(subset):
        logging.error('Missing required columns in input data')
        raise ValueError('Input data missing required columns')


    # Apply transformations
    subset = create_age_buckets(subset)  
    subset = create_height_bucket_cols(subset, 'height')  # Note: External function
    
    # Convert floor count to numeric, handling errors
    subset['floor_count_numeric'] = pd.to_numeric(
        subset['premise_floor_count'],
        errors='coerce'
    )
    subset['height_numeric'] = pd.to_numeric(subset['height'], errors='coerce')
    
    # Calculate average floor height
    subset['av_fl_height'] = subset['height_numeric'] / subset['floor_count_numeric']
    
    # Filter by reasonable floor heights
    min_height, max_height = height_range
    subset = subset[subset['av_fl_height'].between(min_height, max_height)]
    
    # Calculate statistics
    stats = (subset.groupby(['map_simple_use', 'premise_age_bucketed', 'height_bucket'])
            ['floor_count_numeric']
            .agg(mean_height='mean', count='size')
            .reset_index())
    
    stats['weighted_height'] = stats['mean_height'] * stats['count']
    return stats
        


def compute_global_fc(
    bbox_list: List[Tuple[float, float, float, float]],
    input_gpk: Path,
    output_path: Path,
    height_range: Tuple[float, float] = (2.5, 5.5)
) -> pd.DataFrame:
    """
    Compute global floor count statistics across multiple bounding boxes.
    
    Args:
        bbox_list: List of bounding box coordinates
        input_gpk: Path to input geopackage file
        output_path: Path to save output CSV
        height_range: Tuple of min and max average floor heights to consider
        
    Returns:
        DataFrame containing global statistics
    """

    
    # Process all bounding boxes
    processed_stats = []
    for bbox in bbox_list:
        try:
            stats = process_single_bbox(bbox, input_gpk, height_range)
            if not stats.empty:
                processed_stats.append(stats)
        except Exception as e:
            logging.error(f'Failed to process bbox {bbox}: {str(e)}')
            continue
    
    if not processed_stats:
        raise ValueError('No valid data processed from any bounding box')
    
    # Combine all statistics
    full_df = pd.concat(processed_stats, ignore_index=True)
    
    # Calculate global statistics
    total_stats = (full_df.groupby(['map_simple_use', 'premise_age_bucketed', 'height_bucket'])
                  .agg(
                      total_count=('count', 'sum'),
                      sum_weighted_height=('weighted_height', 'sum')
                  )
                  .reset_index())
    
    # Calculate final weighted means
    total_stats['global_average_floorcount'] = (
        total_stats['sum_weighted_height'] / total_stats['total_count']
    )
    
    # Clean up and save results
    final_stats = total_stats.drop(columns=['sum_weighted_height'])
    
    # Ensure output directory exists
    output_path = Path(output_path)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Save results
    output_file = output_path / 'global_average_floor_count_bucket.csv'
    final_stats.to_csv(output_file)
    logging.info(f'Results saved to {output_file}')

    filtered = final_stats[(final_stats['map_simple_use']=='Residential')  & ( final_stats['total_count']>15) ]
    output_file = output_path / 'global_average_floor_count_bucket_clean.csv'
    final_stats.to_csv(output_file)
    return final_stats




def compute_global_heights(
    bbox_list: List[Tuple[float, float, float, float]], 
    input_gpk: str, 
    output_path: str,
    height_range: Tuple[float, float] = (2.5, 5.5)
) -> pd.DataFrame:
    """
    Compute global height statistics for geographic regions specified by bounding boxes.
    
    Args:
        bbox_list: List of bounding boxes (each box is tuple of minx, miny, maxx, maxy)
        input_gpk: Path to input geopackage file
        output_path: Directory path for output CSV
        height_range: Tuple of (min, max) acceptable average floor heights
    
    Returns:
        pd.DataFrame: Processed statistics DataFrame
    """
    
    # Use list comprehension for processing valid subsets
    processed_stats = []
    
    for bbox in bbox_list:
        logger.info(f'Processing bounding box: {bbox}')
        

        # Read and process subset
        subset = gpd.read_file(input_gpk, bbox=bbox)
        
        if subset.empty:
            logger.warning(f'Empty subset for bounding box: {bbox}')
            continue
            
        # Process subset
        subset = (subset
                    .pipe(create_age_buckets)
                    .pipe(create_height_bucket_cols, 'height'))
        
        # Convert floor count to numeric and calculate average floor height
        subset['floor_count_numeric'] = pd.to_numeric(
            subset['premise_floor_count'], 
            errors='coerce'
        )
        subset['height_numeric'] = pd.to_numeric( subset['height'], errors='coerce')
            
        # Calculate average floor height
        mask = (subset['floor_count_numeric'] > 0)  # Prevent division by zero
        subset.loc[mask, 'av_fl_height'] = (
            subset.loc[mask, 'height_numeric'] / 
            subset.loc[mask, 'floor_count_numeric']
        )
        
        # Filter by valid height range
        min_height, max_height = height_range
        height_mask = subset['av_fl_height'].between(min_height, max_height)
        subset = subset[height_mask]
        
        # Group and calculate statistics
        stats = (subset
                .groupby(['map_simple_use', 'premise_age_bucketed', 'floor_count_numeric'])
                .agg(
                    mean_height=('height', 'mean'),
                    count=('height', 'size')
                )
                .reset_index())
        
        stats['weighted_height'] = stats['mean_height'] * stats['count']
        processed_stats.append(stats)
            

    # Combine all processed data
    if not processed_stats:
        raise ValueError("No valid data was processed from any bounding box")
        
    full_df = pd.concat(processed_stats, ignore_index=True)
    
    # Calculate final statistics
    total_stats = (full_df
                  .groupby(['map_simple_use', 'premise_age_bucketed', 'floor_count_numeric'])
                  .agg(
                      total_count=('count', 'sum'),
                      sum_weighted_height=('weighted_height', 'sum')
                  )
                  .reset_index())
    
    # Calculate and format final results
    total_stats['global_average_height'] = (
        total_stats['sum_weighted_height'] / total_stats['total_count']
    )
    total_stats = total_stats.drop(columns=['sum_weighted_height'])
    
    # Save results
    output_path = Path(output_path)
    output_file = output_path / 'global_average_heights_bucket.csv'
    total_stats.to_csv(output_file)
    logger.info(f'Results saved to {output_file}')
    
    return total_stats
