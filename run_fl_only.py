"""
Compute average floor height (mean, std) per premise_type, either
nationally or per region.

Splits the input geopackage into spatial chunks (bounding boxes),
computes per-chunk sum/sum_sq/count of av_fl_height grouped by
premise_type, then aggregates across chunks for exact global stats.

Output: floor_height_by_typology.csv with columns:
    premise_type, mean_floor_height, std_floor_height, count
"""

import os
import time
import logging
from pathlib import Path
from typing import List, Tuple

import numpy as np
import pandas as pd
import geopandas as gpd
from osgeo import ogr
from tqdm import tqdm

from src.pre_process_buildings import create_age_buckets, create_height_bucket_cols
from src.global_av import filter_bboxes_to_boundary
from src.buildings import calculate_bounding_boxes

# Minimum plausible floor height in metres — drop anything below
MIN_FLOOR_HEIGHT = 1.5
# Maximum plausible floor height in metres — drop anything above
MAX_FLOOR_HEIGHT = 10.0


def process_single_bbox(
    bbox: Tuple[float, float, float, float],
    input_gpk: Path,
) -> pd.DataFrame:
    """
    For one bounding box, return per-premise_type aggregates:
        premise_type | fh_sum | fh_sum_sq | fh_count

    These allow exact mean/std reconstruction when combined across chunks.
    """
    subset = gpd.read_file(input_gpk, bbox=bbox)

    if subset.empty:
        return pd.DataFrame()

    subset['floor_count_numeric'] = pd.to_numeric(
        subset['premise_floor_count'], errors='coerce'
    )
    subset['height_numeric'] = pd.to_numeric(subset['height'], errors='coerce')

    # Average floor height = building height / floor count
    valid = (subset['floor_count_numeric'] > 0) & subset['height_numeric'].notna()
    subset.loc[valid, 'av_fl_height'] = (
        subset.loc[valid, 'height_numeric'] / subset.loc[valid, 'floor_count_numeric']
    )

    # Keep only rows with plausible floor heights
    usable = subset.loc[
        valid
        & subset['av_fl_height'].between(MIN_FLOOR_HEIGHT, MAX_FLOOR_HEIGHT)
        & subset['premise_type'].notna()
    ]

    if usable.empty:
        return pd.DataFrame()

    return (
        usable
        .groupby('premise_type')['av_fl_height']
        .agg(
            fh_sum='sum',
            fh_sum_sq=lambda x: (x ** 2).sum(),
            fh_count='count',
        )
        .reset_index()
    )


def aggregate_floor_height_stats(chunk_dfs: List[pd.DataFrame]) -> pd.DataFrame:
    """
    Combine per-chunk stats into global mean and std per premise_type.

        mean = total_sum / total_count
        std  = sqrt(total_sum_sq / total_count - mean²)
    """
    if not chunk_dfs:
        return pd.DataFrame(
            columns=['premise_type', 'mean_floor_height', 'std_floor_height', 'count']
        )

    combined = pd.concat(chunk_dfs, ignore_index=True)

    agg = (
        combined
        .groupby('premise_type')
        .agg(
            total_sum=('fh_sum', 'sum'),
            total_sum_sq=('fh_sum_sq', 'sum'),
            total_count=('fh_count', 'sum'),
        )
        .reset_index()
    )

    agg['mean_floor_height'] = agg['total_sum'] / agg['total_count']
    variance = (agg['total_sum_sq'] / agg['total_count']) - agg['mean_floor_height'] ** 2
    agg['std_floor_height'] = np.sqrt(variance.clip(lower=0))

    return (
        agg[['premise_type', 'mean_floor_height', 'std_floor_height', 'total_count']]
        .rename(columns={'total_count': 'count'})
        .sort_values('premise_type')
        .reset_index(drop=True)
    )


def compute_floor_heights(
    bbox_list: List[Tuple[float, float, float, float]],
    input_gpk: Path,
    output_path: Path,
) -> pd.DataFrame:
    """
    Process all bounding boxes and save floor_height_by_typology.csv.
    """
    chunk_dfs = []
    failed = 0

    for bbox in tqdm(bbox_list, desc="Processing chunks", unit="chunk"):
        try:
            df = process_single_bbox(bbox, input_gpk)
            if not df.empty:
                chunk_dfs.append(df)
        except Exception as e:
            logging.error(f'Failed bbox {bbox}: {e}')
            failed += 1

    total = len(bbox_list)
    print(f"  Chunks with data: {len(chunk_dfs)}/{total}  |  Failed: {failed}")

    if not chunk_dfs:
        raise ValueError('No valid data from any bounding box')

    result = aggregate_floor_height_stats(chunk_dfs)

    output_path = Path(output_path)
    output_path.mkdir(parents=True, exist_ok=True)
    out_file = output_path / 'floor_height_by_typology.csv'
    result.to_csv(out_file, index=False)
    print(f"  Saved {len(result)} premise types → {out_file}")

    return result


# ── Entry points ─────────────────────────────────────────────


def _run_national(bounding_boxes, input_gpk, base_output_path):
    boundary_path = (
        '/home/gb669/rds/hpc-work/energy_map/NebulaDataset/'
        'input_data_sources/england_wales_outer_shapefile.shp'
    )
    bounding_boxes = filter_bboxes_to_boundary(bounding_boxes, boundary_path=boundary_path)
    print(f"  {len(bounding_boxes)} boxes within boundary")

    output_path = os.path.join(base_output_path, 'national')
    compute_floor_heights(bounding_boxes, input_gpk, output_path)


def _run_regional(bounding_boxes, input_gpk, base_output_path, reg_shp, region_name=None):
    from shapely.geometry import box

    regions = gpd.read_file(reg_shp)
    all_region_names = sorted(regions['RGN21NM'].unique())

    if region_name is not None:
        if region_name not in all_region_names:
            raise ValueError(
                f"Region '{region_name}' not found. "
                f"Available regions: {all_region_names}"
            )
        target_regions = [region_name]
        print(f"  Running single region: {region_name}")
    else:
        target_regions = all_region_names
        print(f"  Running all {len(target_regions)} regions")

    bbox_geoms = [box(b[0], b[2], b[1], b[3]) for b in bounding_boxes]
    bbox_gdf = gpd.GeoDataFrame(
        {'bbox_idx': range(len(bounding_boxes))},
        geometry=bbox_geoms,
        crs=regions.crs,
    )

    for region_name in target_regions:
        region_start = time.time()
        safe_name = region_name.replace(' ', '_').replace("'", "")
        output_path = os.path.join(base_output_path, 'regional', safe_name)

        region_geom = regions.loc[regions['RGN21NM'] == region_name, 'geometry'].unary_union
        mask = bbox_gdf.intersects(region_geom)
        regional_indices = bbox_gdf.loc[mask, 'bbox_idx'].tolist()
        regional_boxes = [bounding_boxes[i] for i in regional_indices]

        if not regional_boxes:
            print(f"  {region_name}: no overlapping chunks, skipping")
            continue

        print(f"\n  {region_name}: {len(regional_boxes)} chunks")
        compute_floor_heights(regional_boxes, input_gpk, output_path)
        print(f"  {region_name} done ({time.time() - region_start:.1f}s)")


def main(mode='national', reg_shp=None, chunk_size=25000, region_name=None):
    input_gpk = (
        '/rds/user/gb669/hpc-work/energy_map/data/building_files/'
        'UKBuildings_Edition_15_new_format_upn.gpkg'
    )
    base_output_path = 'src/global_avs_regional'

    print("=" * 60)
    print("  Floor Height by Typology")
    print(f"  Mode: {mode} | Chunk size: {chunk_size}m")
    if region_name:
        print(f"  Region: {region_name}")
    print(f"  Floor height range: [{MIN_FLOOR_HEIGHT}, {MAX_FLOOR_HEIGHT}]m")
    print("=" * 60)

    # Open dataset and get extent
    ds = ogr.Open(input_gpk)
    if ds is None:
        raise RuntimeError(f"Could not open {input_gpk}")
    extent = ds.GetLayer().GetExtent()

    bounding_boxes = calculate_bounding_boxes(
        extent, chunk_height=chunk_size, chunk_width=chunk_size
    )
    print(f"  {len(bounding_boxes)} bounding boxes generated")

    if mode == 'national':
        _run_national(bounding_boxes, input_gpk, base_output_path)
    elif mode == 'regional':
        if reg_shp is None:
            raise ValueError("reg_shp required for regional mode")
        _run_regional(bounding_boxes, input_gpk, base_output_path, reg_shp, region_name)
    else:
        raise ValueError(f"Unknown mode '{mode}'")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Floor height by typology")
    parser.add_argument('--mode', default='regional', choices=['national', 'regional'])
    parser.add_argument('--region', default=None, help="Single region name, e.g. 'London'")
    parser.add_argument('--chunk-size', type=int, default=25000)
    parser.add_argument(
        '--reg-shp',
        default=(
            '/home/gb669/rds/hpc-work/energy_map/NebulaDataset/'
            'input_data_sources/regional_shp/RGN_DEC_2021_EN_BFC.shp'
        ),
    )
    args = parser.parse_args()

    main(
        mode=args.mode,
        reg_shp=args.reg_shp,
        chunk_size=args.chunk_size,
        region_name=args.region,
    )