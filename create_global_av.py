"""
Creates aggregate statistics for average floor counts across building and age categories.
Filters and Processing:
- Includes only residential buildings
- Requires minimum 15 entries per bucket
- Groups by building type and age buckets
- Splits building data into chunks (bounding boxes) and process each due to memory 
- Aggregates across the boxes 
- Calculates mean floor count per group
Returns:
Saves DataFrame with average floor counts aggregated by building and age categories.
Note: Assumes input data is in format of UK Buildings 2022 
"""
from src.global_av import compute_global_fc, filter_bboxes_to_boundary
import geopandas as gpd
import glob
import pandas as pd
from osgeo import ogr
import os
import time
from tqdm import tqdm
from src.buildings import calculate_bounding_boxes


def main():
    input_gpk = '/rds/user/gb669/hpc-work/energy_map/data/building_files/UKBuildings_Edition_15_new_format_upn.gpkg'
    output_path = 'src/global_avs_new/testing'
    os.makedirs(output_path, exist_ok=True)

    print("=" * 60)
    print("  Building Floor Count Aggregation")
    print("=" * 60)

    # Step 1: Open dataset
    print("\n[1/4] Opening dataset...")
    start = time.time()
    ds = ogr.Open(input_gpk)
    if ds is None:
        print(f"  ERROR: Could not open {input_gpk}")
        return
    layer = ds.GetLayer()
    extent = layer.GetExtent()
    print(f"  Done ({time.time() - start:.1f}s)")
    print(f"  Extent: x=[{extent[0]:.1f}, {extent[1]:.1f}] y=[{extent[2]:.1f}, {extent[3]:.1f}]")

    # Step 2: Calculate bounding boxes
    print("\n[2/4] Calculating bounding boxes...")
    start = time.time()
    bounding_boxes = calculate_bounding_boxes(extent, chunk_height=10000, chunk_width=10000)
    total_boxes = len(bounding_boxes)
    print(f"  Generated {total_boxes} bounding boxes ({time.time() - start:.1f}s)")

    # Step 3: Filter to boundary
    print("\n[3/4] Filtering bounding boxes to boundary...")
    start = time.time()
    boundary_path = '/home/gb669/rds/hpc-work/energy_map/NebulaDataset/input_data_sources/england_wales_outer_shapefile.shp'
    bounding_boxes = filter_bboxes_to_boundary(bounding_boxes, boundary_path=boundary_path)
    filtered_boxes = len(bounding_boxes)
    print(f"  {filtered_boxes}/{total_boxes} boxes within boundary ({time.time() - start:.1f}s)")

    # Step 4: Compute global floor counts
    print(f"\n[4/4] Computing global floor counts across {filtered_boxes} chunks...")
    start = time.time()
    bounding_boxes=bounding_boxes[0:10]
    compute_global_fc(bounding_boxes, input_gpk, output_path)
    elapsed = time.time() - start

    print("\n" + "=" * 60)
    print(f"  Complete! Processed {filtered_boxes} chunks in {elapsed / 60:.1f} min")
    print(f"  Output saved to: {output_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()