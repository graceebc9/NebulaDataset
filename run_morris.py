#!/usr/bin/env python3

import os
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from SALib.sample import morris as morris_sampler
from SALib.analyze import morris

from tqdm import tqdm

# Add the parent directory to the Python path
# sys.path.append('../')
from src.postcode_utils import load_onsud_data, load_ids_from_file

from src.fuel_calc import find_data_pc_joint, pre_process_building_data, process_buildings, check_duplicate_primary_key 
 
# Constants
REGION_ID=os.getenv('REGION_ID')
BATCH_PATH = f'/home/gb669/rds/hpc-work/energy_map/data/batches_10k/{REGION_ID}/batch_2.txt'
BATCH_NAME = f'{REGION_ID}_batch_2'
PC_SHP_PATH = os.getenv('PC_SHP_PATH')
BUILDING_PATH = os.getenv('BUILDING_PATH')

BASE_PATH = '/home/gb669/rds/hpc-work/energy_map/NebulaDataset/morris_anaylsis'
ONSUD_DATA = 'DEC_2022'
PC_COUNT =int(os.getenv('PC_COUNT'))
n_morris=int(os.getenv('N_MORRIS'))
overlap = False 

def debug_wrapper_function(X, pc, onsud_data, INPUT_GPK, target_col):
    """Debug version of wrapper function with detailed logging"""
    MAX_THRESHOLD_FLOOR_HEIGHT, MIN_THRESH_FL_HEIGHT, height_multiplier, premise_area_multiplier = X
    
    print(f"\n--- DEBUG WRAPPER for {pc} ---")
    print(f"Parameters: MAX_THRESH={MAX_THRESHOLD_FLOOR_HEIGHT:.2f}, MIN_THRESH={MIN_THRESH_FL_HEIGHT:.2f}")
    print(f"           height_mult={height_multiplier:.2f}, area_mult={premise_area_multiplier:.2f}")
    
    # Load original UPRN data
    original_uprn = load_uprn_data(pc, onsud_data, INPUT_GPK)
    if original_uprn is None:
        print("❌ original_uprn is None")
        return 0.0
    
    print(f"✅ Loaded {len(original_uprn)} UPRN records")
    
    # Create a copy of the sampled data to modify
    modified_data = original_uprn.copy()
    
    # Check original values
    print(f"Original height range: {modified_data['height'].min():.2f} to {modified_data['height'].max():.2f}")
    print(f"Original area range: {modified_data['premise_area'].min():.2f} to {modified_data['premise_area'].max():.2f}")
    
    # Apply multipliers to height and premise_area
    modified_data['height'] *= height_multiplier
    modified_data['premise_area'] *= premise_area_multiplier
    
    # Check modified values
    print(f"Modified height range: {modified_data['height'].min():.2f} to {modified_data['height'].max():.2f}")
    print(f"Modified area range: {modified_data['premise_area'].min():.2f} to {modified_data['premise_area'].max():.2f}")

    # Process the modified UPRN data
    result = process_uprn_match_df(modified_data, MIN_THRESH_FL_HEIGHT, MAX_THRESHOLD_FLOOR_HEIGHT)
    
    print(f"Result type: {type(result)}")
    
    if result is None:
        print("❌ process_uprn_match_df returned None")
        return 0.0
    
    if isinstance(result, dict):
        print(f"Result keys: {list(result.keys())}")
        if target_col in result:
            value = result[target_col]
            print(f"✅ Found {target_col} = {value}")
            return value
        else:
            print(f"❌ Target column '{target_col}' not found in result")
            print(f"Available keys: {list(result.keys())}")
            return 0.0
    else:
        print(f"❌ Result is not a dictionary: {result}")
        return 0.0


# Test this debug function
def test_debug_wrapper(sampled_postcodes, onsud_data_tuple, BUILDING_PATH, target_col):
    """Test the debug wrapper function"""
    print("\n=== TESTING DEBUG WRAPPER ===")
    
    # Use first postcode for testing
    pc = sampled_postcodes[0]
    X = [5.0, 2.0, 1.0, 1.0]  # Baseline parameters
    
    result = debug_wrapper_function(X, pc, onsud_data_tuple, BUILDING_PATH, target_col)
    print(f"\nFinal result: {result}")
    
    return result
    
    
def check_uprn_data(sampled_postcodes, onsud_data_tuple, INPUT_GPK):
    """Check if UPRN data is being loaded"""
    print("\n=== UPRN DATA CHECK ===")
    
    # Test first 3 postcodes
    for i, pc in enumerate(sampled_postcodes[:3]):
        uprn_data = load_uprn_data(pc, onsud_data_tuple, INPUT_GPK)
        
        if uprn_data is None or len(uprn_data) == 0:
            print(f"❌ {pc}: No data")
        else:
            print(f"✅ {pc}: {len(uprn_data)} records")
            if i == 0:  # Show sample for first valid postcode
                print(f"   Columns: {list(uprn_data.columns)}")
                return uprn_data  # Return sample for further checks
    
    return None



def check_building_columns(sample_data):
    """Check if required columns exist and are numeric"""
    print("\n=== BUILDING COLUMNS CHECK ===")
    
    if sample_data is None:
        print("❌ No sample data")
        return False
    
    required_cols = ['height', 'premise_area']
    
    for col in required_cols:
        if col in sample_data.columns:
            print(f"✅ {col}: exists, type={sample_data[col].dtype}")
            print(f"   Range: {sample_data[col].min():.2f} to {sample_data[col].max():.2f}")
        else:
            print(f"❌ {col}: missing")
            return False
    
    return True


def check_parameter_effect(sample_data):
    """Test if parameter changes actually affect the data"""
    print("\n=== PARAMETER EFFECT CHECK ===")
    
    if sample_data is None:
        print("❌ No sample data")
        return False
    
    # Make a copy and apply multipliers
    modified = sample_data.copy()
    modified['height'] *= 1.1
    modified['premise_area'] *= 0.9
    
    height_changed = not modified['height'].equals(sample_data['height'])
    area_changed = not modified['premise_area'].equals(sample_data['premise_area'])
    
    print(f"Height multiplier effect: {'✅ Changed' if height_changed else '❌ No change'}")
    print(f"Area multiplier effect: {'✅ Changed' if area_changed else '❌ No change'}")
    
    return height_changed and area_changed


def check_postcodes_exist(sampled_postcodes, onsud_data):
    """Check if sampled postcodes exist in building data"""
    print("\n=== POSTCODE EXISTENCE CHECK ===")
    
    available_postcodes = set(onsud_data['POSTCODE'].unique())
    sampled_set = set(sampled_postcodes)
    
    valid_count = len(sampled_set.intersection(available_postcodes))
    total_sampled = len(sampled_set)
    
    print(f"Sampled postcodes: {total_sampled}")
    print(f"Valid postcodes: {valid_count}")
    print(f"Match rate: {valid_count/total_sampled*100:.1f}%")
    
    if valid_count == 0:
        print("❌ No sampled postcodes found in building data!")
        return False
    
    return True


# Add this to your main() function after sampling:
def run_validation_checks(sampled_postcodes, onsud_data, onsud_data_tuple, BUILDING_PATH):
    """Run all validation checks"""
    print("Starting validation checks...")
    
    # Check postcode sampling
    if not check_postcodes_exist(sampled_postcodes, onsud_data):
        return False
    
    # Check UPRN data loading
    sample_uprn = check_uprn_data(sampled_postcodes, onsud_data_tuple, BUILDING_PATH)
    if sample_uprn is None:
        return False
    
    # Check building data quality
    if not check_building_columns(sample_uprn):
        return False
    
    # Check parameter effects
    if not check_parameter_effect(sample_uprn):
        return False
    
    print("✅ All validation checks passed!")
    return True
    
    
def check_data_variance(sampled_postcodes, onsud_data_tuple, INPUT_GPK, max_check=10):
    """Check if building data has sufficient variance within postcodes"""
    print("\n=== DATA VARIANCE CHECK ===")
    
    low_variance_count = 0
    
    for i, pc in enumerate(sampled_postcodes[:max_check]):
        uprn_data = load_uprn_data(pc, onsud_data_tuple, INPUT_GPK)
        
        if uprn_data is None or len(uprn_data) == 0:
            continue
            
        height_var = uprn_data['height'].var()
        area_var = uprn_data['premise_area'].var()
        
        print(f"{pc}: {len(uprn_data)} buildings")
        print(f"  Height variance: {height_var:.4f}")
        print(f"  Area variance: {area_var:.4f}")
        
        if height_var == 0 and area_var == 0:
            low_variance_count += 1
            print(f"  ⚠️  Zero variance in both dimensions")
        elif height_var < 0.01 and area_var < 1000:
            low_variance_count += 1
            print(f"  ⚠️  Very low variance")
        else:
            print(f"  ✅ Good variance")
    
    print(f"\nLow variance postcodes: {low_variance_count}/{min(max_check, len(sampled_postcodes))}")
    
    if low_variance_count > max_check * 0.8:
        print("❌ Most postcodes have low data variance - this explains zero sensitivity!")
        return False
    
    return True


def test_single_postcode_morris(pc, onsud_data_tuple, INPUT_GPK, target_col):
    """Test Morris analysis on a single postcode with detailed output"""
    print(f"\n=== TESTING MORRIS ON {pc} ===")
    
    # Generate small Morris sample for testing
    from SALib.sample import morris as morris_sampler
    param_values = morris_sampler.sample(PROBLEM, N=4, num_levels=4)  # Small sample
    
    print(f"Testing {len(param_values)} parameter combinations...")
    
    results = []
    for i, X in enumerate(param_values):
        result = wrapper_function(X, pc, onsud_data_tuple, INPUT_GPK, target_col)
        results.append(result)
        print(f"  Combination {i+1}: {X} -> {result}")
    
    Y = np.array(results)
    print(f"\nOutput array: {Y}")
    print(f"Variance: {np.var(Y)}")
    print(f"Range: {np.min(Y)} to {np.max(Y)}")
    
    if np.var(Y) == 0:
        print("❌ Zero variance - no sensitivity detected")
        return False
    else:
        print("✅ Non-zero variance detected")
        return True
        
        
    
# Problem definition
PROBLEM = {
    'num_vars': 4,
    'names': ['MAX_THRESHOLD_FLOOR_HEIGHT', 'MIN_THRESH_FL_HEIGHT', 'height_multiplier', 'premise_area_multiplier'],
    'bounds': [[4.8, 5.8], [1.5, 3.5], [0.9, 1.1], [0.9, 1.1]]
}

def setup_paths():
    output_path = os.path.join(BASE_PATH, 'GSA', BATCH_NAME)
    os.makedirs(output_path, exist_ok=True)
    
    label = BATCH_PATH.split('/')[-2]
    batch_id = BATCH_PATH.split('/')[-1].split('.')[0].split('_')[-1]
    onsud_path = os.path.join(os.path.dirname(BATCH_PATH), f'onsud_{batch_id}.csv')
    
    return output_path, onsud_path

def stratified_postcode_sample(onsud_data, n_samples=1001):
    print('starting stratified_postcode_sample')
    postcode_counts = onsud_data.groupby('POSTCODE').size().reset_index(name='UPRN_COUNT')
    postcode_counts['stratum'] = pd.qcut(postcode_counts['UPRN_COUNT'], q=5, labels=['Very Low', 'Low', 'Medium', 'High', 'Very High'])
    
    samples_per_stratum = n_samples // 5
    sampled_postcodes = []
    for stratum in postcode_counts['stratum'].unique():
        stratum_postcodes = postcode_counts[postcode_counts['stratum'] == stratum]['POSTCODE']
        sampled_postcodes.extend(stratum_postcodes.sample(n=samples_per_stratum, replace=True))
    
    if len(sampled_postcodes) < n_samples:
        additional_samples = n_samples - len(sampled_postcodes)
        sampled_postcodes.extend(postcode_counts['POSTCODE'].sample(n=additional_samples, replace=True))
    
    return pd.Series(sampled_postcodes)


def load_uprn_data(pc, onsud_data, INPUT_GPK ):
    """Process one postcode, deriving building attributes and electricity and fuel info.
    
    Inputs: 
    
    pc: postcode 
    onsud_data: output of find_postcode_for_ONSUD_file, tuples of data, pc_shp 
    gas_df: gas uk gov data
    elec_df: uk goc elec data 
    INPUT_GPK: building file verisk 
    overlap: bool, is this for the overlapping postcodes? 
    batch_dir = needed for overlap - where are the batche stored?
    path_to_schp: path to postcode shapefiles location , needed for overlap 
    """
    
    pc = pc.strip() 
    if overlap ==True: 
        print('starting overlap pc')
        onsud_data = custom_load_onsud(pc, batch_dir)
        print('finding pc')
        onsud_data = find_postcode_for_ONSUD_file(onsud_data, path_to_pcshp )
        print('pc found')
    
 
    uprn_match= find_data_pc_joint(pc, onsud_data, input_gpk=INPUT_GPK)
    return uprn_match 


def process_uprn_match_df(uprn_match,  MIN_THRESH_FL_HEIGHT, MAX_THRESHOLD_FLOOR_HEIGHT ):
    
    if uprn_match is None  or len(uprn_match)==0:
        print('Empty uprn match')
        dc = None 
        
    else:
 
        df  = pre_process_building_data(uprn_match, MIN_THRESH_FL_HEIGHT, MAX_THRESHOLD_FLOOR_HEIGHT)    
        
        if len(df)!=len(uprn_match):
            raise Exception('Error in pre process - some cols dropped? ')
        dc = process_buildings(df)
        if df is not None:
            if check_duplicate_primary_key(df, 'upn'):
                print('Duplicate primary key found for upn')
                sys.exit()
    return dc 



def wrapper_function(X, pc, onsud_data, INPUT_GPK, target_col):
    """Wrapper function for the postcode processing model."""
    MAX_THRESHOLD_FLOOR_HEIGHT, MIN_THRESH_FL_HEIGHT, height_multiplier, premise_area_multiplier = X
    # Load original UPRN data
    original_uprn = load_uprn_data(pc, onsud_data, INPUT_GPK)
    if original_uprn is None:
        print('Empty uprn match')
        return 0.0

    # Create a copy of the sampled data to modify
    modified_data = original_uprn.copy()
    
    # Apply multipliers to height and premise_area
    modified_data['height'] *= height_multiplier
    modified_data['premise_area'] *= premise_area_multiplier

    # Process the modified UPRN data
    result = process_uprn_match_df(modified_data, MIN_THRESH_FL_HEIGHT, MAX_THRESHOLD_FLOOR_HEIGHT)
    
    # Calculate and return the output metric
    return result.get(target_col, 0)


def run_morris_analysis(pc, onsud_data, INPUT_GPK, target_col, N):
    param_values = morris_sampler.sample(PROBLEM, N=N, num_levels=4, optimal_trajectories=None)
    Y = np.array([wrapper_function(X, pc, onsud_data, INPUT_GPK, target_col) for X in param_values])
      # Add these lines to fix the error:
    Y = np.array(Y, dtype=np.float64)  # Ensure float type
    # Check if all values are the same (no sensitivity)
    if np.var(Y) == 0:
        print(f"Warning: No variation in output for {pc} (all values = {Y[0]})")
    
    # Debug info (remove after fixing):
    print(f"Y dtype: {Y.dtype}, shape: {Y.shape}, sample: {Y[:5]}")
    return morris.analyze(PROBLEM, param_values, Y, conf_level=0.95, print_to_console=False)

def custom_horizontal_bar_plot(ax, data, sortby, unit):
    y_pos = np.arange(len(data['names']))
    sorted_indices = np.argsort(data[sortby])
    ax.barh(y_pos, [data[sortby][i] for i in sorted_indices], align='center')
    ax.set_yticks(y_pos)
    ax.set_yticklabels([data['names'][i] for i in sorted_indices])
    ax.set_xlabel(f"{sortby} ({unit})")



def plot_and_save_results(df_summary, df_results, output_path, pc_count):
    # Original bar plots
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
    
    custom_horizontal_bar_plot(ax1, 
                               {'names': PROBLEM['names'], 'mu_star': df_summary['mean_mu_star'].values}, 
                               sortby='mu_star', 
                               unit="Mean Heated Volume")
    custom_horizontal_bar_plot(ax2, 
                               {'names': PROBLEM['names'], 'sigma': df_summary['mean_sigma'].values}, 
                               sortby='sigma', 
                               unit="Mean Heated Volume")
    
    ax1.set_title("Overall Parameter Importance")
    ax2.set_title("Overall Parameter Interactions")
    plt.tight_layout()
    
    fig.savefig(os.path.join(output_path, f'morris_sensitivity_plot_pc_{pc_count}.png'), dpi=300, bbox_inches='tight')
    plt.close(fig)

    # New scatter plot
    fig, ax = plt.subplots(figsize=(10, 8))
    
    colors = ['r', 'g', 'b', 'c']
    for i, name in enumerate(PROBLEM['names']):
        mu_star_values = df_results[[col for col in df_results.columns if col.endswith('_mu_star')]].loc[name]
        sigma_values = df_results[[col for col in df_results.columns if col.endswith('_sigma')]].loc[name]
        
        ax.scatter(mu_star_values, sigma_values, c=colors[i], label=name, alpha=0.6)
        
        # Plot mean point
        mean_mu_star = df_summary.loc[name, 'mean_mu_star']
        mean_sigma = df_summary.loc[name, 'mean_sigma']
        ax.scatter(mean_mu_star, mean_sigma, c=colors[i], s=100, marker='*', edgecolors='black')
        
        # Plot error bars
        std_mu_star = df_summary.loc[name, 'std_mu_star']
        std_sigma = df_summary.loc[name, 'std_sigma']
        ax.errorbar(mean_mu_star, mean_sigma, xerr=std_mu_star, yerr=std_sigma, c=colors[i], capsize=5)

    ax.set_xlabel(r'$\mu^*$')
    ax.set_ylabel(r'$\sigma$')
    ax.set_title(r'Morris Sensitivity Analysis: $\mu^*$ vs $\sigma$')
    ax.legend()
    ax.grid(True, linestyle='--', alpha=0.7)

    plt.tight_layout()
    fig.savefig(os.path.join(output_path, f'morris_sensitivity_scatter_pc_{pc_count}__nmorris_{n_morris}.png'), dpi=300, bbox_inches='tight')
    plt.close(fig)


def main():
    target_col = 'clean_res_scaled_fl_area_meta_res_total'
    output_path, onsud_path = setup_paths()
    
    print('Non overlap starting')
    onsud_data_tuple = load_onsud_data(onsud_path, PC_SHP_PATH)
    batch_ids = load_ids_from_file(BATCH_PATH)
    
    onsud_data = onsud_data_tuple[0]
    sampled_postcodes = stratified_postcode_sample(onsud_data, n_samples=1000)
    
    if not run_validation_checks(sampled_postcodes, onsud_data, onsud_data_tuple, BUILDING_PATH):
        print("❌ Validation failed - stopping execution") 
        return
    # Add after your existing validation checks:
    if not check_data_variance(sampled_postcodes, onsud_data_tuple, BUILDING_PATH):
        print("❌ Data variance issue detected")
        
    # Test Morris on a single postcode
    test_single_postcode_morris(sampled_postcodes[0], onsud_data_tuple, BUILDING_PATH, target_col)
    
    sampled_data = onsud_data[onsud_data['POSTCODE'].isin(sampled_postcodes)]
    
    print(f"Number of sampled postcodes: {len(sampled_postcodes)}")
    print(f"Number of UPRNs in sampled data: {len(sampled_data)}")
    
    sampled_postcode_counts = sampled_data.groupby('POSTCODE').size().reset_index(name='UPRN_COUNT')
    print(sampled_postcode_counts['UPRN_COUNT'].describe())
    
    test_debug_wrapper(sampled_postcodes, onsud_data_tuple, BUILDING_PATH, target_col)
    results = {}

    selected_postcodes=sampled_postcodes[0:PC_COUNT]
    for pc in tqdm(selected_postcodes, desc="Processing postcodes", unit="postcode") :
        results[pc] = run_morris_analysis(pc, onsud_data_tuple, BUILDING_PATH, target_col , N= n_morris)
    
    df_results = pd.DataFrame(index=PROBLEM['names'])
    for pc, result in results.items():
        df_results[f'{pc}_mu'] = result['mu']
        df_results[f'{pc}_mu_star'] = result['mu_star']
        df_results[f'{pc}_sigma'] = result['sigma']
    
    df_summary = pd.DataFrame({
        'mean_mu': df_results[[col for col in df_results.columns if col.endswith('_mu')]].mean(axis=1),
        'mean_mu_star': df_results[[col for col in df_results.columns if col.endswith('_mu_star')]].mean(axis=1),
        'mean_sigma': df_results[[col for col in df_results.columns if col.endswith('_sigma')]].mean(axis=1),
        'std_mu': df_results[[col for col in df_results.columns if col.endswith('_mu')]].std(axis=1),
        'std_mu_star': df_results[[col for col in df_results.columns if col.endswith('_mu_star')]].std(axis=1),
        'std_sigma': df_results[[col for col in df_results.columns if col.endswith('_sigma')]].std(axis=1),
    })
    
    print("Summary Statistics:")
    print(df_summary)
    
    print('Saving results') 
    plot_and_save_results(df_summary, df_results, output_path, PC_COUNT)
    
    df_results.to_csv(os.path.join(output_path, f'morris_sensitivity_results_pc_{PC_COUNT}__nmorris_{n_morris}.csv'))
    df_summary.to_csv(os.path.join(output_path, f'morris_sensitivity_summary_pc_{PC_COUNT}__nmorris_{n_morris}.csv'))
    
    print(f"Results and plot saved in {output_path}")

if __name__ == "__main__":
    main()