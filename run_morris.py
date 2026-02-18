#!/usr/bin/env python3
"""
Morris sensitivity analysis for building energy model parameters.

Runs Morris method on three output metrics (scaled_area_mode, scaled_area_min,
scaled_area_max) to assess how parameter uncertainty propagates through the
uncertainty envelope produced by the building pipeline.

Usage:
    python run_morris.py               # Production run
    python run_morris.py --debug       # Run with validation/debug checks
    python run_morris.py --debug-only  # Run validation checks only (no analysis)

Required environment variables:
    REGION_ID, PC_SHP_PATH, BUILDING_PATH, PC_COUNT, N_MORRIS
"""

import argparse
import os
import sys

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from SALib.sample import morris as morris_sampler
from SALib.analyze import morris
from tqdm import tqdm

from src.postcode_utils import load_onsud_data, load_ids_from_file
from src.fuel_calc import (
    find_data_pc_joint,
    pre_process_building_data,
    process_buildings,
    check_duplicate_primary_key,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Output columns from process_buildings / post_proc_new_fuel
TARGET_COLS = {
    'mode': 'clean_res_scaled_area_mode_total',
    'min':  'clean_res_scaled_area_min_total',
    'max':  'clean_res_scaled_area_max_total',
}

TARGET_LABELS = {
    'mode': 'Scaled Area — Mode (most likely)',
    'min':  'Scaled Area — Min (conservative)',
    'max':  'Scaled Area — Max (upper bound)',
}

TARGET_COLORS = {
    'mode': '#2196F3',
    'min':  '#4CAF50',
    'max':  '#FF9800',
}


def load_config():
    """Load and validate configuration from environment variables."""
    required_vars = ['REGION_ID', 'PC_SHP_PATH', 'BUILDING_PATH', 'PC_COUNT', 'N_MORRIS']
    missing = [v for v in required_vars if os.getenv(v) is None]
    if missing:
        print(f"Error: Missing required environment variables: {', '.join(missing)}")
        sys.exit(1)

    region_id = os.getenv('REGION_ID')
    return {
        'REGION_ID': region_id,
        'BATCH_PATH': f'/home/gb669/rds/hpc-work/energy_map/data/batches_10k/{region_id}/batch_2.txt',
        'BATCH_NAME': f'{region_id}_batch_2',
        'PC_SHP_PATH': os.getenv('PC_SHP_PATH'),
        'BUILDING_PATH': os.getenv('BUILDING_PATH'),
        'BASE_PATH': '/home/gb669/rds/hpc-work/energy_map/NebulaDataset/morris_analysis',
        'PC_COUNT': int(os.getenv('PC_COUNT')),
        'N_MORRIS': int(os.getenv('N_MORRIS')),
    }


# Morris problem definition
PROBLEM = {
    'num_vars': 4,
    'names': [
        'MAX_THRESHOLD_FLOOR_HEIGHT',
        'MIN_THRESH_FL_HEIGHT',
        'height_multiplier',
        'premise_area_multiplier',
    ],
    'bounds': [[4, 7], [1.5, 3.5], [0.9, 1.1], [0.9, 1.1]],
}


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def setup_paths(config):
    output_path = os.path.join(config['BASE_PATH'], 'GSA', config['BATCH_NAME'])
    os.makedirs(output_path, exist_ok=True)

    batch_id = config['BATCH_PATH'].split('/')[-1].split('.')[0].split('_')[-1]
    onsud_path = os.path.join(os.path.dirname(config['BATCH_PATH']), f'onsud_{batch_id}.csv')

    return output_path, onsud_path


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def stratified_postcode_sample(onsud_data, n_samples=1000):
    """Return a stratified sample of postcodes based on UPRN count quintiles."""
    print('Starting stratified postcode sample')
    postcode_counts = onsud_data.groupby('POSTCODE').size().reset_index(name='UPRN_COUNT')
    postcode_counts['stratum'] = pd.qcut(
        postcode_counts['UPRN_COUNT'], q=5,
        labels=['Very Low', 'Low', 'Medium', 'High', 'Very High'],
    )

    samples_per_stratum = n_samples // 5
    sampled_postcodes = []
    for stratum in postcode_counts['stratum'].unique():
        stratum_pcs = postcode_counts.loc[
            postcode_counts['stratum'] == stratum, 'POSTCODE'
        ]
        sampled_postcodes.extend(stratum_pcs.sample(n=samples_per_stratum, replace=True))

    shortfall = n_samples - len(sampled_postcodes)
    if shortfall > 0:
        sampled_postcodes.extend(
            postcode_counts['POSTCODE'].sample(n=shortfall, replace=True)
        )

    return pd.Series(sampled_postcodes)


def load_uprn_data(pc, onsud_data, input_gpk):
    """Load UPRN-matched building data for a single postcode."""
    pc = pc.strip()
    return find_data_pc_joint(pc, onsud_data, input_gpk=input_gpk)


# ---------------------------------------------------------------------------
# Model wrapper
# ---------------------------------------------------------------------------

def process_uprn_match_df(uprn_match, min_thresh, max_thresh):
    """Pre-process and compute building-level results from UPRN data."""
    if uprn_match is None or len(uprn_match) == 0:
        return None

    df = pre_process_building_data(uprn_match, min_thresh, max_thresh)

    if len(df) != len(uprn_match):
        raise ValueError(
            f'Pre-processing dropped rows: {len(uprn_match)} -> {len(df)}'
        )

    if check_duplicate_primary_key(df, 'upn'):
        raise ValueError('Duplicate primary key found for upn')

    return process_buildings(df)


def evaluate_sample(X, uprn_data):
    """Evaluate the model for a single Morris parameter sample.

    The model is run once; all three target outputs are extracted from the
    same result dictionary.

    Returns
    -------
    dict  {target_key: float}
    """
    max_thresh, min_thresh, height_mult, area_mult = X

    modified = uprn_data.copy()
    modified['height'] *= height_mult
    modified['premise_area'] *= area_mult

    result = process_uprn_match_df(modified, min_thresh, max_thresh)
    print('result: ', result) 
    if result is None:
        return {key: 0.0 for key in TARGET_COLS}

    return {key: result.get(col, 0.0) for key, col in TARGET_COLS.items()}


# ---------------------------------------------------------------------------
# Morris analysis
# ---------------------------------------------------------------------------

def run_morris_analysis_multi(uprn_data, n_morris):
    """Run Morris SA for a single postcode across all three target outputs.

    The model is evaluated once per sample point; the three Y vectors are
    then analysed independently.

    Returns
    -------
    dict {target_key: SALib result} — keys present only where variance > 0.
    Returns None if all outputs have zero variance.
    """
    param_values = morris_sampler.sample(
        PROBLEM, N=n_morris, num_levels=4, optimal_trajectories=None,
    )

    # Single evaluation loop — model called once per sample
    raw = [evaluate_sample(X, uprn_data) for X in param_values]

    sa_results = {}
    for key in TARGET_COLS:
        print(key)
        Y = np.array([r[key] for r in raw], dtype=np.float64)

        if np.var(Y) == 0:
            print(f"  Warning: zero variance for {key} (all = {Y[0]})")
            continue

        sa_results[key] = morris.analyze(
            PROBLEM, param_values, Y, conf_level=0.95, print_to_console=False,
        )

    return sa_results if sa_results else None


# ---------------------------------------------------------------------------
# Results aggregation
# ---------------------------------------------------------------------------

def aggregate_results(all_results):
    """Aggregate per-postcode Morris results into DataFrames per target.

    Returns
    -------
    results_by_target : dict {target_key: DataFrame}
    summaries_by_target : dict {target_key: DataFrame}
    """
    results_by_target = {}
    summaries_by_target = {}

    for key in TARGET_COLS:
        df = pd.DataFrame(index=PROBLEM['names'])

        for pc, sa_dict in all_results.items():
            if key not in sa_dict:
                continue
            res = sa_dict[key]
            df[f'{pc}_mu'] = res['mu']
            df[f'{pc}_mu_star'] = res['mu_star']
            df[f'{pc}_sigma'] = res['sigma']

        if df.empty:
            print(f"  No valid results for target '{key}'")
            continue

        results_by_target[key] = df
        summaries_by_target[key] = _build_summary(df)

    return results_by_target, summaries_by_target


def _build_summary(df_results):
    """Compute mean/std summary across postcodes."""
    def _agg(suffix, func):
        cols = [c for c in df_results.columns if c.endswith(suffix)]
        if not cols:
            return pd.Series(0.0, index=df_results.index)
        return getattr(df_results[cols], func)(axis=1)

    return pd.DataFrame({
        'mean_mu': _agg('_mu', 'mean'),
        'mean_mu_star': _agg('_mu_star', 'mean'),
        'mean_sigma': _agg('_sigma', 'mean'),
        'std_mu': _agg('_mu', 'std'),
        'std_mu_star': _agg('_mu_star', 'std'),
        'std_sigma': _agg('_sigma', 'std'),
    })


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------

def _horizontal_bar(ax, names, values, xlabel):
    sorted_idx = np.argsort(values)
    y_pos = np.arange(len(names))
    ax.barh(y_pos, values[sorted_idx], align='center')
    ax.set_yticks(y_pos)
    ax.set_yticklabels([names[i] for i in sorted_idx])
    ax.set_xlabel(xlabel)


def plot_per_target(summary, df_results, target_key, output_path, config):
    """Bar + scatter plots for a single target output."""
    label = TARGET_LABELS[target_key]
    pc_count = config['PC_COUNT']
    n_morris = config['N_MORRIS']

    # Bar plots
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5))
    _horizontal_bar(ax1, PROBLEM['names'], summary['mean_mu_star'].values,
                    rf'$\mu^*$ ({label})')
    _horizontal_bar(ax2, PROBLEM['names'], summary['mean_sigma'].values,
                    rf'$\sigma$ ({label})')
    ax1.set_title('Parameter Importance')
    ax2.set_title('Parameter Interactions')
    plt.suptitle(label, fontsize=13)
    plt.tight_layout()
    fig.savefig(
        os.path.join(output_path, f'morris_bar_{target_key}_pc{pc_count}_n{n_morris}.png'),
        dpi=300, bbox_inches='tight',
    )
    plt.close(fig)

    # Scatter: mu* vs sigma per parameter
    fig, ax = plt.subplots(figsize=(10, 8))
    colors = ['#e41a1c', '#377eb8', '#4daf4a', '#984ea3']
    mu_star_cols = [c for c in df_results.columns if c.endswith('_mu_star')]
    sigma_cols = [c for c in df_results.columns if c.endswith('_sigma')]

    for i, name in enumerate(PROBLEM['names']):
        ax.scatter(df_results.loc[name, mu_star_cols],
                   df_results.loc[name, sigma_cols],
                   c=colors[i], label=name, alpha=0.5, s=30)
        m_mu = summary.loc[name, 'mean_mu_star']
        m_sig = summary.loc[name, 'mean_sigma']
        ax.scatter(m_mu, m_sig, c=colors[i], s=120, marker='*',
                   edgecolors='k', zorder=5)
        ax.errorbar(m_mu, m_sig,
                    xerr=summary.loc[name, 'std_mu_star'],
                    yerr=summary.loc[name, 'std_sigma'],
                    c=colors[i], capsize=4, linewidth=1)

    ax.set_xlabel(r'$\mu^*$')
    ax.set_ylabel(r'$\sigma$')
    ax.set_title(rf'Morris SA: $\mu^*$ vs $\sigma$ — {label}')
    ax.legend(fontsize=9)
    ax.grid(True, linestyle='--', alpha=0.5)
    plt.tight_layout()
    fig.savefig(
        os.path.join(output_path, f'morris_scatter_{target_key}_pc{pc_count}_n{n_morris}.png'),
        dpi=300, bbox_inches='tight',
    )
    plt.close(fig)


def plot_comparative(summaries, output_path, config):
    """Comparative plots across the three area bounds."""
    pc_count = config['PC_COUNT']
    n_morris = config['N_MORRIS']
    keys = [k for k in ['min', 'mode', 'max'] if k in summaries]

    if len(keys) < 2:
        print("  Skipping comparative plot — fewer than 2 targets have results")
        return

    n_params = len(PROBLEM['names'])
    x = np.arange(n_params)
    width = 0.8 / len(keys)

    # Grouped bar: mu*
    fig, ax = plt.subplots(figsize=(12, 6))
    for j, key in enumerate(keys):
        vals = summaries[key].loc[PROBLEM['names'], 'mean_mu_star'].values
        errs = summaries[key].loc[PROBLEM['names'], 'std_mu_star'].values
        ax.bar(x + j * width, vals, width, yerr=errs,
               label=TARGET_LABELS[key], color=TARGET_COLORS[key],
               capsize=3, alpha=0.85)
    ax.set_xticks(x + width * (len(keys) - 1) / 2)
    ax.set_xticklabels(PROBLEM['names'], rotation=25, ha='right')
    ax.set_ylabel(r'$\mu^*$')
    ax.set_title(r'Parameter Importance ($\mu^*$) Across Area Bounds')
    ax.legend()
    ax.grid(axis='y', linestyle='--', alpha=0.4)
    plt.tight_layout()
    fig.savefig(
        os.path.join(output_path, f'morris_compare_mu_star_pc{pc_count}_n{n_morris}.png'),
        dpi=300, bbox_inches='tight',
    )
    plt.close(fig)

    # Grouped bar: sigma
    fig, ax = plt.subplots(figsize=(12, 6))
    for j, key in enumerate(keys):
        vals = summaries[key].loc[PROBLEM['names'], 'mean_sigma'].values
        errs = summaries[key].loc[PROBLEM['names'], 'std_sigma'].values
        ax.bar(x + j * width, vals, width, yerr=errs,
               label=TARGET_LABELS[key], color=TARGET_COLORS[key],
               capsize=3, alpha=0.85)
    ax.set_xticks(x + width * (len(keys) - 1) / 2)
    ax.set_xticklabels(PROBLEM['names'], rotation=25, ha='right')
    ax.set_ylabel(r'$\sigma$')
    ax.set_title(r'Parameter Interactions ($\sigma$) Across Area Bounds')
    ax.legend()
    ax.grid(axis='y', linestyle='--', alpha=0.4)
    plt.tight_layout()
    fig.savefig(
        os.path.join(output_path, f'morris_compare_sigma_pc{pc_count}_n{n_morris}.png'),
        dpi=300, bbox_inches='tight',
    )
    plt.close(fig)

    # Overlay scatter: all targets on one mu* vs sigma plot
    fig, ax = plt.subplots(figsize=(10, 8))
    markers = {'min': 'v', 'mode': 'o', 'max': '^'}

    for key in keys:
        s = summaries[key]
        for i, name in enumerate(PROBLEM['names']):
            ax.scatter(
                s.loc[name, 'mean_mu_star'], s.loc[name, 'mean_sigma'],
                c=TARGET_COLORS[key], marker=markers[key], s=100,
                edgecolors='k', linewidths=0.5, zorder=5,
            )
            ax.errorbar(
                s.loc[name, 'mean_mu_star'], s.loc[name, 'mean_sigma'],
                xerr=s.loc[name, 'std_mu_star'], yerr=s.loc[name, 'std_sigma'],
                c=TARGET_COLORS[key], capsize=3, linewidth=0.8, alpha=0.6,
            )
            if key == 'mode':
                ax.annotate(
                    name, (s.loc[name, 'mean_mu_star'], s.loc[name, 'mean_sigma']),
                    textcoords='offset points', xytext=(8, 4), fontsize=8,
                )

    legend_elements = [
        Line2D([0], [0], marker=markers[k], color='w',
               markerfacecolor=TARGET_COLORS[k], markersize=10,
               markeredgecolor='k', label=TARGET_LABELS[k])
        for k in keys
    ]
    ax.legend(handles=legend_elements, fontsize=9)
    ax.set_xlabel(r'$\mu^*$')
    ax.set_ylabel(r'$\sigma$')
    ax.set_title(r'Morris SA Comparison: $\mu^*$ vs $\sigma$ Across Area Bounds')
    ax.grid(True, linestyle='--', alpha=0.4)
    plt.tight_layout()
    fig.savefig(
        os.path.join(output_path, f'morris_compare_scatter_pc{pc_count}_n{n_morris}.png'),
        dpi=300, bbox_inches='tight',
    )
    plt.close(fig)


# ---------------------------------------------------------------------------
# Debug / validation (only with --debug or --debug-only)
# ---------------------------------------------------------------------------

def _check_postcodes_exist(sampled_postcodes, onsud_data):
    available = set(onsud_data['POSTCODE'].unique())
    sampled = set(sampled_postcodes)
    valid = len(sampled & available)
    total = len(sampled)
    print(f"  Sampled: {total}, Valid: {valid} ({valid / total * 100:.1f}%)")
    if valid == 0:
        print("  ✗ No sampled postcodes found in building data")
        return False
    return True


def _check_uprn_loading(sampled_postcodes, onsud_data_tuple, input_gpk):
    for pc in sampled_postcodes[:3]:
        data = load_uprn_data(pc, onsud_data_tuple, input_gpk)
        if data is not None and len(data) > 0:
            print(f"  ✓ {pc}: {len(data)} records, columns={list(data.columns)}")
            return data
        print(f"  ✗ {pc}: no data")
    return None


def _check_columns(sample):
    if sample is None:
        return False
    for col in ('height', 'premise_area'):
        if col not in sample.columns:
            print(f"  ✗ Missing column: {col}")
            return False
        print(f"  ✓ {col}: {sample[col].dtype}, "
              f"range [{sample[col].min():.2f}, {sample[col].max():.2f}]")
    return True


def _check_parameter_effect(sample):
    if sample is None:
        return False
    mod = sample.copy()
    mod['height'] *= 1.1
    mod['premise_area'] *= 0.9
    ok = (not mod['height'].equals(sample['height'])
          and not mod['premise_area'].equals(sample['premise_area']))
    print(f"  {'✓' if ok else '✗'} Multiplier effect: "
          f"{'detected' if ok else 'NOT detected'}")
    return ok


def _check_data_variance(sampled_postcodes, onsud_data_tuple, input_gpk, max_check=10):
    low_var = 0
    checked = 0
    for pc in sampled_postcodes[:max_check]:
        data = load_uprn_data(pc, onsud_data_tuple, input_gpk)
        if data is None or len(data) == 0:
            continue
        checked += 1
        h_var = data['height'].var()
        a_var = data['premise_area'].var()
        is_low = (h_var == 0 and a_var == 0) or (h_var < 0.01 and a_var < 1000)
        if is_low:
            low_var += 1
        print(f"  {pc}: height_var={h_var:.4f}, area_var={a_var:.4f}"
              f" {'⚠ low' if is_low else '✓'}")
    if checked > 0 and low_var > checked * 0.8:
        print("  ✗ Most postcodes have low data variance")
        return False
    return True


def _test_single_morris(pc, onsud_data_tuple, input_gpk):
    """Quick Morris test on one postcode, checking all three targets."""
    data = load_uprn_data(pc, onsud_data_tuple, input_gpk)
    if data is None or len(data) == 0:
        print(f"  ✗ No data for {pc}")
        return False

    param_values = morris_sampler.sample(PROBLEM, N=4, num_levels=4)
    raw = [evaluate_sample(X, data) for X in param_values]

    all_ok = True
    for key, col in TARGET_COLS.items():
        Y = np.array([r[key] for r in raw])
        variance = np.var(Y)
        ok = variance > 0
        print(f"  {key} ({col}): var={variance:.4f}, "
              f"range=[{Y.min():.2f}, {Y.max():.2f}] {'✓' if ok else '✗'}")
        if not ok:
            all_ok = False

    return all_ok


def run_debug_checks(sampled_postcodes, onsud_data, onsud_data_tuple, input_gpk):
    """Run all validation/debug checks. Returns True if all pass."""
    all_ok = True

    print("\n[DEBUG] Postcode existence check")
    if not _check_postcodes_exist(sampled_postcodes, onsud_data):
        all_ok = False

    print("\n[DEBUG] UPRN loading check")
    sample = _check_uprn_loading(sampled_postcodes, onsud_data_tuple, input_gpk)
    if sample is None:
        all_ok = False

    print("\n[DEBUG] Column check")
    if not _check_columns(sample):
        all_ok = False

    print("\n[DEBUG] Parameter effect check")
    if not _check_parameter_effect(sample):
        all_ok = False

    print("\n[DEBUG] Data variance check")
    if not _check_data_variance(sampled_postcodes, onsud_data_tuple, input_gpk):
        all_ok = False

    print("\n[DEBUG] Single-postcode Morris test (all targets)")
    if not _test_single_morris(
        sampled_postcodes.iloc[0], onsud_data_tuple, input_gpk,
    ):
        all_ok = False

    print(f"\n{'✓ All debug checks passed' if all_ok else '✗ Some checks failed'}")
    return all_ok


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--debug', action='store_true',
                        help='Run validation checks before analysis')
    parser.add_argument('--debug-only', action='store_true',
                        help='Run validation checks only')
    args = parser.parse_args()

    config = load_config()
    output_path, onsud_path = setup_paths(config)

    # Load data
    print('Loading ONSUD data...')
    onsud_data_tuple = load_onsud_data(onsud_path, config['PC_SHP_PATH'])
    onsud_data = onsud_data_tuple[0]

    sampled_postcodes = stratified_postcode_sample(onsud_data, n_samples=1000)

    # Debug checks (optional)
    if args.debug or args.debug_only:
        ok = run_debug_checks(
            sampled_postcodes, onsud_data, onsud_data_tuple,
            config['BUILDING_PATH'],
        )
        if args.debug_only:
            sys.exit(0 if ok else 1)
        if not ok:
            print("Debug checks failed — stopping.")
            return

    # ---- Run Morris analysis across all targets ----
    selected = sampled_postcodes.iloc[:config['PC_COUNT']]
    all_results = {}
    skipped = 0

    for pc in tqdm(selected, desc="Processing postcodes", unit="pc"):
        uprn_data = load_uprn_data(pc, onsud_data_tuple, config['BUILDING_PATH'])
        if uprn_data is None or len(uprn_data) == 0:
            skipped += 1
            continue

        sa_results = run_morris_analysis_multi(uprn_data, config['N_MORRIS'])
        if sa_results is not None:
            all_results[pc] = sa_results
        else:
            skipped += 1

    print(f"\nCompleted: {len(all_results)} postcodes analysed, {skipped} skipped")

    if not all_results:
        print("No valid results — exiting.")
        return

    # ---- Aggregate ----
    results_by_target, summaries_by_target = aggregate_results(all_results)

    for key, summary in summaries_by_target.items():
        print(f"\n=== {TARGET_LABELS[key]} ===")
        print(summary)

    # ---- Save plots ----
    for key in results_by_target:
        plot_per_target(
            summaries_by_target[key], results_by_target[key],
            key, output_path, config,
        )

    plot_comparative(summaries_by_target, output_path, config)

    # ---- Save CSVs ----
    pc_count = config['PC_COUNT']
    n_morris = config['N_MORRIS']

    for key, df in results_by_target.items():
        df.to_csv(os.path.join(
            output_path, f'morris_results_{key}_pc{pc_count}_n{n_morris}.csv',
        ))

    for key, df in summaries_by_target.items():
        df.to_csv(os.path.join(
            output_path, f'morris_summary_{key}_pc{pc_count}_n{n_morris}.csv',
        ))

    print(f"\nResults saved to {output_path}")


if __name__ == '__main__':
    main()