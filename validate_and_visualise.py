"""

validate_and_visualise.py



Validates and visualises fuel calculation results across all regions.

Loads all batch log files, derives error/uncertainty metrics, and produces:

  1. National summary statistics + CSV

  2. Regional comparison charts + CSVs

  3. Error clustering analysis + CSVs

  4. Diagnostic scatter plots + CSVs



Usage:

    python validate_and_visualise.py --data_dir /path/to/intermediate_data/fuel --output_dir /path/to/output



Each figure is saved as PNG + its underlying data as CSV.

"""


import seaborn as sns 
import argparse

import os

import glob

import sys

import warnings

import numpy as np

import pandas as pd

import matplotlib

matplotlib.use('Agg')

import matplotlib.pyplot as plt

import matplotlib.ticker as mticker

from matplotlib.gridspec import GridSpec

from pathlib import Path



warnings.filterwarnings('ignore', category=FutureWarning)



# ============================================================

# Style

# ============================================================

STYLE = {

    'bg': '#F8F5F0',

    'card': '#FFFFFF',

    'text': '#1B3A4B',

    'muted': '#6B7C8A',

    'blue': '#2D9CDB',

    'coral': '#E07A5F',

    'amber': '#F0B95B',

    'teal': '#4ECDC4',

    'purple': '#A78BFA',

    'green': '#6BCB77',

    'red': '#EF5350',

    'dark': '#1B3A4B',

}



def setup_style():

    plt.rcParams.update({

        'figure.facecolor': STYLE['bg'],

        'axes.facecolor': STYLE['card'],

        'axes.edgecolor': '#E0DCD4',

        'axes.labelcolor': STYLE['text'],

        'text.color': STYLE['text'],

        'xtick.color': STYLE['muted'],

        'ytick.color': STYLE['muted'],

        'grid.color': '#E0DCD4',

        'grid.alpha': 0.7,

        'font.family': 'sans-serif',

        'font.size': 10,

        'axes.titlesize': 13,

        'axes.titleweight': 'bold',

        'figure.titlesize': 16,

        'figure.titleweight': 'bold',

    })



# ============================================================

# Data Loading

# ============================================================



def discover_batch_files(data_dir):

    """Find all log files across region folders."""

    pattern = os.path.join(data_dir, '*', '*_log_file.csv')

    files = glob.glob(pattern)

    if not files:

        pattern = os.path.join(data_dir, '**', '*_log_file.csv')

        files = glob.glob(pattern, recursive=True)

    print(f"Discovered {len(files)} batch files")

    return sorted(files)





def load_all_batches(files):

    """Load and concatenate all batch files with region labels."""

    frames = []

    errors = []

    for f in files:

        try:

            df = pd.read_csv(f, low_memory=False)

            region = Path(f).parent.name

            batch_id = Path(f).stem.replace('_log_file', '')

            df['_region'] = region

            df['_batch'] = batch_id

            df['_source_file'] = f

            frames.append(df)

        except Exception as e:

            errors.append({'file': f, 'error': str(e)})



    if errors:

        print(f"WARNING: {len(errors)} files failed to load:")

        for err in errors[:10]:

            print(f"  {err['file']}: {err['error']}")



    if not frames:

        print("ERROR: No data loaded")

        sys.exit(1)



    combined = pd.concat(frames, ignore_index=True)

    print(f"Loaded {len(combined)} postcodes from {len(frames)} batch files across {combined['_region'].nunique()} regions")

    return combined, errors





def validate_schema(df):

    """Check required columns exist."""

    required_cols = [

        'postcode', 'clean_res_total_buildings',

        'clean_res_area_est_global_total', 'clean_res_area_est_raw_total',

        'clean_res_area_est_filled_total',

        'clean_res_area_mode_total', 'clean_res_area_min_total', 'clean_res_area_max_total',

        'clean_res_scaled_area_mode_total', 'clean_res_scaled_area_min_total',

        'clean_res_scaled_area_max_total',

        'clean_res_premise_area_total',

    ]

    missing = [c for c in required_cols if c not in df.columns]

    if missing:

        print(f"ERROR: Missing required columns: {missing}")

        print(f"Available columns: {sorted(df.columns.tolist())}")

        sys.exit(1)

    print("Schema validation passed")





# ============================================================

# Derived Metrics

# ============================================================



def derive_metrics(df):

    """Add all derived analysis columns."""

    d = df.copy()



    d['spread_pct'] = ((d['clean_res_area_max_total'] - d['clean_res_area_min_total'])

                       / d['clean_res_area_mode_total'] * 100)

    d['global_vs_raw_pct'] = ((d['clean_res_area_est_global_total'] - d['clean_res_area_est_raw_total'])

                              / d['clean_res_area_est_raw_total'] * 100)

    d['raw_vs_filled_pct'] = ((d['clean_res_area_est_raw_total'] - d['clean_res_area_est_filled_total'])

                              / d['clean_res_area_est_filled_total'] * 100)



    d['avg_area_per_bldg'] = d['clean_res_area_mode_total'] / d['clean_res_total_buildings']

    d['avg_premise_area'] = d['clean_res_premise_area_total'] / d['clean_res_total_buildings']

    d['implied_avg_floors'] = d['avg_area_per_bldg'] / d['avg_premise_area']



    d['envelope_width'] = d['clean_res_area_max_total'] - d['clean_res_area_min_total']

    d['abs_global_vs_raw'] = (d['clean_res_area_est_global_total'] - d['clean_res_area_est_raw_total']).abs()

    d['abs_raw_vs_filled'] = (d['clean_res_area_est_raw_total'] - d['clean_res_area_est_filled_total']).abs()



    d['outcode'] = d['postcode'].str.strip().str.split(' ').str[0]



    conditions = [

        d['spread_pct'] < 5,

        d['spread_pct'] < 15,

        d['spread_pct'] < 30,

        d['spread_pct'] < 60,

        d['spread_pct'] >= 60,

    ]

    labels = ['Tight (<5%)', 'Low (5-15%)', 'Medium (15-30%)', 'High (30-60%)', 'Very High (>60%)']

    d['spread_severity'] = np.select(conditions, labels, default='No data')

    
    d['eui_mode_gas'] = d['total_gas'] / d['clean_res_scaled_area_mode_total'] 
    d['eui_upper_gas'] = d['total_gas'] / d['clean_res_scaled_area_min_total']
    d['eui_lower_gas'] = d['total_gas'] / d['clean_res_scaled_area_max_total']


    
    d['eui_mode_elec'] = d['total_elec'] / d['clean_res_scaled_area_mode_total'] 
    d['eui_upper_elec'] = d['total_elec'] / d['clean_res_scaled_area_min_total']
    d['eui_lower_elec'] = d['total_elec'] / d['clean_res_scaled_area_max_total']


    return d





# ============================================================

# Validation Checks

# ============================================================



def run_validation(df, output_dir):

    """Run data quality checks and save results."""

    checks = []



    # Duplicates

    dupes = df.duplicated(subset='postcode', keep=False)

    n_dupes = dupes.sum()

    checks.append({'check': 'Duplicate postcodes', 'count': n_dupes,

                   'pct': n_dupes / len(df) * 100, 'status': 'FAIL' if n_dupes > 0 else 'PASS'})

    if n_dupes > 0:

        dupe_pcs = df[dupes][['postcode', '_region', '_batch', '_source_file']].drop_duplicates()

        dupe_pcs.to_csv(os.path.join(output_dir, 'validation_duplicate_postcodes.csv'), index=False)



    # Null rates

    key_cols = ['clean_res_area_mode_total', 'clean_res_area_min_total', 'clean_res_area_max_total',

                'clean_res_scaled_area_mode_total', 'clean_res_total_buildings', 'clean_res_premise_area_total']

    for col in key_cols:

        null_rate = df[col].isna().mean() * 100

        checks.append({'check': f'Nulls in {col}', 'count': df[col].isna().sum(),

                       'pct': null_rate, 'status': 'FAIL' if null_rate > 50 else ('WARN' if null_rate > 20 else 'PASS')})



    # Implausible implied floors

    valid = df[df['implied_avg_floors'].notna()]

    suspect = valid[(valid['implied_avg_floors'] < 0.5) | (valid['implied_avg_floors'] > 6)]

    checks.append({'check': 'Implausible implied floors (<0.5 or >6)', 'count': len(suspect),

                   'pct': len(suspect) / max(len(valid), 1) * 100,

                   'status': 'WARN' if len(suspect) > len(valid) * 0.05 else 'PASS'})



    # Extreme spread

    valid_spread = df[df['spread_pct'].notna()]

    extreme = valid_spread[valid_spread['spread_pct'] > 100]

    checks.append({'check': 'Extreme spread >100%', 'count': len(extreme),

                   'pct': len(extreme) / max(len(valid_spread), 1) * 100,

                   'status': 'WARN' if len(extreme) > len(valid_spread) * 0.05 else 'PASS'})



    # Negative areas

    for col in ['clean_res_area_mode_total', 'clean_res_area_min_total']:

        neg = df[df[col] < 0]

        checks.append({'check': f'Negative values in {col}', 'count': len(neg),

                       'pct': len(neg) / len(df) * 100, 'status': 'FAIL' if len(neg) > 0 else 'PASS'})



    # Inverted envelope

    both_valid = df[df['clean_res_area_min_total'].notna() & df['clean_res_area_max_total'].notna()]

    inverted = both_valid[both_valid['clean_res_area_min_total'] > both_valid['clean_res_area_max_total']]

    checks.append({'check': 'Min area > Max area (inverted envelope)', 'count': len(inverted),

                   'pct': len(inverted) / max(len(both_valid), 1) * 100,

                   'status': 'FAIL' if len(inverted) > 0 else 'PASS'})



    checks_df = pd.DataFrame(checks)

    checks_df.to_csv(os.path.join(output_dir, 'validation_checks.csv'), index=False)



    print("\n=== VALIDATION RESULTS ===")

    for _, row in checks_df.iterrows():

        icon = {'PASS': '✓', 'WARN': '⚠', 'FAIL': '✗'}[row['status']]

        print(f"  {icon} {row['check']}: {row['count']} ({row['pct']:.1f}%) [{row['status']}]")



    return checks_df





# ============================================================

# Summary Statistics

# ============================================================



def compute_national_summary(df, output_dir):

    """Compute and save national-level summary."""

    valid = df[df['clean_res_area_mode_total'].notna()]



    summary = {

        'total_postcodes': len(df),

        'postcodes_with_data': len(valid),

        'pct_with_data': len(valid) / len(df) * 100,

        'total_regions': df['_region'].nunique(),

        'total_batches': df['_batch'].nunique(),

        'median_buildings_per_pc': valid['clean_res_total_buildings'].median(),

        'mean_buildings_per_pc': valid['clean_res_total_buildings'].mean(),

        'median_area_mode': valid['clean_res_area_mode_total'].median(),

        'median_area_min': valid['clean_res_area_min_total'].median(),

        'median_area_max': valid['clean_res_area_max_total'].median(),

        'median_scaled_area_mode': valid['clean_res_scaled_area_mode_total'].median(),

        'median_spread_pct': valid['spread_pct'].median(),

        'mean_spread_pct': valid['spread_pct'].mean(),

        'pct_spread_under_5': (valid['spread_pct'] < 5).mean() * 100,

        'pct_spread_5_15': ((valid['spread_pct'] >= 5) & (valid['spread_pct'] < 15)).mean() * 100,

        'pct_spread_15_30': ((valid['spread_pct'] >= 15) & (valid['spread_pct'] < 30)).mean() * 100,

        'pct_spread_30_60': ((valid['spread_pct'] >= 30) & (valid['spread_pct'] < 60)).mean() * 100,

        'pct_spread_over_60': (valid['spread_pct'] >= 60).mean() * 100,

        'median_global_vs_raw_pct': valid['global_vs_raw_pct'].median(),

        'pct_fc_corrected': (valid['raw_vs_filled_pct'].abs() > 1).mean() * 100,

        'median_implied_floors': valid['implied_avg_floors'].median(),

        'pct_suspect_floors': ((valid['implied_avg_floors'] < 1) | (valid['implied_avg_floors'] > 4)).mean() * 100,

        'median_envelope_width_m2': valid['envelope_width'].median(),

        'median_abs_global_vs_raw_m2': valid['abs_global_vs_raw'].median(),

        'median_abs_raw_vs_filled_m2': valid['abs_raw_vs_filled'].median(),

    }



    pd.DataFrame([summary]).to_csv(os.path.join(output_dir, '01_national_summary.csv'), index=False)



    print("\n=== NATIONAL SUMMARY ===")

    for k, v in summary.items():

        print(f"  {k}: {v:.2f}" if isinstance(v, float) else f"  {k}: {v}")



    return summary





def compute_regional_summary(df, output_dir):

    """Compute per-region summary."""

    valid = df[df['clean_res_area_mode_total'].notna()]



    agg = valid.groupby('_region').agg(

        n_postcodes=('postcode', 'count'),

        n_batches=('_batch', 'nunique'),

        median_buildings=('clean_res_total_buildings', 'median'),

        median_area_mode=('clean_res_area_mode_total', 'median'),

        median_scaled_area_mode=('clean_res_scaled_area_mode_total', 'median'),

        median_spread_pct=('spread_pct', 'median'),

        mean_spread_pct=('spread_pct', 'mean'),

        p95_spread_pct=('spread_pct', lambda x: x.quantile(0.95)),

        pct_high_spread=('spread_pct', lambda x: (x > 30).mean() * 100),

        median_global_vs_raw=('global_vs_raw_pct', 'median'),

        pct_fc_corrected=('raw_vs_filled_pct', lambda x: (x.abs() > 1).mean() * 100),

        median_implied_floors=('implied_avg_floors', 'median'),

        pct_suspect_floors=('implied_avg_floors', lambda x: ((x < 1) | (x > 4)).mean() * 100),

        median_footprint=('avg_premise_area', 'median'),

    ).sort_values('median_spread_pct', ascending=False).reset_index()



    agg.to_csv(os.path.join(output_dir, '02_regional_summary.csv'), index=False)

    print(f"\nRegional summary saved: {len(agg)} regions")

    return agg





# ============================================================

# Plotting

# ============================================================



def save_fig(fig, output_dir, name):

    path = os.path.join(output_dir, f'{name}.png')

    fig.savefig(path, dpi=150, bbox_inches='tight', facecolor=fig.get_facecolor())

    plt.close(fig)

    print(f"  Saved: {name}.png")





def plot_01_national_distributions(df, output_dir):

    """Overlapping histograms and box plots of min/mode/max — national."""

    valid = df[df['clean_res_area_mode_total'].notna()].copy()



    for prefix, label in [('clean_res_area', 'Gross Floor Area'), ('clean_res_scaled_area', 'Scaled GIA')]:

        fig, axes = plt.subplots(1, 2, figsize=(14, 5))

        fig.suptitle(f'National Distribution — {label} (m²)', fontweight='bold', fontsize=14)



        ax = axes[0]

        for col, name, color in [

            (f'{prefix}_min_total', 'Min', STYLE['blue']),

            (f'{prefix}_mode_total', 'Mode', STYLE['dark']),

            (f'{prefix}_max_total', 'Max', STYLE['coral']),

        ]:

            vals = valid[col].dropna()

            clip = vals.quantile(0.98)

            ax.hist(vals[vals <= clip], bins=60, alpha=0.5, color=color, label=name, edgecolor='none')

        ax.set_xlabel('Floor Area (m²)')

        ax.set_ylabel('Count')

        ax.set_title('Overlapping Distributions')

        ax.legend()

        ax.grid(True, alpha=0.3)



        ax = axes[1]

        data_box = []

        labels_box = []

        colors_box = [STYLE['blue'], STYLE['dark'], STYLE['coral']]

        for col, name in [(f'{prefix}_min_total', 'Min'), (f'{prefix}_mode_total', 'Mode'), (f'{prefix}_max_total', 'Max')]:

            vals = valid[col].dropna()

            data_box.append(vals[vals <= vals.quantile(0.98)])

            labels_box.append(name)

        bp = ax.boxplot(data_box, labels=labels_box, patch_artist=True, widths=0.5,

                        medianprops=dict(color=STYLE['text'], linewidth=2))

        for patch, color in zip(bp['boxes'], colors_box):

            patch.set_facecolor(color)

            patch.set_alpha(0.4)

        ax.set_ylabel('Floor Area (m²)')

        ax.set_title('Box Plot Comparison')

        ax.grid(True, alpha=0.3, axis='y')



        fig.tight_layout()

        safe = label.replace(' ', '_').lower()

        save_fig(fig, output_dir, f'01_national_distribution_{safe}')

        valid[['postcode', '_region', f'{prefix}_min_total', f'{prefix}_mode_total', f'{prefix}_max_total']].to_csv(

            os.path.join(output_dir, f'01_national_distribution_{safe}.csv'), index=False)





def plot_02_spread_severity(df, output_dir):

    """Stacked bar of spread severity — national and per region."""

    valid = df[df['spread_pct'].notna()].copy()

    severity_order = ['Tight (<5%)', 'Low (5-15%)', 'Medium (15-30%)', 'High (30-60%)', 'Very High (>60%)']

    severity_colors = [STYLE['green'], STYLE['teal'], STYLE['amber'], STYLE['coral'], STYLE['red']]



    nat_counts = valid['spread_severity'].value_counts().reindex(severity_order, fill_value=0)



    fig, axes = plt.subplots(1, 2, figsize=(14, max(5, valid['_region'].nunique() * 0.3)),

                             gridspec_kw={'width_ratios': [1, 2.5]})

    fig.suptitle('Uncertainty Spread Severity', fontweight='bold', fontsize=14)



    ax = axes[0]

    ax.pie(nat_counts, labels=None, colors=severity_colors,

           autopct=lambda p: f'{p:.0f}%' if p > 3 else '', startangle=90, textprops={'fontsize': 9})

    ax.set_title('National', fontsize=11)

    ax.legend(severity_order, loc='lower left', fontsize=8)



    ax = axes[1]

    regional = valid.groupby(['_region', 'spread_severity']).size().unstack(fill_value=0)

    regional = regional.reindex(columns=severity_order, fill_value=0)

    regional_pct = regional.div(regional.sum(axis=1), axis=0) * 100

    regional_pct = regional_pct.sort_values('Very High (>60%)', ascending=True)

    regional_pct.plot(kind='barh', stacked=True, ax=ax, color=severity_colors, edgecolor='none', width=0.7)

    ax.set_xlabel('Percentage of Postcodes')

    ax.set_title('By Region', fontsize=11)

    ax.legend(fontsize=7, loc='lower right')

    ax.grid(True, alpha=0.3, axis='x')



    fig.tight_layout()

    save_fig(fig, output_dir, '02_spread_severity')

    csv_out = regional.copy()

    csv_out['total'] = csv_out.sum(axis=1)

    csv_out.to_csv(os.path.join(output_dir, '02_spread_severity.csv'))





def plot_03_regional_comparison(regional_df, output_dir):

    """Bar charts comparing key metrics across regions."""

    if len(regional_df) > 40:

        top = regional_df.nlargest(15, 'median_spread_pct')

        bottom = regional_df.nsmallest(15, 'median_spread_pct')

        plot_df = pd.concat([top, bottom]).drop_duplicates()

        suffix = ' (Top & Bottom 15)'

    else:

        plot_df = regional_df

        suffix = ''



    plot_df = plot_df.sort_values('median_spread_pct', ascending=True)



    fig, axes = plt.subplots(1, 4, figsize=(20, max(6, len(plot_df) * 0.25)))

    fig.suptitle(f'Regional Comparison{suffix}', fontweight='bold', fontsize=14)



    metrics = [

        ('median_spread_pct', 'Median Spread %', STYLE['coral']),

        ('median_global_vs_raw', 'Median Global vs Raw %', STYLE['purple']),

        ('pct_fc_corrected', '% FC Corrected', STYLE['teal']),

        ('pct_suspect_floors', '% Suspect Floors', STYLE['amber']),

    ]

    for ax, (col, label, color) in zip(axes, metrics):

        ax.barh(plot_df['_region'], plot_df[col], color=color, alpha=0.7, edgecolor='none')

        ax.set_xlabel(label, fontsize=9)

        ax.set_title(label, fontsize=10, fontweight='bold')

        ax.grid(True, alpha=0.3, axis='x')

        ax.tick_params(axis='y', labelsize=8)



    fig.tight_layout()

    save_fig(fig, output_dir, '03_regional_comparison')





def plot_04_global_vs_raw(df, output_dir):

    """How the global height lookup diverges from reported FC."""

    valid = df[df['global_vs_raw_pct'].notna() & df['global_vs_raw_pct'].between(-200, 300)].copy()



    fig, axes = plt.subplots(2, 2, figsize=(14, 10))

    fig.suptitle('Global Height Lookup vs Reported Floor Count', fontweight='bold', fontsize=14)



    # Histogram

    ax = axes[0, 0]

    ax.hist(valid['global_vs_raw_pct'].clip(-100, 150), bins=80, color=STYLE['purple'], alpha=0.6, edgecolor='none')

    ax.axvline(0, color=STYLE['text'], linewidth=1, linestyle='--', alpha=0.5)

    ax.set_xlabel('Global vs Raw (%)')

    ax.set_ylabel('Count')

    ax.set_title('Distribution of Divergence')

    ax.grid(True, alpha=0.3)



    # Scatter

    ax = axes[0, 1]

    sample = valid.sample(min(5000, len(valid)), random_state=42)

    clip_val = sample['clean_res_area_est_raw_total'].quantile(0.98)

    s = sample[sample['clean_res_area_est_raw_total'] <= clip_val]

    sc = ax.scatter(s['clean_res_area_est_raw_total'], s['clean_res_area_est_global_total'],

                    c=s['spread_pct'].clip(0, 80), cmap='RdYlBu_r', s=6, alpha=0.4)

    mx = max(s['clean_res_area_est_raw_total'].max(), s['clean_res_area_est_global_total'].max())

    ax.plot([0, mx], [0, mx], '--', color=STYLE['muted'], linewidth=1, label='1:1')

    ax.set_xlabel('Area from Raw FC (m²)')

    ax.set_ylabel('Area from Global Avg (m²)')

    ax.set_title('Per-Postcode Agreement')

    ax.legend(fontsize=8)

    plt.colorbar(sc, ax=ax, label='Spread %', shrink=0.8)

    ax.grid(True, alpha=0.3)



    # Regional median

    ax = axes[1, 0]

    reg = valid.groupby('_region')['global_vs_raw_pct'].median().sort_values()

    colors = [STYLE['coral'] if v > 10 else STYLE['blue'] if v < -10 else STYLE['muted'] for v in reg]

    reg.plot(kind='barh', ax=ax, color=colors, edgecolor='none')

    ax.axvline(0, color=STYLE['text'], linewidth=1, linestyle='--', alpha=0.5)

    ax.set_xlabel('Median Global vs Raw (%)')

    ax.set_title('Regional Median Divergence')

    ax.grid(True, alpha=0.3, axis='x')

    ax.tick_params(axis='y', labelsize=8)



    # Driver decomposition

    ax = axes[1, 1]

    ve = valid[valid['envelope_width'] > 0].copy()

    ve['pct_from_global'] = (ve['abs_global_vs_raw'] / ve['envelope_width'] * 100).clip(0, 150)

    ve['pct_from_fc'] = (ve['abs_raw_vs_filled'] / ve['envelope_width'] * 100).clip(0, 150)

    ax.hist(ve['pct_from_global'], bins=50, alpha=0.6, color=STYLE['purple'], label='Global vs Raw', edgecolor='none')

    ax.hist(ve['pct_from_fc'], bins=50, alpha=0.6, color=STYLE['teal'], label='FC Correction', edgecolor='none')

    ax.set_xlabel('% of Envelope Width')

    ax.set_ylabel('Count')

    ax.set_title('Envelope Driver Decomposition')

    ax.legend()

    ax.grid(True, alpha=0.3)



    fig.tight_layout()

    save_fig(fig, output_dir, '04_global_vs_raw')

    valid[['postcode', '_region', 'global_vs_raw_pct', 'raw_vs_filled_pct', 'spread_pct',

           'envelope_width', 'abs_global_vs_raw', 'abs_raw_vs_filled']].to_csv(

        os.path.join(output_dir, '04_global_vs_raw.csv'), index=False)





def plot_05_implied_floors(df, output_dir):

    """Implied floor count diagnostics."""

    valid = df[df['implied_avg_floors'].notna() & df['implied_avg_floors'].between(0, 10)].copy()



    fig, axes = plt.subplots(1, 3, figsize=(16, 5))

    fig.suptitle('Implied Average Floor Count Diagnostics', fontweight='bold', fontsize=14)



    ax = axes[0]

    ax.hist(valid['implied_avg_floors'].clip(0.5, 6), bins=50, color=STYLE['dark'], alpha=0.6, edgecolor='none')

    ax.axvline(1, color=STYLE['red'], linewidth=1, linestyle='--', alpha=0.7, label='Suspect')

    ax.axvline(4, color=STYLE['red'], linewidth=1, linestyle='--', alpha=0.7)

    ax.set_xlabel('Implied Avg Floors')

    ax.set_ylabel('Count')

    ax.set_title('Distribution')

    ax.legend(fontsize=8)

    ax.grid(True, alpha=0.3)



    ax = axes[1]

    sample = valid.sample(min(5000, len(valid)), random_state=42)

    s = sample[sample['avg_premise_area'] < sample['avg_premise_area'].quantile(0.98)]

    sc = ax.scatter(s['avg_premise_area'], s['implied_avg_floors'],

                    c=s['spread_pct'].clip(0, 80), cmap='RdYlBu_r', s=6, alpha=0.4)

    ax.axhline(1, color=STYLE['red'], linewidth=1, linestyle='--', alpha=0.5)

    ax.axhline(4, color=STYLE['red'], linewidth=1, linestyle='--', alpha=0.5)

    ax.set_xlabel('Avg Footprint (m²)')

    ax.set_ylabel('Implied Avg Floors')

    ax.set_title('vs Footprint')

    plt.colorbar(sc, ax=ax, label='Spread %', shrink=0.8)

    ax.grid(True, alpha=0.3)



    ax = axes[2]

    reg = valid.groupby('_region')['implied_avg_floors'].median().sort_values()

    reg.plot(kind='barh', ax=ax, color=STYLE['teal'], alpha=0.7, edgecolor='none')

    ax.axvline(2, color=STYLE['muted'], linewidth=1, linestyle='--', alpha=0.5)

    ax.set_xlabel('Median Implied Floors')

    ax.set_title('By Region')

    ax.grid(True, alpha=0.3, axis='x')

    ax.tick_params(axis='y', labelsize=8)



    fig.tight_layout()

    save_fig(fig, output_dir, '05_implied_floors')

    valid[['postcode', '_region', 'implied_avg_floors', 'avg_premise_area',

           'spread_pct', 'clean_res_total_buildings']].to_csv(

        os.path.join(output_dir, '05_implied_floors.csv'), index=False)





def plot_06_uncertainty_vs_size(df, output_dir):

    """Does postcode size affect uncertainty?"""

    valid = df[df['spread_pct'].notna()].copy()



    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    fig.suptitle('Uncertainty vs Postcode Size', fontweight='bold', fontsize=14)



    ax = axes[0]

    sample = valid.sample(min(5000, len(valid)), random_state=42)

    sc = ax.scatter(sample['clean_res_total_buildings'], sample['spread_pct'].clip(0, 150),

                    c=sample['global_vs_raw_pct'].clip(-50, 80), cmap='RdYlBu_r', s=6, alpha=0.4)

    ax.set_xlabel('Number of Buildings')

    ax.set_ylabel('Spread %')

    ax.set_title('Spread vs Building Count')

    plt.colorbar(sc, ax=ax, label='Global vs Raw %', shrink=0.8)

    ax.grid(True, alpha=0.3)



    ax = axes[1]

    valid['bldg_bin'] = pd.cut(valid['clean_res_total_buildings'],

                               bins=[0, 5, 10, 20, 30, 50, 100, 500],

                               labels=['1-5', '6-10', '11-20', '21-30', '31-50', '51-100', '100+'])

    binned = valid.groupby('bldg_bin', observed=True).agg(

        median_spread=('spread_pct', 'median'),

        q25=('spread_pct', lambda x: x.quantile(0.25)),

        q75=('spread_pct', lambda x: x.quantile(0.75)),

        n=('spread_pct', 'count')

    ).reset_index()



    ax.bar(range(len(binned)), binned['median_spread'], color=STYLE['blue'], alpha=0.6, edgecolor='none')

    ax.errorbar(range(len(binned)), binned['median_spread'],

                yerr=[binned['median_spread'] - binned['q25'], binned['q75'] - binned['median_spread']],

                fmt='none', color=STYLE['text'], capsize=4, linewidth=1.5)

    ax.set_xticks(range(len(binned)))

    ax.set_xticklabels([f"{r['bldg_bin']}\n(n={r['n']})" for _, r in binned.iterrows()], fontsize=8)

    ax.set_xlabel('Building Count Bin')

    ax.set_ylabel('Spread %')

    ax.set_title('Binned Median Spread (IQR)')

    ax.grid(True, alpha=0.3, axis='y')



    fig.tight_layout()

    save_fig(fig, output_dir, '06_uncertainty_vs_size')

    binned.to_csv(os.path.join(output_dir, '06_uncertainty_vs_size.csv'), index=False)





def plot_07_regional_facets(df, output_dir):

    """Per-region histograms of spread — faceted."""

    valid = df[df['spread_pct'].notna()].copy()

    regions = sorted(valid['_region'].unique())

    n = len(regions)

    if n == 0:

        return



    ncols = min(5, n)

    nrows = (n + ncols - 1) // ncols

    fig, axes = plt.subplots(nrows, ncols, figsize=(ncols * 3, nrows * 2.5), squeeze=False)

    fig.suptitle('Spread % Distribution by Region', fontweight='bold', fontsize=14, y=1.01)



    for idx, region in enumerate(regions):

        r, c = divmod(idx, ncols)

        ax = axes[r][c]

        rd = valid[valid['_region'] == region]['spread_pct'].clip(0, 120)

        ax.hist(rd, bins=30, color=STYLE['coral'], alpha=0.6, edgecolor='none')

        ax.set_title(f'{region} (n={len(rd)})', fontsize=9, fontweight='bold')

        ax.set_xlim(0, 120)

        ax.tick_params(labelsize=7)

        ax.grid(True, alpha=0.3)



    for idx in range(n, nrows * ncols):

        r, c = divmod(idx, ncols)

        axes[r][c].set_visible(False)



    fig.tight_layout()

    save_fig(fig, output_dir, '07_regional_spread_facets')

    valid.groupby('_region')['spread_pct'].describe(percentiles=[.05, .25, .5, .75, .95]).to_csv(

        os.path.join(output_dir, '07_regional_spread_facets.csv'))





def plot_08_eui_impact(df, output_dir):

    """How the uncertainty envelope translates to EUI uncertainty."""

    if 'total_gas' not in df.columns or 'clean_res_scaled_area_mode_total' not in df.columns:

        print("  Skipping EUI plot — missing columns")

        return



    valid = df[

        df['total_gas'].notna() & (df['clean_res_scaled_area_mode_total'] > 0) &

        (df['clean_res_scaled_area_min_total'] > 0)

    ].copy()



    if len(valid) < 10:

        print("  Skipping EUI plot — insufficient data")

        return


    print(valid.columns.tolist() ) 
    valid['eui_range_pct'] = (valid['eui_upper_gas'] - valid['eui_lower_gas']) / valid['eui_mode_gas'] * 100



    fig, axes = plt.subplots(1, 3, figsize=(16, 5))

    fig.suptitle('Impact on Gas EUI Uncertainty', fontweight='bold', fontsize=14)



    ax = axes[0]

    for col, name, color in [('eui_lower_gas', 'Lower', STYLE['blue']), ('eui_mode_gas', 'Mode', STYLE['dark']),

                              ('eui_upper_gas', 'Upper', STYLE['coral'])]:

        v = valid[col].clip(0, valid[col].quantile(0.98))

        ax.hist(v, bins=50, alpha=0.5, color=color, label=name, edgecolor='none')

    ax.set_xlabel('Gas EUI (kWh/m²)')

    ax.set_ylabel('Count')

    ax.set_title('EUI Distribution Bounds')

    ax.legend(fontsize=8)

    ax.grid(True, alpha=0.3)



    ax = axes[1]

    clip = valid['eui_range_pct'].clip(0, 150)

    ax.hist(clip, bins=50, color=STYLE['amber'], alpha=0.6, edgecolor='none')

    ax.axvline(clip.median(), color=STYLE['text'], linewidth=2, linestyle='--',

               label=f'Median: {clip.median():.0f}%')

    ax.set_xlabel('EUI Range as % of Mode')

    ax.set_ylabel('Count')

    ax.set_title('EUI Uncertainty')

    ax.legend()

    ax.grid(True, alpha=0.3)



    ax = axes[2]

    sample = valid.sample(min(3000, len(valid)), random_state=42)

    ax.scatter(sample['spread_pct'].clip(0, 120), sample['eui_range_pct'].clip(0, 150),

               s=6, alpha=0.3, color=STYLE['purple'])

    ax.set_xlabel('Area Spread %')

    ax.set_ylabel('EUI Range %')

    ax.set_title('Area → EUI Uncertainty')

    ax.grid(True, alpha=0.3)



    fig.tight_layout()

    save_fig(fig, output_dir, '08_eui_impact')

    valid[['postcode', '_region', 'eui_mode_gas', 'eui_lower_gas', 'eui_upper_gas',

           'eui_range_pct', 'spread_pct']].to_csv(

        os.path.join(output_dir, '08_eui_impact.csv'), index=False)





def plot_09_outcode_clustering(df, output_dir):

    """Identify outcodes where errors cluster — top worst performers."""

    valid = df[df['spread_pct'].notna()].copy()



    oc = valid.groupby('outcode').agg(

        n=('spread_pct', 'count'),

        region=('_region', 'first'),

        median_spread=('spread_pct', 'median'),

        mean_spread=('spread_pct', 'mean'),

        pct_high_spread=('spread_pct', lambda x: (x > 30).mean() * 100),

        median_global_vs_raw=('global_vs_raw_pct', 'median'),

        median_implied_floors=('implied_avg_floors', 'median'),

        median_footprint=('avg_premise_area', 'median'),

        median_buildings=('clean_res_total_buildings', 'median'),

    ).reset_index()



    # Only outcodes with enough data

    oc_sig = oc[oc['n'] >= 5].sort_values('median_spread', ascending=False)

    oc_sig.to_csv(os.path.join(output_dir, '09_outcode_clustering.csv'), index=False)



    # Top 30 worst

    top = oc_sig.head(30).sort_values('median_spread', ascending=True)



    fig, axes = plt.subplots(1, 3, figsize=(18, max(6, len(top) * 0.25)))

    fig.suptitle('Top 30 Worst Outcodes by Median Spread (n≥5)', fontweight='bold', fontsize=14)



    ax = axes[0]

    ax.barh(top['outcode'], top['median_spread'], color=STYLE['coral'], alpha=0.7, edgecolor='none')

    ax.set_xlabel('Median Spread %')

    ax.set_title('Median Spread')

    ax.grid(True, alpha=0.3, axis='x')

    ax.tick_params(axis='y', labelsize=7)



    ax = axes[1]

    ax.barh(top['outcode'], top['median_global_vs_raw'], color=STYLE['purple'], alpha=0.7, edgecolor='none')

    ax.axvline(0, color=STYLE['text'], linewidth=1, linestyle='--', alpha=0.5)

    ax.set_xlabel('Median Global vs Raw %')

    ax.set_title('Height Lookup Divergence')

    ax.grid(True, alpha=0.3, axis='x')

    ax.tick_params(axis='y', labelsize=7)



    ax = axes[2]

    ax.barh(top['outcode'], top['median_implied_floors'], color=STYLE['teal'], alpha=0.7, edgecolor='none')

    ax.axvline(2, color=STYLE['muted'], linewidth=1, linestyle='--', alpha=0.5)

    ax.set_xlabel('Median Implied Floors')

    ax.set_title('Implied Floor Count')

    ax.grid(True, alpha=0.3, axis='x')

    ax.tick_params(axis='y', labelsize=7)



    fig.tight_layout()

    save_fig(fig, output_dir, '09_outcode_clustering')





def plot_10_footprint_analysis(df, output_dir):

    """Spread by footprint size quintile — national and per region."""

    valid = df[df['spread_pct'].notna() & df['avg_premise_area'].notna()].copy()



    try:

        valid['fp_quintile'] = pd.qcut(valid['avg_premise_area'], 5,

                                        labels=['Q1 (small)', 'Q2', 'Q3', 'Q4', 'Q5 (large)'])

    except ValueError:

        print("  Skipping footprint analysis — cannot create quintiles")

        return



    # National

    nat = valid.groupby('fp_quintile', observed=True).agg(

        n=('spread_pct', 'count'),

        median_spread=('spread_pct', 'median'),

        median_global_vs_raw=('global_vs_raw_pct', 'median'),

        median_floors=('implied_avg_floors', 'median'),

    ).reset_index()



    fig, axes = plt.subplots(1, 3, figsize=(15, 5))

    fig.suptitle('Error Metrics by Footprint Size Quintile', fontweight='bold', fontsize=14)



    ax = axes[0]

    ax.bar(range(len(nat)), nat['median_spread'], color=STYLE['coral'], alpha=0.7, edgecolor='none')

    ax.set_xticks(range(len(nat)))

    ax.set_xticklabels(nat['fp_quintile'], fontsize=8, rotation=15)

    ax.set_ylabel('Median Spread %')

    ax.set_title('Spread vs Footprint')

    ax.grid(True, alpha=0.3, axis='y')



    ax = axes[1]

    ax.bar(range(len(nat)), nat['median_global_vs_raw'], color=STYLE['purple'], alpha=0.7, edgecolor='none')

    ax.set_xticks(range(len(nat)))

    ax.set_xticklabels(nat['fp_quintile'], fontsize=8, rotation=15)

    ax.set_ylabel('Median Global vs Raw %')

    ax.set_title('Height Divergence vs Footprint')

    ax.grid(True, alpha=0.3, axis='y')



    ax = axes[2]

    ax.bar(range(len(nat)), nat['median_floors'], color=STYLE['teal'], alpha=0.7, edgecolor='none')

    ax.set_xticks(range(len(nat)))

    ax.set_xticklabels(nat['fp_quintile'], fontsize=8, rotation=15)

    ax.set_ylabel('Median Implied Floors')

    ax.set_title('Floors vs Footprint')

    ax.axhline(2, color=STYLE['muted'], linewidth=1, linestyle='--')

    ax.grid(True, alpha=0.3, axis='y')



    fig.tight_layout()

    save_fig(fig, output_dir, '10_footprint_analysis')

    nat.to_csv(os.path.join(output_dir, '10_footprint_analysis.csv'), index=False)

import argparse

import os

import glob

import sys

import warnings

import numpy as np

import pandas as pd

import matplotlib

matplotlib.use('Agg')

import matplotlib.pyplot as plt

import matplotlib.ticker as mticker

from matplotlib.gridspec import GridSpec

from pathlib import Path



warnings.filterwarnings('ignore', category=FutureWarning)



# ============================================================

# Style

# ============================================================

STYLE = {

    'bg': '#F8F5F0',

    'card': '#FFFFFF',

    'text': '#1B3A4B',

    'muted': '#6B7C8A',

    'blue': '#2D9CDB',

    'coral': '#E07A5F',

    'amber': '#F0B95B',

    'teal': '#4ECDC4',

    'purple': '#A78BFA',

    'green': '#6BCB77',

    'red': '#EF5350',

    'dark': '#1B3A4B',

}



def setup_style():

    plt.rcParams.update({

        'figure.facecolor': STYLE['bg'],

        'axes.facecolor': STYLE['card'],

        'axes.edgecolor': '#E0DCD4',

        'axes.labelcolor': STYLE['text'],

        'text.color': STYLE['text'],

        'xtick.color': STYLE['muted'],

        'ytick.color': STYLE['muted'],

        'grid.color': '#E0DCD4',

        'grid.alpha': 0.7,

        'font.family': 'sans-serif',

        'font.size': 10,

        'axes.titlesize': 13,

        'axes.titleweight': 'bold',

        'figure.titlesize': 16,

        'figure.titleweight': 'bold',

    })


 



def plot_11_regional_energy(df, output_dir):
    """Compare energy consumption metrics across regions to disentangle
    height errors from genuinely different energy use."""

    energy_cols = ['total_gas', 'avg_gas', 'median_gas', 'num_meters_gas',
                   'total_elec', 'avg_elec', 'median_elec', 'num_meters_elec']
    missing = [c for c in energy_cols if c not in df.columns]
    if missing:
        print(f"  Skipping regional energy plot — missing: {missing}")
        return

    valid = df[df['avg_gas'].notna() & df['clean_res_area_mode_total'].notna()].copy()
    if len(valid) < 10:
        print("  Skipping regional energy plot — insufficient data")
        return

    # Derived per-meter and per-building metrics
    valid['gas_per_building'] = valid['total_gas'] / valid['clean_res_total_buildings']
    valid['elec_per_building'] = valid['total_elec'] / valid['clean_res_total_buildings']

    # --- Figure A: Regional box plots of avg_gas, avg_elec, gas_per_meter ---
    regions_sorted = valid.groupby('_region')['avg_gas'].median().sort_values(ascending=False).index.tolist()

    fig, axes = plt.subplots(2, 3, figsize=(18, 10))
    fig.suptitle('Regional Energy Consumption Comparison', fontweight='bold', fontsize=14)

    plot_configs = [
        ('avg_gas', 'Avg Gas per Meter (kWh)', STYLE['coral'], axes[0, 0]),
        ('median_gas', 'Median Gas per Meter (kWh)', STYLE['amber'], axes[0, 1]),
        ('gas_per_building', 'Gas per Building (kWh)', STYLE['red'], axes[0, 2]),
        ('avg_elec', 'Avg Elec per Meter (kWh)', STYLE['blue'], axes[1, 0]),
        ('median_elec', 'Median Elec per Meter (kWh)', STYLE['teal'], axes[1, 1]),
        ('elec_per_building', 'Elec per Building (kWh)', STYLE['purple'], axes[1, 2]),
    ]

    for col, title, color, ax in plot_configs:
        data_by_region = [valid[valid['_region'] == r][col].dropna().clip(
            upper=valid[col].quantile(0.98)) for r in regions_sorted]
        bp = ax.boxplot(data_by_region, labels=regions_sorted, patch_artist=True, widths=0.6,
                        medianprops=dict(color=STYLE['text'], linewidth=1.5),
                        flierprops=dict(marker='.', markersize=2, alpha=0.3))
        for patch in bp['boxes']:
            patch.set_facecolor(color)
            patch.set_alpha(0.4)
        ax.set_title(title, fontsize=10, fontweight='bold')
        ax.tick_params(axis='x', labelsize=7, rotation=45)
        ax.grid(True, alpha=0.3, axis='y')

    fig.tight_layout()
    save_fig(fig, output_dir, '11a_regional_energy_boxplots')

    # CSV: regional energy summary
    reg_energy = valid.groupby('_region').agg(
        n=('postcode', 'count'),
        median_avg_gas=('avg_gas', 'median'),
        mean_avg_gas=('avg_gas', 'mean'),
        p25_avg_gas=('avg_gas', lambda x: x.quantile(0.25)),
        p75_avg_gas=('avg_gas', lambda x: x.quantile(0.75)),
        median_median_gas=('median_gas', 'median'),
        median_total_gas=('total_gas', 'median'),
        median_gas_per_bldg=('gas_per_building', 'median'),
        median_num_meters_gas=('num_meters_gas', 'median'),
        median_avg_elec=('avg_elec', 'median'),
        mean_avg_elec=('avg_elec', 'mean'),
        p25_avg_elec=('avg_elec', lambda x: x.quantile(0.25)),
        p75_avg_elec=('avg_elec', lambda x: x.quantile(0.75)),
        median_median_elec=('median_elec', 'median'),
        median_total_elec=('total_elec', 'median'),
        median_elec_per_bldg=('elec_per_building', 'median'),
        median_num_meters_elec=('num_meters_elec', 'median'),
        median_spread_pct=('spread_pct', 'median'),
        median_eui_mode=('eui_mode_gas', 'median') if 'eui_mode_gas' in valid.columns else ('avg_gas', 'count'),
    ).sort_values('median_avg_gas', ascending=False).reset_index()
    reg_energy.to_csv(os.path.join(output_dir, '11a_regional_energy_boxplots.csv'), index=False)

    # --- Figure B: Energy vs Spread — is high EUI driven by area errors? ---
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle('Energy Consumption vs Area Uncertainty — Are High-Spread Regions Just Wrong, or Different?',
                 fontweight='bold', fontsize=13)

    # 1. avg_gas vs spread, coloured by region
    ax = axes[0, 0]
    sample = valid.sample(min(5000, len(valid)), random_state=42)
    sc = ax.scatter(sample['spread_pct'].clip(0, 120), sample['avg_gas'].clip(0, sample['avg_gas'].quantile(0.98)),
                    c=sample['avg_gas'].clip(0, sample['avg_gas'].quantile(0.98)),
                    cmap='RdYlBu_r', s=6, alpha=0.4)
    ax.set_xlabel('Area Spread %')
    ax.set_ylabel('Avg Gas per Meter (kWh)')
    ax.set_title('Gas Consumption vs Area Uncertainty')
    plt.colorbar(sc, ax=ax, label='Avg Gas', shrink=0.8)
    ax.grid(True, alpha=0.3)

    # 2. Regional median avg_gas vs median spread — the key diagnostic
    ax = axes[0, 1]
    reg = valid.groupby('_region').agg(
        med_spread=('spread_pct', 'median'),
        med_gas=('avg_gas', 'median'),
        med_elec=('avg_elec', 'median'),
        n=('postcode', 'count'),
    ).reset_index()
    ax.scatter(reg['med_spread'], reg['med_gas'], s=reg['n'].clip(10, 500) * 0.3,
               color=STYLE['coral'], alpha=0.7, edgecolor=STYLE['text'], linewidth=0.5)
    for _, r in reg.iterrows():
        ax.annotate(r['_region'], (r['med_spread'], r['med_gas']),
                    fontsize=6, ha='center', va='bottom', color=STYLE['muted'])
    ax.set_xlabel('Median Spread %')
    ax.set_ylabel('Median Avg Gas (kWh)')
    ax.set_title('Regional: Gas vs Spread (size=n postcodes)')
    ax.grid(True, alpha=0.3)

    # 3. Same for electricity
    ax = axes[1, 0]
    ax.scatter(reg['med_spread'], reg['med_elec'], s=reg['n'].clip(10, 500) * 0.3,
               color=STYLE['blue'], alpha=0.7, edgecolor=STYLE['text'], linewidth=0.5)
    for _, r in reg.iterrows():
        ax.annotate(r['_region'], (r['med_spread'], r['med_elec']),
                    fontsize=6, ha='center', va='bottom', color=STYLE['muted'])
    ax.set_xlabel('Median Spread %')
    ax.set_ylabel('Median Avg Elec (kWh)')
    ax.set_title('Regional: Elec vs Spread')
    ax.grid(True, alpha=0.3)

    # 4. Low-spread vs high-spread postcodes — compare energy distributions
    ax = axes[1, 1]
    low_spread = valid[valid['spread_pct'] < 10]
    high_spread = valid[valid['spread_pct'] > 30]
    clip_gas = valid['avg_gas'].quantile(0.98)
    ax.hist(low_spread['avg_gas'].clip(0, clip_gas), bins=50, alpha=0.5, color=STYLE['green'],
            label=f'Low spread <10% (n={len(low_spread)})', edgecolor='none', density=True)
    ax.hist(high_spread['avg_gas'].clip(0, clip_gas), bins=50, alpha=0.5, color=STYLE['coral'],
            label=f'High spread >30% (n={len(high_spread)})', edgecolor='none', density=True)
    ax.set_xlabel('Avg Gas per Meter (kWh)')
    ax.set_ylabel('Density')
    ax.set_title('Gas Distribution: Low vs High Uncertainty Postcodes')
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.3)

    fig.tight_layout()
    save_fig(fig, output_dir, '11b_energy_vs_spread')
    reg.to_csv(os.path.join(output_dir, '11b_energy_vs_spread.csv'), index=False)

    # --- Figure C: Gas per meter vs meters/buildings ratio ---
    # This shows if London's high EUI is from genuinely high consumption
    # or from meter-to-building mismatch (flats sharing meters)
    fig, axes = plt.subplots(1, 3, figsize=(16, 5))
    fig.suptitle('Meter-to-Building Ratio — Are Meters Shared?', fontweight='bold', fontsize=14)

    valid['meters_per_building'] = valid['num_meters_gas'] / valid['clean_res_total_buildings']

    ax = axes[0]
    reg_meters = valid.groupby('_region')['meters_per_building'].median().sort_values()
    colors = [STYLE['coral'] if v < 0.8 else STYLE['green'] if v > 1.1 else STYLE['muted'] for v in reg_meters]
    reg_meters.plot(kind='barh', ax=ax, color=colors, edgecolor='none')
    ax.axvline(1.0, color=STYLE['text'], linewidth=1, linestyle='--', alpha=0.5, label='1:1')
    ax.set_xlabel('Median Gas Meters per Building')
    ax.set_title('By Region')
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.3, axis='x')
    ax.tick_params(axis='y', labelsize=8)

    ax = axes[1]
    sample = valid.sample(min(5000, len(valid)), random_state=42)
    sc = ax.scatter(sample['meters_per_building'].clip(0, 3),
                    sample['avg_gas'].clip(0, sample['avg_gas'].quantile(0.98)),
                    c=sample['spread_pct'].clip(0, 80), cmap='RdYlBu_r', s=6, alpha=0.4)
    ax.set_xlabel('Gas Meters per Building')
    ax.set_ylabel('Avg Gas per Meter (kWh)')
    ax.set_title('Consumption vs Meter Density')
    plt.colorbar(sc, ax=ax, label='Spread %', shrink=0.8)
    ax.grid(True, alpha=0.3)

    ax = axes[2]
    reg2 = valid.groupby('_region').agg(
        med_meters_ratio=('meters_per_building', 'median'),
        med_spread=('spread_pct', 'median'),
    ).reset_index()
    ax.scatter(reg2['med_meters_ratio'], reg2['med_spread'],
               s=80, color=STYLE['purple'], alpha=0.7, edgecolor=STYLE['text'], linewidth=0.5)
    for _, r in reg2.iterrows():
        ax.annotate(r['_region'], (r['med_meters_ratio'], r['med_spread']),
                    fontsize=6, ha='center', va='bottom', color=STYLE['muted'])
    ax.set_xlabel('Median Meters per Building')
    ax.set_ylabel('Median Spread %')
    ax.set_title('Regional: Meter Ratio vs Uncertainty')
    ax.grid(True, alpha=0.3)

    fig.tight_layout()
    save_fig(fig, output_dir, '11c_meter_building_ratio')
    valid[['postcode', '_region', 'meters_per_building', 'avg_gas', 'avg_elec',
           'num_meters_gas', 'clean_res_total_buildings', 'spread_pct']].to_csv(
        os.path.join(output_dir, '11c_meter_building_ratio.csv'), index=False)

 
 
 
def plot_11_energy_distributions(df, output_dir):

    """

    Box plots for Gas/Elec EUI and Per-Meter distributions across regions,

    and granular London sensitivity analysis.

    """

    metrics = []

    if 'eui_mode_gas' in df.columns:

        metrics.extend([('eui_mode_gas', 'Gas EUI (kWh/m²)', STYLE['coral']), 

                        ('avg_gas', 'Avg Gas per Meter (kWh)', STYLE['amber'])])

    if 'eui_mode_elec' in df.columns:

        metrics.extend([('eui_mode_elec', 'Elec EUI (kWh/m²)', STYLE['blue']), 

                        ('avg_elec', 'Avg Elec per Meter (kWh)', STYLE['teal'])])



    if not metrics:
        print('missing metrics') 
        return



    # --- Regional Boxplots ---

    fig, axes = plt.subplots(2, 2, figsize=(18, 12))

    axes = axes.flatten()


    regions = df.groupby('_region')['avg_gas'].median().sort_values(ascending=False).index.tolist()


    for i, (col, label, color) in enumerate(metrics):

        plot_data = [df[df['_region'] == r][col].dropna() for r in regions]

        bp = axes[i].boxplot(plot_data, labels=regions, patch_artist=True, showfliers=False)

        for patch in bp['boxes']:

            patch.set_facecolor(color)

            patch.set_alpha(0.6)

        axes[i].set_title(f"Regional {label}")

        axes[i].tick_params(axis='x', rotation=45, labelsize=8)

        axes[i].grid(True, alpha=0.3, axis='y')



    fig.tight_layout(rect=[0, 0.03, 1, 0.95])

    save_fig(fig, output_dir, '11_regional_energy_boxplots')



    # --- London Sensitivity (4-Bar Comparison) ---

    london = df[df['_region'].str.lower().str.contains('ln', na=False)].copy()

    if london.empty:

        return



    fig, axes = plt.subplots(2, 2, figsize=(16, 12))

    fig.suptitle('London Energy Metrics: Sensitivity to Uncertainty Spread', fontweight='bold')

    axes = axes.flatten()



    for i, (col, label, color) in enumerate(metrics):

        # Define the 4 filtering stages

        data_groups = [

            london[col].dropna(),                                      # All

            london[london['spread_pct'] <= 30][col].dropna(),         # Excl High/V.High

            london[london['spread_pct'] <= 15][col].dropna(),         # Excl Medium+

            london[london['spread_pct'] <= 5][col].dropna()           # Tight Only

        ]

        

        labels = ['All London', 'Excl. High (>30%)', 'Excl. Med+ (>15%)', 'Tight Only (<5%)']

        

        bp = axes[i].boxplot(data_groups, labels=labels, patch_artist=True, showfliers=False, widths=0.6)

        

        # Varying alpha to show "thinning" of data

        alphas = [0.3, 0.5, 0.7, 0.9]

        for patch, a in zip(bp['boxes'], alphas):

            patch.set_facecolor(color)

            patch.set_alpha(a)

            

        axes[i].set_title(label)

        axes[i].set_ylabel('Value')

        axes[i].grid(True, alpha=0.3, axis='y')



    fig.tight_layout(rect=[0, 0.03, 1, 0.95])

    save_fig(fig, output_dir, '12_london_sensitivity_granular')



def plot_17_london_correlation_reversal(df, output_dir):
    """
    Visualize how the spread-energy correlation changes with filtering.
    Key diagnostic for distinguishing real consumption from measurement error.
    """
    london = df[df['_region'].str.lower().str.contains('ln', na=False)].copy()
    
    if london.empty:
        return
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('London: Spread-Energy Correlation Reversal Analysis', 
                 fontweight='bold', fontsize=14)
    
    filters = [
        (london, 'All London', STYLE['coral'], axes[0, 0]),
        (london[london['spread_pct'] <= 30], 'Excl. High/V.High (≤30%)', STYLE['amber'], axes[0, 1]),
        (london[london['spread_pct'] <= 15], 'Excl. Medium+ (≤15%)', STYLE['blue'], axes[1, 0]),
        (london[london['spread_pct'] <= 5], 'Tight Only (≤5%)', STYLE['green'], axes[1, 1])
    ]
    
    for filtered_data, label, color, ax in filters:
        valid = filtered_data[['spread_pct', 'avg_gas']].dropna()
        
        if len(valid) > 100:
            # Sample for visualization
            sample = valid.sample(min(3000, len(valid)), random_state=42)
            
            # Scatter
            ax.scatter(sample['spread_pct'], sample['avg_gas'].clip(0, sample['avg_gas'].quantile(0.98)),
                      s=3, alpha=0.3, color=color)
            
            # Trend line
            z = np.polyfit(valid['spread_pct'], valid['avg_gas'], 1)
            p = np.poly1d(z)
            x_trend = np.linspace(valid['spread_pct'].min(), valid['spread_pct'].max(), 100)
            ax.plot(x_trend, p(x_trend), '--', color=STYLE['text'], linewidth=2, 
                   label=f'Trend (r={valid.corr().iloc[0,1]:.3f})')
            
            # Median line
            ax.axhline(valid['avg_gas'].median(), color=STYLE['dark'], 
                      linestyle=':', linewidth=1.5, alpha=0.7,
                      label=f'Median: {valid["avg_gas"].median():.0f} kWh')
            
            ax.set_xlabel('Spread %')
            ax.set_ylabel('Avg Gas per Meter (kWh)')
            ax.set_title(f'{label} (n={len(valid):,})', fontweight='bold')
            ax.legend(fontsize=8)
            ax.grid(True, alpha=0.3)
    
    fig.tight_layout()
    save_fig(fig, output_dir, '17_london_correlation_reversal')
    
import seaborn as sns

def plot_14_regional_correlation_heatmaps(df, output_dir):
    """
    Generates a correlation heatmap for each region for a specific set of metrics.
    Helps identify if area errors are driving EUI results differently by region.
    """
    # Define columns to correlate
    cols_to_corr = [
        'clean_res_area_mode_total', 
        'spread_pct', 
        'implied_avg_floors',
        'avg_premise_area',
        'clean_res_total_buildings',
        'avg_gas', 
        'avg_elec'
    ]
    
    # Filter for columns that actually exist
    valid_cols = [c for c in cols_to_corr if c in df.columns]
    regions = sorted(df['_region'].unique())
    
    # Create a sub-directory for these heatmaps as they can be numerous
    corr_dir = os.path.join(output_dir, '14_regional_correlations')
    os.makedirs(corr_dir, exist_ok=True)

    print(f"Generating correlation heatmaps for {len(regions)} regions...")

    for region in regions:
        reg_data = df[df['_region'] == region][valid_cols].dropna()
        
        if len(reg_data) < 10:
            continue
            
        # Calculate Correlation Matrix
        corr_matrix = reg_data.corr()
        
        # Plotting
        fig, ax = plt.subplots(figsize=(10, 8))
        
        # Mask the upper triangle for better readability
        mask = np.triu(np.ones_like(corr_matrix, dtype=bool))
        
        sns.heatmap(
            corr_matrix, 
            mask=mask, 
            annot=True, 
            fmt=".2f", 
            cmap='coolwarm', 
            vmin=-1, vmax=1, center=0,
            linewidths=.5, 
            cbar_kws={"shrink": .8},
            ax=ax
        )
        
        ax.set_title(f'Feature Correlation: {region} (n={len(reg_data)})', fontweight='bold')
        
        # Save figure and data
        safe_name = region.replace(' ', '_').lower()
        save_fig(fig, corr_dir, f'corr_{safe_name}')
        corr_matrix.to_csv(os.path.join(corr_dir, f'corr_{safe_name}.csv'))

    print(f"  Saved regional correlation heatmaps to: {corr_dir}")
    
    
def plot_15_london_filtered_correlations(df, output_dir):
    """
    Correlation analysis for London data at different uncertainty filtering levels.
    Shows how relationships between metrics change as we remove high-spread postcodes.
    """
    london = df[df['_region'].str.lower().str.contains('ln', na=False)].copy()
    
    if london.empty:
        print("  Skipping London filtered correlations — no London data found")
        return
    
    # Define columns to correlate
    cols_to_corr = [
        'clean_res_area_mode_total', 
        'spread_pct', 
        'implied_avg_floors',
        'avg_premise_area',
        'clean_res_total_buildings',
        'avg_gas', 
        'avg_elec',
        'eui_mode_gas',
        'eui_mode_elec'
    ]
    
    # Filter for columns that actually exist
    valid_cols = [c for c in cols_to_corr if c in london.columns]
    
    # Define filtering stages
    filters = [
        ('all', london, 'All London'),
        ('excl_high', london[london['spread_pct'] <= 30], 'Excl. High/V.High (≤30%)'),
        ('excl_medium', london[london['spread_pct'] <= 15], 'Excl. Medium+ (≤15%)'),
        ('tight_only', london[london['spread_pct'] <= 5], 'Tight Only (≤5%)')
    ]
    
    # Create directory for outputs
    london_dir = os.path.join(output_dir, '15_london_filtered_correlations')
    os.makedirs(london_dir, exist_ok=True)
    
    # Generate heatmap for each filtering stage
    fig, axes = plt.subplots(2, 2, figsize=(20, 18))
    fig.suptitle('London Correlation Analysis: Impact of Removing High-Spread Postcodes', 
                 fontweight='bold', fontsize=16, y=0.995)
    axes = axes.flatten()
    
    for idx, (filter_name, filtered_data, label) in enumerate(filters):
        reg_data = filtered_data[valid_cols].dropna()
        
        if len(reg_data) < 10:
            axes[idx].text(0.5, 0.5, f'Insufficient data\n(n={len(reg_data)})', 
                          ha='center', va='center', fontsize=12)
            axes[idx].set_title(f'{label} (n={len(reg_data)})', fontweight='bold')
            continue
        
        # Calculate correlation matrix
        corr_matrix = reg_data.corr()
        
        # Mask upper triangle
        mask = np.triu(np.ones_like(corr_matrix, dtype=bool))
        
        # Plot heatmap
        sns.heatmap(
            corr_matrix, 
            mask=mask, 
            annot=True, 
            fmt=".2f", 
            cmap='coolwarm', 
            vmin=-1, vmax=1, center=0,
            linewidths=.5, 
            cbar_kws={"shrink": .8},
            ax=axes[idx],
            annot_kws={'fontsize': 8}
        )
        
        axes[idx].set_title(f'{label} (n={len(reg_data):,})', fontweight='bold', fontsize=12)
        axes[idx].tick_params(axis='both', labelsize=9)
        
        # Save individual correlation matrix to CSV
        corr_matrix.to_csv(os.path.join(london_dir, f'corr_london_{filter_name}.csv'))
    
    fig.tight_layout()
    save_fig(fig, london_dir, 'london_correlation_progression')
    
    # Generate comparative table showing how key correlations change
    comparison_data = []
    target_pairs = [
        ('spread_pct', 'avg_gas', 'Spread vs Avg Gas'),
        ('spread_pct', 'eui_mode_gas', 'Spread vs Gas EUI'),
        ('spread_pct', 'avg_elec', 'Spread vs Avg Elec'),
        ('spread_pct', 'eui_mode_elec', 'Spread vs Elec EUI'),
        ('avg_gas', 'eui_mode_gas', 'Avg Gas vs Gas EUI'),
        ('clean_res_area_mode_total', 'avg_gas', 'Area vs Avg Gas'),
        ('implied_avg_floors', 'avg_gas', 'Floors vs Avg Gas'),
    ]
    
    for filter_name, filtered_data, label in filters:
        reg_data = filtered_data[valid_cols].dropna()
        
        if len(reg_data) < 10:
            continue
            
        corr_matrix = reg_data.corr()
        
        row = {'filter': label, 'n': len(reg_data)}
        
        for col1, col2, pair_label in target_pairs:
            if col1 in corr_matrix.index and col2 in corr_matrix.columns:
                row[pair_label] = corr_matrix.loc[col1, col2]
            else:
                row[pair_label] = np.nan
        
        comparison_data.append(row)
    
    comparison_df = pd.DataFrame(comparison_data)
    comparison_df.to_csv(os.path.join(london_dir, 'correlation_progression_summary.csv'), index=False)
    
    # Visualize correlation changes
    if len(comparison_df) > 0:
        fig, ax = plt.subplots(figsize=(14, 8))
        
        x_pos = np.arange(len(comparison_df))
        width = 0.12
        
        colors = [STYLE['coral'], STYLE['amber'], STYLE['blue'], STYLE['teal'], 
                 STYLE['purple'], STYLE['green'], STYLE['red']]
        
        for idx, (_, _, pair_label) in enumerate(target_pairs):
            if pair_label in comparison_df.columns:
                offset = (idx - len(target_pairs)/2) * width
                ax.bar(x_pos + offset, comparison_df[pair_label], 
                      width, label=pair_label, color=colors[idx % len(colors)], alpha=0.8)
        
        ax.set_xlabel('Filtering Level', fontsize=12, fontweight='bold')
        ax.set_ylabel('Correlation Coefficient', fontsize=12, fontweight='bold')
        ax.set_title('How Key Correlations Change with Spread Filtering', 
                    fontsize=14, fontweight='bold')
        ax.set_xticks(x_pos)
        ax.set_xticklabels(comparison_df['filter'], rotation=15, ha='right')
        ax.axhline(y=0, color=STYLE['text'], linestyle='--', linewidth=1, alpha=0.5)
        ax.legend(fontsize=9, loc='upper left', bbox_to_anchor=(1, 1))
        ax.grid(True, alpha=0.3, axis='y')
        
        fig.tight_layout()
        save_fig(fig, london_dir, 'correlation_progression_bars')
    
    print(f"  London filtered correlations saved to: {london_dir}")
    print("\n=== CORRELATION PROGRESSION SUMMARY ===")
    print(comparison_df.to_string(index=False))


def plot_16_london_energy_diagnostics(df, output_dir):
    """
    Detailed energy diagnostics for London at different filtering levels.
    Helps determine if high EUI is real or an artifact of area errors.
    """
    london = df[df['_region'].str.lower().str.contains('ln', na=False)].copy()
    
    if london.empty or 'avg_gas' not in london.columns:
        print("  Skipping London energy diagnostics — insufficient data")
        return
    
    # Define filtering stages
    filters = [
        ('all', london, 'All London', STYLE['coral']),
        ('excl_high', london[london['spread_pct'] <= 30], 'Excl. High/V.High', STYLE['amber']),
        ('excl_medium', london[london['spread_pct'] <= 15], 'Excl. Medium+', STYLE['blue']),
        ('tight_only', london[london['spread_pct'] <= 5], 'Tight Only', STYLE['green'])
    ]
    
    london_dir = os.path.join(output_dir, '16_london_energy_diagnostics')
    os.makedirs(london_dir, exist_ok=True)
    
    # Summary statistics for each filtering level
    summary_data = []
    
    for filter_name, filtered_data, label, _ in filters:
        for metric in ['avg_gas', 'avg_elec', 'eui_mode_gas', 'eui_mode_elec']:
            if metric not in filtered_data.columns:
                continue
                
            data = filtered_data[metric].dropna()
            
            if len(data) > 0:
                summary_data.append({
                    'filter': label,
                    'metric': metric,
                    'n': len(data),
                    'mean': data.mean(),
                    'median': data.median(),
                    'std': data.std(),
                    'p25': data.quantile(0.25),
                    'p75': data.quantile(0.75),
                    'p95': data.quantile(0.95),
                })
    
    summary_df = pd.DataFrame(summary_data)
    summary_df.to_csv(os.path.join(london_dir, 'energy_summary_by_filter.csv'), index=False)
    
    # Visualization: Distribution changes
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('London Energy Distributions: Impact of Spread Filtering', 
                 fontweight='bold', fontsize=14)
    
    metrics_to_plot = [
        ('avg_gas', 'Avg Gas per Meter (kWh)', axes[0, 0]),
        ('avg_elec', 'Avg Elec per Meter (kWh)', axes[0, 1]),
        ('eui_mode_gas', 'Gas EUI (kWh/m²)', axes[1, 0]),
        ('eui_mode_elec', 'Elec EUI (kWh/m²)', axes[1, 1])
    ]
    
    for metric, label, ax in metrics_to_plot:
        if metric not in london.columns:
            continue
        
        for filter_name, filtered_data, filter_label, color in filters:
            data = filtered_data[metric].dropna()
            
            if len(data) > 5:
                clip_val = data.quantile(0.98)
                data_clipped = data[data <= clip_val]
                
                ax.hist(data_clipped, bins=50, alpha=0.4, color=color, 
                       label=f'{filter_label} (n={len(data):,})', 
                       edgecolor='none', density=True)
        
        ax.set_xlabel(label, fontsize=10)
        ax.set_ylabel('Density', fontsize=10)
        ax.set_title(label, fontsize=11, fontweight='bold')
        ax.legend(fontsize=8)
        ax.grid(True, alpha=0.3)
    
    fig.tight_layout()
    save_fig(fig, london_dir, 'energy_distributions_filtered')
    
    # Statistical comparison table
    print("\n=== LONDON ENERGY DIAGNOSTICS ===")
    for metric in ['avg_gas', 'avg_elec', 'eui_mode_gas', 'eui_mode_elec']:
        metric_summary = summary_df[summary_df['metric'] == metric]
        if not metric_summary.empty:
            print(f"\n{metric}:")
            print(metric_summary[['filter', 'n', 'median', 'mean', 'p95']].to_string(index=False))
    
    return summary_df
    
    
def plot_18_regional_boxplots_filtered(df, output_dir):
    """
    4 separate box plots per filtering level, one metric each.
    - Consistent y-axis scale across filters for the same metric
    - Colorbar showing the median value scale
    - Grand median line with value label
    - Per-box median labels (white or black depending on box colour)
    Saves to subfolders: 18_unfiltered/ and 18_excl_high_spread/
    No titles. Fliers excluded.
    """

    metrics = [
        ('avg_gas',       'Avg Gas per Meter (kWh)'),
        ('avg_elec',      'Avg Elec per Meter (kWh)'),
        ('eui_mode_gas',  'Gas EUI — Mode (kWh/m²)'),
        ('eui_mode_elec', 'Elec EUI — Mode (kWh/m²)'),
    ]
    metrics = [(col, label) for col, label in metrics if col in df.columns]
    if not metrics:
        print("  Skipping regional boxplots — no energy columns found")
        return

    filter_configs = [
        ('18_unfiltered',
         df,
         'unfiltered'),
        ('18_excl_high_spread',
         df[~df['spread_severity'].isin(['High (30-60%)', 'Very High (>60%)'])],
         'excl_high_spread'),
    ]

    # ------------------------------------------------------------------
    # Pre-compute consistent y-axis limits PER METRIC across both filters
    # and global median value range for a shared colormap
    # ------------------------------------------------------------------
    y_limits   = {}   # col -> (ymin, ymax)
    norm_limits = {}  # col -> (vmin, vmax) for colormap

    for col, _ in metrics:
        all_vals = df[col].dropna()
        if len(all_vals) == 0:
            continue
        # y-axis: 0 to 98th-percentile of the unfiltered data so scale is fair
        ymax = all_vals.quantile(0.98)
        y_limits[col] = (0, ymax * 1.08)   # small headroom for labels

        # colour range: min/max of regional medians across BOTH filter sets
        all_medians = []
        for _, fdata, _ in filter_configs:
            for r in fdata['_region'].unique():
                d = fdata[fdata['_region'] == r][col].dropna()
                if len(d) > 0:
                    all_medians.append(d.median())
        norm_limits[col] = (min(all_medians), max(all_medians))

    # Consistent region order: median avg_gas descending (unfiltered)
    region_order = (
        df.groupby('_region')['avg_gas']
        .median()
        .sort_values(ascending=False)
        .index.tolist()
        if 'avg_gas' in df.columns
        else sorted(df['_region'].unique())
    )
    n_regions = len(region_order)
    fig_width  = max(14, n_regions * 0.9)

    cmap = plt.cm.RdYlBu_r

    def _label_colour(rgba):
        """Return 'white' or 'black' depending on perceived brightness of rgba."""
        r, g, b, _ = rgba
        # Standard luminance formula
        luminance = 0.299 * r + 0.587 * g + 0.114 * b
        return 'white' if luminance < 0.5 else 'black'

    for folder_name, data, suffix in filter_configs:

        out_dir = os.path.join(output_dir, folder_name)
        os.makedirs(out_dir, exist_ok=True)

        for col, ylabel in metrics:

            if col not in y_limits:
                continue

            plot_data = [
                data[data['_region'] == r][col].dropna()
                for r in region_order
            ]

            if all(len(d) == 0 for d in plot_data):
                print(f"  Skipping {col} ({suffix}) — no data")
                continue

            # ── figure ────────────────────────────────────────────────
            fig, ax = plt.subplots(figsize=(fig_width, 7))
            fig.patch.set_facecolor(STYLE['bg'])
            ax.set_facecolor(STYLE['card'])

            bp = ax.boxplot(
                plot_data,
                labels=region_order,
                patch_artist=True,
                showfliers=False,
                widths=0.55,
                medianprops=dict(color=STYLE['text'], linewidth=2),
                whiskerprops=dict(color=STYLE['muted'], linewidth=1.2),
                capprops=dict(color=STYLE['muted'], linewidth=1.2),
                boxprops=dict(linewidth=1.2),
            )

            # ── colour boxes using the SHARED norm for this metric ─────
            vmin, vmax = norm_limits[col]
            norm = plt.Normalize(vmin=vmin, vmax=vmax)

            medians = [d.median() if len(d) > 0 else np.nan for d in plot_data]

            for patch, med in zip(bp['boxes'], medians):
                if np.isnan(med):
                    continue
                rgba = cmap(norm(med))
                patch.set_facecolor(rgba)
                patch.set_alpha(0.85)

            # ── grand median line ──────────────────────────────────────
            all_vals = pd.concat(plot_data) if any(len(d) > 0 for d in plot_data) else pd.Series(dtype=float)
            grand_median = all_vals.median()
            ymin_ax, ymax_ax = y_limits[col]

            ax.axhline(
                grand_median,
                color=STYLE['dark'],
                linewidth=1.4,
                linestyle='--',
                alpha=0.75,
                zorder=3,
            )
            ax.text(
                n_regions + 0.55,
                grand_median,
                f'Overall median\n{grand_median:,.0f}',
                va='center',
                ha='left',
                fontsize=8,
                color=STYLE['dark'],
                zorder=4,
            )

            # ── per-box median label ───────────────────────────────────
            for i, (med, patch, d) in enumerate(zip(medians, bp['boxes'], plot_data)):
                if np.isnan(med) or len(d) == 0:
                    continue

                rgba      = cmap(norm(med))
                txt_color = _label_colour(rgba)
                x_pos     = i + 1

                # position: just above the median line
                # but cap so it doesn't escape the axes
                label_y = min(med * 1.04, ymax_ax * 0.97)

                ax.text(
                    x_pos,
                    label_y,
                    f'{med:,.0f}',
                    ha='center',
                    va='bottom',
                    fontsize=7,
                    fontweight='bold',
                    color=txt_color,
                    zorder=5,
                )

            # ── axes formatting ────────────────────────────────────────
            ax.set_ylim(y_limits[col])
            ax.set_ylabel(ylabel, fontsize=12, color=STYLE['text'])
            ax.tick_params(axis='x', rotation=45, labelsize=9, colors=STYLE['muted'])
            ax.tick_params(axis='y', labelsize=9,  colors=STYLE['muted'])
            ax.grid(True, alpha=0.35, axis='y', color='#E0DCD4')
            ax.set_axisbelow(True)

            for spine in ax.spines.values():
                spine.set_edgecolor('#E0DCD4')

            # ── n= annotations below boxes ────────────────────────────
            for i, d in enumerate(plot_data):
                ax.text(
                    i + 1,
                    ymin_ax,
                    f'n={len(d):,}',
                    ha='center',
                    va='bottom',
                    fontsize=6,
                    color=STYLE['muted'],
                )

            # ── colorbar ──────────────────────────────────────────────
            sm  = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
            sm.set_array([])
            cbar = fig.colorbar(sm, ax=ax, shrink=0.6, pad=0.01, aspect=30)
            cbar.set_label(f'Median {ylabel}', fontsize=9, color=STYLE['text'])
            cbar.ax.tick_params(labelsize=8, colors=STYLE['muted'])
            cbar.outline.set_edgecolor('#E0DCD4')

            fig.tight_layout()

            fname = f'{col}_{suffix}'
            fpath = os.path.join(out_dir, f'{fname}.png')
            fig.savefig(fpath, dpi=150, bbox_inches='tight',
                        facecolor=fig.get_facecolor())
            plt.close(fig)
            print(f"  Saved: {folder_name}/{fname}.png")

            # ── companion CSV ──────────────────────────────────────────
            csv_rows = []
            for region, d in zip(region_order, plot_data):
                if len(d) == 0:
                    continue
                csv_rows.append({
                    'region':  region,
                    'metric':  col,
                    'filter':  suffix,
                    'n':       len(d),
                    'median':  d.median(),
                    'mean':    d.mean(),
                    'p25':     d.quantile(0.25),
                    'p75':     d.quantile(0.75),
                    'p05':     d.quantile(0.05),
                    'p95':     d.quantile(0.95),
                })
            pd.DataFrame(csv_rows).to_csv(
                os.path.join(out_dir, f'{fname}.csv'), index=False
            )
            
            
# ============================================================

# significnae testing with mann whitney u for lonfg tail ewnergy data 

# ============================================================



from scipy.stats import mannwhitneyu

 

def test_regional_significance(df, output_dir, metric ):
    """
    Performs pairwise Mann-Whitney U tests with Bonferroni correction 
    and Rank-Biserial Correlation as effect size, ordered by the metric value.
    """
    
    
    # --- NEW: Sort regions by the median of the metric ---
    # This ensures the heatmap shows a logical progression (e.g., low EUI to high EUI)
    region_stats = df.groupby('_region')[metric].median().sort_values()
    regions = region_stats.index.tolist()
    # ----------------------------------------------------

    n_regions = len(regions)
    n_comparisons = (n_regions * (n_regions - 1)) // 2
    bonferroni_threshold = 0.05 / n_comparisons

    # Matrices for visualization (now using the sorted 'regions' list)
    p_matrix = pd.DataFrame(np.nan, index=regions, columns=regions)
    effect_matrix = pd.DataFrame(0.0, index=regions, columns=regions)
    
    results = []

    print(f" Running {n_comparisons} pairwise tests for {metric}...")
    print(f" Bonferroni corrected alpha: {bonferroni_threshold:.6f}")
    
    for i in range(n_regions):
        for j in range(i + 1, n_regions):
            reg_a, reg_b = regions[i], regions[j]
            
            group_a = df[df['_region'] == reg_a][metric].dropna()
            group_b = df[df['_region'] == reg_b][metric].dropna()
            
            n1, n2 = len(group_a), len(group_b)
            
            if n1 > 5 and n2 > 5:
                stat, p_val = mannwhitneyu(group_a, group_b, alternative='two-sided')
                
                # Rank-Biserial Correlation (Effect Size)
                # r = 1 - (2U / (n1 * n2))
                effect_size = 1 - (2 * stat / (n1 * n2))
                
                p_matrix.loc[reg_a, reg_b] = p_val
                p_matrix.loc[reg_b, reg_a] = p_val
                
                # We use the order in the sorted list to keep directionality consistent
                effect_matrix.loc[reg_a, reg_b] = effect_size
                effect_matrix.loc[reg_b, reg_a] = -effect_size 
                
                results.append({
                    'region_1': reg_a,
                    'region_2': reg_b,
                    'p_value': p_val,
                    'significant_bonferroni': p_val < bonferroni_threshold,
                    'effect_size_r_rb': effect_size,
                    'abs_effect_strength': 'Large' if abs(effect_size) > 0.5 else 
                                           'Medium' if abs(effect_size) > 0.3 else 
                                           'Small' if abs(effect_size) > 0.1 else 'Negligible',
                    'n1': n1,
                    'n2': n2
                })

    # Save Results
    results_df = pd.DataFrame(results)
    results_df.to_csv(os.path.join(output_dir, f'stats_{metric}_significance.csv'), index=False)

    # Visualization: Effect Size Heatmap
    fig, ax = plt.subplots(figsize=(12, 10))
    
    # Masking the upper triangle
    mask = np.triu(np.ones_like(effect_matrix, dtype=bool))
    
    sns.heatmap(effect_matrix, mask=mask, annot=True, fmt=".2f", cmap='RdBu_r', 
                center=0, cbar_kws={'label': 'Rank-Biserial Correlation ($r_{rb}$)'}, ax=ax)
    
    ax.set_title(f'Regional Effect Sizes: {metric} (Ordered by Median)\n(Positive = Row Higher, Negative = Column Higher)', 
                  fontweight='bold')
    
    # Assuming save_fig is a helper function you have defined elsewhere
    if 'save_fig' in globals():
        save_fig(fig, output_dir, f'13_effect_size_heatmap_{metric}')
    else:
        plt.savefig(os.path.join(output_dir, f'13_effect_size_heatmap_{metric}.png'))


# ============================================================

# Summary distirbutions 

# ============================================================



def compute_distribution_table(df, output_dir):
    """
    Produces a clean tabular summary of the distribution for key metrics.
    Useful for 'Table 1' style reporting in documents.
    """
    # Define the metrics you want to summarize
    target_metrics = [
        'clean_res_area_mode_total', 
        'clean_res_scaled_area_mode_total',
        'spread_pct', 
        'implied_avg_floors',
        'eui_mode_gas', 
        'eui_mode_elec',
        'avg_gas',
        'avg_elec'
    ]
    
    # Filter only for metrics that actually exist in the dataframe
    cols = [c for c in target_metrics if c in df.columns]
    
    if not cols:
        print("  Skipping distribution table — no target columns found.")
        return

    # Calculate statistics
    stats = df[cols].describe(percentiles=[.05, .25, .50, .75, .95]).T
    
    # Add extra useful columns
    stats['skew'] = df[cols].skew()
    stats['null_pct'] = (df[cols].isna().sum() / len(df)) * 100
    
    # Rename for clarity
    stats = stats.rename(columns={
        'count': 'N',
        'mean': 'Mean',
        'std': 'Std Dev',
        'min': 'Min',
        '5%': 'P05',
        '25%': 'P25',
        '50%': 'Median',
        '75%': 'P75',
        '95%': 'P95',
        'max': 'Max'
    })

    # Save to CSV
    output_path = os.path.join(output_dir, '00_metrics_distribution_table.csv')
    stats.to_csv(output_path)
    
    print("\n=== METRICS DISTRIBUTION TABLE ===")
    print(stats[['N', 'Mean', 'Median', 'P95', 'null_pct']].round(2).to_string())
    
    return stats
# ============================================================

# Main

# ============================================================



def main():

    parser = argparse.ArgumentParser(description='Validate and visualise fuel calculation results')

    parser.add_argument('--data_dir', required=True, help='Path to intermediate_data/fuel directory')

    parser.add_argument('--output_dir', required=True, help='Path to save outputs')

    args = parser.parse_args()



    os.makedirs(args.output_dir, exist_ok=True)

    setup_style()



    print("=" * 60)

    print("DISCOVERING AND LOADING DATA")

    print("=" * 60)

    files = discover_batch_files(args.data_dir)

    if not files:

        print(f"No batch files found in {args.data_dir}")

        sys.exit(1)



    df, load_errors = load_all_batches(files)

    if load_errors:

        pd.DataFrame(load_errors).to_csv(os.path.join(args.output_dir, 'load_errors.csv'), index=False)



    validate_schema(df)



    print("\nDeriving analysis metrics...")

    df = derive_metrics(df)



    print("\n" + "=" * 60)

    print("RUNNING VALIDATION CHECKS")

    print("=" * 60)

    run_validation(df, args.output_dir)



    print("\n" + "=" * 60)

    print("COMPUTING SUMMARIES")

    print("=" * 60)

    compute_national_summary(df, args.output_dir)

    regional_df = compute_regional_summary(df, args.output_dir)



    print("\n" + "=" * 60)

    print("GENERATING FIGURES")

    print("=" * 60)

    plot_18_regional_boxplots_filtered(df, args.output_dir)
    plot_17_london_correlation_reversal(df, args.output_dir)

    plot_01_national_distributions(df, args.output_dir)

    plot_02_spread_severity(df, args.output_dir)

    plot_03_regional_comparison(regional_df, args.output_dir)

    plot_04_global_vs_raw(df, args.output_dir)

    plot_05_implied_floors(df, args.output_dir)

    plot_06_uncertainty_vs_size(df, args.output_dir)

    plot_07_regional_facets(df, args.output_dir)

    plot_08_eui_impact(df, args.output_dir)

    plot_09_outcode_clustering(df, args.output_dir)

    plot_10_footprint_analysis(df, args.output_dir)


    plot_11_energy_distributions(df, args.output_dir)
    
    
    plot_11_regional_energy(df, args.output_dir)
    plot_14_regional_correlation_heatmaps(df, args.output_dir)
    plot_15_london_filtered_correlations(df, args.output_dir)
    plot_16_london_energy_diagnostics(df, args.output_dir)
    
    
    test_regional_significance(df, args.output_dir, metric = 'eui_mode_gas' )
    test_regional_significance(df, args.output_dir, metric = 'eui_mode_elec' )
    test_regional_significance(df, args.output_dir, metric = 'avg_gas' )
    test_regional_significance(df, args.output_dir, metric = 'avg_elec' )
    
    compute_distribution_table(df, args.output_dir  ) 
    
    print("\n" + "=" * 60)

    print(f"COMPLETE — all outputs in {args.output_dir}")

    print("=" * 60)

if __name__ == '__main__':
    main()