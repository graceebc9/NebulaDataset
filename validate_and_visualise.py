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

    valid['eui_mode'] = valid['total_gas'] / valid['clean_res_scaled_area_mode_total']
    valid['eui_upper'] = valid['total_gas'] / valid['clean_res_scaled_area_min_total']
    valid['eui_lower'] = valid['total_gas'] / valid['clean_res_scaled_area_max_total']
    valid['eui_range_pct'] = (valid['eui_upper'] - valid['eui_lower']) / valid['eui_mode'] * 100

    fig, axes = plt.subplots(1, 3, figsize=(16, 5))
    fig.suptitle('Impact on Gas EUI Uncertainty', fontweight='bold', fontsize=14)

    ax = axes[0]
    for col, name, color in [('eui_lower', 'Lower', STYLE['blue']), ('eui_mode', 'Mode', STYLE['dark']),
                              ('eui_upper', 'Upper', STYLE['coral'])]:
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
    valid[['postcode', '_region', 'eui_mode', 'eui_lower', 'eui_upper',
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

    print("\n" + "=" * 60)
    print(f"COMPLETE — all outputs in {args.output_dir}")
    print("=" * 60)


if __name__ == '__main__':
    main()