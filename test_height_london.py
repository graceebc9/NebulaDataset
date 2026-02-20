"""
Height Heterogeneity Analysis
==============================
Tests the hypothesis that low-rise buildings near taller buildings
get worse area confidence because the height estimation is contaminated
by neighbouring tall structures (e.g. LiDAR/DSM bleed, footprint errors).

Approach:
1. For each postcode, calculate the height heterogeneity of nearby postcodes
   (within a radius) using the typology mix as a proxy for height variation
2. Focus on postcodes dominated by low-rise typologies
3. Test whether nearby tall-building presence predicts lower confidence
   in those low-rise postcodes
4. Compare this effect in London vs elsewhere
"""

import pandas as pd
import numpy as np
import statsmodels.api as sm
from scipy.spatial import cKDTree
from scipy import stats
import warnings
import os
import sys

warnings.filterwarnings('ignore')

DATA_PATH = sys.argv[1] if len(sys.argv) > 1 else "data.csv"
OUTPUT_DIR = sys.argv[2] if len(sys.argv) > 2 else "."

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

# Search radius in degrees (approx 500m ≈ 0.0045 deg lat at London's latitude)
# We'll test multiple radii
RADII_DEG = {
    '250m': 0.00225,
    '500m': 0.0045,
    '1km': 0.009,
    '2km': 0.018,
}

# Low-rise typologies (ground truth: these are 1-2 storey buildings)
LOW_RISE_TYPOLOGIES = [
    'Small low terraces_pct',
    'Standard size semi detached_pct',
    'Standard size detached_pct',
    '2 storeys terraces with t rear extension_pct',
    'Large semi detached_pct',
    'Large detached_pct',
    'Very large detached_pct',
]

# Tall typologies (3+ storeys — these could contaminate nearby height estimates)
TALL_TYPOLOGIES = [
    '3-4 storey and smaller flats_pct',
    'Tall terraces 3-4 storeys_pct',
    'Medium height flats 5-6 storeys_pct',
    'Tall flats 6-15 storeys_pct',
    'Very tall point block flats_pct',
]

# Threshold: postcode is "low-rise dominated" if >X% low-rise
LOW_RISE_THRESHOLD = 60  # percent

# ─────────────────────────────────────────────
# LOAD DATA
# ─────────────────────────────────────────────

print("=" * 70)
print("HEIGHT HETEROGENEITY ANALYSIS")
print("=" * 70)

df = pd.read_csv(DATA_PATH)
print(f"Loaded {len(df):,} rows")

# Derive confidence metric
df['confidence_metric'] = (
    (df['residential_GEA_total_max'] - df['residential_GEA_total_min'])
    / df['residential_GEA_total_mode']
)
df['confidence_metric'] = df['confidence_metric'].replace([np.inf, -np.inf], np.nan)
df['is_london'] = (df['region'] == 'LN').astype(int)

# Total low-rise and tall percentage per postcode
df['pct_low_rise'] = df[LOW_RISE_TYPOLOGIES].sum(axis=1)
df['pct_tall'] = df[TALL_TYPOLOGIES].sum(axis=1)

# Filter to postcodes with valid coordinates and confidence
df_valid = df[['postcode', 'latitude', 'longitude', 'confidence_metric',
               'is_london', 'region', 'pct_low_rise', 'pct_tall',
               'residential_total_buildings', 'postcode_density',
               'Pre 1919_pct'] + LOW_RISE_TYPOLOGIES + TALL_TYPOLOGIES].dropna()

print(f"Valid postcodes with coordinates: {len(df_valid):,}")

# ─────────────────────────────────────────────
# BUILD SPATIAL INDEX
# ─────────────────────────────────────────────

print("\nBuilding spatial index...")
coords = df_valid[['latitude', 'longitude']].values
tree = cKDTree(coords)

# ─────────────────────────────────────────────
# CALCULATE NEIGHBOURHOOD METRICS
# ─────────────────────────────────────────────

# We'll use 500m as the primary radius, then test sensitivity to others
PRIMARY_RADIUS = '500m'
radius_deg = RADII_DEG[PRIMARY_RADIUS]

print(f"\nCalculating neighbourhood metrics (primary radius: {PRIMARY_RADIUS})...")

# For each postcode, find neighbours within radius
# and compute: mean tall %, max tall %, std of tall %, count of tall-dominated neighbours

tall_pct_values = df_valid['pct_tall'].values
low_rise_values = df_valid['pct_low_rise'].values
conf_values = df_valid['confidence_metric'].values
london_values = df_valid['is_london'].values

# Query all points at once for the primary radius
neighbours_list = tree.query_ball_tree(tree, r=radius_deg)

print("Computing neighbourhood statistics...")

n = len(df_valid)
nbr_mean_tall = np.zeros(n)
nbr_max_tall = np.zeros(n)
nbr_std_tall = np.zeros(n)
nbr_tall_count = np.zeros(n)  # count of neighbours with >30% tall buildings
nbr_count = np.zeros(n)
nbr_height_contrast = np.zeros(n)  # max tall nearby minus own tall %

for i in range(n):
    nbrs = neighbours_list[i]
    # Exclude self
    nbrs = [j for j in nbrs if j != i]
    
    if len(nbrs) > 0:
        nbr_tall = tall_pct_values[nbrs]
        nbr_mean_tall[i] = np.mean(nbr_tall)
        nbr_max_tall[i] = np.max(nbr_tall)
        nbr_std_tall[i] = np.std(nbr_tall)
        nbr_tall_count[i] = np.sum(nbr_tall > 30)
        nbr_count[i] = len(nbrs)
        nbr_height_contrast[i] = np.max(nbr_tall) - tall_pct_values[i]
    
    if i % 100000 == 0 and i > 0:
        print(f"  Processed {i:,} / {n:,} postcodes...")

df_valid = df_valid.copy()
df_valid['nbr_mean_tall_pct'] = nbr_mean_tall
df_valid['nbr_max_tall_pct'] = nbr_max_tall
df_valid['nbr_std_tall_pct'] = nbr_std_tall
df_valid['nbr_tall_dominated_count'] = nbr_tall_count
df_valid['nbr_count'] = nbr_count
df_valid['nbr_height_contrast'] = nbr_height_contrast

print(f"Done. Mean neighbours per postcode: {nbr_count.mean():.1f}")


# ─────────────────────────────────────────────
# ANALYSIS 1: Low-rise postcodes near tall buildings
# ─────────────────────────────────────────────

print("\n" + "=" * 70)
print("ANALYSIS 1: Do low-rise postcodes near tall buildings have worse confidence?")
print("=" * 70)

# Filter to low-rise dominated postcodes
low_rise_mask = df_valid['pct_low_rise'] > LOW_RISE_THRESHOLD
df_low_rise = df_valid[low_rise_mask].copy()
print(f"\nLow-rise dominated postcodes (>{LOW_RISE_THRESHOLD}% low-rise): {len(df_low_rise):,}")
print(f"  London: {df_low_rise['is_london'].sum():,}")
print(f"  Non-London: {(~df_low_rise['is_london'].astype(bool)).sum():,}")

# Split by whether there are tall buildings nearby
has_tall_nearby = df_low_rise['nbr_max_tall_pct'] > 30
no_tall_nearby = df_low_rise['nbr_max_tall_pct'] <= 30

print(f"\n  Low-rise postcodes WITH tall neighbours (>30% tall within {PRIMARY_RADIUS}): {has_tall_nearby.sum():,}")
print(f"  Low-rise postcodes WITHOUT tall neighbours: {no_tall_nearby.sum():,}")

# Compare confidence
for label, subset_label in [("All regions", None), ("London", 1), ("Non-London", 0)]:
    if subset_label is not None:
        sub = df_low_rise[df_low_rise['is_london'] == subset_label]
    else:
        sub = df_low_rise
    
    tall_nearby = sub[sub['nbr_max_tall_pct'] > 30]
    no_tall = sub[sub['nbr_max_tall_pct'] <= 30]
    
    if len(tall_nearby) >= 30 and len(no_tall) >= 30:
        mean_with = tall_nearby['confidence_metric'].mean()
        mean_without = no_tall['confidence_metric'].mean()
        t_stat, p_val = stats.ttest_ind(
            tall_nearby['confidence_metric'],
            no_tall['confidence_metric'],
            equal_var=False
        )
        print(f"\n  {label}:")
        print(f"    With tall neighbours:    mean={mean_with:.4f} (N={len(tall_nearby):,})")
        print(f"    Without tall neighbours: mean={mean_without:.4f} (N={len(no_tall):,})")
        print(f"    Difference: {mean_with - mean_without:.4f}, t={t_stat:.2f}, p={p_val:.2e}")


# ─────────────────────────────────────────────
# ANALYSIS 2: Regression — neighbourhood tall % predicting confidence
# ─────────────────────────────────────────────

print("\n" + "=" * 70)
print("ANALYSIS 2: OLS — Does nearby tall building % predict confidence")
print("            in low-rise postcodes, controlling for own characteristics?")
print("=" * 70)

# Features for the regression
own_features = ['pct_low_rise', 'pct_tall', 'Pre 1919_pct',
                'residential_total_buildings', 'postcode_density']
nbr_features = ['nbr_mean_tall_pct', 'nbr_max_tall_pct', 'nbr_height_contrast']

for label, data in [("All low-rise", df_low_rise),
                     ("London low-rise", df_low_rise[df_low_rise['is_london'] == 1]),
                     ("Non-London low-rise", df_low_rise[df_low_rise['is_london'] == 0])]:
    
    if len(data) < 100:
        print(f"\n  {label}: insufficient data ({len(data)} rows)")
        continue
    
    features = own_features + nbr_features
    if label == "All low-rise":
        features = features + ['is_london']
    
    X = data[features].copy()
    y = data['confidence_metric'].copy()
    
    # Drop any remaining NaN
    valid = X.notna().all(axis=1) & y.notna()
    X = X[valid]
    y = y[valid]
    
    X = sm.add_constant(X)
    model = sm.OLS(y, X).fit(cov_type='HC3')
    
    print(f"\n{'─' * 70}")
    print(f"  {label} (N={int(model.nobs):,}, R²={model.rsquared:.4f})")
    print(f"{'─' * 70}")
    
    # Show neighbourhood variables specifically
    results = pd.DataFrame({
        'Variable': model.params.index,
        'Coefficient': model.params.values,
        'Std Error': model.bse.values,
        'p-value': model.pvalues.values,
    })
    results['Sig'] = results['p-value'].apply(
        lambda p: '***' if p < 0.001 else ('**' if p < 0.01 else ('*' if p < 0.05 else ''))
    )
    
    # Compute standardised betas
    x_stds = X.drop('const', axis=1, errors='ignore').std()
    y_std = y.std()
    params_no_const = model.params.drop('const', errors='ignore')
    betas = params_no_const * (x_stds / y_std)
    beta_df = pd.DataFrame({'Variable': betas.index, 'Std_Beta': betas.values})
    results = results.merge(beta_df, on='Variable', how='left')
    
    results = results.sort_values('Std_Beta', ascending=False, key=abs, na_position='last')
    print(results[results['Variable'] != 'const'].to_string(index=False))


# ─────────────────────────────────────────────
# ANALYSIS 3: Binned analysis — confidence by height contrast
# ─────────────────────────────────────────────

print("\n" + "=" * 70)
print("ANALYSIS 3: Binned — confidence by neighbourhood height contrast")
print("=" * 70)
print("(For low-rise postcodes, binned by max tall % in neighbourhood)")

bins = [0, 5, 15, 30, 50, 100]
labels_bins = ['0-5%', '5-15%', '15-30%', '30-50%', '50-100%']

df_low_rise['nbr_tall_bin'] = pd.cut(
    df_low_rise['nbr_max_tall_pct'], bins=bins, labels=labels_bins, include_lowest=True
)

for region_label, region_val in [("London", 1), ("Non-London", 0)]:
    sub = df_low_rise[df_low_rise['is_london'] == region_val]
    print(f"\n  {region_label}:")
    print(f"  {'Max tall % nearby':<20} {'N':>8} {'Mean conf':>10} {'Median conf':>12} {'Std':>8}")
    print(f"  {'-'*20} {'-'*8} {'-'*10} {'-'*12} {'-'*8}")
    
    for bin_label in labels_bins:
        grp = sub[sub['nbr_tall_bin'] == bin_label]
        if len(grp) > 0:
            print(f"  {bin_label:<20} {len(grp):>8,} {grp['confidence_metric'].mean():>10.4f} "
                  f"{grp['confidence_metric'].median():>12.4f} {grp['confidence_metric'].std():>8.4f}")


# ─────────────────────────────────────────────
# ANALYSIS 4: Sensitivity to radius
# ─────────────────────────────────────────────

print("\n" + "=" * 70)
print("ANALYSIS 4: Sensitivity to search radius")
print("=" * 70)
print("(Testing whether the effect changes with different neighbourhood radii)")

for radius_name, r_deg in RADII_DEG.items():
    if radius_name == PRIMARY_RADIUS:
        # Already computed
        nbr_max = nbr_max_tall
    else:
        # Recompute for this radius
        nbrs_r = tree.query_ball_tree(tree, r=r_deg)
        nbr_max = np.zeros(n)
        for i in range(n):
            nbrs = [j for j in nbrs_r[i] if j != i]
            if len(nbrs) > 0:
                nbr_max[i] = np.max(tall_pct_values[nbrs])
    
    # For low-rise postcodes, correlation between nbr_max_tall and confidence
    lr_mask = low_rise_values[df_valid.index.isin(df_low_rise.index)] if hasattr(df_valid.index, 'isin') else low_rise_mask.values
    
    # Use df_low_rise indices relative to df_valid
    lr_indices = df_low_rise.index
    lr_conf = conf_values[df_valid.index.get_indexer(lr_indices)]
    lr_nbr_max = nbr_max[df_valid.index.get_indexer(lr_indices)]
    
    # Split London / non-London
    lr_london = london_values[df_valid.index.get_indexer(lr_indices)]
    
    for region_label, region_mask in [("London", lr_london == 1), ("Non-London", lr_london == 0)]:
        valid = np.isfinite(lr_conf[region_mask]) & np.isfinite(lr_nbr_max[region_mask])
        if valid.sum() > 30:
            r_val, p_val = stats.spearmanr(
                lr_nbr_max[region_mask][valid],
                lr_conf[region_mask][valid]
            )
            print(f"  Radius {radius_name:>5}, {region_label:<12}: "
                  f"Spearman rho={r_val:.4f}, p={p_val:.2e}, N={valid.sum():,}")


# ─────────────────────────────────────────────
# ANALYSIS 5: Specific typology deep-dive
# ─────────────────────────────────────────────

print("\n" + "=" * 70)
print("ANALYSIS 5: Specific typology deep-dive")
print("=" * 70)
print("For each low-rise typology, does nearby tall building presence")
print("increase confidence metric? (London only)\n")

for typ_col in LOW_RISE_TYPOLOGIES:
    typ_name = typ_col.replace('_pct', '')
    
    # Postcodes dominated by this typology
    typ_mask = df_valid[typ_col] > 60
    typ_london = df_valid[typ_mask & (df_valid['is_london'] == 1)]
    
    if len(typ_london) < 50:
        continue
    
    # Split by tall neighbours
    tall_nearby = typ_london[typ_london['nbr_max_tall_pct'] > 30]
    no_tall = typ_london[typ_london['nbr_max_tall_pct'] <= 30]
    
    if len(tall_nearby) >= 20 and len(no_tall) >= 20:
        mean_with = tall_nearby['confidence_metric'].mean()
        mean_without = no_tall['confidence_metric'].mean()
        t_stat, p_val = stats.ttest_ind(
            tall_nearby['confidence_metric'],
            no_tall['confidence_metric'],
            equal_var=False
        )
        sig = '***' if p_val < 0.001 else ('**' if p_val < 0.01 else ('*' if p_val < 0.05 else ''))
        print(f"  {typ_name:<45}")
        print(f"    With tall nbrs: {mean_with:.4f} (N={len(tall_nearby):,})")
        print(f"    Without:        {mean_without:.4f} (N={len(no_tall):,})")
        print(f"    Diff: {mean_with - mean_without:+.4f}, p={p_val:.2e} {sig}\n")
    else:
        n_tall = len(tall_nearby)
        n_no = len(no_tall)
        print(f"  {typ_name:<45} — insufficient split (tall_nbr={n_tall}, no_tall={n_no})\n")


# ─────────────────────────────────────────────
# SAVE
# ─────────────────────────────────────────────

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Save the enriched low-rise dataset for further analysis/plotting
df_low_rise.to_csv(os.path.join(OUTPUT_DIR, "low_rise_with_neighbourhood.csv"), index=False)

print(f"\nOutputs saved to {OUTPUT_DIR}/")
print("Done!")