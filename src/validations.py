import pandas as pd
from pathlib import Path
from typing import Dict, List, Set, Tuple

def load_log_files() -> Dict[str, pd.DataFrame]:
    """
    Loads all three log files into dataframes.
    
    Returns:
        Dict[str, pd.DataFrame]: Dictionary with attribute names as keys and dataframes as values
    """
    base_path = 'final_dataset/attribute_logs'
    attributes = ['age', 'fuel', 'type']
    
    dfs = {}
    for attr in attributes:
        file_path = f"{base_path}/{attr}_log_file.csv"
        try:
            dfs[attr] = pd.read_csv(file_path)
        except Exception as e:
            raise Exception(f"Error loading {attr} log file: {str(e)}")
    
    return dfs

def validate_log_consistency() -> Dict[str, Dict]:
    """
    Validates consistency across all three log files for:
    - Same regions present
    - Same batches per region
    - Same counts (len) per batch
    
    Returns:
        Dict containing validation results and any inconsistencies found
    """
    try:
        dfs = load_log_files()
        
        results = {
            'valid': True,
            'region_consistency': {},
            'batch_consistency': {},
            'count_consistency': {},
            'summary': {}
        }
        
        # Get all unique regions and batches across all files
        all_regions: Set[str] = set()
        region_batches: Dict[str, Dict[str, Set[int]]] = {}
        
        for attr, df in dfs.items():
            all_regions.update(df['region'].unique())
            region_batches[attr] = {
                region: set(df[df['region'] == region]['batch'])
                for region in df['region'].unique()
            }
        
        # Check region consistency
        for attr, df in dfs.items():
            missing_regions = all_regions - set(df['region'].unique())
            results['region_consistency'][attr] = {
                'valid': not missing_regions,
                'missing_regions': list(missing_regions)
            }
            if missing_regions:
                results['valid'] = False
        
        # Check batch consistency within regions
        for region in all_regions:
            batch_inconsistencies = {}
            for attr, df in dfs.items():
                other_attrs = [a for a in dfs.keys() if a != attr]
                for other_attr in other_attrs:
                    diff = region_batches[attr].get(region, set()) - region_batches[other_attr].get(region, set())
                    if diff:
                        results['valid'] = False
                        batch_inconsistencies[f"{attr}_vs_{other_attr}"] = list(diff)
            
            if batch_inconsistencies:
                results['batch_consistency'][region] = batch_inconsistencies
        
        # Check count consistency for matching batches
        count_inconsistencies = []
        for region in all_regions:
            common_batches = set.intersection(*[
                region_batches[attr].get(region, set())
                for attr in dfs.keys()
            ])
            
            for batch in common_batches:
                counts = {
                    attr: int(df[(df['region'] == region) & (df['batch'] == batch)]['len'].iloc[0])
                    for attr, df in dfs.items()
                }
                
                if len(set(counts.values())) > 1:
                    results['valid'] = False
                    count_inconsistencies.append({
                        'region': region,
                        'batch': batch,
                        'counts': counts
                    })
        
        results['count_consistency'] = {
            'valid': len(count_inconsistencies) == 0,
            'inconsistencies': count_inconsistencies
        }
        
        results['summary'] = {
            'total_regions': len(all_regions),
            'regions': list(sorted(all_regions)),
            'files_checked': list(dfs.keys()),
            'overall_valid': results['valid']
        }
        
        return results
    
    except Exception as e:
        return {
            'valid': False,
            'error': str(e)
        }

def validate_batch_lengths(default_length: int = 10000) -> Dict[str, Dict]:
    """
    Validates that only one batch per region has a length different from the default value.
    
    Args:
        default_length: The expected length for most batches
    
    Returns:
        Dict containing validation results for each attribute file
    """
    attributes = ['age', 'fuel', 'type']
    base_path = 'final_dataset/attribute_logs'
    results = {'valid': True}
    
    for attr in attributes:
        file_path = f"{base_path}/{attr}_log_file.csv"
        
        try:
            df = pd.read_csv(file_path)
            anomalies = df.groupby('region').apply(
                lambda x: sum(x['len'] != default_length)
            )
            
            invalid_regions = anomalies[anomalies > 1].index.tolist()
            anomaly_details = df[df['len'] != default_length].groupby('region').apply(
                lambda x: x[['batch', 'len']].to_dict('records')
            ).to_dict()
            
            attr_valid = len(invalid_regions) == 0
            results[attr] = {
                'valid': attr_valid,
                'invalid_regions': invalid_regions,
                'anomaly_details': anomaly_details
            }
            
            if not attr_valid:
                results['valid'] = False
            
        except Exception as e:
            results[attr] = {
                'valid': False,
                'error': str(e)
            }
            results['valid'] = False
    
    return results

def validate_region_variations(default_length: int = 10000) -> Dict:
    """
    Validates that each region has at least one batch with a non-default length.
    
    Args:
        default_length: The expected default length
    
    Returns:
        Dict containing validation results
    """
    base_path = 'final_dataset/attribute_logs'
    attributes = ['age', 'fuel', 'type']
    
    results = {
        'valid': True,
        'regions_without_variation': set(),
        'variation_details': {},
        'error': None
    }
    
    try:
        all_regions: Set[str] = set()
        regions_with_variation: Set[str] = set()
        
        for attr in attributes:
            file_path = f"{base_path}/{attr}_log_file.csv"
            df = pd.read_csv(file_path)
            
            all_regions.update(df['region'].unique())
            variations = df[df['len'] != default_length]
            regions_with_variation.update(variations['region'].unique())
            
            for _, row in variations.iterrows():
                region = row['region']
                if region not in results['variation_details']:
                    results['variation_details'][region] = []
                
                results['variation_details'][region].append({
                    'file': attr,
                    'batch': row['batch'],
                    'length': row['len']
                })
        
        results['regions_without_variation'] = all_regions - regions_with_variation
        results['valid'] = len(results['regions_without_variation']) == 0
        
        for region in results['variation_details']:
            results['variation_details'][region].sort(key=lambda x: (x['file'], x['batch']))
        
        return results
    
    except Exception as e:
        return {
            'valid': False,
            'regions_without_variation': set(),
            'variation_details': {},
            'error': str(e)
        }

def print_validation_summary(variation_results: Dict, 
                           batch_results: Dict, 
                           consistency_results: Dict) -> None:
    """
    Prints a comprehensive summary of all validation results.
    
    Args:
        variation_results: Results from validate_region_variations
        batch_results: Results from validate_batch_lengths
        consistency_results: Results from validate_log_consistency
    """
    all_valid = (
        variation_results.get('valid', False) and 
        batch_results.get('valid', False) and 
        consistency_results.get('valid', False)
    )
    
    print("\n=== VALIDATION SUMMARY ===")
    print(f"\nOverall Validation Status: {'✓ PASSED' if all_valid else '✗ FAILED'}")
    
    # Region Variations Check
    print("\n1. Region Variations Check:")
    status = "✓ Passed" if variation_results.get('valid', False) else "✗ Failed"
    print(f"Status: {status}")
    if not variation_results.get('valid', False):
        print("Regions missing variations:", 
              ', '.join(sorted(variation_results.get('regions_without_variation', set()))))
    
    # Batch Length Check
    print("\n2. Batch Length Check:")
    status = "✓ Passed" if batch_results.get('valid', False) else "✗ Failed"
    print(f"Status: {status}")
    if not batch_results.get('valid', False):
        for attr, result in batch_results.items():
            if attr != 'valid' and not result.get('valid', True):
                print(f"- {attr}: Invalid regions:", ', '.join(result.get('invalid_regions', [])))
    
    # Consistency Check
    print("\n3. Log Consistency Check:")
    status = "✓ Passed" if consistency_results.get('valid', False) else "✗ Failed"
    print(f"Status: {status}")
    if not consistency_results.get('valid', False):
        if consistency_results.get('region_consistency'):
            for attr, result in consistency_results['region_consistency'].items():
                if not result['valid']:
                    print(f"- {attr} missing regions:", ', '.join(result['missing_regions']))
        if consistency_results.get('count_consistency', {}).get('inconsistencies'):
            print("- Count inconsistencies found in:", 
                  len(consistency_results['count_consistency']['inconsistencies']), "batches")
    
    # Final Summary
    print("\n=== FINAL STATUS ===")
    if all_valid:
        print("✓ All validation checks passed successfully!")
    else:
        print("✗ One or more validation checks failed. Please review the details above.")

def call_validations(default_length: int = 10000) -> None:
    """
    Runs all validation checks and prints a comprehensive summary.
    
    Args:
        default_length: The expected default batch length
    """
    print(f"Running validations with default batch size: {default_length}")
    
    variation_results = validate_region_variations(default_length)
    batch_results = validate_batch_lengths(default_length)
    consistency_results = validate_log_consistency()
    
    print_validation_summary(variation_results, batch_results, consistency_results)