import pandas as pd
from pathlib import Path

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
            if missing_regions:
                results['valid'] = False
                results['region_consistency'][attr] = {
                    'valid': False,
                    'missing_regions': list(missing_regions)
                }
            else:
                results['region_consistency'][attr] = {
                    'valid': True,
                    'missing_regions': []
                }
        
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
            # Get batches that exist in all files for this region
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
        
        # Add summary
        results['summary'] = {
            'total_regions': len(all_regions),
            'regions': list(all_regions),
            'files_checked': list(dfs.keys()),
            'overall_valid': results['valid']
        }
        
        return results
    
    except Exception as e:
        return {
            'valid': False,
            'error': str(e)
        }

def print_consistency_results(results: Dict) -> None:
    """
    Pretty prints the consistency validation results.
    
    Args:
        results: Dictionary containing validation results
    """
    if 'error' in results:
        print(f"Error during validation: {results['error']}")
        return
        
    print("\n=== Log Files Consistency Validation ===")
    print(f"\nOverall Status: {'✓ Valid' if results['valid'] else '✗ Invalid'}")
    
    print("\n--- Region Consistency ---")
    for attr, result in results['region_consistency'].items():
        if result['missing_regions']:
            print(f"\n✗ {attr} is missing regions: {', '.join(result['missing_regions'])}")
        else:
            print(f"\n✓ {attr} has all regions")
    
    print("\n--- Batch Consistency ---")
    if results['batch_consistency']:
        for region, inconsistencies in results['batch_consistency'].items():
            print(f"\nRegion {region} has inconsistencies:")
            for comparison, batches in inconsistencies.items():
                print(f"  ✗ {comparison}: Extra batches {batches}")
    else:
        print("✓ All regions have consistent batches across files")
    
    print("\n--- Count Consistency ---")
    if results['count_consistency']['inconsistencies']:
        print("\nFound count inconsistencies:")
        for inc in results['count_consistency']['inconsistencies']:
            print(f"\n  Region {inc['region']}, Batch {inc['batch']}:")
            for attr, count in inc['counts'].items():
                print(f"    {attr}: {count}")
    else:
        print("✓ All matching batches have consistent counts")
    
    print("\n--- Summary ---")
    print(f"Total regions: {results['summary']['total_regions']}")
    print(f"Regions: {', '.join(results['summary']['regions'])}")
    print(f"Files checked: {', '.join(results['summary']['files_checked'])}")



def validate_batch_lengths(default_length=10000):
    """
    Validates that only one batch per region has a length different from the default value
    across age, fuel, and type log files.
    
    Args:
        default_length (int): The expected length for most batches (default: 10000)
    
    Returns:
        dict: Dictionary containing validation results for each attribute file
    """
    # Define the attributes and their file paths
    attributes = ['age', 'fuel', 'type']
    base_path = 'final_dataset/attribute_logs'
    print(f'Running validatiions based on a batch size of {default_length}')
    results = {}
    
    for attr in attributes:
        file_path = f"{base_path}/{attr}_log_file.csv"
        
        try:
            # Read the log file
            df = pd.read_csv(file_path)
            
            # Group by region and count how many batches don't match default_length
            anomalies = df.groupby('region').apply(
                lambda x: sum(x['len'] != default_length)
            )
            
            # Check which regions have more than one anomaly
            invalid_regions = anomalies[anomalies > 1].index.tolist()
            
            # Get details of all non-default length batches
            anomaly_details = df[df['len'] != default_length].groupby('region').apply(
                lambda x: x[['batch', 'len']].to_dict('records')
            ).to_dict()
            
            results[attr] = {
                'valid': len(invalid_regions) == 0,
                'invalid_regions': invalid_regions,
                'anomaly_details': anomaly_details
            }
            
        except Exception as e:
            results[attr] = {
                'valid': False,
                'error': str(e)
            }
    
    return results

def print_validation_results(results):
    """
    Pretty prints the validation results.
    
    Args:
        results (dict): Results from validate_batch_lengths function
    """
    for attr, result in results.items():
        print(f"\n=== {attr.upper()} Log File ===")
        
        if 'error' in result:
            print(f"Error processing file: {result['error']}")
            continue
            
        if result['valid']:
            print("✓ Valid: Each region has at most one non-standard length batch")
        else:
            print("✗ Invalid: Following regions have multiple non-standard length batches, processing has likely not finished:")
            for region in result['invalid_regions']:
                print(f"\nRegion: {region}")
                



def call_validations():
    results = validate_batch_lengths()
    print_validation_results(results)

    # Run length / consistnecy both validations
    batch_results = validate_batch_lengths()
    consistency_results = validate_log_consistency()

    # Print results
    print("=== Batch Length Validation ===")
    print_validation_results(batch_results)

    print("\n=== Consistency Validation ===")
    print_consistency_results(consistency_results)