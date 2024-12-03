import unittest
import pandas as pd
import numpy as np
from typing import List, Dict, Optional
import sys
sys.path.append('../')  
from src.fuel_calc import calc_df_sum_attribute, process_buildings

import unittest
import pandas as pd
import numpy as np
from typing import List, Dict, Optional

COLS = ['premise_area', 'total_fl_area_H', 'total_fl_area_FC', 'total_fl_area_valfc', 
        'total_fl_area_meta', 'total_fl_area_avg', 'base_floor', 'basement_heated_vol', 
        'listed_bool', 'uprn_count']

COLS_OB = ['premise_area', 'total_fl_area_H', 'total_fl_area_FC', 'uprn_count']

RES_USE_TYPES = [
    'Medium height flats 5-6 storeys', 'Small low terraces',
    '3-4 storey and smaller flats', 'Tall terraces 3-4 storeys',
    'Large semi detached', 'Standard size detached',
    'Standard size semi detached', '2 storeys terraces with t rear extension',
    'Semi type house in multiples', 'Tall flats 6-15 storeys',
    'Large detached', 'Very tall point block flats',
    'Very large detached', 'Planned balanced mixed estates',
    'Linked and step linked premises'
]

class TestBuildingProcessing(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        
        # Create residential data with ALL required columns from both COLS and COLS_OB
        self.residential_data = {
            'map_simple_use': ['Residential', 'Residential', 'Commercial', 'Mixed Use'],
            'premise_type': [RES_USE_TYPES[0], 'Domestic outbuilding', None, None],
            # Include ALL columns from COLS
            'premise_area': [100.0, 200.0, 300.0, 400.0],
            'total_fl_area_H': [150.0, 250.0, 350.0, 450.0],
            'total_fl_area_FC': [120.0, 220.0, 320.0, 420.0],
            'total_fl_area_valfc': [130.0, 230.0, 330.0, 430.0],
            'total_fl_area_meta': [140.0, 240.0, 340.0, 440.0],
            'total_fl_area_avg': [145.0, 245.0, 345.0, 445.0],
            'base_floor': [1, 1, 1, 1],
            'basement_heated_vol': [100, 200, 300, 400],
            'listed_bool': [True, False, True, False],
            'uprn_count': [1, 1, 1, 1]
        }

    def test_calc_df_sum_attribute_empty_df(self):
        """Test handling of empty DataFrame."""
        empty_df = pd.DataFrame()
        result = calc_df_sum_attribute(empty_df, COLS)
        
        # Verify only the expected nulls are generated based on input cols
        self.assertIn('total_buildings', result)
        for col in COLS:
            self.assertIn(f'{col}_total', result)
            self.assertTrue(np.isnan(result[f'{col}_total']))

    def test_process_buildings_null_input(self):
        """Test process_buildings with null input."""
        result = process_buildings(None)
        
        # Check correct nulls generated for each prefix with correct columns
        # clean_res_ uses COLS
        for col in COLS:
            self.assertIn(f'clean_res_{col}_total', result)
            self.assertTrue(np.isnan(result[f'clean_res_{col}_total']))
            
        # outb_res_ uses COLS_OB
        for col in COLS_OB:
            self.assertIn(f'outb_res_{col}_total', result)
            self.assertTrue(np.isnan(result[f'outb_res_{col}_total']))
            
        # all_types_ uses COLS_OB
        for col in COLS_OB:
            self.assertIn(f'all_types_{col}_total', result)
            self.assertTrue(np.isnan(result[f'all_types_{col}_total']))

    def test_process_buildings_with_residential_data(self):
        """Test process_buildings with residential data."""
        df = pd.DataFrame(self.residential_data)
        result = process_buildings(df)
        
        # Basic counts
        self.assertEqual(result['all_types_total_buildings'], 4)
        self.assertEqual(result['mixed_alltypes_count'], 1)
        self.assertEqual(result['comm_alltypes_count'], 1)
        self.assertEqual(result['all_residential_types_count'], 2)

        # Check each section has correct columns based on their col list
        # clean_res_ section (uses COLS)
        self.assertEqual(result['clean_res_total_buildings'], 1)
        for col in COLS:
            self.assertIn(f'clean_res_{col}_total', result)
            
        # outb_res_ section (uses COLS_OB)
        self.assertEqual(result['outb_res_total_buildings'], 1)
        for col in COLS_OB:
            self.assertIn(f'outb_res_{col}_total', result)
            
        # all_types_ section (uses COLS_OB)
        for col in COLS_OB:
            self.assertIn(f'all_types_{col}_total', result)

    def test_unexpected_residential_types(self):
        """Test handling of unexpected residential types."""
        # Include ALL columns from COLS in test data
        data = {
            'map_simple_use': ['Residential'],
            'premise_type': ['Unexpected Type']
        }
        # Add all required columns
        for col in COLS:
            data[col] = [100.0]
            
        df = pd.DataFrame(data)
        
        with self.assertRaises(ValueError) as context:
            process_buildings(df)
        self.assertIn("Unexpected residential types", str(context.exception))



import unittest
import pandas as pd
import numpy as np
from typing import List, Dict

def generate_nulls(cols: List[str], prefix: str = '') -> Dict:
    """Helper function to generate null results for empty DataFrame"""
    return {
        f'{prefix}total_buildings': 0,
        **{f'{prefix}{col}_total': None for col in cols}
    }



class TestCalcDfSumAttribute(unittest.TestCase):

    def test_dataframe_with_nulls(self):
        """Test handling of DataFrame with null values"""
        data = {
            'premise_area': [100.0, None, 300.0, None],
            'total_fl_area_H': [200.0, 400.0, np.nan, None],
            'other_col': [1.0, 2.0, None, 4.0]
        }
        df = pd.DataFrame(data)
        cols = ['premise_area', 'total_fl_area_H', 'other_col']
        
        result = calc_df_sum_attribute(df, cols)
        
        self.assertEqual(result['total_buildings'], 4)
        self.assertEqual(result['premise_area_total'], 400.0)
        self.assertEqual(result['premise_area_null_count'], 2)
        self.assertEqual(result['total_fl_area_H_total'], 600.0)
        self.assertEqual(result['total_fl_area_H_null_count'], 2)
        self.assertEqual(result['other_col_total'], 7.0)
        self.assertNotIn('other_col_null_count', result)

    def test_prefix_handling(self):
        """Test handling of prefix parameter"""
        data = {
            'premise_area': [100.0, None, 300.0],
            'total_fl_area_H': [200.0, 400.0, None]
        }
        df = pd.DataFrame(data)
        cols = ['premise_area', 'total_fl_area_H']
        prefix = 'test_'
        
        result = calc_df_sum_attribute(df, cols, prefix)
        
        self.assertEqual(result['test_total_buildings'], 3)
        self.assertEqual(result['test_premise_area_total'], 400.0)
        self.assertEqual(result['test_premise_area_null_count'], 1)
        self.assertEqual(result['test_total_fl_area_H_total'], 600.0)
        self.assertEqual(result['test_total_fl_area_H_null_count'], 1)

    def test_all_nulls(self):
        """Test handling of columns with all null values"""
        data = {
            'premise_area': [None, None, None],
            'total_fl_area_H': [None, None, None]
        }
        df = pd.DataFrame(data)
        cols = ['premise_area', 'total_fl_area_H']
        
        result = calc_df_sum_attribute(df, cols)
        
        self.assertEqual(result['total_buildings'], 3)
        self.assertTrue(pd.isna(result['premise_area_total']))
        self.assertEqual(result['premise_area_null_count'], 3)
        self.assertTrue(pd.isna(result['total_fl_area_H_total']))
        self.assertEqual(result['total_fl_area_H_null_count'], 3)

    def test_mixed_column_types(self):
        """Test handling of mixed normal and area columns"""
        data = {
            'premise_area': [100.0, None, 300.0],
            'non_area_col': [1.0, 2.0, 3.0],
            'total_fl_area_H': [200.0, 400.0, None]
        }
        df = pd.DataFrame(data)
        cols = ['premise_area', 'non_area_col', 'total_fl_area_H']
        
        result = calc_df_sum_attribute(df, cols)
        
        self.assertEqual(result['total_buildings'], 3)
        self.assertEqual(result['premise_area_total'], 400.0)
        self.assertEqual(result['premise_area_null_count'], 1)
        self.assertEqual(result['non_area_col_total'], 6.0)
        self.assertNotIn('non_area_col_null_count', result)
        self.assertEqual(result['total_fl_area_H_total'], 600.0)
        self.assertEqual(result['total_fl_area_H_null_count'], 1)


if __name__ == '__main__':
    unittest.main()