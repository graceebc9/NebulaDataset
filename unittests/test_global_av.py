import pytest
import pandas as pd
import geopandas as gpd
from pathlib import Path
from unittest.mock import Mock, patch
import numpy as np

import sys
sys.path.append('../')
from src.global_av import validate_input_data, process_single_bbox

import unittest
from unittest.mock import patch, Mock
import pandas as pd
import geopandas as gpd
from pathlib import Path
import numpy as np

# test the global floor averages. the height buckets 0-2m is missing because of our thresholds of 2.5m min floor height filters those out.
#  for entries falling into this height bucket we expect the height to be false and hence the mehtod will onyl have a FC reading 



class TestBoundingBoxProcessor(unittest.TestCase):
    def setUp(self):
        """Set up test data before each test"""
        self.test_data = {
            'map_simple_use': ['residential', 'residential', 'commercial', 'commercial'],
            'premise_age': ['1910', '1910', '1960', '1960'],
            'premise_age_bucketed': ['pre 1919', 'pre 1919', '1945-1964', '1945-1964'],
            'height': [5, 9.0, 6.0, 12.0],
            'height_bucket': ['3-6m', '6-9m', '6-9m', '9-12m'],
            'premise_floor_count': ['2', '3', '2', '4'],
            'geometry': [None] * 4  # Placeholder geometries
        }
        self.mock_gdf = gpd.GeoDataFrame(self.test_data)
        self.test_file = Path('dummy.gpkg')
        self.bbox = (0, 0, 1, 1)

    def test_successful_processing(self):
        """Test successful processing with valid input data"""
        with patch('geopandas.read_file', return_value=self.mock_gdf), \
             patch('src.global_av.validate_input_data', return_value=True), \
             patch('src.global_av.create_age_buckets', return_value=self.mock_gdf), \
             patch('src.global_av.create_height_bucket_cols', return_value=self.mock_gdf):
            
            result = process_single_bbox(self.bbox, self.test_file)
            
            # Check expected columns exist
            expected_columns = ['map_simple_use', 'premise_age_bucketed', 'height_bucket', 
                              'mean_height', 'count', 'weighted_height']
            for col in expected_columns:
                self.assertIn(col, result.columns)
            
            # Test aggregation for residential pre-1919 buildings
            residential_pre_1919 = result[
                (result['map_simple_use'] == 'residential') & 
                (result['premise_age_bucketed'] == 'pre 1919')
            ]
            
            self.assertEqual(len(residential_pre_1919), 2)  # Should have two height buckets
            
            # Check 3-6m height bucket
            r1 = residential_pre_1919[residential_pre_1919['height_bucket'] == '3-6m']
            self.assertEqual(r1['mean_height'].iloc[0], 2.0)  # 2 floors
            self.assertEqual(r1['count'].iloc[0], 1)
            self.assertEqual(r1['weighted_height'].iloc[0], 2.0)  # 2 * 1
            
            # Check 6-9m height bucket
            r2 = residential_pre_1919[residential_pre_1919['height_bucket'] == '6-9m']
            self.assertEqual(r2['mean_height'].iloc[0], 3.0)  # 3 floors
            self.assertEqual(r2['count'].iloc[0], 1)
            self.assertEqual(r2['weighted_height'].iloc[0], 3.0)  # 3 * 1



    def test_empty_input(self):
        """Test handling of empty input"""
        empty_gdf = gpd.GeoDataFrame()
        with patch('geopandas.read_file', return_value=empty_gdf):
            result = process_single_bbox(self.bbox, self.test_file)
            self.assertTrue(result.empty)

    def test_invalid_input(self):
        """Test handling of invalid input data"""
        with patch('geopandas.read_file', return_value=self.mock_gdf), \
             patch('src.global_av.validate_input_data', return_value=False):
            
            with self.assertRaisesRegex(ValueError, 'Input data missing required columns'):
                process_single_bbox(self.bbox, self.test_file)

    def test_non_numeric_values(self):
        """Test handling of non-numeric values"""
        bad_data = self.mock_gdf.copy()
        bad_data.loc[0, 'premise_floor_count'] = 'invalid'
        
        with patch('geopandas.read_file', return_value=bad_data), \
             patch('src.global_av.validate_input_data', return_value=True), \
             patch('src.global_av.create_age_buckets', return_value=bad_data), \
             patch('src.global_av.create_height_bucket_cols', return_value=bad_data):
            
            result = process_single_bbox(self.bbox, self.test_file)
            self.assertLess(len(result), len(bad_data))

if __name__ == '__main__':
    unittest.main()