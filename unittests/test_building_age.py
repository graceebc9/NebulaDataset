import unittest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock
import sys 
sys.path.append('/Users/gracecolverd/New_dataset')
from src.pre_process_buildings import pre_process_building_data
from src.postcode_utils import check_duplicate_primary_key, find_data_pc_joint
from src.age_perc_calc import calc_filtered_percentage_of_building_age, calc_res_clean_counts, process_postcode_building_age

class TestBuildingAgeCalculations(unittest.TestCase):
    
    def setUp(self):
        self.df = pd.DataFrame({
            'premise_use': ['Residential', 'Residential', 'Commercial', 'Residential'],
            'premise_age': ['Pre 1837', '1980-1989', '1990-1999', '1870-1918'],
            'upn': [1, 2, 3, 4]
        })
        self.age_types = [
            'Pre 1919',
            '1919-1944',
            '1945-1959',
            '1960-1979',
            '1980-1989',
            '1990-1999',
            'Post 1999'
        ]
        
    def test_calc_filtered_percentage_of_building_age(self):
        result = calc_filtered_percentage_of_building_age(self.df, self.age_types)
        expected = {'Pre 1919': 2, '1980-1989': 1, 'None_age': 0}
        self.assertEqual(result['Pre 1919'], expected['Pre 1919'])
        self.assertEqual(result['1980-1989'], expected['1980-1989'])
        self.assertEqual(result['None_age'], expected['None_age'])

    def test_calc_res_clean_counts(self):
        result = calc_res_clean_counts(self.df)
        expected = 3  # Only three residential premises
        self.assertEqual(result, expected)

   
if __name__ == '__main__':
    unittest.main()
