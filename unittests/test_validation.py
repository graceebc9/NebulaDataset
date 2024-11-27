import unittest
import pandas as pd
import sys 
sys.path.append('/Users/gracecolverd/New_dataset')
from src.validations import validate_dataset  # Adjust this if the path is incorrect

class TestDatasetValidation(unittest.TestCase):
    def test_validation_failure_due_to_tolerance(self):
        # Create a sample DataFrame where one test should fail
        data = {
            'all_types_build_vol': [100, 200, 300],
            'all_types_build_vol_inc_basement': [50, 160, 285],
            'all_res_build_vol': [190, 290, 390],
            'all_res_build_vol_inc_basement': [90, 190, 290],
            'clean_res_build_vol': [100, 440, 200]
        }
        df_test = pd.DataFrame(data)

        # Run the validation function with a very small tolerance
        errors = validate_dataset(df_test, tolerance=1e-13)
        
        # Check that the error list is not empty
        self.assertTrue(errors, "No errors found when at least one was expected due to tolerance issues.")
        
        # Optionally check for specific error messages if needed
        expected_error = 'Validation failed: all_types_build_vol is not always >= all_res_build_vol within tolerance 1e-13, found 3 violations.'
        self.assertIn(expected_error, errors, "Specific error message not found in errors list.")

         # Check for specific error messages if needed
        expected_error = 'Validation failed: all_types_build_vol_inc_basement is not always >= all_types_build_vol within tolerance 1e-13, found 3 violations.'
        self.assertIn(expected_error, errors, "Specific error message not found in errors list.")

        # Check for specific error messages if needed
        expected_error = 'Validation failed: all_types_build_vol_inc_basement is not always >= all_res_build_vol_inc_basement within tolerance 1e-13, found 3 violations.'
        self.assertIn(expected_error, errors, "Specific error message not found in errors list.")

       # Check for specific error messages if needed
        expected_error = 'Validation failed: all_res_build_vol is not always >= clean_res_build_vol within tolerance 1e-13, found 1 violations.'
        self.assertIn(expected_error, errors, "Specific error message not found in errors list.")

if __name__ == '__main__':
    unittest.main()

