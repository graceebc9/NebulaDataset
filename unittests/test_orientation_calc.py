import unittest
import geopandas as gpd
from shapely.geometry import Polygon
import sys 
sys.path.append('/Users/gracecolverd/New_dataset')
from src.orientation_calc import calculate_orientation, categorize_orientation, calc_orientation_percentage

class TestOrientationCalc(unittest.TestCase):

    def setUp(self):
        # Create a simple GeoDataFrame with building geometries for testing
        data = {
            'geometry': [
                Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),  # Square building
                Polygon([(0, 0), (2, 0), (2, 1), (0, 1)]),  # Rectangular building
                Polygon([(0, 0), (1, 0), (1, 2), (0, 2)])   # Another rectangular building
            ]
        }
        self.gdf = gpd.GeoDataFrame(data, geometry='geometry')

    def test_calculate_orientation(self):
        # Test orientation calculation
        for geom in self.gdf['geometry']:
            orientation = calculate_orientation(geom)
            self.assertTrue(0 <= orientation < 360, f"Orientation {orientation} out of range")

    def test_categorize_orientation(self):
        # Test orientation categorization
        orientations = [0, 45, 90, 135, 180, 225, 270, 315]
        expected_categories = ['North', 'Northeast', 'East', 'Southeast', 'South', 'Southwest', 'West', 'Northwest']
        for angle, expected in zip(orientations, expected_categories):
            category = categorize_orientation(angle)
            self.assertEqual(category, expected, f"Expected {expected} but got {category}")

    def test_calc_orientation_percentage(self):
        # Add orientation and category to GeoDataFrame
        self.gdf['orientation'] = self.gdf['geometry'].apply(calculate_orientation)
        self.gdf['orientation_category'] = self.gdf['orientation'].apply(categorize_orientation)
        
        # Calculate orientation percentages
        result = calc_orientation_percentage(self.gdf)
        self.assertIsInstance(result, dict, "Result should be a dictionary")
        
        # Check that all categories are present in the result
        categories = ['North', 'Northeast', 'East', 'Southeast', 'South', 'Southwest', 'West', 'Northwest']
        for category in categories:
            self.assertIn(category, result, f"Category {category} missing in result")
            self.assertIsInstance(result[category], float, f"Category {category} should be a float")
            self.assertTrue(0 <= result[category] <= 100, f"Category {category} percentage {result[category]} out of range")

if __name__ == '__main__':
    unittest.main()
