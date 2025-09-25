import unittest
import numpy as np
from shapely.geometry import Polygon
import sys 
sys.path.append('../')
from src.pre_process_buildings import min_side  
class TestMinSide(unittest.TestCase):
    def test_square(self):
        # Create a simple square polygon
        square = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
        self.assertAlmostEqual(min_side(square), 1.0)
    
    def test_rectangle(self):
        # Create a 2x3 rectangle
        rectangle = Polygon([(0, 0), (3, 0), (3, 2), (0, 2)])
        self.assertAlmostEqual(min_side(rectangle), 2.0)
    
    def test_rotated_rectangle(self):
        # Create a rotated rectangle (45 degrees)
        # This creates a 1x2 rectangle rotated 45 degrees
        points = [
            (0, 0),
            (np.sqrt(2), np.sqrt(2)),
            (np.sqrt(2)/2, np.sqrt(2)*3/2),
            (-np.sqrt(2)/2, np.sqrt(2)/2)
        ]
        rotated_rect = Polygon(points)
        self.assertAlmostEqual(min_side(rotated_rect), 1.0)
    
    def test_irregular_polygon(self):
        # Create an irregular polygon that should have a minimum rotated rectangle
        irregular = Polygon([
            (0, 0), (2, 1), (3, 4), (1, 3), (-1, 2)
        ])
        # The exact value would depend on the specific minimum rotated rectangle
        result = min_side(irregular)
        self.assertTrue(isinstance(result, float))
        self.assertTrue(result > 0)
    
    def test_triangular_polygon(self):
        # Create a right triangle
        triangle = Polygon([(0, 0), (3, 0), (0, 4)])
        result = min_side(triangle)
        # The minimum rotated rectangle will have width equal to the height
        # of the triangle, which is the perpendicular distance from the base
        self.assertAlmostEqual(result, 3.0)

if __name__ == '__main__':
    unittest.main()