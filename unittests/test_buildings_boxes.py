import unittest
from itertools import combinations
import sys
sys.path.append('../')  
from src.buildings import calculate_bounding_boxes  

def check_full_coverage(boxes, original_extent):
    """Helper function to verify that boxes cover the entire original extent"""
    minX, maxX, minY, maxY = original_extent
    # Check if any point of the original extent is not covered
    for box in boxes:
        box_minX, box_minY, box_maxX, box_maxY = box
        if (box_minX < minX or box_maxX > maxX or 
            box_minY < minY or box_maxY > maxY):
            return False
    
    # Convert boxes to set of covered coordinates
    covered_area = 0
    for box in boxes:
        box_minX, box_minY, box_maxX, box_maxY = box
        box_area = (box_maxX - box_minX) * (box_maxY - box_minY)
        covered_area += box_area
    
    # Compare with original area
    original_area = (maxX - minX) * (maxY - minY)
    return abs(covered_area - original_area) < 1e-10

def check_no_overlap(boxes):
    """Helper function to verify that no boxes overlap"""
    for box1, box2 in combinations(boxes, 2):
        minX1, minY1, maxX1, maxY1 = box1
        minX2, minY2, maxX2, maxY2 = box2
        
        # Check if boxes overlap
        if (minX1 < maxX2 and maxX1 > minX2 and
            minY1 < maxY2 and maxY1 > minY2):
            # Calculate overlap area
            overlap_area = (min(maxX1, maxX2) - max(minX1, minX2)) * \
                         (min(maxY1, maxY2) - max(minY1, minY2))
            if overlap_area > 1e-10:  # Allow for floating point imprecision
                return False
    return True

class TestBoundingBoxCalculation(unittest.TestCase):
    def test_single_box(self):
        """Test when extent is smaller than chunk size"""
        extent = (0, 50000, 0, 50000)
        boxes = calculate_bounding_boxes(extent, 100000, 100000)
        
        self.assertEqual(len(boxes), 1)
        self.assertEqual(boxes[0], (0, 0, 50000, 50000))
        self.assertTrue(check_full_coverage(boxes, extent))
        self.assertTrue(check_no_overlap(boxes))

    def test_multiple_boxes(self):
        """Test when extent requires multiple boxes"""
        extent = (0, 150000, 0, 150000)
        boxes = calculate_bounding_boxes(extent, 100000, 100000)
        
        self.assertEqual(len(boxes), 4)
        self.assertTrue(check_full_coverage(boxes, extent))
        self.assertTrue(check_no_overlap(boxes))

    def test_uneven_division(self):
        """Test when extent doesn't divide evenly by chunk size"""
        extent = (0, 120000, 0, 180000)
        boxes = calculate_bounding_boxes(extent, 100000, 100000)
        
        self.assertTrue(check_full_coverage(boxes, extent))
        self.assertTrue(check_no_overlap(boxes))
        
        # Verify last box dimensions are correct
        last_box = boxes[-1]
        self.assertEqual(last_box[2] - last_box[0], 20000)  # Width
        self.assertEqual(last_box[3] - last_box[1], 80000)  # Height

    def test_negative_coordinates(self):
        """Test with negative coordinates"""
        extent = (-150000, 50000, -100000, 100000)
        boxes = calculate_bounding_boxes(extent, 100000, 100000)
        
        self.assertTrue(check_full_coverage(boxes, extent))
        self.assertTrue(check_no_overlap(boxes))
        
        # Check if boxes start at correct negative coordinates
        self.assertEqual(boxes[0][0], -150000)
        self.assertEqual(boxes[0][1], -100000)

    def test_different_chunk_sizes(self):
        """Test with different width and height chunk sizes"""
        extent = (0, 200000, 0, 150000)
        boxes = calculate_bounding_boxes(extent, 80000, 50000)
        
        self.assertTrue(check_full_coverage(boxes, extent))
        self.assertTrue(check_no_overlap(boxes))

    def test_zero_area_extent(self):
        """Test with zero area extent"""
        extent = (100000, 100000, 100000, 100000)
        boxes = calculate_bounding_boxes(extent)
        
        self.assertEqual(len(boxes), 1)
        self.assertEqual(boxes[0], (100000, 100000, 100000, 100000))

if __name__ == '__main__':
    unittest.main()