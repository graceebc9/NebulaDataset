import unittest
import sys
sys.path.append('/Users/gracecolverd/New_dataset')
from sentinel_utils.utils import extents_overlap, process_shapefile  # Replace with the actual module name
from unittest.mock import patch, MagicMock
import geopandas as gpd
import rasterio
import numpy as np
from datetime import datetime
from shapely.geometry import box

class TestExtentsOverlap(unittest.TestCase):

    def test_extents_overlap_partial(self):
        extent1 = (0, 0, 10, 10)
        extent2 = (5, 5, 15, 15)
        self.assertTrue(extents_overlap(extent1, extent2))

    def test_extents_overlap_within(self):
        extent1 = (0, 0, 20, 20)
        extent2 = (5, 5, 15, 15)
        self.assertTrue(extents_overlap(extent1, extent2))

    def test_extents_touching(self):
        extent1 = (0, 0, 10, 10)
        extent2 = (10, 10, 20, 20)
        self.assertFalse(extents_overlap(extent1, extent2))

    def test_extents_no_overlap(self):
        extent1 = (0, 0, 10, 10)
        extent2 = (20, 20, 30, 30)
        self.assertFalse(extents_overlap(extent1, extent2))

class TestProcessShapefile(unittest.TestCase):

    @patch('sentinel_utils.utils.mask')
    @patch('sentinel_utils.utils.rasterio.open')
    @patch('sentinel_utils.utils.gpd.read_file')
    def test_process_shapefile(self, mock_read_file, mock_rasterio_open, mock_mask):
        # Create a mock GeoDataFrame
        mock_gdf = gpd.GeoDataFrame({
            'geometry': [box(0, 0, 5, 5), box(5, 5, 10, 10)],
            'POSTCODE': ['12345', '67890']
        }, crs='EPSG:4326')
        mock_read_file.return_value = mock_gdf.to_crs('EPSG:4326')

        # Mock rasterio.open and mask
        mock_src = MagicMock()
        mock_src.bounds = (0, 0, 10, 10)
        mock_src.crs.to_string.return_value = 'EPSG:4326'
        mock_rasterio_open.return_value.__enter__.return_value = mock_src
        mock_mask.return_value = (np.array([[[1, 2], [3, 4]]]), None)

        tif_path = 'test.tif'
        shp_path = 'test.shp'
        tif_crs = 'EPSG:4326'
        results = []
        date_time = datetime.now()

        # Call the function
        process_shapefile(tif_path, shp_path, tif_crs, results, date_time)

        # Check the results
        expected_results = [
            {'postcode': '12345', 'mean_value': 2.5, 'date_time': date_time, 'tif_path': tif_path, 'shp_path': shp_path},
            {'postcode': '67890', 'mean_value': 2.5, 'date_time': date_time, 'tif_path': tif_path, 'shp_path': shp_path}
        ]
        self.assertEqual(results, expected_results)

    @patch('sentinel_utils.utils.mask')
    @patch('sentinel_utils.utils.rasterio.open')
    @patch('sentinel_utils.utils.gpd.read_file')
    def test_process_shapefile_mask_contains_null(self, mock_read_file, mock_rasterio_open, mock_mask):
        # Create a mock GeoDataFrame
        mock_gdf = gpd.GeoDataFrame({
            'geometry': [box(0, 0, 5, 5)],
            'POSTCODE': ['12345']
        }, crs='EPSG:4326')
        mock_read_file.return_value = mock_gdf.to_crs('EPSG:4326')

        # Mock rasterio.open and mask
        mock_src = MagicMock()
        mock_src.bounds = (0, 0, 10, 10)
        mock_src.crs.to_string.return_value = 'EPSG:4326'
        mock_rasterio_open.return_value.__enter__.return_value = mock_src
        mock_mask.return_value = (np.array([[[1, 2], [np.nan, 4]]]), None)

        tif_path = 'test.tif'
        shp_path = 'test.shp'
        tif_crs = 'EPSG:4326'
        results = []
        date_time = datetime.now()

        # Call the function
        process_shapefile(tif_path, shp_path, tif_crs, results, date_time)

        # Check the results
        expected_results = [
            {'postcode': '12345', 'mean_value': 2.3333333333333335, 'date_time': date_time, 'tif_path': tif_path, 'shp_path': shp_path}
        ]
        self.assertEqual(results, expected_results)

    @patch('sentinel_utils.utils.mask')
    @patch('sentinel_utils.utils.rasterio.open')
    @patch('sentinel_utils.utils.gpd.read_file')
    def test_process_shapefile_mask_null(self, mock_read_file, mock_rasterio_open, mock_mask):
        # Create a mock GeoDataFrame
        mock_gdf = gpd.GeoDataFrame({
            'geometry': [box(0, 0, 5, 5)],
            'POSTCODE': ['12345']
        }, crs='EPSG:4326')
        mock_read_file.return_value = mock_gdf.to_crs('EPSG:4326')

        # Mock rasterio.open and mask
        mock_src = MagicMock()
        mock_src.bounds = (0, 0, 10, 10)
        mock_src.crs.to_string.return_value = 'EPSG:4326'
        mock_rasterio_open.return_value.__enter__.return_value = mock_src
        mock_mask.return_value = (np.array([]), None)

        tif_path = 'test.tif'
        shp_path = 'test.shp'
        tif_crs = 'EPSG:4326'
        results = []
        date_time = datetime.now()

        # Call the function
        process_shapefile(tif_path, shp_path, tif_crs, results, date_time)

        # Check the results
        expected_results = [
            {'postcode': '12345', 'mean_value': np.nan, 'date_time': date_time, 'tif_path': tif_path, 'shp_path': shp_path}
        ]
        self.assertEqual(len(results), len(expected_results))
        self.assertTrue(np.isnan(results[0]['mean_value']))

if __name__ == '__main__':
    unittest.main()
