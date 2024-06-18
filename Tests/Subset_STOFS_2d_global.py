#========================================================

"""
Author: Atieh Alipour

Description: This script reads a STOFS-2d-Global NetCDF file from an S3 bucket using Thalassa API, subsets the data based on a bounding box,
             and saves the subset to a new NetCDF file.

Arguments:
    --bucket_name: Specifies the name of the S3 bucket where the STOFS data is stored.
    --model_name: Name of the STOFS model used (e.g., 'stofs_2d_glo').
    --dates: List of dates in YYYYMMDD format for which data subsets are to be generated.
    --cycles: List of cycle times in HH format (e.g., 00, 06, 12, 18) indicating the times of day for which forecasts are generated.
    --regions: String argument containing bounding boxes for regions as tuples (min_lon, max_lon, min_lat, max_lat).
    --stofs_files: List of STOFS file names to be processed (e.g., 'fields.cwl', 'fields.htp', 'fields.swl').
    --region_names: Names corresponding to each region specified in --regions.

Example:

python Subset_STOFS_2d_global.py --bucket_name "noaa-gestofs-pds" \
                        --model_name "stofs_2d_glo" \
                        --dates "20240516" "20240517" "20240518" \
                        --cycles "00" "06" "12" "18" \
                        --regions "(144.5, 145.9, 13.1, 15.5)(151.3, 152.2, 6.8, 7.8)(170.8, 172.1, 6.7, 7.5)" \
                        --stofs_files "fields.cwl" "fields.htp" "fields.swl" \
                        --region_names "Marianas" "Chuuk Lagoon" "Majuro"


"""

#========================================================

import dask
import geoviews as gv
import holoviews as hv
import numcodecs
import numpy as np
import pandas as pd
import shapely
import xarray as xr
import matplotlib.pyplot as plt
import s3fs  # Importing the s3fs library for accessing S3 buckets
import time  # Importing the time library for recording execution time
import shapely  # Importing shapely for geometric operations 
import thalassa  # Importing thalassa library for STOFS data analysis
from thalassa import api  # Importing thalassa API for data handling
from thalassa import normalization
from thalassa import utils
from holoviews import opts as hvopts
from holoviews import streams
from holoviews.streams import PointerXY
from holoviews.streams import Tap
import bokeh.plotting as bp
import panel as pn
from os.path import exists
import os
import argparse  # Importing argparse for command-line argument parsing


#========================================================

def read_netcdf_from_s3(bucket_name, key):
    """
    Function to read a NetCDF file from an S3 bucket using thalassa API.
    
    Parameters:
    - bucket_name: Name of the S3 bucket
    - key: Key/path to the NetCDF file in the bucket
    
    Returns:
    - ds: xarray Dataset containing the NetCDF data
    """
    s3 = s3fs.S3FileSystem(anon=True)
    url = f"s3://{bucket_name}/{key}"
    

    ds = xr.open_dataset(s3.open(url, 'rb'), drop_variables=['nvel'])
    return ds

#========================================================

def normalize_data(ds, name, cycle, bucket_name, base_key, field_cwl , filename, date):
    """
    Function to modify/normalize a dataset using the Thalassa package.

    Parameters:
    - ds: xarray Dataset containing the data
    - name: folder name 
    - bucket_name: Name of the S3 bucket
    - base_key: Base key for the dataset in the S3 bucket
    - schout: adcirc like file name
    - filename: Original filename to be replaced
    - date: Date string for the new filename
    
    Returns:
    - normalized_ds: Thalassa dataset ready for cropping or visualizing
    """

    if 'element' in ds:
        normalized_ds = thalassa.normalize(ds)
    else:
        
        key = f'{base_key}/{name}.{filename}'
        ds_with_element_key = key.replace(filename,  f't{cycle}z.{field_cwl}.nc')
        ds_with_element = read_netcdf_from_s3(bucket_name, ds_with_element_key)  # Read NetCDF data from S3 bucket

        # Modify the field2d.nc file based on schout_adcirc.nc file
        ds['nele'] = ds_with_element['nele']
        ds['nvertex'] = ds_with_element['nvertex']
        ds['element'] = ds_with_element['element']

        # Normalize data
        normalized_ds = thalassa.normalize(ds)

    return normalized_ds

#========================================================

def subset_thalassa(ds, box):
    """
    Function to subset a thalassa Dataset based on a bounding box using shapely.
    
    Parameters:
    - ds: thalassa Dataset containing the data
    - box: Tuple representing the bounding box (x_min, x_max, y_min, y_max)
    
    Returns:
    - new_ds: Subset of the input dataset within the specified bounding box
    """
    bbox = shapely.box(box[0], box[2], box[1], box[3])  # Create a shapely box from the bounding box coordinates
    new_ds = thalassa.crop(ds, bbox)  # Crop the dataset using the bounding box
    return new_ds

#========================================================


def save_subset_to_netcdf(xarray_ds, output_file):
    """
    Function to save a subset of an xarray Dataset to a NetCDF file.
    
    Parameters:
    - xarray_ds: Subset of the xarray Dataset
    - output_file: Path to save the output NetCDF file
    """
    xarray_ds.to_netcdf(output_file)  # Save the subset to a NetCDF file

#========================================================

def parse_regions(regions_str):
    """
    Function to parse regions from a string into a list of tuples.

    Parameters:
    - regions_str (str): String containing regions in the format '(min_lon, max_lon, min_lat, max_lat)(min_lon, max_lon, min_lat, max_lat)(min_lon, max_lon, min_lat, max_lat)'

    Returns:
    - regions (list of tuples): List of tuples representing bounding boxes for regions.
    """
    # Remove any spaces and split by ')( ' pattern
    regions_list = regions_str.strip().split(')(')

    # Strip parentheses from the first and last region definitions
    regions_list[0] = regions_list[0].lstrip('(')
    regions_list[-1] = regions_list[-1].rstrip(')')

    # Split each region definition and convert to tuple of floats
    regions = [tuple(map(float, region.split(','))) for region in regions_list]

    return regions

#========================================================

def main(bucket_name, model_name, dates, cycles, regions, stofs_files, region_names):
    """
    Main function to process STOFS data subsets.

    Parameters:
    - bucket_name (str): Name of the S3 bucket.
    - model_name (str): Name of the STOFS model.
    - dates (list of str): List of dates in YYYYMMDD format.
    - cycles (list of str): List of cycles in HH format (e.g., 00, 06, 12, 18).
    - regions (list of tuples): List of tuples representing bounding boxes for regions.
    - stofs_files (list of str): List of STOFS file names.
    - region_names (list of str): List of names corresponding to each region.

    """
    for date in dates:
        for cycle in cycles:
            base_key = f'{model_name}.{date}'

            for stofs_file in stofs_files:
                filename = f't{cycle}z.{stofs_file}.nc'
                key = f'{base_key}/{model_name}.{filename}'
                
                dataset = read_netcdf_from_s3(bucket_name, key)
                normalized_dataset = normalize_data(dataset, model_name, cycle, bucket_name, base_key,
                                                    'fields.cwl', filename, date)
                for idx, box in enumerate(regions):
                    ds_subset = subset_thalassa(normalized_dataset, box)

                    output_dir = f'./{region_names[idx]}/{date}'
                    os.makedirs(output_dir, exist_ok=True)

                    output_file = f'{output_dir}/{filename}'
                    save_subset_to_netcdf(ds_subset, output_file)
                    print(f'Saved subset to: {output_file}')

#========================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Subset STOFS data from S3')
    parser.add_argument('--bucket_name', type=str, help='Name of the S3 bucket')
    parser.add_argument('--model_name', type=str, help='Name of the model')
    parser.add_argument('--dates', nargs='+', type=str, help='Dates in YYYYMMDD format')
    parser.add_argument('--cycles', nargs='+', type=str, help='Cycles in HH format (e.g., 00, 06, 12, 18)')
    parser.add_argument('--regions', type=str, help='Bounding boxes for regions as string (min_lon, max_lon, min_lat, max_lat)(min_lon, max_lon, min_lat, max_lat)(min_lon, max_lon, min_lat, max_lat)')
    parser.add_argument('--stofs_files', nargs='+', type=str, help='STOFS file names')
    parser.add_argument('--region_names', nargs='+', type=str, help='Names corresponding to each region')

    args = parser.parse_args()

    regions = parse_regions(args.regions)
    
        
    main(args.bucket_name, args.model_name, args.dates, args.cycles, regions, args.stofs_files, args.region_names)





