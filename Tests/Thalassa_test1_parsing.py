#========================================================

"""
Author: Atieh Alipour
Description: This script reads a NetCDF file from an S3 bucket using Thalassa API, subsets the data based on a bounding box,
             and saves the subset to a new NetCDF file.

Usage:
    python script_name.py -bucket BUCKET_NAME -key FILE_KEY -box X_MIN X_MAX Y_MIN Y_MAX

Arguments:
    - bucket: Name of the S3 bucket where the NetCDF file is located.
    - key: Key/path to the NetCDF file in the S3 bucket.
    - box: Bounding box coordinates in the format X_MIN X_MAX Y_MIN Y_MAX.

Example:
    python Thalassa_test1_parsing.py -bucket 'noaa-gestofs-pds' -key '_para3/stofs_2d_glo.20231130/stofs_2d_glo.t00z.fields.cwl.nc' -box -70 -60 40 50
"""

#========================================================

import s3fs  # Importing the s3fs library for accessing S3 buckets
import xarray as xr  # Importing xarray library for working with multidimensional arrays
import time  # Importing the time library for recording execution time
import shapely  # Importing shapely for geometric operations 
import thalassa  # Importing thalassa library for STOFS data analysis
from thalassa import api  # Importing thalassa API for data handling
import argparse  # Importing argparse for command-line argument parsing
import numpy as np  
import pandas as pd 

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
    ds = api.open_dataset(s3.open(url, 'rb'))
    return ds

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
    bbox = shapely.box(box[0], box[2], box[1], box[3])
    new_ds = thalassa.crop(ds, bbox)
    return new_ds

#========================================================

def save_subset_to_netcdf(xarray_ds, input_file):
    """
    Function to save a subset of an xarray Dataset to a NetCDF file with a dynamically generated name.
    
    Parameters:
    - xarray_ds: Subset of the xarray Dataset
    - input_file: Path to the input NetCDF file
    """
    # Extract the dataset name from the input file path without the extension
    dataset_name = input_file.split('/')[-1].split('.')[0]
    
    # Generate the output file name using the dataset name and "subset"
    output_file = f"{dataset_name}_subset.nc"
    
    # Save the subset to the output NetCDF file
    xarray_ds.to_netcdf(output_file)

#========================================================

# Parse command-line arguments

parser = argparse.ArgumentParser(description='Subset NetCDF data from an S3 bucket.')
parser.add_argument('-bucket', help='Name of the S3 bucket', required=True)  # Argument for S3 bucket name
parser.add_argument('-key', help='Key/path to the NetCDF file in the bucket', required=True)  # Argument for file key/path
parser.add_argument('-box', nargs='+', type=float, help='Bounding box coordinates (x_min, x_max, y_min, y_max)', required=True)  # Argument for bounding box coordinates
args = parser.parse_args()  # Parsing the command-line arguments

# Testing Thalassa library

start_time = time.time()  # Record the start time

bucket_name = args.bucket  # Assigning the S3 bucket name from the command-line argument
key = args.key  # Assigning the key/path to the NetCDF file from the command-line argument
box = tuple(args.box)  

# Reading NetCDF data from the specified S3 bucket and key
dataset = read_netcdf_from_s3(bucket_name, key)

end_time = time.time()  # Record the end time
execution_time = end_time - start_time  # Calculating execution time
print(f"Execution time for reading data: {execution_time} seconds")  # Printing the execution time for reading data

start_time = time.time()  # Start measuring time again for subsetting

# Performing subset operation using Thalassa library
ds2 = subset_thalassa(dataset, box)

end_time = time.time()  # Record the end time
execution_time = end_time - start_time  # Calculating execution time for subsetting
print(f"Execution time for subsetting: {execution_time} seconds")  # Printing the execution time for subsetting

start_time = time.time()  # Record the start time

save_subset_to_netcdf(ds2, args.key)  # Saving the subset data to a NetCDF file

end_time = time.time()  # Record the end time
execution_time = end_time - start_time  # Calculating execution time for writing
print(f"Execution time for writing: {execution_time} seconds")  # Printing the execution time for writing
