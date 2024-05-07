#========================================================

import s3fs  # Importing the s3fs library for accessing S3 buckets
import xarray as xr  # Importing xarray library for working with multidimensional arrays
import time  # Importing the time library for recording execution time
import shapely  # Importing shapely for geometric operations 
import thalassa  # Importing thalassa library for STOFS data analysis
from thalassa import api  # Importing thalassa API for data handling
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
    s3 = s3fs.S3FileSystem(anon=True)  # Enable anonymous access to the S3 bucket
    url = f"s3://{bucket_name}/{key}"
    ds = api.open_dataset(s3.open(url, 'rb'), drop_variables=['nvel'])  # Open NetCDF dataset and drop 'nvel' variable
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

# Testing Thalassa library

start_time = time.time()  # Record the start time

bucket_name = 'noaa-gestofs-pds'
key = '_para3/stofs_2d_glo.20231130/stofs_2d_glo.t00z.fields.cwl.nc'

dataset = read_netcdf_from_s3(bucket_name, key)  # Read NetCDF data from S3 bucket

end_time = time.time()  # Record the end time
execution_time = end_time - start_time  # Calculate execution time
print(f"Execution time for reading data: {execution_time} seconds")  # Print execution time

start_time = time.time()  # Record the start time

# Define the bounding box
box = (-70, -60, 40, 50)

ds2 = subset_thalassa(dataset, box)  # Subset the thalassa dataset

end_time = time.time()  # Record the end time
execution_time = end_time - start_time  # Calculate execution time
print(f"Execution time for subsetting: {execution_time} seconds")  # Print execution time

start_time = time.time()  # Record the start time

output_file = 'stofs_subset2.nc'

save_subset_to_netcdf(ds2, output_file)  # Save the subset to a NetCDF file

end_time = time.time()  # Record the end time
execution_time = end_time - start_time  # Calculate execution time

print(f"Execution time for writing: {execution_time} seconds")  # Print execution time

#========================================================
