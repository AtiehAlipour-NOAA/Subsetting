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
    python xugrid_test1_parsing.py -bucket 'noaa-gestofs-pds' -key '_para3/stofs_2d_glo.20231130/stofs_2d_glo.t00z.fields.cwl.nc' -box -70 -60 40 50
"""

#========================================================

import s3fs  # Importing the s3fs library for accessing S3 buckets
import xarray as xr  # Importing xarray library for working with multidimensional arrays
import time  # Importing the time library for recording execution time
import xugrid as xu  # Importing xugrid library for working with unstructured grids
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
    ds = xr.open_dataset(s3.open(url, 'rb'), drop_variables=['nvel'])  # Open NetCDF dataset and drop 'nvel' variable
    return ds

#========================================================

def subset_ugrid(ds, box):
    """
    Function to subset an unstructured grid dataset based on a bounding box.
                                                                        
    Parameters:
    - ds: xarray Dataset containing the unstructured grid data
    - box: Tuple representing the bounding box (x_min, x_max, y_min, y_max)
                                                                                        
    Returns:
    - new_ds: Subset of the input dataset within the specified bounding box
    """
    new_ds = ds.ugrid.sel(y=slice(box[2], box[3]), x=slice(box[0], box[1]))  # Subset based on coordinates
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

    xarray_ds.ugrid.to_netcdf(output_file)  # Save the subset to a NetCDF file


#========================================================

def convert_to_xarray(dataset):

    """
    Function to convert a dataset to an xarray Dataset format.
    
    Parameters:
    - dataset: Input dataset to convert
    
    Returns:
    - xarray_ds: Converted xarray Dataset
    """
    

    xarray_data = {}
    
    for varname, variable in dataset.variables.items():
        xarray_data[varname] = (variable.dims, variable.values)
    xarray_ds = xr.Dataset(xarray_data, attrs=dataset.attrs)  # Create xarray Dataset
    
    return xarray_ds

#========================================================

# Parse command-line arguments
parser = argparse.ArgumentParser(description='Subset NetCDF data from an S3 bucket.')
parser.add_argument('-bucket', help='Name of the S3 bucket', required=True)
parser.add_argument('-key', help='Key/path to the NetCDF file in the bucket', required=True)
parser.add_argument('-box', nargs='+', type=float, help='Bounding box coordinates (x_min, x_max, y_min, y_max)', required=True)
args = parser.parse_args()


# Testing xugrid library
start_time = time.time()  # Record the start time

# Assigning command-line arguments to variables
bucket_name = args.bucket  # Name of the S3 bucket
key = args.key  # Key/path to the NetCDF file in the bucket
box = tuple(args.box)  # Bounding box coordinates

# Reading NetCDF data from the specified S3 bucket and key
dataset = read_netcdf_from_s3(bucket_name, key)

end_time = time.time()  # Record the end time
execution_time = end_time - start_time  # Calculate execution time
print(f"Execution time for reading data: {execution_time} seconds")  # Print execution time

start_time = time.time()  # Record the start time

# Convert dataset to Ugrid format
uds = xu.UgridDataset(dataset)

end_time = time.time()  # Record the end time
execution_time = end_time - start_time  # Calculate execution time
print(f"Execution time for converting to Ugrid: {execution_time} seconds")  # Print execution time

start_time = time.time()  # Record the start time

# Subset the Ugrid dataset
uds2 = subset_ugrid(uds, box)

end_time = time.time()  # Record the end time
execution_time = end_time - start_time  # Calculate execution time
print(f"Execution time for subsetting: {execution_time} seconds")  # Print execution time

start_time = time.time()  # Record the start time

# Save the subset to a NetCDF file
save_subset_to_netcdf(uds2, args.key)
end_time = time.time()  # Record the end time
execution_time = end_time - start_time  # Calculate execution time
print(f"Execution time for writing: {execution_time} seconds")  # Print execution time
                                                                                              
