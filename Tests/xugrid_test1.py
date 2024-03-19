#========================================================

import s3fs  # Importing the s3fs library for accessing S3 buckets
import xarray as xr  # Importing xarray library for working with multidimensional arrays
import time  # Importing the time library for recording execution time
import xugrid as xu  # Importing xugrid library for working with unstructured grids

#========================================================

def read_netcdf_from_s3(bucket_name, key):
    """
    Function to read a NetCDF file from an S3 bucket.
    
    Parameters:
    - bucket_name: Name of the S3 bucket
    - key: Key/path to the NetCDF file in the bucket
    
    Returns:
    - ds: xarray Dataset containing the NetCDF data
    """
    s3 = s3fs.S3FileSystem(anon=True)  # Enable anonymous access to the S3 bucket
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

def save_subset_to_netcdf(xarray_ds, output_file):
    """
    Function to save a subset of an xarray Dataset to a NetCDF file.
    
    Parameters:
    - xarray_ds: Subset of the xarray Dataset
    - output_file: Path to save the output NetCDF file
    """
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

# Testing xugrid library

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

uds = xu.UgridDataset(dataset)  # Convert dataset to Ugrid format

end_time = time.time()  # Record the end time
execution_time = end_time - start_time  # Calculate execution time
print(f"Execution time for converting to Ugrid: {execution_time} seconds")  # Print execution time

start_time = time.time()  # Record the start time

uds2 = subset_ugrid(uds, box)  # Subset the Ugrid dataset

end_time = time.time()  # Record the end time
execution_time = end_time - start_time  # Calculate execution time
print(f"Execution time for subsetting: {execution_time} seconds")  # Print execution time

start_time = time.time()  # Record the start time

output_file = 'stofs_subset1.nc'

save_subset_to_netcdf(uds2, output_file)  # Save the subset to a NetCDF file

end_time = time.time()  # Record the end time
execution_time = end_time - start_time  # Calculate execution time

print(f"Execution time for writing: {execution_time} seconds")  # Print execution time

#========================================================
