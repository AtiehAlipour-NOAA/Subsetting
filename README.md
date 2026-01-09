# Subsetting Tools for STOFS Model Output

This repository contains tools for subsetting STOFS model output.

## Download

To download the available codes and files from this repository, clone it to your local machine using Git:

   ```
   git clone https://github.com/AtiehAlipour-NOAA/Subsetting.git
   ```

If you are running the code on your local machine, click [here](https://github.com/AtiehAlipour-NOAA/Subsetting/archive/refs/heads/main.zip)  to download the entire repository. After downloading, extract the contents to access the files.

## Setup Python Environment on HPC

We recommend setting up a Conda virtual environment to manage dependencies and ensure a consistent environment. If you haven't already, install Conda by following the instructions [here](https://docs.anaconda.com/free/miniconda/).

Once Conda is installed, create a new virtual environment using the provided `environment.yml` file. Run the following command:
   ```
  conda env create -n subsetting -f environment.yml
   ```

The `environment.yml` file contains the necessary packages and dependencies for this project.

## Setup Python Environment on Windows

Please use Anaconda to set up the Python environment and run the codes.
Install Anaconda, which will also install Python. Anaconda can be [downloaded freely](https://www.anaconda.com/download/). If you are a NOAA employee, please contact your local IT to install.

Open the Anaconda Powershell Prompt and navigate to the directory where you extracted the Subsetting files and where the requirements.txt file is located.

Run the following command to create a virtual environment called env_subsetting:

  ```
 python -m venv env_subsetting
  ```
Activate the environment with the command below:
 
  ```
 .\env_subsetting\Scripts\activate
  ```

After activation, you should see (env_subsetting) before (base) in the command line.

Now use the following command to install the necessary libraries and set up the environment for the subsetting code using the requirements.txt file:

  ```
 pip install -r requirements.txt
  ```

Once the installation is complete, start Jupyter Notebook by typing:

  ```
 jupyter notebook
  ```

This will open a new web browser tab or window where you can access and run the code.

## Usage

Example codes for subsetting multiple files are available in the `notebooks` folder. You can refer to these examples to understand how to use the tools provided in this repository.

**Baseline codes** are available in the `Tests` folder.

Feel free to explore the repository and contribute to its development!

