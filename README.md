# opendata-coawst
Scripts and code related to the USGS COAWST US East and Gulf Coast forecast model archive dataset on AWS Open Data Program.  The model archive can be explored using the `COAWST_explore.ipynb` notebook.  

#### [Rendered version of the COAWST_explore notebook](https://nbviewer.org/gist/rsignell/7a4ccbbe91bfd682380129d2a67db88a)

## Launch in SageMaker Studio Lab
If you have an AWS SageMaker Studio Lab account, you can open in Studio Lab using the button below, then when prompted, choose to download the whole repo and to build the conda environment.   If you don't have an account, you can [sign up for free](https://studiolab.sagemaker.aws) (no AWS account required).

[![Open In SageMaker Studio Lab](https://studiolab.sagemaker.aws/studiolab.svg)](https://studiolab.sagemaker.aws/import/github/https://github.com/fs-jbzambon/opendata-coawst/blob/main/COAWST_explore.ipynb)

## Run with Coiled
You can run this notebook on a Jupyterlab instance on AWS us-west-2 using Coiled.     For example:
```
coiled env create --name pangeo-notebook --workspace esip-lab --conda coiled_pangeo_notebook_env.yml
coiled env create --name esip-pangeo-arm --workspace esip-lab --conda pangeo_env.yml --architecture aarch64  

coiled notebook start --region us-west-2 --vm-type m5.xlarge --software pangeo-notebook --name unconf --workspace esip-lab
< open a terminal on jupyterlab>
git clone https://github.com/fs-jbzambon/opendata-coawst.git
< run COAWST_explore notebook!>
```
## Data Processing Steps
### Rechunking the NetCDF files 
The [official USGS Data Publication for these files](https://www.sciencebase.gov/catalog/item/610acd4fd34ef8d7056893da) lists the [Globus Endpoint](https://app.globus.org/file-manager?origin_id=2e58c429-d1cf-4808-85a7-0d8214a4547e&origin_path=%2F) from which the original NetCDF files can be obtained.  These NetCDF files have 12 or 13 hourly time steps, and were rechunked to be more performant on the cloud and better support a variety of use cases. 

The scripts here were run on an HPC system with the files residing at `/proj/usgs-share/Projects/COAWST`, following the same directory structure as the Globus endpoint (for example, `/proj/usgs-share/Projects/COAWST/2009/coawst_us_20090821_01.nc`)

The first script, `coawst2zarr.py`, runs in parallel using Dask using all CPUs available on the node:
* Creates an empty Zarr dataset exactly one week long (168 hourly time steps)
* Finds NetCDF files with data within this time period
* Writes each time step of data found into the proper location in the Zarr dataset
* After all the data has been written, uses [rechunker](https://github.com/pangeo-data/rechunker) to create a new zarr dataset with chunk sizes `nt = 168, ny = 168, nx = 224`.

The second script, `zarr2nc.py`, runs serially using only one CPU:
* Converts a zarr dataset into a NetCDF4 file, specifying compression settings. 

We process all the weeks of the archive using a SLURM job array.  This allows weeks to be processed in parallel subject to the availabilty of nodes:
* `run_zarr2coawst.sh` is submitted, which creates all the rechunked week-long zarr datasets
* `run_zarr2nc.sh` is submitted, which converts the week-long rechunked zarr datasets into week-long NetCDF files

### Creating references for the rechunked NetCDF files
The Jupyter notebook `coawst_open_data_create_refs.ipynb` reads the remote NetCDF files on the AWS Open Data bucket and creates a references file using Kerchunk.  This references file can then be used to open the entire collection of NetCDF files as Xarray DataSet using the Zarr library.




