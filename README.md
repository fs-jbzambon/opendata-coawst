# opendata-coawst
Scripts and code related to the USGS COAWST US East and Gulf Coast forecast model archive dataset on AWS Open Data Program. 

## Processing Steps
### Rechunking the NetCDF files 
The [official USGS Data Publication for these files](https://www.sciencebase.gov/catalog/item/610acd4fd34ef8d7056893da) lists the [Globus Endpoint](https://app.globus.org/file-manager?origin_id=2e58c429-d1cf-4808-85a7-0d8214a4547e&origin_path=%2F) from which the original NetCDF files can be obtained.  These NetCDF files have 12 or 13 hourly time steps, and were rechunked to be more performant on the cloud and better support a variety of use cases. 

The scripts here were run on an HPC system with the files residing at `/proj/usgs-share/Projects/COAWST`, following the same directory structure as the Globus endpoint (for example, `/proj/usgs-share/Projects/COAWST/2009/coawst_us_20090821_01.nc`)

The first script, `coawst2zarr.py`, runs in parallel using Dask uses all CPUs:
* Creates an empty Zarr dataset exactly one week long (168 hourly time steps)
* Finds NetCDF files with data within this time period
* Writes each time step of data found into the proper location in the Zarr dataset
* After all the data has been written, uses [rechunker](https://github.com/pangeo-data/rechunker) to create a new zarr dataset with chunk sizes `nt = 168, ny = 168, nx = 224`.

The second script, 'zarr2nc.py', runs serially using only one CPU:
* Converts a zarr dataset into a NetCDF4 file, specifying compression settings. 

We process all the weeks of the archive using a SLURM job array.  This allows weeks to be processed in parallel subject to the availabilty of nodes:
* `run_zarr2coawst.sh` is submitted, which creates all the rechunked week-long zarr datasets
* `run_zarr2nc.sh` is submitted, which converts the week-long rechunked zarr datasets into week-long NetCDF files
