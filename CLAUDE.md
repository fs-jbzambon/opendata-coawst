# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Purpose

Infrastructure and notebooks for converting USGS COAWST (Coupled Ocean Atmosphere Wave and Sediment Transport) model output (US East & Gulf Coast, 2009–2022) from NetCDF to cloud-optimized formats and publishing to AWS Open Data (`s3://usgs-coawst/useast-archive`).

## Data Processing Pipeline

The workflow runs on USGS HPC (Vortex) with SLURM job arrays:

1. **NetCDF → Zarr**: `coawst2zarr.py` processes one week of data per job, using Dask for parallelism. Each week becomes a Zarr store, then gets rechunked for cloud efficiency.
2. **Zarr → NetCDF**: `zarr2nc.py` converts rechunked Zarr back to compressed NetCDF4 for S3 publication.
3. **S3 publication**: `run_aws_coawst_copy.sh` syncs output to S3 via `aws s3 sync`.
4. **Kerchunk references**: `coawst_open_data_create_refs.ipynb` creates virtual Zarr references from remote S3 NetCDF files, enabling fast access without full re-upload.

SLURM submission scripts: `run_coawst2zarr.sh`, `run_zarr2nc.sh`.

## Access / Exploration

- **Notebook**: `COAWST_explore.ipynb` — main entry point for interactive data exploration; works on SageMaker Studio Lab, Coiled, or local Jupyter.
- **Intake catalog**: `coawst_intake.yml` — programmatic access to remote Zarr datasets (supports both Zarr v2 and v3).

## Environments

Multiple conda environment files target different contexts:

| File | Use |
|------|-----|
| `environment.yml` | Base local environment (Python 3.11) |
| `coiled_pangeo_notebook_env.yml` | Full Coiled/Pangeo cloud environment (Python 3.12) |
| `pangeo_coiled_env.yml` | Alternative Coiled environment |

Create/update env: `conda env create -f environment.yml` or `mamba env create -f environment.yml`.

## Key Tech Stack

- **Xarray + Zarr + Kerchunk**: cloud-native array access
- **Rechunker**: rewriting Zarr stores with optimal chunk shapes
- **Dask / Dask Gateway / Coiled**: distributed parallel processing
- **fsspec / s3fs**: cloud filesystem abstraction
- **Intake**: data catalog for remote dataset discovery
- **HvPlot / GeoViews / Datashader**: geospatial interactive visualization

## Source Data Location

Raw NetCDF files are on USGS HPC at `/proj/usgs-share/Projects/COAWST`, also accessible via the USGS Globus endpoint.
