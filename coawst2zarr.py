#!/usr/bin/env python
# coding: utf-8

# # Create one week COAWST Netcdf
# 
# * create an empty zarr template for the 1 week dataset 
# * write the first 12 steps of each NetCDF file into the zarr dataset using `region=`
# * rechunk the resulting 1 week zarr dataset using rechunker 
# * convert the rechunked zarr dataset to NetCDF


import dask.distributed
from dask.distributed import Client, LocalCluster, performance_report
import numpy as np
import xarray as xr
import pandas as pd
from rechunker import rechunk
import fsspec
from pathlib import Path
import shutil
import zarr
import time
import numpy as np
import os

fs = fsspec.filesystem('file', skip_instance_cache=True, use_listings_cache=False)

prefix = '/vortexfs1/share/usgs-share/Projects/COAWST'
#prefix = '/proj/usgs-share/Projects/COAWST'    # better?

fgood = sorted(fs.glob(f'{prefix}/20*/coawst_us_20*.nc'))

print(len(fgood))
print(fgood[1])
print(fgood[-1])

start = xr.open_dataset(fgood[0]).ocean_time[1].values
print(start)
end = xr.open_dataset(fgood[-1]).ocean_time[-1].values
print(end)

coords = ['s_rho', 's_w', 'lon_rho', 'lat_rho', 'lon_u', 'lat_u', 'lon_v', 'lat_v', 'lon_psi', 'lat_psi']

chunk_plan={}
for coord in coords:
    chunk_plan[coord]=-1

nt_vals = 12
chunk_plan['ocean_time'] = nt_vals

# Use last forecast as the template

ds0 = xr.open_dataset(fgood[-5], decode_timedelta=False, chunks=chunk_plan)

print(len(xr.open_dataset(fgood[0]).data_vars))
print(len(ds0.data_vars))


def time_vars(ds):
    not_tvars=[]
    tvars=[]
    for var in ds.data_vars:
        if 'ocean_time' not in ds[var].dims:
            not_tvars.append(var)
        else:
            tvars.append(var)
    return tvars, not_tvars

tvars0, not_tvars0 = time_vars(ds0)

source_dataset = ds0[tvars0].drop_vars(coords)

dates = pd.date_range(start=start, end=end, freq='1h')

weeks = pd.date_range(start=start, end=end, freq='7d')

try: 
    week_index = int(os.environ['SLURM_ARRAY_TASK_ID'])
except:
    week_index = 10

print(weeks[week_index])
fname_date = weeks[week_index].strftime('%Y-%m-%d')

out_prefix = '/vortexfs1/usgs/rsignell/coawst-archive'
dst_zarr = f'{out_prefix}/dst_{week_index}.zarr'
dst_rechunked = f'{out_prefix}/rechunk_{fname_date}_{week_index:04d}.zarr'
zarr_temp = f'{out_prefix}/tmp_{week_index}.zarr'

date0 = pd.date_range(start=weeks[week_index], periods=24*7, freq='1h')

file_dates = pd.date_range(start=date0[0], periods=14, freq='12h')

template = (source_dataset.chunk().pipe(xr.zeros_like).isel(ocean_time=0, drop=True).expand_dims(ocean_time=len(date0)))
template['ocean_time'] = date0
template = template.chunk({'ocean_time': nt_vals})
template.to_zarr(dst_zarr, compute=False, consolidated=True, mode='w')

ds0[not_tvars0].to_zarr(dst_zarr, mode='a')

def date2filename(date):
    run_year = date.strftime('%Y')
    run_date = date.strftime('%Y%m%d_%H')
    return  f'{prefix}/{run_year}/coawst_us_{run_date}.nc'
    
files = [date2filename(date) for date in file_dates]

i = 0
for file in files:
    i = i + 1
    try:
        ds = xr.open_dataset(file, decode_timedelta=False, 
                             chunks=chunk_plan)
        ds = ds.isel(ocean_time=slice(-nt_vals,None))
        tvars, constant_vars = time_vars(ds)
        dst = ds.drop_vars(coords)[tvars]
        try:
            dst = dst.rename({'wet_dry_masking':'wetdry_mask_rho'})
        except:
            pass
        try:
            dst = dst.drop_vars('ero_flux')
        except:
            pass
        try:
            dst = dst.drop_vars('Ub_swan')
        except:
            pass
        try:
            dst = dst.drop_vars('Wave_dissip')
        except:
            pass
        istart = (i-1)*nt_vals
        istop = i*nt_vals
        print(istart,istop)
        # make sure the data fits in here:
        if dst['ocean_time'][0] == template['ocean_time'][istart:istop][0]:
            dst.to_zarr(dst_zarr, region={'ocean_time': slice(istart,istop)})
            print(istart,istop,file)
        else:
            fname = Path(file).stem
            print(f'Skipping {fname}: {dst.ocean_time.values[0]}')
            print(dst['ocean_time'][0] - template['ocean_time'][istart:istop][0])
    except:
        print(f'failed to process {file}') 
        pass

nt = 168
ny = 168
nx = 224


dst_chunks={'ocean_time':nt, 
        'eta_rho':ny, 'eta_u':ny, 'eta_v':ny, 'eta_psi':ny, 
        'xi_rho':nx, 'xi_u':nx, 'xi_v':nx, 'xi_psi':nx, 
        's_rho':1, 's_w':1, 'Nbed':1}

# take the first step of the dataset with only time vars and expand the time dim

max_mem = '12GB'    # 75% of worker mem

def rechunker_wrapper(source_store, target_store, temp_store, chunks=None, 
                      mem='2GB', consolidated=True, verbose=True):

    # convert str to paths
    def maybe_convert_to_path(p):
        if isinstance(p, str):
            return Path(p)
        else:
            return p

    source_store = maybe_convert_to_path(source_store)
    target_store = maybe_convert_to_path(target_store)
    temp_store = maybe_convert_to_path(temp_store)

    # erase target and temp stores
    if temp_store.exists():
        shutil.rmtree(temp_store)

    if target_store.exists():
        shutil.rmtree(target_store)


    if isinstance(source_store, xr.Dataset):
        g = source_store  # trying to work directly with a dataset
        ds_chunk = g
    else:
        g = zarr.group(str(source_store))
        # get the correct shape from loading the store as xr.dataset and parse the chunks
        ds_chunk = xr.open_zarr(str(source_store))
        

    # convert all paths to strings
    source_store = str(source_store)
    target_store = str(target_store)
    temp_store = str(temp_store)

    group_chunks = {}
    # newer tuple version that also takes into account when specified chunks are larger than the array
    for var in ds_chunk.variables:
        # pick appropriate chunks from above, and default to full length chunks for dimensions that are not in `chunks` above.
        group_chunks[var] = []
        for di in ds_chunk[var].dims:
            if di in chunks.keys():
                if chunks[di] > len(ds_chunk[di]):
                    group_chunks[var].append(len(ds_chunk[di]))
                else:
                    group_chunks[var].append(chunks[di])

            else:
                group_chunks[var].append(len(ds_chunk[di]))

        group_chunks[var] = tuple(group_chunks[var])
    if verbose:
        print(f"Rechunking to: {group_chunks}")
    rechunked = rechunk(g, group_chunks, mem, target_store, temp_store=temp_store)
    rechunked.execute(retries=10)
    if consolidated:
        if verbose:
            print('consolidating metadata')
        zarr.convenience.consolidate_metadata(target_store)
    if verbose:
        print('removing temp store')
    shutil.rmtree(temp_store)
    if verbose:
        print('done')

rechunker_wrapper(dst_zarr, dst_rechunked, zarr_temp, mem=max_mem, consolidated=True, verbose=False,
                  chunks=dst_chunks)


def rechunker_cleanup(target_store, temp_store):

    # convert str to paths
    def maybe_convert_to_path(p):
        if isinstance(p, str):
            return Path(p)
        else:
            return p

    target_store = maybe_convert_to_path(target_store)
    temp_store = maybe_convert_to_path(temp_store)

    # erase target and temp stores
    if temp_store.exists():
        shutil.rmtree(temp_store)

    if target_store.exists():
        shutil.rmtree(target_store)

rechunker_cleanup(dst_zarr, zarr_temp)
