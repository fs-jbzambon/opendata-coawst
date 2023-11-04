# convert zarr dataset to python based on job array index
import xarray as xr
import fsspec
import os

def zname2ncname(zarr):
    a = zarr.split('_')
    week_date = a[1]
    week_index = a[2].split('.')[0]
    return f'/proj/usgs/rsignell/coawst-archive/nc/coawst_{week_date}_{week_index}.nc'
    
def make_nc(zarr):
    ds = xr.open_dataset(zarr, engine='zarr', chunks={})
    encoding={}
    for var in ds.variables:
        encoding[var] = dict(zlib=True, complevel=5, 
                         fletcher32=False, shuffle=True,
                         chunksizes=ds[var].encoding['chunks'])
    nc_out = zname2ncname(zarr)
    ds.to_netcdf(nc_out, encoding=encoding, mode='w')

fs = fsspec.filesystem('file')
zarr_list = fs.glob('/proj/usgs/rsignell/coawst-archive/rechunk_*_????.zarr')

try: 
    week_index = int(os.environ['SLURM_ARRAY_TASK_ID'])
except:
    week_index = 3
    
zs = f'{week_index:04d}'
matching_zarr = [s for s in zarr_list if zs in s][0]

print(matching_zarr)

make_nc(matching_zarr)
