#!/usr/bin/env python
"""
Build Icechunk store for USGS COAWST US East / Gulf Coast hindcast.

Source:  s3://usgs-coawst/useast-archive/*.nc  (HDF5/NetCDF4, ~168 steps/file)
Store:   s3://usgs-coawst/useast-archive/icechunk/coawst-useast.icechunk
Creds:   AWS_PROFILE=coawst_open_data (write); anon=True (source reads)

Local run:
    AWS_PROFILE=coawst_open_data python -u build_icechunk.py

Coiled (detached):
    coiled run --software coawst-icechunk --workspace esip-lab \\
        --region us-west-2 --vm-type r5.xlarge \\
        --env AWS_PROFILE=coawst_open_data \\
        --detach \\
        -- python -u build_icechunk.py
"""
import os
import sys
import time

import coiled
import numpy as np
import s3fs
import xarray as xr
import icechunk
from icechunk import (
    ManifestConfig,
    ManifestSplitCondition,
    ManifestSplitDimCondition,
    ManifestSplittingConfig,
)
from dask.distributed import Client

sys.stdout.reconfigure(line_buffering=True)

# ── Constants ──────────────────────────────────────────────────────────────────
BATCH_SIZE  = 30
MAX_BATCHES = None
bucket      = 'usgs-coawst'
region      = 'us-west-2'
PROD_PREFIX = 'useast-archive/icechunk/coawst-useast.icechunk'
SKIP_VARS   = ['dstart']
TIME_DIM    = 'ocean_time'

# ── Credentials: set AWS_PROFILE so s3fs/obstore/icechunk all pick it up ───────
_profile = os.environ.get('AWS_PROFILE', 'coawst_open_data')
os.environ['AWS_PROFILE'] = _profile
print(f'Using AWS profile: {_profile!r}')

# ── Coiled Dask cluster ────────────────────────────────────────────────────────
cluster = coiled.Cluster(
    n_workers=10,
    software='coawst-icechunk',
    workspace='esip-lab',
    region=region,
    worker_vm_types=['m5.xlarge'],
    scheduler_vm_types=['m5.xlarge'],
    name='coawst-icechunk',
    shutdown_on_close=True,
)
client = Client(cluster)
print(f'Dask dashboard: {client.dashboard_link}')

# ── Source files ───────────────────────────────────────────────────────────────
fs    = s3fs.S3FileSystem(anon=True)
flist = sorted(fs.glob(f'{bucket}/useast-archive/*.nc'))
flist = [f's3://{f}' for f in flist]
print(f'{len(flist)} source files')

# ── Icechunk repo ──────────────────────────────────────────────────────────────
split_config = ManifestSplittingConfig.from_dict({
    ManifestSplitCondition.AnyArray(): {
        ManifestSplitDimCondition.DimensionName(TIME_DIM): 365 * 24
    }
})
config = icechunk.RepositoryConfig(
    manifest=ManifestConfig(splitting=split_config),
)
config.set_virtual_chunk_container(
    icechunk.VirtualChunkContainer(
        url_prefix=f's3://{bucket}/',
        store=icechunk.s3_store(region=region, anonymous=True),
    ),
)
storage = icechunk.s3_storage(
    bucket=bucket,
    prefix=PROD_PREFIX,
    region=region,
    from_env=True,
)
repo    = icechunk.Repository.open_or_create(storage, config)

# ── Worker: build virtual dataset for one HDF5/NetCDF4 file ───────────────────
def make_vds_remote(url):
    """
    VirtualiZarr 2 virtual dataset for one COAWST file.
    ocean_time is loaded as actual values (small: ~168 per file) to allow concat.
    All other variables become virtual (byte-range) references into S3.
    """
    from virtualizarr import open_virtual_dataset
    from virtualizarr.parsers import HDFParser
    from obspec_utils.registry import ObjectStoreRegistry
    from obstore.store import S3Store

    _store   = S3Store.from_url('s3://usgs-coawst',
                                config={'skip_signature': True, 'region': 'us-west-2'})
    registry = ObjectStoreRegistry({'s3://usgs-coawst': _store})

    return open_virtual_dataset(
        url,
        registry=registry,
        parser=HDFParser(),
        loadable_variables=['ocean_time'],
        drop_variables=['dstart'],
    )


def get_ntime_remote(url):
    """Read ocean_time length from HDF5 header without loading data."""
    import s3fs
    import xarray as xr
    _fs = s3fs.S3FileSystem(anon=True)
    with xr.open_dataset(_fs.open(url), engine='h5netcdf',
                         decode_times=False, chunks=None) as ds:
        return int(ds.dims['ocean_time'])


# ── Reference dataset on coordinator (identify static vs time-varying vars) ────
print('Building reference vds on coordinator ...')
_vds0 = make_vds_remote(flist[0])
time_var_names = frozenset(
    name for name, var in _vds0.data_vars.items()
    if var.dims and var.dims[0] == TIME_DIM
)
print(f'  {len(time_var_names)} time-varying vars, '
      f'{len(_vds0.data_vars) - len(time_var_names)} static vars')

# ── Resume: check existing committed steps ─────────────────────────────────────
try:
    creds_ro   = icechunk.containers_credentials({
        f's3://{bucket}/': icechunk.s3_credentials(anonymous=True)
    })
    repo_ro    = icechunk.Repository.open(storage, config,
                                          authorize_virtual_chunk_access=creds_ro)
    session_ro = repo_ro.readonly_session('main')
    ds_existing = xr.open_zarr(session_ro.store, consolidated=False, chunks=None)
    n_existing  = len(ds_existing[TIME_DIM])
    print(f'Store has {n_existing:,} committed time steps')
    if n_existing > 0:
        print(f'  time range: {ds_existing[TIME_DIM].values[0]} → '
              f'{ds_existing[TIME_DIM].values[-1]}')
except Exception as e:
    n_existing = 0
    print(f'Store is empty or new: {e}')

# ── Count time steps per file in parallel ─────────────────────────────────────
print('Reading time step counts in parallel ...')
t0_hdrs  = time.perf_counter()
ntimes   = np.array(client.gather(client.map(get_ntime_remote, flist)), dtype=np.int32)
cumsteps = np.concatenate([[0], np.cumsum(ntimes)])
total_expected = int(cumsteps[-1])
print(f'  {time.perf_counter()-t0_hdrs:.1f}s  total expected: {total_expected:,} steps')

start_file_idx = int(np.searchsorted(cumsteps, n_existing, side='left'))
if start_file_idx >= len(flist):
    print('All files already committed — nothing to do.')
    client.close(); cluster.close()
    raise SystemExit(0)

step_count  = int(cumsteps[start_file_idx])
first_write = (n_existing == 0)
n_batches   = (len(flist) - start_file_idx + BATCH_SIZE - 1) // BATCH_SIZE
print(f'Starting from file {start_file_idx}  step_count={step_count:,}  '
      f'({n_batches} batches remaining)')

# ── Main batch loop ────────────────────────────────────────────────────────────
t0_total = time.perf_counter()
session  = repo.writable_session('main')

for batch_start in range(start_file_idx, len(flist), BATCH_SIZE):
    batch_num_so_far = (batch_start - start_file_idx) // BATCH_SIZE + 1
    if MAX_BATCHES is not None and batch_num_so_far > MAX_BATCHES:
        print(f'Reached MAX_BATCHES={MAX_BATCHES}, stopping.')
        break

    t0_batch     = time.perf_counter()
    batch_files  = flist[batch_start : batch_start + BATCH_SIZE]
    batch_ntimes = ntimes[batch_start : batch_start + BATCH_SIZE]
    batch_num    = batch_num_so_far
    batch_end    = min(batch_start + BATCH_SIZE - 1, len(flist) - 1)
    print(f'  Batch {batch_num}/{n_batches}: submitting files {batch_start}–{batch_end} ...')

    batch_vds = client.gather(client.map(make_vds_remote, batch_files))
    print(f'  Batch {batch_num}/{n_batches}: gathered {len(batch_vds)} vds  '
          f'({time.perf_counter()-t0_batch:.1f}s)')

    print(f'  Batch {batch_num}/{n_batches}: concat ...')
    # Filter to variables with consistent dtypes across all files in the batch.
    # Some COAWST weekly files differ in dtype for a few variables; those are dropped.
    consistent_vars = []
    for name in time_var_names:
        dtypes = {vds[name].dtype for vds in batch_vds if name in vds}
        if len(dtypes) == 1:
            consistent_vars.append(name)
        else:
            print(f'    WARNING: dropping {name!r} — inconsistent dtypes across batch: {dtypes}')
    time_only    = [vds[consistent_vars] for vds in batch_vds]
    batch_concat = xr.concat(time_only, dim=TIME_DIM,
                             data_vars='minimal', coords='minimal',
                             compat='override')

    print(f'  Batch {batch_num}/{n_batches}: to_icechunk ...')
    if first_write:
        static_vars = {k: v for k, v in _vds0.data_vars.items()
                       if k not in time_var_names}
        full_batch  = xr.merge([batch_concat, xr.Dataset(static_vars)],
                               compat='override')
        full_batch.attrs = _vds0.attrs
        full_batch.vz.to_icechunk(session.store)
        first_write = False
    else:
        batch_concat.vz.to_icechunk(session.store, append_dim=TIME_DIM)

    print(f'  Batch {batch_num}/{n_batches}: committing ...')
    step_count += int(batch_ntimes.sum())
    snap        = session.commit(
        f'Batch files {batch_start}–{batch_end} | '
        f'steps {step_count - int(batch_ntimes.sum())}–{step_count}'
    )
    session = repo.writable_session('main')

    pct           = step_count / total_expected * 100
    elapsed_batch = time.perf_counter() - t0_batch
    print(f'  Batch {batch_num}/{n_batches}: done — files {batch_start}–{batch_end}  '
          f'+{int(batch_ntimes.sum()):,} steps  total={step_count:,} ({pct:.1f}%)  '
          f'{elapsed_batch:.1f}s  snap={snap[:8]}')

total_elapsed = time.perf_counter() - t0_total
print(f'\nComplete! {step_count:,} steps from {len(flist)} files '
      f'in {total_elapsed / 60:.1f} min')

client.close()
cluster.close()
