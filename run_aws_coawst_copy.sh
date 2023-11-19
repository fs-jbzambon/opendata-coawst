#!/bin/bash
#SBATCH --partition=usgs                # Select queue 
#SBATCH --qos=usgs
#SBATCH --job-name=aws-copy              # Job name
#SBATCH --mail-type=ALL                 # Mail events 
#SBATCH --mail-user=rsignell@usgs.gov   # Where to send mail	
#SBATCH --ntasks=1                    # Number of MPI ranks
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1               # Number of cores per MPI rank 
#SBATCH --mem-per-cpu=2048mb            # Memory per processor
#SBATCH --time=24:00:00                # Time limit hrs:min:sec
#SBATCH --output=aws-copy.log           # Standard output and error log

date
 
echo "Running AWS transfer"

source activate pangeo
# aws s3 cp /vortexfs1/usgs/rsignell/HRRR/zarr/hrrr s3://esip-qhub/noaa/HRRR/2019 --profile esip-qhub --recursive

# aws s3 cp /vortexfs1/usgs/rsignell/rechunk/breach_matanzas_GTM_inwave.nc s3://esip-qhub/usgs/rsignell/breach_matanzas_GTM_inwave.nc --profile esip-qhub
aws s3 sync /proj/usgs/rsignell/coawst-archive/nc s3://usgs-coawst/useast-archive --profile coawst_open_data
 
date
