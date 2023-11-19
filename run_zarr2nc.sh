#!/bin/bash
#SBATCH --partition=usgs                # Select queue 
#SBATCH --qos=usgs
#SBATCH --job-name=zarr2nc             # Job name
#SBATCH --mail-type=ALL                 # Mail events 
#SBATCH --mail-user=rsignell@usgs.gov   # Where to send mail	
##SBATCH --ntasks=1                    # Number of MPI ranks
#SBATCH --nodes=1
##SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1                # Number of cores per MPI rank 
#SBATCH --time=00:40:00                # Time limit hrs:min:sec
#SBATCH --output=zarr2nc.log           # Standard output and error log

#SBATCH --array=736-740

date

export PATH="$PATH:$HOME/mambaforge/bin"

echo "Running Zarr2NC"

source activate pangeo

python zarr2nc.py 
 
date
