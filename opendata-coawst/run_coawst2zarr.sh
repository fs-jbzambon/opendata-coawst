#!/bin/bash
#SBATCH --partition=usgs                # Select queue 
#SBATCH --qos=usgs
#SBATCH --job-name=coawst2zarr             # Job name
#SBATCH --mail-type=ALL                 # Mail events 
#SBATCH --mail-user=rsignell@usgs.gov   # Where to send mail	
##SBATCH --ntasks=1                    # Number of MPI ranks
#SBATCH --nodes=1
##SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=36               # Number of cores per MPI rank 
#SBATCH --time=00:40:00                # Time limit hrs:min:sec
#SBATCH --output=coawst2zarr.log           # Standard output and error log

#SBATCH --array=736-740
##SBATCH --array=501-735

date

export PATH="$PATH:$HOME/mambaforge/bin"

 
echo "Running COAWST2Zarr "

source activate pangeo

python coawst2zarr.py 
 
date
