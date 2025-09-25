#!/bin/bash
#SBATCH -A CULLEN-SL3-CPU
#SBATCH -p icelake
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=00:25:00
#SBATCH --mail-type=NONE
#SBATCH --mem=8G
#SBATCH --output=logs/nebula_%A_%a.out
#SBATCH --error=logs/nebula_%A_%a.err

# Load required modules
. /etc/profile.d/modules.sh
module purge
module load rhel7/default-ccl

# Initialize conda
CONDA_BASE=/usr/local/software/archive/linux-scientific7-x86_64/gcc-9/miniconda3-4.7.12.1-rmuek6r3f6p3v6fdj7o2klyzta3qhslh
source $CONDA_BASE/etc/profile.d/conda.sh

# Activate the nebula environment
conda activate /home/gb669/.conda/envs/nebula


# Set environment variables
export SLURM_SUBMIT_DIR='/home/gb669/rds/hpc-work/energy_map/NebulaDataset'
cd $SLURM_SUBMIT_DIR

# Create logs directory
mkdir -p logs

# Get the batch path for this array job
batch_path=$(sed -n "${SLURM_ARRAY_TASK_ID}p" batch_paths.txt)

# Set paths
export PC_SHP_PATH='/rds/user/gb669/hpc-work/energy_map/data/postcode_polygons/codepoint-poly_5267291'
export BUILDING_PATH='/rds/user/gb669/hpc-work/energy_map/data/building_files/UKBuildings_Edition_15_new_format_upn.gpkg'
export ONSUD_BASE='/home/gb669/rds/hpc-work/energy_map/data/onsud_files/Data'
export GAS_PATH='/home/gb669/rds/hpc-work/energy_map/data/input_data_sources/energy_data/Postcode_level_gas_2022.csv'
export ELEC_PATH='/home/gb669/rds/hpc-work/energy_map/data/input_data_sources/energy_data/Postcode_level_all_meters_electricity_2022.csv'
export ENERGY='no'
export AGE='yes'
export TYPE='yes'

# Log job info
echo "Job started at: $(date)"
echo "Running on node: $HOSTNAME"
echo "Processing batch path: $batch_path"

# Run the processing script
python generate_building_stock.py "$batch_path"