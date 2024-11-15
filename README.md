# NEBULA Dataset Generation

This repository contains scripts for generating the NEBULA dataset, a postcode-level dataset for neighbourhood energy modelling. 

## Prerequisites

# Install requirements
```
# Create new environment
conda create -n nebula python=3.10

# Activate environment
conda activate nebula

# Install requirements
pip install -r requirements.txt
conda install libtiff==4.4.0
```

### Required Data
#### User provided (not open licence)
1. Building Stock Data (Verisk)
3. Postcode Shapefiles (Edina)

#### Provided in Drive - download zip file and place in input_data_sources

1. Gas and Electricity data (DESNZ) (2022)
2. ONS ONSUD UPRN to Postcode Mapping (2022)
2. Global Averages for building floor count (derivation script is provided)
3. Overlapping postcodes text file (overlap between regions, excluded in dataset)
4. VStreet Postcodes (excluded)


## Directory Structure

```
input_data_sources/
    ├── gas data
    ├── electricity data
    ├── postcode shapefiles
    ├── Building stock data 
    ├── OA to LSOA mapping
    ├── 
batches/  # Stores batch lists used in processing pipeline, more info in src/split_onsud_file
    ...
src/
    ├── global_avs/
        ├── Global average tables
    ├── age_perc_calc.py   # Age percentage calculations
    ├── age_perc_proc.py   # Age percentage processing
    ├── fuel_calc.py       # Fuel type calculations
    ├── fuel_proc.py       # Fuel type processing
    ├── global_av.py       # Generation of global averages
    ├── multi_thread.py    # Multithreading utilities
    ├── pc_main.py        # Framework for postcode level processing
    ├── post_process.py   # Post-processing utilities
    ├── postcode_utils.py # Utils functions
    ├── pre_process_buildings.py # Building data preprocessing
    ├── split_onsud_file.py     # ONSUD file splitting
    ├── type_calc.py      # Building type calculations
    └── type_proc.py      # Building type processing
main.py # Dataset generation script 

```

## Usage

1. Set up your environment and install dependencies (requirements.txt recommended)
2. Place input data in appropriate directories
3. update any variables as needed in main.py
4. Execute the main processing pipeline:
```bash
python main.py
```
5. If any problems, you can re-run subsections of the pipeline from within main.py

## Output

The pipeline generates postcode-level statistics including:
- Building age distributions
- Building type distributions
- Local temperature data (HDD and CDD)
- Sociodemographics
- Global averages and statistics

## Notes

- Check `overlapping_pcs.txt` for any postcode overlap issues
- The `global_avs/` directory contains reference averages

<!-- ## File Descriptions

- `postcode_utils.py`: Helper functions for postcode processing
- `multi_thread.py`: Parallel processing implementation
- `global_av.py`: Global average calculations
- `post_process.py`: Final data cleanup and validation
 -->
