---
noteId: "b4ba84a0a36d11ef89de79e5b895e518"
tags: []

---

# Source Code Documentation

## Main Processing Files
- `pc_main.py`: Core framework for postcode-level data processing
- `main.py`: Primary dataset generation script

## Building Data Processing
- `pre_process_buildings.py`: Initial building data preprocessing
- `global_av.py`: Generates global building averages

## Age Processing
- `age_perc_calc.py`: Calculates building age percentages
- `age_perc_proc.py`: Processes age distribution data

## Building Type Processing
- `type_calc.py`: Calculates building type distributions
- `type_proc.py`: Processes building typology data

## Fuel Processing
- `fuel_calc.py`: Calculates fuel type distributions
- `fuel_proc.py`: Processes fuel usage data

## Utilities
- `multi_thread.py`: Multithreading functionality
- `postcode_utils.py`: General postcode processing utilities
- `post_process.py`: Final data cleanup and processing
- `split_onsud_file.py`: ONSUD file splitting utilities

## Global Averages
The `global_avs/` directory contains reference tables used for building statistics calculations.