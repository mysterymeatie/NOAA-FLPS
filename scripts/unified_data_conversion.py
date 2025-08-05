#!/usr/bin/env python3
"""
Unified Data Conversion Pipeline Orchestrator

This script calls the specialized processing scripts for each data source
to convert them into a unified NetCDF format.
"""

import subprocess
import sys
from pathlib import Path

def run_script(script_path):
    """Run a python script as a subprocess."""
    try:
        print(f"--- Running {script_path.name} ---")
        # Ensure we use the same python interpreter that is running this script
        result = subprocess.run([sys.executable, str(script_path)], check=True, capture_output=True, text=True)
        print(result.stdout)
        if result.stderr:
            print("Stderr:", result.stderr)
        print(f"--- Finished {script_path.name} ---\n")
    except subprocess.CalledProcessError as e:
        print(f"!!! ERROR running {script_path.name} !!!")
        print(e.stdout)
        print(e.stderr)
        sys.exit(1) # Exit if a script fails

def main():
    """Run the full data conversion pipeline."""
    print("=== STARTING UNIFIED DATA CONVERSION PIPELINE ===\n")
    
    # Get the directory of the current script to build paths to others
    base_path = Path(__file__).parent.joinpath('data/scripts/processing')

    scripts_to_run = [
        base_path / 'process_srtm.py',
        base_path / 'process_modis.py',
        base_path / 'process_calfire.py',
        base_path / 'process_hrrr.py',
    ]
    
    for script in scripts_to_run:
        if not script.exists():
            print(f"Error: Script not found at {script}")
            sys.exit(1)
        run_script(script)
        
    print("\n=== FULL CONVERSION PIPELINE COMPLETED SUCCESSFULLY ===")
    print("Next steps:")
    print("1. Verify coordinate alignment across all NetCDF files in data/unified/")
    print("2. Create ML dataset from unified NetCDF files")
    print("3. Run quality control checks")

if __name__ == '__main__':
    main()
