import subprocess
import os
import sys

# Path to scripts folder (relative to this file)
SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))

# List of update scripts
scripts = [
    "update_era5.py",
    "update_nasa.py",
    "update_openmeteo.py"
]

def run_script(script_name):
    script_path = os.path.join(SCRIPTS_DIR, script_name)
    print(f"\n Running {script_name} ...")
    try:
        subprocess.run([sys.executable, script_path], check=True)
        print(f" {script_name} completed successfully.")
    except subprocess.CalledProcessError as e:
        print(f" {script_name} failed with error: {e}")

def main():
    for script in scripts:
        run_script(script)

if __name__ == "__main__":
    main()
