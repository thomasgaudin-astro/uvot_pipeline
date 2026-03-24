# -*- coding: utf-8 -*-
import os
import subprocess

import pandas as pd
import numpy as np

import math
import time
import gc

import re

import shutil
from astropy.wcs import WCS
from astropy.io import fits
from astropy.table import QTable, Table
import astropy.units as u
from astropy.coordinates import SkyCoord

from swifttools.swift_too import TOO, Resolve, ObsQuery, Data

from tqdm import tqdm

import requests
from requests.auth import HTTPBasicAuth

import tkinter as tk
from tkinter import filedialog

# Execution backend: "native" (mac) or "wsl" (windows)
HEASOFT_BACKEND = "wsl"   # change to "native" on mac


class DownloadError(Exception):
    """Raise when requests status quo does not return 200."""
    pass



def run_heasoft_command(command):
    """
    Runs HEASOFT commands through the appropriate backend.
    WSL ONLY - uses conda henv environment.
    """
    print(f"\n[SYSTEM]: Running HEASOFT command...")
    
    if HEASOFT_BACKEND == "wsl":
        full_cmd = f"conda activate henv && {command}"
        result = subprocess.run(
            ["wsl", "bash", "-ic", full_cmd],
            text=True,
            capture_output=True,
        )
        
        if result.returncode != 0:
            print("  [RESULT]: FAILED")
            print("--- Error Details ---")
            print(result.stderr)
        else:
            print("  [RESULT]: SUCCESS")
        
        return result
    
    else:
        raise NotImplementedError("backend not yet implemented for this function")  # THOMAS this is were you have to put your version of the backend that has to run
        # Your "subprocess.run" I think it might just be "subprocess.run(['bash', '-i', '-c', uvotdetect_command],capture_output=True,text=True)" for you but I would rather have you do it
        # All future functions like "run_fkeyprint" use this instead, as then there only has to be one check for the system.


# UTILS FOR CROSS-PLATFORM PATHS 

def prepare_path(path):
    """
    - On Windows/WSL: Translates C:\ to /mnt/c/
    - On Mac/Linux: Returns the path exactly as it is.
    """
    if HEASOFT_BACKEND == "native":
        return path  # Do nothing for Mac users, I dont know if you need anything here. I guess no buuuuut.
    
    # WSL logic only executes if backend is 'wsl'
    abs_path = os.path.abspath(path)
    drive, rest = os.path.splitdrive(abs_path)
    if drive:
        drive_letter = drive[0].lower()
        return f"/mnt/{drive_letter}{rest.replace('\\', '/')}"
    return abs_path.replace('\\', '/')

def find_obs_file(base_path, obsid, band, file_type='sk'):
    """
    Finds the actual path of a file even if the folder has extra date tags. This is 100% only becuase I have done that.
    """
    target_filename = f"sw{obsid}{band}_{file_type}.img.gz"
    for root, dirs, files in os.walk(base_path):
        # Check if we are in the correct subfolder structure
        if obsid in root and root.endswith(os.path.join("uvot", "image")):
            if target_filename in files:
                return os.path.join(root, target_filename)
    return None




##
# This isnt used anywere Else currently This is Thomas Version That I dont know how to integrate and I dont want to touch, So I dont.    
def create_uvotdetect_bash_command(source_path, output_path, exposure_path, reg_path):

    # Construct bash command
    bash_command = f"""
    bash -c '
    source {os.environ['HEADAS']}/headas-init.sh
    uvotdetect \\
        infile={source_path} \\
        outfile={output_path} \\
        expfile={exposure_path} \\
        threshold=3 \\
        sexfile=DEFAULT \\
        plotsrc=NO \\
        regfile={reg_path} \\
        zerobkg=0.03 \\
        expopt=BETA \\
        calibrate=YES \\
        clobber=YES
    '
    """

    return bash_command


def run_uvotdetect(uvotdetect_command):

    # Run the command
    result = subprocess.run(
        ['bash', '-i', '-c', uvotdetect_command],
        capture_output=True,
        text=True
    )

    # print("STDOUT:\n", result.stdout)
    # print("STDERR:\n", result.stderr)

    return result.stdout

def run_uvotdetect_verbose(uvotdetect_command):

    # Run the command
    result = subprocess.run(
        ['bash', '-i', '-c', uvotdetect_command],
        capture_output=True,
        text=True
    )

    print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)

    return result.stdout



#WSL UVOTDETECT version, Thomas if you so desire and think my logical bellow is good and would like to use it, you can edit to the code to add
# If WSL elements, As currently this is later called with a If WSL rather then being built in.
def batch_run_uvotdetect_wsl(base_path):
    
# The six UVOT filter bands we care about
    BANDS = ["uvv", "uuu", "ubb", "um2", "uw1", "uw2"]

    def get_extension_count(filepath):
        try:
            with fits.open(filepath) as hdul:
                return len(hdul) - 1
        except Exception as e:
            print(f"  Error reading FITS: {e}")
            return 0

    print("\n" + "=" * 70)
    print("BATCH UVOTDETECT")
    print("=" * 70)

    # Walk the entire directory tree under base_path, looking for directories named "image" 
    for root, dirs, files in os.walk(base_path):
        if os.path.basename(root) != "image":
            continue

        # Convert the Windows path to a WSL-compatible path
        img_dir_heasoft = prepare_path(root)

        print(f"\n Processing image directory:")
        print(f"{root}")

        # Regex to match UVOT sky image filenames:
        obsid_pattern = re.compile(r"sw(\d{11})([a-z0-9]+)_sk\.img\.gz")

        for file in files:
            # Try to match the filename against the expected pattern
            match = obsid_pattern.match(file)
            if not match:
                continue

            OBSID, band = match.groups()

            # Skip non-UVOT bands (e.g. XRT files that might match the pattern and I did install some of those by accident. Also safty first.)
            if band not in BANDS:
                continue

            print(f"\n Found SK image: {file}")

            # Use the helper function to get the full resolved path to the SK file
            sk_file_path = find_obs_file(base_path, OBSID, band, file_type='sk')

            if not sk_file_path:
                print(f" Could not find SK file for OBSID={OBSID}, band={band}")
                continue

            # Check how many image extensions the FITS file contains
            ext_count = get_extension_count(sk_file_path)
            print(f" {ext_count} image extension(s) found")

            # Get just the filename (no directory) for the HEASOFT command
            sk_filename = os.path.basename(sk_file_path)

            if ext_count > 1:
                # MULTI-EXTENSION: create a detect file for EACH extension
                print(f" Creating detect files for {ext_count} extensions...")

                for ext in range(1, ext_count + 1):
                    # Name the output file with the extension number I.E. uw1_detect_ext1.fits, uw1_detect_ext2.fits
                    detect_ext = f"{band}_detect_ext{ext}.fits"
                    detect_ext_path = os.path.join(root, detect_ext)

                    # Skip if this extension's detect file already exists
                    if os.path.exists(detect_ext_path):
                        print(f" Extension {ext} detect exists — skipping")
                        continue

                    print(f" Running detect on extension {ext}")

                    detect_ext_filename = os.path.basename(detect_ext_path)

                    #the uvotdetect command.
                    uvotdetect_cmd = (
                        f"cd '{img_dir_heasoft}' && "
                        f"uvotdetect << end\n"
                        f"{sk_filename}[{ext}]\n"
                        f"{detect_ext_filename}\n"
                        f"NONE\n"
                        f"3\n"
                        f"end"
                    )
                    run_heasoft_command(uvotdetect_cmd)

            else:
                # SINGLE EXTENSION: create one detect file for the band, I.E. uw1_detect.fits
                detect_base = f"{band}_detect.fits"
                detect_path = os.path.join(root, detect_base)

                if os.path.exists(detect_path):
                    print(" Detect file already exists — skipping")
                    continue

                detect_filename = os.path.basename(detect_path)

                print(" Running single-extension detect...")

                # No [ext] suffix needed the whole file is one extension
                uvotdetect_cmd = (
                    f"cd '{img_dir_heasoft}' && "
                    f"uvotdetect << end\n"
                    f"{sk_filename}\n"
                    f"{detect_filename}\n"
                    f"NONE\n"
                    f"3\n"
                    f"end"
                )
                run_heasoft_command(uvotdetect_cmd)

    print("\n UVOT Detect processing complete!")




########################################################################### 
def run_fkeyprint(fkeyprint_command):

    result = run_heasoft_command(fkeyprint_command)

    
    # print("STDOUT:\n", result.stdout)
    # print("STDERR:\n", result.stderr)

    return result.stdout

def run_fkeyprint_verbose(fkeyprint_command):

    result = run_heasoft_command(fkeyprint_command)
    
    print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)

    return result.stdout

def create_fappend_bash_command(file1_name, file2_name):

    file11_name = prepare_path(file1_name)
    file22_name = prepare_path(file2_name)


    return f'fappend ="{file11_name}" outfile="{file22_name}"'

def run_fappend(fappend_command):

    result = run_heasoft_command(fappend_command)
    
    # print("STDOUT:\n", result.stdout)
    # print("STDERR:\n", result.stderr)

    return result.stdout

def run_fappend_verbose(fappend_command):

    result = run_heasoft_command(fappend_command)

    print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)

    return result.stdout
####################################################################################


############################################ Bellow is unicorr for Windows 


def create_uvotunicorr_full_command_wsl(ref_frame, obs_frame, band, ref_snapshot, obs_snapshot, obspath=None):
    """
    WSL version of uvotunicorr command creator.
    
    Args:
        ref_frame: Reference ObsID
        obs_frame: Observation ObsID  
        band: Filter band
        ref_snapshot: Extension number for REFERENCE
        obs_snapshot: Extension number for OBSERVATION
        obspath: Directory path
    """
    
    # Build file paths
    if obspath:
        ref_filepath = os.path.join(obspath, f'sw{ref_frame}{band}_sk.img')
        obs_filepath = os.path.join(obspath, f'sw{obs_frame}{band}_sk.img')
        ref_reg_filepath = os.path.join(obspath, 'ref.reg')
        obs_reg_filepath = os.path.join(obspath, 'obs.reg')
    else:
        ref_filepath = f'sw{ref_frame}{band}_sk.img'
        obs_filepath = f'sw{obs_frame}{band}_sk.img'
        ref_reg_filepath = 'ref.reg'
        obs_reg_filepath = 'obs.reg'
    
    # Convert paths for WSL
    ref_filepath = prepare_path(ref_filepath)
    obs_filepath = prepare_path(obs_filepath)
    ref_reg_filepath = prepare_path(ref_reg_filepath)
    obs_reg_filepath = prepare_path(obs_reg_filepath)
    
    # Add DIFFERENT extensions for ref vs obs
    ref_filepath += f'[{ref_snapshot}]'  # Use ref's extension
    obs_filepath += f'[{obs_snapshot}]'  # Use obs's extension, the used to be matching that was a bad idea.
    
    # Build command
    command = (
        f"uvotunicorr "
        f"obsfile='{obs_filepath}' "
        f"reffile='{ref_filepath}' "
        f"obsreg='{obs_reg_filepath}' "
        f"refreg='{ref_reg_filepath}'"
    )
    
    return command




##################################### Bellow is unicorr for MACOS 
def create_uvotunicorr_bash_command(ref_frame, obs_frame, obspath=None):

    if obspath:
        ref_filepath = obspath+f'/sw{ref_frame}uw1_sk.img[1]'
        obs_filepath = obspath+f'/sw{obs_frame}uw1_sk.img[1]'
        ref_reg_filepath = obspath+'/ref.reg'
        obs_reg_filepath = obspath+'/obs.reg'
    else:
        ref_filepath = f'sw{ref_frame}uw1_sk.img[1]'
        obs_filepath = f'sw{obs_frame}uw1_sk.img[1]'
        ref_reg_filepath = 'ref.reg'
        obs_reg_filepath = 'obs.reg'
    
    bash_command = f"""
        bash -c '
        uvotunicorr obsfile={obs_filepath} reffile={ref_filepath} obsreg={obs_reg_filepath} refreg={ref_reg_filepath}
        '
        """

    return bash_command

def create_uvotunicorr_too_bash_command(ref_frame, obs_frame, band, snapshot, obspath=None):

    if obspath:
        ref_filepath = obspath+f'/sw{ref_frame}{band}_sk.img[{snapshot}]'
        obs_filepath = obspath+f'/sw{obs_frame}{band}_sk.img[{snapshot}]'
        ref_reg_filepath = obspath+'/ref.reg'
        obs_reg_filepath = obspath+'/obs.reg'
    else:
        ref_filepath = f'sw{ref_frame}{band}_sk.img[{snapshot}]'
        obs_filepath = f'sw{obs_frame}{band}_sk.img[{snapshot}]'
        ref_reg_filepath = 'ref.reg'
        obs_reg_filepath = 'obs.reg'
    
    bash_command = f"""
        bash -c '
        uvotunicorr obsfile={obs_filepath} reffile={ref_filepath} obsreg={obs_reg_filepath} refreg={ref_reg_filepath}
        '
        """

    return bash_command

def run_uvotunicorr(uvotunicorr_command):

    run_heasoft_command(uvotunicorr_command)


    # print("STDOUT:\n", result.stdout)
    # print("STDERR:\n", result.stderr)

    return result.stdout

def run_uvotunicorr_verbose(uvotunicorr_command):

    run_heasoft_command(uvotunicorr_command)

    print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)

    return result.stdout

def create_uvotimsum_too_bash_command(source_name, obsid, band, file_type, exclude=None, ref_frame=False):
    
    infile_path = f'./{source_name}/TOO/{obsid}/uvot/image/sw{obsid}{band}_{file_type}.img.gz'

    if ref_frame == True:
        outfile_path = f'./{source_name}/Ref_Frames/{obsid}_{band}_summed.fits'
    else:
        if file_type == 'sk':
            outfile_path = f'./{source_name}/TOO/{obsid}/uvot/image/{band}_summed.fits'
        
        elif file_type == 'ex':
            outfile_path = f'./{source_name}/TOO/{obsid}/uvot/image/{band}_ex_summed.fits'
    
    if exclude == None:
        bash_command = f"""
            bash -c '
            uvotimsum infile="{infile_path}" outfile="{outfile_path}"
            '
            """
    else:
        bash_command = f"""
            bash -c '
            uvotimsum infile="{infile_path}" outfile="{outfile_path}" exclude={exclude}
            '
            """

    return bash_command

def create_uvotimsum_master_ref_bash_command(source_name, group_name):

    infile_path = f'./{source_name}/Ref_Frames/{group_name}_summed.fits'
    outfile_path = f'./{source_name}/Ref_Frames/{group_name}_master.fits'

    bash_command = f"""
            bash -c '
            uvotimsum infile="{infile_path}" outfile="{outfile_path}" exclude=0
            '
            """

    return bash_command

def run_uvotimsum(uvotimsum_command):
    # Capture the return value here:
    result = run_heasoft_command(uvotimsum_command) 
    return result.stdout

    # print("STDOUT:\n", result.stdout)
    # print("STDERR:\n", result.stderr)

    return result.stdout

def run_uvotimsum_verbose(uvotimsum_command):

    run_heasoft_command(uvotimsum_command)

    print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)

    return result.stdout

def create_uvotsource_bash_command(tile_name, obsid, source_reg_file, bkg_reg_file, target_name):

    trunc_obs_filepath = f'./S-CUBED/{tile_name}/UVOT/{obsid}/uvot/image/'
    obs_filepath = f'./S-CUBED/{tile_name}/UVOT/{obsid}/uvot/image/sw{obsid}uw1_sk.img'
    exp_filepath  = f'./S-CUBED/{tile_name}/UVOT/{obsid}/uvot/image/sw{obsid}uw1_ex.img.gz'
    
    bash_command = f"""
        bash -c '
        uvotsource image="{obs_filepath}" srcreg="{source_reg_file}" bkgreg="{bkg_reg_file}" sigma=5 zerofile=CALDB coinfile=CALDB psffile=CALDB lssfile=CALDB expfile="{exp_filepath}" syserr=NO frametime=DEFAULT apercorr=NONE output=ALL outfile="{trunc_obs_filepath + target_name}_source.fits" cleanup=YES clobber=YES chatter=1

        '
        """

    return bash_command

def create_uvotsource_too_bash_command(source_name, obsid, band, snapshot, source_reg_file, bkg_reg_file):

    trunc_obs_filepath = f'./{source_name}/TOO/{obsid}/uvot/image/'
    obs_filepath = f'./{source_name}/TOO/{obsid}/uvot/image/sw{obsid}{band}_sk.img[{snapshot}]'
    exp_filepath  = f'./{source_name}/TOO/{obsid}/uvot/image/sw{obsid}{band}_ex.img.gz[{snapshot}]'

    if snapshot == 1:
    
        bash_command = f"""
            bash -c '
            uvotsource image="{obs_filepath}" srcreg="{source_reg_file}" bkgreg="{bkg_reg_file}" sigma=5 zerofile=CALDB coinfile=CALDB psffile=CALDB lssfile=CALDB expfile="{exp_filepath}" syserr=NO frametime=DEFAULT apercorr=NONE output=ALL outfile="{trunc_obs_filepath}{band}_source.fits" cleanup=YES clobber=YES chatter=1

            '
            """
    else:
        bash_command = f"""
            bash -c '
            uvotsource image="{obs_filepath}" srcreg="{source_reg_file}" bkgreg="{bkg_reg_file}" sigma=5 zerofile=CALDB coinfile=CALDB psffile=CALDB lssfile=CALDB expfile="{exp_filepath}" syserr=NO frametime=DEFAULT apercorr=NONE output=ALL outfile="{trunc_obs_filepath}{band}_source{snapshot}.fits" cleanup=YES clobber=YES chatter=1

            '
            """

    return bash_command

def create_uvotsource_summed_bash_command(source_name, obsid, band, source_reg_file, bkg_reg_file):

    trunc_obs_filepath = f'./{source_name}/TOO/{obsid}/uvot/image/'
    obs_filepath = f'./{source_name}/TOO/{obsid}/uvot/image/{band}_summed.fits'
    exp_filepath  = f'./{source_name}/TOO/{obsid}/uvot/image/{band}_ex_summed.fits'
    
    bash_command = f"""
        bash -c '
        uvotsource image="{obs_filepath}" srcreg="{source_reg_file}" bkgreg="{bkg_reg_file}" sigma=5 zerofile=CALDB coinfile=CALDB psffile=CALDB lssfile=CALDB expfile="{exp_filepath}" syserr=NO frametime=DEFAULT apercorr=NONE output=ALL outfile="{trunc_obs_filepath}{band}_source.fits" cleanup=YES clobber=YES chatter=1

        '
        """
def create_uvotimsum_command(infile, outfile):
    return f'uvotimsum infile="{infile}" outfile="{outfile}"'

    return bash_command

def run_uvotsource(uvotsource_command):

    run_heasoft_command(uvotsource_command)

    # print("STDOUT:\n", result.stdout)
    # print("STDERR:\n", result.stderr)

    return result.stdout

def run_uvotsource_verbose(uvotsource_command):

    run_heasoft_command(uvotsource_command)

    print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)

    return result.stdout

def check_for_undownloaded_files(tile_name, new_tile_name, tile_ra, tile_dec):

    undownloaded_files = []

    #Run ObsQuery for all files in the region of the sky that we are interested in
    query = ObsQuery(ra=tile_ra, dec=tile_dec, radius = 0.18)

    #loop through all queried observations
    #only check observations where file name is desired S-CUBED tile
    #if directory doesn't exist for observation, append to undownloaded files  
    for ind, q in enumerate(query):
        if (q.targname == new_tile_name) & (q.exposure.total_seconds() > 30):
            obsid = query[ind].obsid
            dirpath = f'./S-CUBED/{tile_name}/UVOT/{obsid}'
            smeared_dirpath = f'./S-CUBED/{tile_name}/Smeared/{obsid}'
            if (os.path.isdir(dirpath) == False) & (os.path.isdir(smeared_dirpath) == False):
                undownloaded_files.append(obsid)

    return undownloaded_files

def download_new_files(undownloaded_files, tile_name, tile_ra, tile_dec):

    #Run ObsQuery for all files in the region of the sky that we are interested in
    query = ObsQuery(ra=tile_ra, dec=tile_dec, radius = 0.18)

    #loop through all queried observations
    #if obsid is in undownloaded_files, download the UVOT data for the observation
    for ind, q in enumerate(query):
        if query[ind].obsid in undownloaded_files:
            Data(obsid=query[ind].obsid, uvot=True, uksdc=True, outdir=f"~/S-CUBED/{tile_name}/UVOT")


def detect_smeared_frames(base_path):
    """
    Walks through base_path and detects smeared frames by analyzing all detect files.
    Returns a list of observation folder paths that contain smeared frames.
    """

    detect_pattern = re.compile(r'.*_detect.*\.fits$')
    
    print("\n Scanning for detect files...")
    
    # Find all detect files
    detect_files = []
    for root, dirs, files in os.walk(base_path):
        if os.path.basename(root) == "image":
            for file in files:
                if detect_pattern.match(file):
                    detect_files.append(os.path.join(root, file))
    
    print(f"Found {len(detect_files)} detect files to analyze")
    
    smeared_obs_folders = set()  # Use set to avoid duplicates
    
    # Analyze each detect file
    for filename in tqdm(detect_files, desc="Analyzing frames"):
        try:
            with fits.open(filename, memmap=False) as hdul:
                # Skip if no data extension
                if len(hdul) < 2:
                    continue
                
                data = hdul[1].data
                
                # Skip if no detections
                if data is None or len(data) == 0:
                    continue
                
                # Convert big-endian data to native byte order
                prof_major = np.array(data['PROF_MAJOR'], dtype=np.float64)
                prof_minor = np.array(data['PROF_MINOR'], dtype=np.float64)
                flags = np.array(data['FLAGS'], dtype=np.int32)
                
                # Create DataFrame
                detected_frame = pd.DataFrame({
                    'PROF_MAJOR': prof_major,
                    'PROF_MINOR': prof_minor,
                    'FLAGS': flags
                })
                
                # Filter for good detections (FLAGS == 0)
                detected_frame = detected_frame[detected_frame['FLAGS'] == 0]
                
                # Skip if no good detections
                if len(detected_frame) == 0:
                    continue
                
                # Calculate eccentricity
                a = np.mean(detected_frame['PROF_MAJOR'])
                b = np.mean(detected_frame['PROF_MINOR'])
                
                # Avoid division by zero or invalid sqrt
                if a > 0 and a >= b:
                    c = math.sqrt(a**2 - b**2)
                    e = c / a
                    
                    # If eccentricity >= 0.5, mark the ENTIRE observation folder
                    if e >= 0.5:
                        obs_folder = get_observation_folder(filename, base_path)
                        
                        if obs_folder and obs_folder not in smeared_obs_folders:
                            smeared_obs_folders.add(obs_folder)
                            print(f"  Smeared observation: {os.path.basename(obs_folder)} (e={e:.3f})")
        
        except Exception as ex:
            print(f"  Error processing {filename}: {ex}")
            continue
    
    smeared_list = list(smeared_obs_folders)
    print(f"\n Detection complete! Found {len(smeared_list)} smeared observation folders.")
    return smeared_list


def remove_smeared(base_path, smeared_obs_folders):
    """
    Moves smeared observation folders to a 'Smeared' directory.
    """
    if not smeared_obs_folders:
        print("No smeared frames to move.")
        return
    
    # Create Smeared directory
    smeared_dir = os.path.join(base_path, "Smeared")
    os.makedirs(smeared_dir, exist_ok=True)
    
    print(f"\n Moving {len(smeared_obs_folders)} smeared observations...")
    
    moved_count = 0
    for obs_folder in smeared_obs_folders:
        try:
            obs_name = os.path.basename(obs_folder)
            destination = os.path.join(smeared_dir, obs_name)
            
            # Skip if already exists in Smeared
            if os.path.exists(destination):
                print(f"   {obs_name} already in Smeared folder, skipping...")
                continue
            
            shutil.move(obs_folder, destination)
            print(f"  Moved: {obs_name}")
            moved_count += 1
            
        except Exception as ex:
            print(f"  Error moving {obs_folder}: {ex}")
    
    print(f"\n Smeared frame removal complete! Moved {moved_count} observation folders.")
    return moved_count


def get_observation_folder(detect_filepath, base_path):
    """
    Gets the top-level observation folder from a detect file path.
    """
    # Normalize paths to handle different separators
    detect_filepath = os.path.normpath(detect_filepath)
    base_path = os.path.normpath(base_path)
    
    # Walk up from the detect file until we find a folder directly under base_path
    current = os.path.dirname(detect_filepath)
    
    while current != base_path and os.path.dirname(current) != base_path:
        current = os.path.dirname(current)
    
    # current should now be the observation folder
    if os.path.dirname(current) == base_path:
        return current
    
    return None
        


# Currently Neither of these do anything "check_aspect_correction"
def check_aspect_correction(filepath):

    aspect_uncorrected = []

    for path in tqdm(sorted(os.listdir(filepath))):
        if path == '.DS_Store':
            continue
        else:
            subpath = os.path.join(filepath, path)

            sourcepath_fill = f'uvot/image/sw{path}uw1_sk.img'
            full_sourcepath = os.path.join(subpath, sourcepath_fill)
            
            exists = os.path.exists(full_sourcepath)
            
            if exists == True:
                fkeyprint_command = create_fkeyprint_bash_command(full_sourcepath)

                aspcorr_output = run_fkeyprint(fkeyprint_command)

                if re.search("ASPCORR = 'DIRECT  '", aspcorr_output):
                    continue
                elif re.search("ASPCORR = 'UNICORR '", aspcorr_output):
                    continue
                else:
                    aspect_uncorrected.append(path)
                
            elif exists == False:
                continue

    return aspect_uncorrected

def check_aspect_correction_verbose(filepath):

    aspect_uncorrected = []

    for path in tqdm(sorted(os.listdir(filepath))):
        if path == '.DS_Store':
            continue
        else:
            subpath = os.path.join(filepath, path)

            sourcepath_fill = f'uvot/image/sw{path}uw1_sk.img'
            full_sourcepath = os.path.join(subpath, sourcepath_fill)
            
            exists = os.path.exists(full_sourcepath)
            
            if exists == True:
                fkeyprint_command = create_fkeyprint_bash_command(full_sourcepath)

                aspcorr_output = run_fkeyprint_verbose(fkeyprint_command)

                if re.search("ASPCORR = 'DIRECT  '", aspcorr_output):
                    continue
                elif re.search("ASPCORR = 'UNICORR '", aspcorr_output):
                    continue
                else:
                    aspect_uncorrected.append(path)
                
            elif exists == False:
                continue

    return aspect_uncorrected

def check_direct_corrections(filepath):

    aspect_direct = []

    for path in tqdm(sorted(os.listdir(filepath))):
        if path == '.DS_Store':
            continue
        else:
            subpath = os.path.join(filepath, path)

            sourcepath_fill = f'uvot/image/sw{path}uw1_sk.img'
            full_sourcepath = os.path.join(subpath, sourcepath_fill)
            
            exists = os.path.exists(full_sourcepath)
            
            if exists == True:
                fkeyprint_command = create_fkeyprint_bash_command(full_sourcepath)

                aspcorr_output = run_fkeyprint(fkeyprint_command)

                if re.search("ASPCORR = 'DIRECT  '", aspcorr_output):
                    continue
                else:
                    aspect_direct.append(path)
                
            elif exists == False:
                continue

    return aspect_direct

def check_direct_corrections_verbose(filepath):

    aspect_direct = []

    for path in tqdm(sorted(os.listdir(filepath))):
        if path == '.DS_Store':
            continue
        else:
            subpath = os.path.join(filepath, path)

            sourcepath_fill = f'uvot/image/sw{path}uw1_sk.img'
            full_sourcepath = os.path.join(subpath, sourcepath_fill)
            
            exists = os.path.exists(full_sourcepath)
            
            if exists == True:
                fkeyprint_command = create_fkeyprint_bash_command(full_sourcepath)

                aspcorr_output = run_fkeyprint_verbose(fkeyprint_command)

                if re.search("ASPCORR = 'DIRECT  '", aspcorr_output):
                    continue
                else:
                    aspect_direct.append(path)
                
            elif exists == False:
                continue

    return aspect_direct

def remove_aspect_uncorrected(in_filepath, out_filepath, aspect_uncorrected_tiles):

    for auct in tqdm(aspect_uncorrected_tiles):
    
        source = os.path.join(in_filepath, auct)
        destination = out_filepath+'AspectNone'
    
        shutil.move(source, destination)
        
def find_brightest_central_stars(detect_path, num_stars=15, side_buffer=5):

    #open detect.fits and read header into dataframe
    with fits.open(detect_path) as hdul:
        detect_header = hdul[0].header
        
    #read header to find central pointing position
    center_ra = detect_header['RA_PNT'] * u.deg
    center_dec = detect_header['DEC_PNT']* u.deg

    #set up buffers
    center_coords = SkyCoord(ra=center_ra, dec=center_dec, frame='fk5')
    position_angle1 = 0 * u.deg
    position_angle2 = 90 * u.deg
    position_angle3 = 180 * u.deg
    position_angle4 = 270 * u.deg
    sep = side_buffer * u.arcmin

    #create upper and lower ra/dec bounds
    dec_max = center_coords.directional_offset_by(position_angle1, sep).dec.degree
    dec_min = center_coords.directional_offset_by(position_angle3, sep).dec.degree
    
    ra_max = center_coords.directional_offset_by(position_angle2, sep).ra.degree
    ra_min = center_coords.directional_offset_by(position_angle4, sep).ra.degree

    #extract sources from detect.fits
    stars = QTable.read(detect_path).to_pandas()
    stars = stars[(stars['RA'] >= ra_min) & (stars['RA'] <= ra_max)]
    stars = stars[(stars['DEC'] >= dec_min) & (stars['DEC'] <= dec_max)]

    #keep only the 15 brightest sources
    bright_stars = stars.sort_values('MAG', ascending=True)
    bright_stars = bright_stars.iloc[:num_stars+1, :]

    nearby_stars = []

    #loop over all bright central stars
    #use positions to calculate separation between each star
    #remove stars closer together than 1 arcminute
    for i in range(num_stars+1):
        for j in range(num_stars+1):
    
            if i != j:
                star1_ra = bright_stars.iloc[i, 0]
                star1_dec = bright_stars.iloc[i, 1]
                star1_coords  = SkyCoord(star1_ra, star1_dec, unit='deg', frame='fk5')
        
                star2_ra = bright_stars.iloc[j, 0]
                star2_dec = bright_stars.iloc[j, 1]
                star2_coords  = SkyCoord(star2_ra, star2_dec, unit='deg', frame='fk5')
        
                sep = star1_coords.separation(star2_coords).to(u.arcsecond) / u.arcsecond
        
                if sep <= 31:
                    nearby_stars.append(j)
                
            else:
                continue
    
    star_indices = [star for star in range(num_stars) if star not in nearby_stars]
    bright_stars = bright_stars.iloc[star_indices, :]

    return bright_stars

def remove_separate_stars(ref_bright_stars, obs_bright_stars):

    sep_frame = pd.DataFrame(columns=obs_bright_stars.index, index=ref_bright_stars.index)

    ref_coords = []
    obs_coords = []
    
    for ind in ref_bright_stars.index:
        ref_star_ra = ref_bright_stars.loc[ind, 'RA']
        ref_star_dec = ref_bright_stars.loc[ind, 'DEC']
    
        ref_star_coords = SkyCoord(ref_star_ra, ref_star_dec, unit='deg', frame='fk5')
        ref_coords.append(ref_star_coords)
    
    for ind in obs_bright_stars.index:
        obs_star_ra = obs_bright_stars.loc[ind, 'RA']
        obs_star_dec = obs_bright_stars.loc[ind, 'DEC']
    
        obs_star_coords = SkyCoord(obs_star_ra, obs_star_dec, unit='deg', frame='fk5')
        obs_coords.append(obs_star_coords)
    
    for obs_ind, obs_star in zip(obs_bright_stars.index, obs_coords):
        for ref_ind, ref_star in zip(ref_bright_stars.index, ref_coords):
    
            sep_frame.loc[ref_ind, obs_ind] = obs_star.separation(ref_star).to(u.arcsecond) / u.arcsecond
    
    sep_frame = sep_frame.where(sep_frame<(30.0)).dropna(axis=1, how='all').dropna(axis=0, how='all')

    if len(sep_frame.index) == len(sep_frame.columns):
        
        ref_bright_stars = ref_bright_stars.loc[list(sep_frame.index), :]
        obs_bright_stars = obs_bright_stars.loc[list(sep_frame.columns), :]

    else:
        print("Bright Stars Did Not Match. Please Adjust Filter Parameters and Try Again.")

    return ref_bright_stars, obs_bright_stars

def create_ref_obs_reg_files(ref_bright_stars, obs_bright_stars, outpath=None):

    ref_circles = []
    ref_coords = []
    
    for ind in ref_bright_stars.index:
        ref_ra = ref_bright_stars.loc[ind, 'RA']
        ref_dec = ref_bright_stars.loc[ind, 'DEC']
    
        ref_star_coords = SkyCoord(ref_ra, ref_dec, unit='deg', frame='fk5')
        # region = CircleSkyRegion(star_coords, radius=5*u.arcsecond)
        # region.write('ref.reg', format='ds9')
        ref_circle = f'circle({ref_ra},{ref_dec},5.000")\n'
        ref_circles.append(ref_circle)
        ref_coords.append(ref_star_coords)
        
    if outpath:
        ref_filename = outpath+'/ref.reg'
    else:
        ref_filename = 'ref.reg'
        
    reg_header = '# Region file format: DS9 version 4.1\nfk5\n'

    ref_circles_sum = "".join(ref_circles)
    ref_reg_text = reg_header + ref_circles_sum
    
    with open(ref_filename, mode='w', encoding='utf-8') as reffile:
        reffile.write(ref_reg_text)

    obs_circles = []
    obs_coords = []
    
    for ind in obs_bright_stars.index:
        obs_ra = obs_bright_stars.loc[ind, 'RA']
        obs_dec = obs_bright_stars.loc[ind, 'DEC']
    
        obs_star_coords = SkyCoord(obs_ra, obs_dec, unit='deg', frame='fk5')
        # region = CircleSkyRegion(star_coords, radius=5*u.arcsecond)
        # region.write('ref.reg', format='ds9')
        obs_circle = f'circle({obs_ra},{obs_dec},5.000")\n'
        obs_circles.append(obs_circle)
        obs_coords.append(obs_star_coords)
        
    if outpath:
        obs_filename = outpath+'/obs.reg'
    else:
        obs_filename = 'obs.reg'
    
    obs_circles_sum = "".join(obs_circles)
    obs_reg_text = reg_header + obs_circles_sum
    
    with open(obs_filename, mode='w', encoding='utf-8') as obsfile:
        obsfile.write(obs_reg_text)

def write_source_reg_files(tile_name, obsid, source_name, source_ra, source_dec):

    #generate source coordinates
    source_coords = SkyCoord(source_ra, source_dec, unit='deg', frame='icrs')
    
    trunc_obs_filepath = f'./S-CUBED/{tile_name}/UVOT/{obsid}/uvot/image/'
    detect_filepath = f'./S-CUBED/{tile_name}/UVOT/{obsid}/uvot/image/detect.fits'
    
    #generate  blank dataframe
    detected_frame = pd.DataFrame(columns=['RA', 'DEC', 'SEP'])
    
    #open detect.fits
    with fits.open(detect_filepath) as hdul:
        head = hdul[0].header
        data = hdul[1].data

    #loop through all the sources in detect.fits, append coordinates to dataframe
    for ind, val in enumerate(data):
        detected_frame.loc[ind, 'RA'] = val['RA']
        detected_frame.loc[ind, 'DEC'] = val['DEC']

    #new .reg filename 
    reg_filename = f'{trunc_obs_filepath + source_name}_source.reg'

    #loop through all the stars in detect.fits
    #find the closest one to coordinate positions of source
    #use those coords to write a new .reg file out in the obs filder
    if len(detected_frame.index) >= 1:
        for ind in detected_frame.index:
            
            #generate a SkyCoord object for each star
            ra = detected_frame.loc[ind, 'RA']
            dec = detected_frame.loc[ind, 'DEC']
            
            star_coords = SkyCoord(ra, dec, unit='deg', frame='fk5')

            #calculate separation to source and append to dataframe
            sep = star_coords.separation(source_coords).to(u.arcsecond)
            detected_frame.loc[ind, 'SEP'] = sep

        #look for star with min separation and grab coordinates of that star
        min_sep = detected_frame['SEP'].idxmin()

        min_ra = detected_frame.loc[min_sep, 'RA']
        min_dec = detected_frame.loc[min_sep, 'DEC']

        #check to see how far away the nearest star is before writing a region file
        #if distance is > 5 arcseconds, no new region file is created.
        if detected_frame.loc[min_sep, 'SEP'] <= (10 * u.arcsecond):
            #generate new region text and write out file
            new_reg_text = f'# Region file format: DS9 version 4.1\nfk5\ncircle({min_ra},{min_dec},5.000")'
        
            with open(reg_filename, mode='w', encoding='utf-8') as regfile:
                regfile.write(new_reg_text)

def find_aspect_none_snapshots(path_to_frame):

    fkeyprint_command = up.create_fkeyprint_bash_command(path_to_frame)

    fkeyprint_out = up.run_fkeyprint(fkeyprint_command)

    corrected = re.findall("# EXTENSION:    [0-9]\nASPCORR = 'DIRECT  '", fkeyprint_out)
    uncorrected = re.findall("# EXTENSION:    [0-9]\nASPCORR = 'NONE    '", fkeyprint_out)
    
    exclude=[]
    
    for frame in uncorrected:
        exclude_frame = re.findall("[0-9]", frame)[0]
        exclude.append(exclude_frame)
    
    if len(exclude) > 0:
        exclude_string = ','.join(exclude)
        return exclude_string
    else:
        print('No snapshots need aspect correction. Excluding no frames from master ref.')
        return None

def create_master_ref_file(source_name, band, ref_files, group_name):
    """
    Takes uvot reference frames and sums them together. 
    INPUT:
        ref_files (list): list of obsids of the relevant reference files.
    """

    if not ref_files:
        raise ValueError("ref_files list is empty")

    os.makedirs(f'./{source_name}/Ref_Frames', exist_ok=True)

    # --- Sum first file ---
    primary_file = ref_files[0]

    primary_imsum_command = create_uvotimsum_too_bash_command(
        source_name, primary_file, band, 'sk', ref_frame=True
    )
    run_uvotimsum(primary_imsum_command)

    os.rename(
        f'./{source_name}/Ref_Frames/{primary_file}_{band}_summed.fits',
        f'./{source_name}/Ref_Frames/{group_name}_summed.fits'
    )

    # --- Append remaining files ---
    for ref_id in ref_files[1:]:
        imsum_command = create_uvotimsum_too_bash_command(
            source_name, ref_id, band, 'sk', ref_frame=True
        )
        run_uvotimsum(imsum_command)

        outfilename = f'./{source_name}/Ref_Frames/{ref_id}_{band}_summed.fits'

        fappend_command = create_fappend_bash_command(
            outfilename,
            f'./{source_name}/Ref_Frames/{group_name}_summed.fits'
        )
        run_fappend(fappend_command)

    # --- Final master sum ---
    mastersum_command = create_uvotimsum_master_ref_bash_command(
        source_name, group_name
    )
    run_uvotimsum(mastersum_command)


def download_ogle_data(ogle_name, source_name):

    ogle = requests.get(f'https://www.astrouw.edu.pl/ogle/ogle4/xrom/{ogle_name}/phot.dat')

    if ogle.status_code != 200:
        raise DownloadError("An Error occurred when downloading the file. Please check the name of the OGLE Source and try again.")
    else:
        ogle_local_filename = f"./OGLE_Outputs/{source_name}.dat"
        with open(ogle_local_filename, 'wb') as f:
            for chunk in ogle.iter_content(chunk_size=8192):
                f.write(chunk)

def download_xrt_data(xrt_num, source_name):

    xrt = requests.get(f'https://www.swift.ac.uk/SMC/data/source{xrt_num}/curve/PC_incbad.qdp', auth=HTTPBasicAuth('smc', 'T1le_th3_$MC'))

    if xrt.status_code != 200:
        raise DownloadError("An Error occurred when downloading the file. Please check the number of the XRT Source and try again.")
    else:
        xrt_local_filename = f"./XRT_Outputs/{source_name}.qdp"
        with open(xrt_local_filename, 'wb') as f:
            for chunk in xrt.iter_content(chunk_size=8192):
                f.write(chunk)

def read_ogle_data(source_name):

    ogle_data = pd.read_csv(f'./OGLE_Outputs/{source_name}.dat', sep=r'\s+', header=None, names=['Time', 'I', 'I_Err', 'Seeing', 'Sky'])
    
    ogle_data['MJD'] = ogle_data['Time'] - 2400000

    return ogle_data

def read_uvot_data(source_name):

    uvot_data = pd.read_csv(f'./UVOT_Outputs/{source_name}_uvot_data.txt', header=None, sep=r'\s+', names=['MJD', 'Mag', 'Mag_Err', 'F_lam', 'F_lam_err'])

    return uvot_data

def read_xrt_data(source_name):

    xrt_data = Table.read(f"./XRT_Outputs/{source_name}.qdp", format='ascii.qdp', table_id=0, names=['MJD', 'CR'])
    xrt_ul_data = Table.read(f"./XRT_Outputs/{source_name}.qdp", format='ascii.qdp', table_id=1, names=['MJD', 'CR'])

    xrt_data['MJD_nerr'] = -1*xrt_data['MJD_nerr']
    xrt_data['CR_nerr'] = -1*xrt_data['CR_nerr']

    return xrt_data, xrt_ul_data



# -------------------- The hunt for Red ASPCORR -----------------------------
# This has given me some pause for some time as what I did in the past was a very basic bit of code that used existing fkeyprint code and read the extension
# That on hindsight didnt work to well for two reasons, 1: I was only reading the first extension and not the whole list(whops) 2: Meant that code only worked for WSL, this needs to be universal.

#So what we now do instead is a scan the FITS file itself for the extension and read the proper sheet. I.E. the fits files themselves have extension as the photos do so they may have-- Sheet 0 (Primary) Sheet 1(Image) Sheet 2(Image), etc.
# So we will have to Loop through all sheets for our hunt
def _scan_header_for_aspcorr_per_extension(file_path):
    """
    Returns a list of ASPCORR statuses, one per image extension.
    Used for building the detailed observations table.
    
    It Also recognizes UNICORR as a corrected status
    """
    if not file_path:
        return []
    
    try:
        with fits.open(file_path) as hdul:
            statuses = []
            for hdu in hdul:
                # Only process image extensions (skip primary header with NAXIS=0)
                naxis = hdu.header.get('NAXIS', 0)
                if naxis >= 2:
                    val = hdu.header.get('ASPCORR', 'NONE')
                    status = str(val).strip().upper()
                    
                    # Treat UNICORR as DIRECT (both are corrected... Might have to change this?) 
                    if status == 'UNICORR':
                        status = 'DIRECT'
                    
                    statuses.append(status)
            return statuses
    except:
        return []


def _scan_header_for_aspcorr(file_path):
    """
    Returns overall status for the file.
    If both DIRECT and NONE exist across extensions, returns 'READYRESUM'.
    
    I also now recognizes UNICORR as a corrected status, This may need to change As UNICORR shouldnt be used for aspect corrections? Possible issue.
    """
    if not file_path:
        return "NONE"
    
    try:
        with fits.open(file_path) as hdul:
            statuses = set()
            for hdu in hdul:
                val = hdu.header.get('ASPCORR', 'NONE')
                status = str(val).strip().upper()
                
                # Treat UNICORR as DIRECT for grouping purposes
                if status == 'UNICORR':
                    status = 'DIRECT'
                
                statuses.add(status)
            
            # READYRESUM logic
            if 'DIRECT' in statuses and 'NONE' in statuses:
                return 'READYRESUM'
            if 'NONE' in statuses:
                return 'NONE'
            if 'DIRECT' in statuses:
                return 'DIRECT'
            return "NONE"
    except:
        return "NONE"

# Essentially the same as above,we are grabing the RA/DEC from the image header. The only funny little thing about this is NAXIS
# A bit of a problem you may run into if you make a few misteps is not all FITS extensions are images, we need to find which ones are. The best way I would find is to look for NAXIS I.E. does the file have Height and Width.
def _get_coords(file_path):
    if not file_path: 
        return None, None
    try:
        with fits.open(file_path) as hdul:  
            for i, hdu in enumerate(hdul):
                naxis = hdu.header.get('NAXIS', 0)
                
                if naxis >= 2:
                    w = WCS(hdu.header)
                    
                    naxis1 = hdu.header.get('NAXIS1', 0)
                    naxis2 = hdu.header.get('NAXIS2', 0)
                    
                    cx, cy = naxis1/2.0, naxis2/2.0
                    ra, dec = w.all_pix2world(cx, cy, 0)
                    return float(ra), float(dec)
            
            print(f" No 2D image found in any HDU")
            return None, None
    except Exception as e:
        print(f" DEBUG _get_coords ERROR: {e}")
        return None, None


# The Engine of the operation
def _run_core_engine(base_folder=None, save_dir=None):
        # This was funky to figure out, the os.path.normpath and strip inputs where only added to have the code be universal and convienent. Atleast it should work
    # Since I only have windows Not 100% sure on it working or not, but it should be striping quotes from drag anddrop folders like Mac allows and Normalzing slashes.
    if not base_folder:
        base_folder = os.path.normpath(input("1. Path to UVOT raw data: ").strip().strip('"').strip("'"))
    if not save_dir:
        save_dir = os.path.normpath(input("2. Save directory: ").strip().strip('"').strip("'"))
    
    if not os.path.exists(save_dir): 
        os.makedirs(save_dir)

    bands_list = ["uvv", "uuu", "ubb", "um2", "uw1", "uw2"]
    raw_results = []
    
    # Now normally you would try to use a walk here I know I did. Thats a bad idea turns out as if you do that it dives into every sub-sub-folder Immediately.
    # Leading to double counting, so instead we try to control it a bit.
    try:
        # EXCLUDE the Smeared folder
        top_folders = [f for f in os.listdir(base_folder) 
                      if os.path.isdir(os.path.join(base_folder, f)) 
                      and f != "Smeared"]
        print(f"DEBUG: Found {len(top_folders)} top-level folders (excluding Smeared)")
        
        # Sample first 3 folders to see what we're dealing with
        print(f"DEBUG: Sample folder names:")
        for i, folder in enumerate(top_folders[:3]):
            print(f"  {i+1}. {folder}")
            
    except Exception as e: 
        print(f"DEBUG: Error listing directories: {e}")
        return None, None, None

    folder_pattern = re.compile(r"(\d{11})")

    # For debuging bellow since it wasnt working.
    matched_folders = 0
    files_found_count = 0
    coords_failed = 0
    
    # Check first matching folder in detail
    first_detailed_check = False

    for folder in top_folders:
        match = folder_pattern.search(folder)
        if not match: 
            print(f"DEBUG: Folder '{folder}' doesn't match pattern - skipping")
            continue
        
        matched_folders += 1
        obsid = match.group(1).zfill(11) # The subfolders we have are just the OBSIDS so this is a quick and easy way to get those now for later.
        # This bit had a lot of work but into it because of an intresting error I wasent expecting to have. We havesummed files which are the Images we want to use but those miss keywords
        # We also have Sky files which we dont want to use unless we have to but those always have keywords like the ASPCORR we need, so we had to scan and use both types if they exist to accomplish differnt goals.
        
        # Detailed check on first folder only
        if not first_detailed_check:
            first_detailed_check = True
            folder_path = os.path.join(base_folder, folder)
            
            # Walk and show structure
            print(f"DEBUG: Folder structure:")
            for root, dirs, files in os.walk(folder_path): 
                rel_path = os.path.relpath(root, folder_path)
                if rel_path == ".":
                    rel_path = "[root]"
                print(f"  {rel_path}/ - {len(files)} files, {len(dirs)} subdirs")
                
                # Show SK files if any
                sk_pattern = re.compile(r'.*_sk.*\.(img|fits|gz)$')
                sk_files = [f for f in files if sk_pattern.match(f)] # Finding the exact files in the folder we are looking for
                if sk_files:
                    print(f" SK files found: {sk_files[:3]}") 
                
                # Show summed files if any
                summed_pattern = re.compile(r'.*summed.*\.(img|fits|gz)$')
                summed_files = [f for f in files if summed_pattern.match(f)] # [f for f in files if ...]: Iterates through each item (f) in the files list.
                if summed_files:
                    print(f" Summed files found: {summed_files[:3]}") # This all just exists for debuging and can be removed!!!!
        
        band_files = {b: {'sum': None, 'sky': None} for b in bands_list}

        for root, _, filenames in os.walk(os.path.join(base_folder, folder)):  # It okay to use walk now since we took care of the double counting to find what we need.
            for f in filenames:
                f_low = f.lower()
                if not any(ext in f_low for ext in ['.img', '.fits', '.gz']): 
                    continue
                for band in bands_list:
                    if band in f_low:
                        if "summed" in f_low: 
                            band_files[band]['sum'] = os.path.join(root, f) # Directly making the path.
                            files_found_count += 1
                        elif "_sk" in f_low: 
                            band_files[band]['sky'] = os.path.join(root, f)
                            files_found_count += 1

        # Use Summed for the image/coords, but Sky for the ASPCORR status, this is very old logic That I am not chaning, this was because I had summed files and belived they would be more useful for the coords. Not really true but since its working I no touchy.
        for band, files in band_files.items():
            sum_f, sky_f = files['sum'], files['sky']
            if not sum_f and not sky_f: 
                continue
            
            target_file = sum_f if sum_f else sky_f  # Setting the target for what files should be processed that being the Summed files.
            status_source_file = sky_f if sky_f else sum_f # Setting the target for what files should be used for ASPCORR that being the sky files.

            ra, dec = _get_coords(target_file) # Get RA,DEC from the img, since we only got the center location before and still should proably need this.
            if ra is None: 
                coords_failed += 1
                continue 
            
            raw_results.append({
                "OBSID": obsid, "Band": band, "RA": ra, "Dec": dec,
                "Full_Path": target_file, 
                "Filename": os.path.basename(target_file),
                "ASPCORR": _scan_header_for_aspcorr(status_source_file)
            })

    print(f"\nDEBUG SUMMARY:")
    print(f"  Folders matching pattern: {matched_folders}")
    print(f"  SK/summed files found: {files_found_count}")
    print(f"  Files with failed coordinate extraction: {coords_failed}")
    print(f"  collected: {len(raw_results)} raw results") #All debuging things to check where it was failing. Fun fact I found it, I never inported astropy.wcs, i added dozens of debuging lines of code just to tell me something I already knew.... Im stupid, and Now im not removing them since I might need them again.

# Just a bit more cleanup
    df = pd.DataFrame(raw_results)
    if df.empty: 
        print("DEBUG: DataFrame is empty - no data found")
        return None, None, None
        
    df = df.drop_duplicates(subset=['OBSID', 'Band'], keep='first') # This is just to 100% make sure that the same file couldnt have been scanned twice by  chekcing for duplicants, the code kept thinking there was more extensions then there was somewhere and now im paranoid.

    # SPATIAL GROUPING 
     # We group by RA/Dec so that OBSIDs looking at the same spot are linked. This is what we where talking about before when it comes to finding nearby frames and sorting them.
    # This allows uncorrected files to use a 'DIRECT' neighbor as a reference, as long as it is within the parameters. (within 240 arcsec, I know this is 6arc min not 7 which is what we are going to check for stars and thats on purpose. I wanted extra wiggle room.).
    merged = df.copy()
    merged['Group_ID'] = -1
    group_cnt = 0
    for i in range(len(merged)):
        if merged.iloc[i]['Group_ID'] != -1: continue
        mask = (np.abs(merged['RA']-merged.iloc[i]['RA']) <= 240/3600) & \
               (np.abs(merged['Dec']-merged.iloc[i]['Dec']) <= 240/3600) & \
               (merged['Group_ID'] == -1)
        merged.loc[mask, 'Group_ID'] = group_cnt
        group_cnt += 1 #This code is makeing a box and find all ids that fit within that box and assiging them them a group id and then making a new one an repating until finished. 
        #Also something I learned from testing this, is some bands in certain groups are taken at differnt angels, which I didnt think would happen.

    
    def check_status(g):
        # A Reference can be a fully DIRECT file OR a READYRESUM file (since it has DIRECT parts).
        # A group needs work if there is a NONE or a READYRESUM present.
        has_ref = (g['ASPCORR'].isin(['DIRECT', 'READYRESUM'])).any()
        needs_work = (g['ASPCORR'].isin(['NONE', 'READYRESUM'])).any()
        # Defines the rest of the grouping types, completed is if the group only has_ref, ready is a mix of both and Orphan is only has_unc
        status = 'COMPLETED' if not needs_work else ('READY' if has_ref else 'ORPHAN')
        return pd.Series({'Status': status, 'Total_Frames': len(g)})
    # Use the auto rename function from above here.
    summary = merged.groupby(['Group_ID', 'Band']).apply(check_status, include_groups=False).reset_index()
    return merged, summary, save_dir


# --- MODES ---
def swift_automation_mode(base_path=None, save_path=None):
    all_frames, summary, _ = _run_core_engine(base_path, save_path)
    return all_frames, summary

def swift_interactive_mode():
    print("\n=== Swift UVOT Interactive Mode ===")
    all_frames, summary, save_dir = _run_core_engine()
    if all_frames is None: return

    summary_path = _get_unique_filename(os.path.join(save_dir, "workload_summary.csv"))
    summary.to_csv(summary_path, index=False)
    
    print(f"\nSummary saved to: {os.path.basename(summary_path)}")
    print("\n--- WORKLOAD SUMMARY ---")
    print(summary['Status'].value_counts().to_frame())
    
    while True:
        choice = input("\nEnter Group_ID to export (or 'q' to quit): ").lower()
        if choice == 'q': break
        try:
            g_id = int(choice)
            g_data = all_frames[all_frames['Group_ID'] == g_id].sort_values(by='ASPCORR', ascending=False)
            if not g_data.empty:
                detail_path = _get_unique_filename(os.path.join(save_dir, f"Group_{g_id}_Details.csv"))
                g_data.to_csv(detail_path, index=False)
                print(f"Exported: {os.path.basename(detail_path)}")
                print(g_data[['OBSID', 'Band', 'ASPCORR', 'Filename']].to_string(index=False))
        except: print("Invalid ID.")

    
#Orhan group sorting
# -------------------- ORPHAN HUNTING EXPANSION (Name patent pending) -----------------------------
def solve_orphan_frames_by_group(base_path=None, save_dir=None, return_data=False, input_df=None, input_summary=None):
    """
    It will
    1. Loads data using the existing swift_automation_mode.
    2. Identifies 'ORPHAN' frames (groups with no valid aspect correction).
    3. Identifies 'REFERENCE' frames (DIRECT or READYRESUM).
    4. For each Orphan, finds the nearest Reference in 4 directions (N, S, E, W).
    5. Saves a CSV for each Orphan with those 4 neighbors.
    """
    
    # Data Input Using existing Funtion from IAC to get the DataFrames directly
    # If you want to skip the input prompts(for automation), Input a base_path and save_dir.
    if input_df is not None and input_summary is not None:
        all_frames, summary_df = input_df, input_summary
        active_save_dir = save_dir if save_dir else os.getcwd()
    else:
        # Calls the core engine from IAC code
        all_frames, summary_df, active_save_dir = _run_core_engine(base_path, save_dir)

    if all_frames is None or summary_df is None:
        print(" No data returned from core engine. Cannot solve orphan frames.")
        return None

    # DEBUG, Print what we got, just to check
    print(f"\n Data loaded:")
    print(f"Total frames: {len(all_frames)}")
    print(f"Summary groups: {len(summary_df)}")
    print(f"Status breakdown:\n{summary_df['Status'].value_counts()}")

    # Identify Orphan Groups 
    # We find which (Group_ID, Band) combos are orphans from the summary report we get above.
    orphan_group_keys = summary_df[summary_df['Status'] == 'ORPHAN'][['Group_ID', 'Band']]
    
    print(f"\n Found {len(orphan_group_keys)} orphan group-band combinations")
    
    if orphan_group_keys.empty:
        print(" No orphan frames found - nothing to solve!")
        return {} if return_data else None
    
    # Pool of the valid reference frames (must have some usable data)
    reference_pool = all_frames[all_frames['ASPCORR'].isin(['DIRECT', 'READYRESUM'])].copy()
    
    print(f" Reference pool: {len(reference_pool)} frames available")
    
    automation_results = {} #Set up container for automation results
    
    # Create folder if saving CSVs, And set up folder if we are in manual/save mode
    if not return_data:
        orphan_save_path = os.path.join(active_save_dir, "Orphan_Solutions")
        if not os.path.exists(orphan_save_path): 
            os.makedirs(orphan_save_path)
        print(f" Saving CSVs to: {orphan_save_path}")

    # Process Every Frame in Every Orphan Group 
    count = 0
    for _, orphan_row in orphan_group_keys.iterrows():
        g_id = orphan_row['Group_ID']
        band = orphan_row['Band']

        # Get ALL individual frames belonging to this specific Orphan Group and Band
        target_frames = all_frames[(all_frames['Group_ID'] == g_id) & (all_frames['Band'] == band)]

        # Filter the reference pool for the matching band (um2 can only use um2, etc.)
        band_refs = reference_pool[reference_pool['Band'] == band].copy()
        
        if band_refs.empty: # If no valid references exist for this specific band, we skip
            print(f" No references for Group {g_id}, Band {band} - skipping")
            continue

        for _, frame in target_frames.iterrows(): #Grab some info we will need life the location both in space and on the computer.
            f_ra, f_dec, f_obsid = frame['RA'], frame['Dec'], frame['OBSID']
            f_path = frame['Full_Path']
            
            # Simple subtraction for distance math
            # dRA: Positive = East, Negative = West
            # dDec: Positive = North, Negative = South, Kinda weird part here if you check SAO it has east being positive even though logic would make it negative.
            band_refs['dRA'] = band_refs['RA'] - f_ra
            band_refs['dDec'] = band_refs['Dec'] - f_dec

            neighbors = []
            
            # Find closest in 4 directions by finding the smallest absolute difference
            # East (+RA) Find frames where dRA is Positive, sort by smallest distance, The rest follow same logic
            east = band_refs[band_refs['dRA'] > 0].sort_values('dRA').head(1)
            # West (-RA) Find frames where dRA is Negative, sort by smallest absolute distance (largest value -> closer to 0)
            west = band_refs[band_refs['dRA'] < 0].sort_values('dRA', ascending=False).head(1)
            # North (+Dec)
            north = band_refs[band_refs['dDec'] > 0].sort_values('dDec').head(1)
            # South (-Dec)
            south = band_refs[band_refs['dDec'] < 0].sort_values('dDec', ascending=False).head(1)

            for n in [east, west, north, south]:
                if not n.empty:  # Safty measure if its not empty run it, Had to add this since one of the orphan frames doesnt have a neighbor in a direction.
                    neighbors.append(n)

            if neighbors:
                # Compile the 4 neighbors
                result_df = pd.concat(neighbors)[['OBSID', 'RA', 'Dec', 'Full_Path', 'Band', 'ASPCORR']]
                result_df.attrs['orphan_path'] = f_path # We store the orphan's own path as metadata in the result for Automation Mode
                
                unique_key = f"{f_obsid}_{band}"
                
                # Write CSV or return result
                if return_data:
                    automation_results[unique_key] = result_df
                else:
                    save_file = os.path.join(orphan_save_path, f"{unique_key}.csv")
                    result_df.to_csv(save_file, index=False)
                    print(f"✅ Created: {os.path.basename(save_file)}")
                
                count += 1

    print(f"\n Directives generated for {count} individual orphan frames.")
    
    if not return_data and count > 0:
        print(f" All CSVs saved to: {orphan_save_path}")
    
    return automation_results if return_data else None


######################################################################################
#Bellow is testing for populating table bads.
def populate_observations_table(base_path, all_frames_df, summary_df):
    """
    Populates observations table with one row per OBSID + Band + Extension.

    uses os.walk to find uvot/image directories and extracts
    OBSIDs from the FITS filenames themselves, so it works regardless of
    folder naming conventions.
    Works with ANY folder structure.... I think.
    """

    possible_bands = ['uvv', 'ubb', 'uuu', 'uw1', 'um2', 'uw2']

    # Regex to pull OBSID and band from UVOT sky image filenames
    # Matches: sw00033038054uw1_sk.img.gz  or  sw03111173093um2_sk.img
    sk_pattern = re.compile(r'^sw(\d{11})([a-z0-9]+)_sk\.img(\.gz)?$') # This is a handy bit of code

    # Initialize empty DataFrame
    obs_table = pd.DataFrame(columns=[
        'ObsID', 'Filter', 'Snapshot', 'Smeared Flag', 'SSS Flag', 'AspCorr Flag',
        'Group_ID', 'Group_Status', 'Extension_Status', 'File_Status',
        'RA', 'Dec', 'Full_Path'
    ])

    counter = 0

    

    # Walk the ENTIRE tree looking for directories that end in uvot/image.
    # This is the key fix, don't assume any specific parent structure.
    for root, dirs, files in os.walk(base_path):
        normalised = os.path.normpath(root)

        # Only process directories that end with uvot/image
        if not normalised.endswith(os.path.join("uvot", "image")):
            continue

        # Skip anything inside a Smeared or NotASPCORR quarantine folder
        path_parts = normalised.split(os.sep)
        if "Smeared" in path_parts or "NotASPCORR" in path_parts:
            continue

        
        # Scan the files in this image directory for SK files.
        # We extract OBSID and band FROM THE FILENAME. this is always
        # reliable regardless of how the parent folders are named.
        # Collect unique (obsid, band) pairs and their file paths in this dir
        found_files = {} 

        for f in files:
            match = sk_pattern.match(f)
            if not match:
                continue
            obsid = match.group(1)
            band = match.group(2)

            if band not in possible_bands:
                continue

            # If both .img and .img.gz exist, prefer .img (uncompressed)
            key = (obsid, band)
            existing = found_files.get(key)
            if existing is None:
                found_files[key] = os.path.join(root, f)
            elif f.endswith('.img') and existing.endswith('.gz'):
                # Prefer uncompressed over compressed
                found_files[key] = os.path.join(root, f)


        
        # Process each (obsid, band) we found in this directory
        for (obsid, band), full_path in found_files.items():

            try:
                hdul = fits.open(full_path)
            except Exception as e:
                print(f"  Warning: Could not open {full_path}: {e}")
                continue

            num_snapshots = len(hdul) - 1  # Subtract 1 for primary HDU

            if num_snapshots < 1:
                hdul.close()
                continue

            # Get per-extension ASPCORR statuses
            extension_statuses = _scan_header_for_aspcorr_per_extension(full_path)

            # Get overall file status
            file_status = _scan_header_for_aspcorr(full_path)

            # Get RA/Dec and Group_ID from all_frames_df
            frame_info = all_frames_df[
                (all_frames_df['OBSID'] == obsid) &
                (all_frames_df['Band'] == band)
            ]

            if frame_info.empty:
                ra, dec, group_id = None, None, -1
            else:
                ra = frame_info.iloc[0]['RA']
                dec = frame_info.iloc[0]['Dec']
                group_id = frame_info.iloc[0]['Group_ID']

            # Get group status from summary
            group_status = "UNKNOWN"
            if group_id != -1:
                group_info = summary_df[
                    (summary_df['Group_ID'] == group_id) &
                    (summary_df['Band'] == band)
                ]
                if not group_info.empty:
                    group_status = group_info.iloc[0]['Status']

            # Process each extension (snapshot)
            for ext in range(1, num_snapshots + 1):
                # Get extension-specific status
                ext_status = extension_statuses[ext - 1] if (ext - 1) < len(extension_statuses) else 'NONE'

                # Determine AspCorr Flag (True if extension has DIRECT correction)
                aspcorr_flag = (ext_status == 'DIRECT')

                # Add row to table
                obs_table.loc[counter] = {
                    'ObsID': obsid,
                    'Filter': band,
                    'Snapshot': ext,
                    'Smeared Flag': False,  # Will be updated later
                    'SSS Flag': False,      # Placeholder
                    'AspCorr Flag': aspcorr_flag,
                    'Group_ID': group_id,
                    'Group_Status': group_status,
                    'Extension_Status': ext_status,
                    'File_Status': file_status,
                    'RA': ra,
                    'Dec': dec,
                    'Full_Path': full_path,
                }

                counter += 1

            hdul.close()

    print(f'Found {counter} snapshots across all uvot/image directories.')
    return obs_table


def update_smeared_flags(obs_table, smeared_list):
    """
    Updates the Smeared Flag column based on the smeared_list.

    Extracts OBSID from smeared folder paths using regex instead of
    assuming the folder name IS the OBSID. That Was a mistake.
    """
    if not smeared_list:
        return obs_table

    print(f"\nUpdating smeared flags for {len(smeared_list)} observations...")

    obsid_pattern = re.compile(r'(\d{11})')

    for smeared_folder in smeared_list:
        # smeared_folder could be a full path or just a folder name
        folder_name = os.path.basename(smeared_folder) if os.sep in str(smeared_folder) else str(smeared_folder)

        match = obsid_pattern.search(folder_name)
        if match:
            obsid = match.group(1)
            mask = obs_table['ObsID'] == obsid
            if mask.any():
                obs_table.loc[mask, 'Smeared Flag'] = True
                print(f"Marked {obsid} as smeared")
            else:
                print(f"Warning: OBSID {obsid} from smeared list not found in obs_table")
        else:
            print(f"Warning: Could not extract OBSID from smeared folder: {smeared_folder}")

    return obs_table
    

def refresh_observations_table_after_correction(obs_table, corrected_obsids, band):
    """
    Updates the obs_table after corrections to reflect new ASPCORR status.
    """
    for obsid, snapshot in corrected_obsids:
        # Find the row(s) to update
        mask = (obs_table['ObsID'] == obsid) & \
               (obs_table['Filter'] == band) & \
               (obs_table['Snapshot'] == snapshot)
        
        if mask.any():
            # Get the file path to re-read ASPCORR
            file_path = obs_table.loc[mask, 'Full_Path'].iloc[0]
            
            # Re-read the extension status
            try:
                if file_path.endswith('.gz'):
                    img_path = file_path[:-3]
                else:
                    img_path = file_path
                
                if os.path.exists(img_path):
                    with fits.open(img_path) as hdul:
                        if snapshot < len(hdul):
                            aspcorr = hdul[snapshot].header.get('ASPCORR', 'NONE')
                            aspcorr = str(aspcorr).strip().upper()
                            
                            # Treat UNICORR as DIRECT (Might have to change this)
                            if aspcorr == 'UNICORR':
                                aspcorr = 'DIRECT'
                            
                            # Update table
                            obs_table.loc[mask, 'Extension_Status'] = aspcorr
                            obs_table.loc[mask, 'AspCorr Flag'] = (aspcorr == 'DIRECT')
                            
            except Exception as e:
                print(f"Warning: Could not update table for {obsid} ext {snapshot}: {e}")
    
    return obs_table


    
def run_uvotsource_pipeline(obs_table, base_path, save_path, source_reg=None, bkg_reg=None, automation_mode=True):
    BANDS = ["uvv", "uuu", "ubb", "um2", "uw1", "uw2"]

    # Build a set of smeared OBSIDs from obs_table so we can skip them.
    # This is the ONLY thing we use obs_table for in 4c/4d, everything
    # else comes from walking the filesystem directly.
    smeared_obsids = set()
    if obs_table is not None and 'Smeared Flag' in obs_table.columns:
        smeared_rows = obs_table[obs_table['Smeared Flag'] == True]
        smeared_obsids = set(smeared_rows['ObsID'].astype(str).unique())
        if smeared_obsids:
            print(f"Will skip {len(smeared_obsids)} smeared OBSIDs")

    #############################################################################
    # GET REGION FILES, TEMPORARY SOLUTION (WILL BE REPLACED, SOONISH?)
    
 
    if source_reg is None or bkg_reg is None:
        print("\n" + "=" * 70)
        print("REGION FILE SELECTION  (temporary — will be automated later)")
        print("=" * 70)
        print("Please select the DS9 region files for uvotsource.")
        print("  • Source region  — small circle centred on your target")
        print("  • Background region — annulus / circle in a source-free area\n")

        root_tk = tk.Tk()
        root_tk.withdraw()

        if source_reg is None:
            source_reg = filedialog.askopenfilename(
                title="Select SOURCE region file (.reg)",
                filetypes=[("Region files", "*.reg"), ("All files", "*.*")],
                initialdir=os.path.dirname(base_path),
            )
            if not source_reg:
                print("No source region file selected — aborting uvotsource step.")
                return None

        if bkg_reg is None:
            bkg_reg = filedialog.askopenfilename(
                title="Select BACKGROUND region file (.reg)",
                filetypes=[("Region files", "*.reg"), ("All files", "*.*")],
                initialdir=os.path.dirname(base_path),
            )
            if not bkg_reg:
                print("No background region file selected — aborting uvotsource step.")
                return None

        try:
            root_tk.destroy()
        except Exception:
            pass

    # Validate that the files exist
    if not os.path.exists(source_reg):
        raise FileNotFoundError(f"Source region file not found: {source_reg}")
    if not os.path.exists(bkg_reg):
        raise FileNotFoundError(f"Background region file not found: {bkg_reg}")

    src_reg_name = os.path.basename(source_reg)
    bkg_reg_name = os.path.basename(bkg_reg)

    print(f"\nSource region : {source_reg}")
    print(f"Background region: {bkg_reg}")

    ###############################################################################
    # Copy and paste regions (Thomas said not to do this, as he perfers calling back a centeral HQ of sort, I however Have already done this and it works... So)

    print("\n" + "-" * 70)
    print("Copying region files to observation directories...")
    print("-" * 70)

    copy_count = 0
    for root_dir, dirs, files in os.walk(base_path):
        # Match directories ending with uvot/image (universal)
        normalised = os.path.normpath(root_dir)
        if normalised.endswith(os.path.join("uvot", "image")):
            try:
                shutil.copy2(source_reg, root_dir)
                shutil.copy2(bkg_reg, root_dir)
                copy_count += 1
            except Exception as e:
                print(f"  Warning — could not copy to {root_dir}: {e}")

    print(f"Copied region files to {copy_count} uvot/image folders.\n")

    
    ###########################################################################
    # DISCOVER ALL uvot/image directories, this is more or less a safty thing
    # I dont want to rely on the obs_table paths which may be differnt after aspect correction as some frames are moved.
    # You know looking back on the code, THIS is 100% increasing the run time, Should proably change this around
    # I need to learn to trust the table again, even after my little incident.
    
    obsid_pattern = re.compile(r"(\d{11})")

    # Collect all (obsid, image_dir) pairs
    image_dirs = []  # list of (obsid_str, full_path_to_image_dir)

    for root_dir, dirs, files in os.walk(base_path):
        normalised = os.path.normpath(root_dir)
        if not normalised.endswith(os.path.join("uvot", "image")):
            continue

        # Extract OBSID from the path
        match = obsid_pattern.search(root_dir)
        if not match:
            continue
        obsid = match.group(1)

        # Skip smeared observations
        if obsid in smeared_obsids:
            continue

        # Skip if this is inside a quarantine folder
        # (Smeared, NotASPCORR, or Orphans)
        path_parts = root_dir.split(os.sep)
        if any(qf in path_parts for qf in ("Smeared", "NotASPCORR", "Orphans")):
            continue

        image_dirs.append((obsid, root_dir))

    print(f"Found {len(image_dirs)} uvot/image directories to process")
    print(f"Unique OBSIDs: {len(set(o for o, _ in image_dirs))}\n")

    if not image_dirs:
        print("No observation directories found — check base_path.")
        return None if automation_mode else None


    
    ##################################################################################
    # Sum Multi extensions files UVOTSUM 
    # 
    # For each image directory, for each band:
    #   1. Find the SK image (uncompressed .img first, then .img.gz)
    #   2. Open the FITS file and read ASPCORR per extension
    #   3. If ALL extensions are DIRECT/UNICORR -> sum normally (no exclude)
    #   4. If SOME extensions are NONE -> sum with exclude= to skip them (I think this is working but at this point this might be broken)
    #   5. If ALL extensions are NONE -> skip entirely (this should be getting qurentined, but something is happening)
    #   6. If only 1 usable extension -> no summation needed, uvotsource  can work on the SK file directly using that extension
    #
    print("=" * 70)
    print("SUMMING MULTI-EXTENSION FILES (uvotimsum) — ASPCORR-AWARE")
    print("=" * 70)

    summed_count = 0
    sum_skipped = 0
    sum_failed = 0
    sum_not_needed = 0
    sum_with_excludes = 0
    exp_summed_count = 0


    def _sum_exposure_map(obsid, band, img_dir, exclude_str=None):
        """
        Sum the exposure map for a given obsid/band using uvotimsum
        with expmap=yes.  This flag tells uvotimsum to preserve
        exposure times during pixel resampling (instead of treating pixel values as photon counts).
        Returns True if a summed exposure map exists after this call.
        """
        exp_summed_outfile = f"{band}_expmap_summed.fits"
        exp_summed_outpath = os.path.join(img_dir, exp_summed_outfile)
 
        if os.path.exists(exp_summed_outpath):
            return True
 
        # Find the raw exposure map
        ex_img = f"sw{obsid}{band}_ex.img"
        ex_gz = f"sw{obsid}{band}_ex.img.gz"
        ex_file = None
        if os.path.exists(os.path.join(img_dir, ex_img)):
            ex_file = ex_img
        elif os.path.exists(os.path.join(img_dir, ex_gz)):
            ex_file = ex_gz
        else:
            return False
 
        # Build uvotimsum command with method=EXPMAP
        # method=EXPMAP tells uvotimsum to sum as exposure maps,
        # preserving exposure times during pixel resampling instead
        # of treating pixel values as photon counts.
        if HEASOFT_BACKEND == "wsl":
            wsl_d = prepare_path(img_dir)
            ecmd = (f"cd '{wsl_d}' && "
                    f"uvotimsum infile='{ex_file}' "
                    f"outfile='{exp_summed_outfile}' "
                    f"method=EXPMAP")
            if exclude_str:
                ecmd += f" exclude={exclude_str}"
        else: ##MAC Version does here, I kinda guessed about how it would look like dont trust me on this.
            ecmd = (f"cd '{img_dir}' && "
                    f"uvotimsum infile='{ex_file}' "
                    f"outfile='{exp_summed_outfile}' "
                    f"method=EXPMAP")
            if exclude_str:
                ecmd += f" exclude={exclude_str}"
 
        print(f"    [expmap] {ecmd}")
 
        run_heasoft_command(ecmd)
        time.sleep(1)
        return os.path.exists(exp_summed_outpath)
 
    for obsid, img_dir in tqdm(image_dirs, desc="Summing extensions", unit="obs"):
        for band in BANDS:
            # Check if a summed SK file already exists, skip if so
            summed_outfile = f"{band}_ex_summed.fits"
            summed_outpath = os.path.join(img_dir, summed_outfile)
            if os.path.exists(summed_outpath):
                sum_skipped += 1
                # SK already summed, but ensure exposure map is too
                if _sum_exposure_map(obsid, band, img_dir):
                    exp_summed_count += 1
                continue

            
            #################################################################################
            # Find the SK image.
            # Priority: uncompressed .img first, then compressed .img.gz.
            
            sk_img = f"sw{obsid}{band}_sk.img"
            sk_gz = f"sw{obsid}{band}_sk.img.gz"
 
            img_file = None
            img_full_path = None
            if os.path.exists(os.path.join(img_dir, sk_img)):
                img_file = sk_img
                img_full_path = os.path.join(img_dir, sk_img)
            elif os.path.exists(os.path.join(img_dir, sk_gz)):
                img_file = sk_gz
                img_full_path = os.path.join(img_dir, sk_gz)
            else:
                continue

            #################################################################################
            # Open the FITS file and check ASPCORR on each image extension.
            # Build a list of which extensions are good (DIRECT/UNICORR)
            # and which are bad (NONE).

            try:
                with fits.open(img_full_path) as hdul:
                    good_exts = []    # Extension numbers with DIRECT/UNICORR
                    bad_exts = []     # Extension numbers with NONE
                    ext_num = 0
 
                    for hdu in hdul:
                        naxis = hdu.header.get('NAXIS', 0)
                        if naxis < 2:
                            continue  # Skip primary HDU / non-image extensions
                        ext_num += 1
                        val = str(hdu.header.get('ASPCORR', 'NONE')).strip().upper()
                        if val == 'DIRECT':
                            good_exts.append(ext_num)
                        else:
                            bad_exts.append(ext_num)
 
                    total_exts = len(good_exts) + len(bad_exts)
            except Exception as e:
                print(f"  [{obsid} / {band}] Error reading FITS: {e}")
                continue
 
            # No image extensions at all, skip
            if total_exts == 0:
                continue
 
            # All extensions are NONE — skip (should have been quarantined, but safety check)
            if len(good_exts) == 0:
                print(f"  [{obsid} / {band}] All {total_exts} extensions are NONE — skipping")
                continue
 
            # Only 1 usable extension total, no summation needed,
            # uvotsource can work on the SK file directly
            if total_exts <= 2 and len(bad_exts) == 0:
                sum_not_needed += 1
                # Still sum the exposure map — it may have multiple
                # extensions even if the SK doesn't need summing
                _sum_exposure_map(obsid, band, img_dir)
                continue
 
            # If only 1 good extension out of many, still no point summing
            # a single frame — but we DO need to note this so uvotsource
            # knows to use that specific extension
            if len(good_exts) == 1 and len(bad_exts) > 0:
                print(f"  [{obsid} / {band}] Only 1 good extension (ext {good_exts[0]}) "
                      f"out of {total_exts} — no summation, uvotsource will use SK directly")
                sum_not_needed += 1
                continue

            #################################################################################
            # Build the uvotimsum command

            if bad_exts:
                exclude_str = ",".join(str(e) for e in bad_exts)
                print(f"  [{obsid} / {band}] {total_exts} extensions: "
                      f"{len(good_exts)} good, {len(bad_exts)} NONE "
                      f"→ summing with exclude={exclude_str}")
            else:
                exclude_str = None
                print(f"  [{obsid} / {band}] {total_exts} extensions, all corrected → summing")
 
            if HEASOFT_BACKEND == "wsl":
                wsl_img_dir = prepare_path(img_dir)
                if exclude_str:
                    sum_cmd = (f"cd '{wsl_img_dir}' && "
                              f"uvotimsum infile='{img_file}' "
                              f"outfile='{summed_outfile}' "
                              f"exclude={exclude_str}")
                else:
                    sum_cmd = (f"cd '{wsl_img_dir}' && "
                              f"uvotimsum '{img_file}' '{summed_outfile}'")
            else: #MACOS goes here, kinda guessed again.
                if exclude_str:
                    sum_cmd = (f"cd '{img_dir}' && "
                              f"uvotimsum infile='{img_file}' "
                              f"outfile='{summed_outfile}' "
                              f"exclude={exclude_str}")
                else:
                    sum_cmd = (f"cd '{img_dir}' && "
                              f"uvotimsum '{img_file}' '{summed_outfile}'")
 
            result = run_heasoft_command(sum_cmd)
 
            # Short delay for WSL filesystem sync
            time.sleep(1)
 
            if os.path.exists(summed_outpath):
                if bad_exts:
                    print(f"✅ Created {summed_outfile} (excluded extensions: {exclude_str})")
                    sum_with_excludes += 1
                else:
                    print(f"✅ Created {summed_outfile}")
                summed_count += 1
                # The emojis will become frequency now, as it was the only quick way to vissually screen fails quickly.
                
                # Sum the exposure map with the SAME exclude list and
                # expmap=yes so uvotimsum treats it as exposure data.
                if _sum_exposure_map(obsid, band, img_dir, exclude_str):
                    print(f"✅ Created {band}_expmap_summed.fits")
                    exp_summed_count += 1
                else:
                    print(f"⚠️ No exposure map found (uvotsource will use expfile=NONE)")
            else:
                print(f"❌ uvotimsum failed for {obsid}/{band}")
                sum_failed += 1
 
    print(f"\nSummation results:")
    print(f" SK images created  : {summed_count}")
    print(f" (with excludes)  : {sum_with_excludes}")
    print(f" Exp maps summed    : {exp_summed_count}")
    print(f" Already existed    : {sum_skipped}")
    print(f" Not needed         : {sum_not_needed} (single extension or ≤2)")
    print(f" Failed             : {sum_failed}\n")

    #################################################################################
    # rUN uvotsource on each  obsid and band

    print("=" * 70)
    print("RUNNING UVOTSOURCE")
    print("=" * 70)
 
    processed = 0
    skipped = 0
    failed = 0
 
    for obsid, img_dir in tqdm(image_dirs, desc="Running uvotsource", unit="obs"):
        for band in BANDS:
            finalsource_file = f"{band}_finalsource.fits"
            finalsource_path = os.path.join(img_dir, finalsource_file)
 
            # Skip if already processed
            if os.path.exists(finalsource_path):
                skipped += 1
                continue

            #################################################################################
            # Decide which input file to use.
            #   1. {band}_ex_summed.fits      — summed multi-extension file
            #   2. sw{OBSID}{band}_sk.img     — uncompressed SK (from uvotunicorr)
            #   3. sw{OBSID}{band}_sk.img.gz  — compressed SK (original download)
            
            summed_file = f"{band}_ex_summed.fits"
            sk_file_img = f"sw{obsid}{band}_sk.img"
            sk_file_gz = f"sw{obsid}{band}_sk.img.gz"
 
            input_file = None
            if os.path.exists(os.path.join(img_dir, summed_file)):
                input_file = summed_file
                # Summed files are trusted, the summation step already
                # excluded NONE extensions via the exclude parameter.
            elif os.path.exists(os.path.join(img_dir, sk_file_img)):
                input_file = sk_file_img
            elif os.path.exists(os.path.join(img_dir, sk_file_gz)):
                input_file = sk_file_gz
            else:
                # No file for this band in this observation — totally normal,
                # not every observation has every band.
                continue

            ################################################################
            # ASPCORR SAFETY CHECK AGAIN 
            # verify the file is actually corrected before running uvotsource on it.
            #
            # If the input is a summed file, we trust it (summation already
            # excluded NONE extensions). If it's a raw SK file, we open
            # it and check that ALL image extensions have DIRECT/UNICORR.
            # 
            if input_file != summed_file:
                input_full_path = os.path.join(img_dir, input_file)
                try:
                    all_good = True
                    has_image_ext = False
                    ext_statuses = []
                    with fits.open(input_full_path) as hdul:
                        for idx, hdu in enumerate(hdul):
                            naxis = hdu.header.get('NAXIS', 0)
                            if naxis < 2:
                                continue
                            has_image_ext = True
                            val = str(hdu.header.get('ASPCORR', 'NONE')).strip().upper()
                            ext_statuses.append((idx, val))
                            if val not in ('DIRECT', 'UNICORR'):
                                all_good = False

                    if not has_image_ext:
                        print(f"  [{obsid} / {band}] No image extensions — skipping")
                        skipped += 1
                        continue

                    if not all_good:
                        print(f"  [{obsid} / {band}] ASPCORR not fully corrected — skipping")
                        print(f"    File: {input_file}")
                        print(f"    Extensions: {ext_statuses}")
                        skipped += 1
                        continue
 
                except Exception as e:
                    print(f"  [{obsid} / {band}] Cannot verify ASPCORR ({e}) — skipping")
                    skipped += 1
                    continue
 
            # Verify region files are present in the directory
            if not os.path.exists(os.path.join(img_dir, src_reg_name)):
                print(f"  [{obsid} / {band}] Source region missing — skipping")
                skipped += 1
                continue
            if not os.path.exists(os.path.join(img_dir, bkg_reg_name)):
                print(f"  [{obsid} / {band}] Background region missing — skipping")
                skipped += 1
                continue
 
            print(f"  [{obsid} / {band}] Using {input_file}")



            ################################################################
            # Find the best available exposure map for this band.
            # Priority matches the SK file priority:
            #   1. {band}_expmap_summed.fits — summed exposure map
            #   2. sw{OBSID}{band}_ex.img    — uncompressed
            #   3. sw{OBSID}{band}_ex.img.gz — compressed
            #   4. NONE                       — no exposure map available
            # 
            exp_img = f"sw{obsid}{band}_ex.img"
            exp_gz = f"sw{obsid}{band}_ex.img.gz"
            exp_summed = f"{band}_expmap_summed.fits"

            exp_file = "NONE"
            if input_file == summed_file:
                # Summed SK, use summed exposure map (extension names match)
                if os.path.exists(os.path.join(img_dir, exp_summed)):
                    exp_file = exp_summed
            else:
                # Raw SK, use raw exposure map (extension names match) Because it can make sums
                # But if it tries to use them has a "2d error" which I dont 100% get, but I sure do know thats what im seeing.
                if os.path.exists(os.path.join(img_dir, exp_img)):
                    exp_file = exp_img
                elif os.path.exists(os.path.join(img_dir, exp_gz)):
                    exp_file = exp_gz

            if exp_file != "NONE":
                print(f"Exposure map: {exp_file}")
            else:
                print(f"Exposure map: NONE (not available)")

            print(f"Running uvotsource ...")
            
            #################################################################################
            # Build and run the uvotsource command

            if HEASOFT_BACKEND == "wsl":
                wsl_img_dir = prepare_path(img_dir)

                # Use heredoc-style interactive input for uvotsource
                # (same approach as the standalone uvotsource script)
                uvotsource_cmd = (
                    f"cd '{wsl_img_dir}' && "
                    f"uvotsource image='{input_file}' "
                    f"srcreg='{src_reg_name}' "
                    f"bkgreg='{bkg_reg_name}' "
                    f"sigma=5 "
                    f"expfile='{exp_file}' "
                    f"zerofile=CALDB coinfile=CALDB psffile=CALDB lssfile=CALDB "
                    f"syserr=NO frametime=DEFAULT apercorr=NONE output=ALL "
                    f"outfile='{finalsource_file}' "
                    f"cleanup=YES clobber=YES chatter=1"
                )

                result = run_heasoft_command(uvotsource_cmd)

            else:
                # MACOS?
                uvotsource_cmd = (
                    f"bash -c '"
                    f"cd \"{img_dir}\" && "
                    f"uvotsource "
                    f"image=\"{input_file}\" "
                    f"srcreg=\"{src_reg_name}\" "
                    f"bkgreg=\"{bkg_reg_name}\" "
                    f"sigma=5 "
                    f"expfile=\"{exp_file}\" "
                    f"zerofile=CALDB coinfile=CALDB psffile=CALDB lssfile=CALDB "
                    f"syserr=NO frametime=DEFAULT apercorr=NONE output=ALL "
                    f"outfile=\"{finalsource_file}\" "
                    f"cleanup=YES clobber=YES chatter=1"
                    f"'"
                )
                result = run_heasoft_command(uvotsource_cmd)

            # Brief pause to let the filesystem sync (especially for WSL)
            time.sleep(1)

            # Check result
            if os.path.exists(finalsource_path):
                print(f" ✅ Created {finalsource_file}")
                processed += 1
            else:
                print(f" ❌ uvotsource did not produce output")
                failed += 1

    print(f"\n{'─' * 70}")
    print(f"UVOTSOURCE SUMMARY")
    print(f" Processed : {processed}")
    print(f" Skipped   : {skipped} (already existed or missing region)")
    print(f" Failed    : {failed}")
    print(f"{'─' * 70}\n")

    #################################################################################
    # Read finalsource files and make Master CSV 
   
    print("=" * 70)
    print("COMPILING PHOTOMETRY DATA")
    print("=" * 70)
 
    all_rows = []
    band_tables = {b: [] for b in BANDS}
    files_found = 0
    files_loaded = 0
 
    for root_dir, dirs, files in os.walk(base_path):
        # Skip quarantine directories
        path_parts = set(root_dir.replace("\\", "/").split("/"))
        if path_parts & {"Smeared", "NotASPCORR", "Orphans"}:
            continue
 
        for f in files:
            if not f.endswith("_finalsource.fits"):
                continue
 
            files_found += 1
            filepath = os.path.join(root_dir, f)
 
            # Extract band from filename (e.g. "uw1_finalsource.fits" -> "uw1")
            band_match = re.match(r"([a-z0-9]+)_finalsource\.fits$", f)
            if not band_match:
                continue
            band = band_match.group(1)
            if band not in BANDS:
                continue
 
            # Extract OBSID from the directory path
            obsid_match = re.search(r"(\d{11})", root_dir)
            obsid = obsid_match.group(1) if obsid_match else "UNKNOWN"
 
            try:
                with fits.open(filepath) as hdul:
                    if len(hdul) < 2 or hdul[1].data is None:
                        print(f"  Warning — no table data in {filepath}")
                        continue
 
                    data = hdul[1].data
                    df = pd.DataFrame(np.array(data).byteswap().newbyteorder())
 
                    # EXTNAME is a FITS header keyword (extension name), not
                    # photometry data. Some versions of astropy/numpy include
                    # it when converting the binary table to an array. If it
                    # ended up as a column, drop it — it becomes NaN during
                    # concat and causes downstream issues.
                    if 'EXTNAME' in df.columns:
                        df.drop(columns=['EXTNAME'], inplace=True)
 
                    # Attach metadata
                    df["OBSID"] = obsid
                    df["BAND"] = band
                    df["SOURCE_FILE"] = filepath
 
                    all_rows.append(df)
                    band_tables[band].append(df)
                    files_loaded += 1
                    print(f"  Loaded {f}  (ObsID {obsid})")
 
            except Exception as e:
                print(f"  Error reading {filepath}: {e}")
 
    # Combine everything
    print(f"\n  Finalsource files found: {files_found}")
    print(f"  Successfully loaded: {files_loaded}")
 
    if all_rows:
        df_all = pd.concat(all_rows, ignore_index=True)
 
        # Drop any columns that are entirely NaN — these can appear when
        # concat merges DataFrames with slightly different column sets
        # (e.g. if one finalsource file has an extra header-derived column
        # like EXTNAME that others don't).
        all_nan_cols = [c for c in df_all.columns if df_all[c].isna().all()]
        if all_nan_cols:
            print(f"  Dropping all-NaN columns: {all_nan_cols}")
            df_all.drop(columns=all_nan_cols, inplace=True)
 
        print(f"  Master table: {len(df_all)} rows, {len(df_all.columns)} columns")
    else:
        df_all = pd.DataFrame()
        print("\n ⚠️ No finalsource files found — photometry table is empty.")
 
    band_dfs = {}
    for band, dfs in band_tables.items():
        band_dfs[band] = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    #################################################################################
    # Write Excel workbook (All_Data + per-band sheets + Summary)
    
    excel_path = os.path.join(save_path, "UVOT_Data_Analysis.xlsx")
    try:
        with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
            # Combined sheet
            if not df_all.empty:
                df_all.to_excel(writer, sheet_name="All_Data", index=False)

            # Per-band sheets
            for band, df in band_dfs.items():
                if not df.empty:
                    sheet = f"Band_{band}"[:31]  # Excel 31-char limit
                    df.to_excel(writer, sheet_name=sheet, index=False)

            # Summary sheet
            summary_rows = []
            if not df_all.empty:
                summary_rows.append(
                    ["Total observations (all bands)", len(df_all)]
                )
                summary_rows.append(
                    ["Unique OBSIDs", df_all["OBSID"].nunique()]
                )
                # Use MET (Mission Elapsed Time) for time range if available
                if "MET" in df_all.columns:
                    met_min = df_all["MET"].min()
                    met_max = df_all["MET"].max()
                    # Convert MET to MJD for readability
                    mjd_min = met_min / 86400.0 + 51910.0
                    mjd_max = met_max / 86400.0 + 51910.0
                    summary_rows.append([
                        "MJD range",
                        f"{mjd_min:.2f} to {mjd_max:.2f}"
                    ])

            for band, df in band_dfs.items():
                if not df.empty:
                    summary_rows.append([f"Observations in {band}", len(df)])

            if summary_rows:
                pd.DataFrame(summary_rows, columns=["Metric", "Value"]).to_excel(
                    writer, sheet_name="Summary", index=False
                )

        print(f" Excel workbook saved: {excel_path}")

    except Exception as e:
        print(f"Error writing Excel file: {e}")

    #################################################################################
    # Return
    if automation_mode:
        return df_all
    else:
        print(f"\n{'=' * 70}")
        print("UVOTSOURCE PIPELINE COMPLETE")
        print(f"{'=' * 70}") 
        return None#Dont put it to None yet, this exists here incase we ever wanted to make it autograph rather then just spit out a table.




def diagnose_obs_table(obs_table):
    print("=" * 70)
    print("OBS_TABLE DIAGNOSTIC")
    print("=" * 70)

    print(f"\nTotal rows: {len(obs_table)}")

    # Extension_Status
    print(f"\n--- Extension_Status ---")
    print(f" dtype: {obs_table['Extension_Status'].dtype}")
    print(f" Unique values (repr): {[repr(v) for v in obs_table['Extension_Status'].unique()]}")
    print(f" Value counts:\n{obs_table['Extension_Status'].value_counts()}")

    nan_count = obs_table['Extension_Status'].isna().sum()
    if nan_count > 0:
        print(f" WARNING: {nan_count} NaN values!")

    # Smeared Flag
    print(f"\n--- Smeared Flag ---")
    print(f" dtype: {obs_table['Smeared Flag'].dtype}")
    print(f" Unique values: {obs_table['Smeared Flag'].unique()}")

    if obs_table['Smeared Flag'].dtype == object:
        print("  WARNING: Smeared Flag is string, not bool!")

    # ObsID type
    print(f"\n--- ObsID ---")
    print(f" dtype: {obs_table['ObsID'].dtype}")
    print(f" Sample: {obs_table['ObsID'].head(3).tolist()}")

    # Simulate filtering for first non-trivial group
    working = obs_table[obs_table['Smeared Flag'] == False].copy()
    print(f"\nAfter smeared filter: {len(working)} rows")

    for group_id in sorted(working['Group_ID'].unique())[:3]:
        gd = working[working['Group_ID'] == group_id]
        gs = gd['Group_Status'].iloc[0]
        if gs in ('ORPHAN', 'COMPLETED'):
            continue
        for band in gd['Filter'].unique():
            bd = gd[gd['Filter'] == band]
            none_count = len(bd[bd['Extension_Status'] == 'NONE'])
            none_filter = bd[bd['Extension_Status'] == 'NONE']
            print(f"\n  Group {group_id}/{band}: total={len(bd)}, "
                  f"NONE_counted={none_count}, NONE_filtered={len(none_filter)}")
            if none_count != len(none_filter):
                print("  *** MISMATCH ***")
            for v in bd['Extension_Status'].unique():
                print(f"    {repr(v)}: {len(bd[bd['Extension_Status'] == v])}")


import time
import gc


def automated_aspect_correction(obs_table, base_path, save_path, side_buffer=7, num_stars=50):
    """
    Automated aspect correction using the observations table.

    KEY DIFFERENCES FROM clean_uvot_tiles.py, So you, Thomas(proably) Will know.

    1. DATA SOURCE:
       clean_uvot_tiles scanned directories with os.listdir() and built file
       paths manually (I.E. f'{filepath}/{obs_frame}/uvot/image/detect.fits').
       This instead pulls all observation metadata from obs_table,
       which was pre-built by populate_observations_table() (As we talked abut). 

    2. GROUP-BASED PROCESSING:
       clean_uvot_tiles processed every observation in a flat loop with a
       single reference frame for the entire tile. This function organizes
       work by Group_ID, which means each group can have its OWN best reference frame. ORPHAN
       groups (observations that couldn't be grouped) are skipped automatically. (As we talked about)

    3. REFERENCE FRAME SELECTION:
       clean_uvot_tiles picked the first DIRECT frame it found as the reference
       for ALL corrections in the tile. This function selects references
       PER GROUP and PER BAND, choosing the best DIRECT frame available for
       each specific correction. It also tries to match the extension number
       first, falling back to extension 1 if no match is found. This is mostly not important 
       It originally was a desperate gamble to get it to do Multi-Extensions 
       The only reason it has not been removed is because it did clean a test 
       frame that wassent fixed by the normal proccess and it barely used processing time.

    4. BAND-AWARE PROCESSING:
       clean_uvot_tiles was hardcoded to 'uw1' only (all paths used 'uw1').
       This function processes all the bands present in the data (uw1, uw2, ubb,
       uvv, um2, uuu) by iterating over unique filters within each group.

    5. RETRY LOGIC:
       clean_uvot_tiles had a manual retry loop where the user could change
       parameters and re-run the entire pipeline from scratch. This function
       has smarter retry logic: it tracks exactly which frames failed and
       only re-attempts those specific frames on retry, rather than
       re-processing everything. (I think thats what yours did if I read it right)

    6. PLATFORM SUPPORT:
       clean_uvot_tiles used direct shell commands (os.system, sh.gunzip)
       and assumed a native macOS/Linux HEASOFT installation. This might/should
       supports both WSL and native backends, however it does need your input
       I need you to but your UNICORR logic in a section bellow.


       You Thomas(proably) will also notice bellow I have tons of comments, 
       Both for bookkeeping reasons (mostly debuging, trying to keep track of the mess I was making) and also
       So you can track exactly in detail what has been changed, and why, I think the why is important on occasion.
    """

    
    ###############################################################################
    # RETRY STATE VARIABLES
    failed_frames_to_retry = None
    attempt_num = 0

    ##############################################################################
    # MAIN RETRY LOOP
    #
    # CHANGE FROM clean_uvot_tiles.py:
    #   clean_uvot_tiles had a similar while loop (run_pipeline == True)
    #   but it wrapped the whole cleaning pipeline (download, detect,
    #   smear removal, unzip, and aspect correction). Here we only loop
    #   around the aspect correction step itself, since all the upstream
    #   work (detection, smear removal, etc.) has already been done and
    #   stored in obs_table.
    while True:
        print("\n" + "=" * 70)
        if attempt_num == 0:
            print("AUTOMATED ASPECT CORRECTION - INITIAL ATTEMPT")
        else:
            print(f"AUTOMATED ASPECT CORRECTION - RETRY ATTEMPT {attempt_num}")
        print("=" * 70)
        print(f"Parameters: side_buffer={side_buffer}, num_stars={num_stars}")
        print(f"HEASOFT Backend: {HEASOFT_BACKEND}")

        ##############################################################################
        # FILTER THE OBSERVATIONS TABLE
        # Remove smeared frames 
        #
        # CHANGE FROM clean_uvot_tiles.py:
        #   clean_uvot_tiles called up.detect_smeared_frames() and then up.remove_smeared()
        #   I instead just filter them out of our working table since
        #   obs_table already has a 'Smeared Flag' column pre-computed.
        working_table = obs_table[obs_table['Smeared Flag'] == False].copy()

        ##############################################################################
        # BUILD RETRY FILTER (attempts > 0 only)
        # 
        # On retry, we really only want to re-attempt the frames that failed, a lesson I have learned.
        # But we keep the FULL working_table available because we still
        # need access to DIRECT reference frames 
        # Originally the error here was I did not keep the table open
        # So when retry was attempted It simply couldn't find reference frames. Hingsight 20/20.
        frames_to_correct = None
        if attempt_num > 0 and failed_frames_to_retry:
            print(f"RETRY MODE: Only correcting {sum(len(v) for v in failed_frames_to_retry.values())} failed frames from previous attempt")

            # Parse the failed frame identifiers ("00033038050_ext1" format)
            # into (ObsID, extension_number) tuples for fast lookup
            frames_to_correct = set()
            for group_band, obsid_list in failed_frames_to_retry.items():
                for obsid_ext in obsid_list:
                    parts = obsid_ext.split('_ext')
                    if len(parts) == 2:
                        obsid = parts[0]
                        ext = int(parts[1])
                        frames_to_correct.add((obsid, ext))

            print(f"Will attempt correction on {len(frames_to_correct)} frames")
            print(f"(Keeping full dataset for reference selection)")

        
        # These track which frames failed during the attempt, and are used
        # both for the final return value and for building the retry bit.
        aspectnone_dict = {}        # count of failures
        aspectnone_tiles_dict = {}  # list of failed ObsID_ext strings

        ###############################################################################
        # GET UNIQUE GROUPS TO PROCESS
        
        # CHANGE FROM clean_uvot_tiles.py:
        #   clean_uvot_tiles iterated over directory names (each ObsID was
        #   a folder,). Ima iterate over Group_IDs, which cluster the related
        #   observations together.
        unique_groups = working_table['Group_ID'].unique()

        if len(working_table) == 0:
            print("No frames to process in this attempt")
            break

        print(f"\nFound {len(unique_groups)} unique groups to process")

        # DEBUG (The first on many... Unless I already deleted most of them): Print the group status on first attempt
        if attempt_num == 0:
            print("\nGroup Status Breakdown:")
            for status in ['COMPLETED', 'READY', 'ORPHAN', 'UNICORR']:
                count = len(working_table[working_table['Group_Status'] == status]['Group_ID'].unique())
                print(f"  {status}: {count} groups")
            print()

        ###############################################################################
        # The Main loop
        
        for group_id in unique_groups:
            group_data = working_table[working_table['Group_ID'] == group_id]
            group_status = group_data['Group_Status'].iloc[0]

            # Skip groups that don't need processing
            Orphans_Exist = 0
            # ORPHAN groups have no related observations to use as references, Thomas will take care of this. I think, Or he will tell me how to.
            if group_status == 'ORPHAN':
                # Record any remaining failures for this group+band combination
                Orphans_Exist += 1
                if attempt_num == 0:
                    print(f"\n[Group {group_id}] Status: ORPHAN - Skipping")
                continue

            # COMPLETED groups.... Are completed.
            if group_status == 'COMPLETED':
                if attempt_num == 0:
                    print(f"\n[Group {group_id}] Status: COMPLETED - Already done")
                continue

            # UNICORR groups Are also completed.
            if group_status == 'UNICORR':
                if attempt_num == 0:
                    print(f"\n[Group {group_id}] Status: UNICORR - Already done")
                continue

            if Orphans_Exist > 0:
                key = f"{group_id}_{band}"
                aspectnone_dict[key] = Orphans_Exist
                # Build list of Orphan frame identifiers for retry/manual inspection
                Orphan_obsids = []
                for idx, obs_row in corrections_needed.iterrows():
                    Orphan_obsids.append(f"{obs_row['ObsID']}_ext{obs_row['Snapshot']}")
                aspectnone_tiles_dict[key] = Orphan_obsids[:remaining]
                
            # Print out to know what the code is on.
            print(f"\n{'=' * 70}")
            print(f"Processing Group {group_id} (Status: {group_status})")
            print(f"{'=' * 70}")

            ###############################################################################
            # Go over bands within the group
            
            # CHANGE FROM clean_uvot_tiles.py:
            #   clean_uvot_tiles was hardcoded to only process 'uw1' (all
            #   file paths contained 'uw1'). This function processes every
            #   band present in the group.
            unique_bands = group_data['Filter'].unique()

            for band in unique_bands:
                band_data = group_data[group_data['Filter'] == band]

                print(f"\n--- Band: {band} ---")
                print(f"Total extensions: {len(band_data)}")
                print(f"DIRECT: {len(band_data[band_data['Extension_Status'] == 'DIRECT'])}")
                print(f"NONE: {len(band_data[band_data['Extension_Status'] == 'NONE'])}")
                print(f"UNICORR: {len(band_data[band_data['Extension_Status'] == 'UNICORR'])}")

                # Next we Find the direct reference frames, These are the references.
                #
                # IMPORTANT!!!!!!!!! We search for references in the FULL band_data,
                # NOT filtered by frames_to_correct. Even on retry, gotta get
                # access to all DIRECT frames as references.
                #
                # CHANGE FROM clean_uvot_tiles.py:
                #   clean_uvot_tiles picked ONE reference for the entire tile:
                #       ref_frame = direct_frames[0]
                #   We pick the best reference per band within each group.
                ref_candidates = band_data[band_data['Extension_Status'] == 'DIRECT']

                if ref_candidates.empty:
                    print(f"Ruh-Roh: ⚠️  No DIRECT reference found for {band} - skipping")
                    continue

                # Find frames that need correction, So NONE
                corrections_needed = band_data[band_data['Extension_Status'] == 'NONE']

                # On retry, make it the frames that failed last time.
                if frames_to_correct is not None:
                    corrections_needed = corrections_needed[
                        corrections_needed.apply(
                            lambda row: (row['ObsID'], row['Snapshot']) in frames_to_correct,
                            axis=1
                        )
                    ]

                print(f"Extensions needing correction: {len(corrections_needed)}")

                if corrections_needed.empty:
                    continue

                # Counters for this band's correction summary, Important for debuging and knowing what failed.
                corrections_attempted = 0
                corrections_successful = 0
                corrections_failed = 0

                #########################################################################
                # Now we correct the frames that are NONE
                # For each frame with ASPCORR='NONE', we are going to:
                #   1. Find a suitable DIRECT reference in the same group/band
                #   2. Detect bright stars in both the reference and observation
                #   3. Match stars between the two frames
                #   4. Run uvotunicorr to compute and apply the pointing correction
                #
                # CHANGE FROM clean_uvot_tiles.py:
                #   clean_uvot_tiles found stars in the reference frame ONCE
                #   and reused them for all corrections:
                #       ref_bright_stars = up.find_brightest_central_stars(ref_detect_path, ...)
                #       for obs_frame in aspect_uncorrected_frames:
                #           ...
                #   I of course Cant do this, Also I think This might have made a bug? 
                #   remove_separate_stars() changed ref_bright_stars, so by the second go of it the
                #   reference star list was getting filtered down.
                #   Anywhoo I need to find new stars for each correction pair anyways.
                for correction_num, (idx, obs_row) in enumerate(corrections_needed.iterrows(), start=1):

                    # Small pause between corrections to avoid overwhelming HEASOFT or filesystem, WSL loves to overwhelm.
                    # Turns out running subsystems in subsystems has some issues.
                    if correction_num > 1:
                        time.sleep(3)

                    obs_obsid = obs_row['ObsID']
                    obs_snapshot = obs_row['Snapshot']
                    obs_full_path = obs_row['Full_Path']

                    print(f"\n  [{correction_num}/{len(corrections_needed)}] "
                          f"Correcting ObsID {obs_obsid}, Extension {obs_snapshot}...")

                    obs_dir = os.path.dirname(obs_full_path)

                    # Bachelor for DIrect files.
                    # We want to find a DIRECT reference that matches our extension
                    # number first. If none exists, fall back to extension 1. 
                    # Again this porably can be removed, but it did fix 1 frame 1 time in a test
                    # Also I dont really want to.
                    #
                    # CHANGE FROM clean_uvot_tiles.py:
                    #   clean_uvot_tiles used a single global reference, I said it before.
                    suitable_ref = None

                    # First pass: try to match the same extension number
                    for _, ref_candidate in ref_candidates.iterrows():
                        candidate_path = ref_candidate['Full_Path']
                        candidate_obsid = ref_candidate['ObsID']
                        candidate_snapshot = ref_candidate['Snapshot']

                        # Skip if extension doesn't match
                        if candidate_snapshot != obs_snapshot:
                            continue

                        # Verify the reference directory and file exist on disk, Just in case Dont want it crashing for no good reason.
                        ref_dir_check = os.path.dirname(candidate_path)
                        if not os.path.exists(ref_dir_check):
                            continue

                        actual_files = os.listdir(ref_dir_check)
                        ref_base = f"sw{candidate_obsid}{band}_sk"

                        # Search for the sky image file
                        ref_file_found = None
                        for f in actual_files:
                            if f.startswith(ref_base):
                                ref_file_found = f
                                break

                        if ref_file_found:
                            suitable_ref = {
                                'obsid': candidate_obsid,
                                'snapshot': candidate_snapshot,
                                'full_path': os.path.join(ref_dir_check, ref_file_found),
                                'dir': ref_dir_check
                            }
                            print(f"Using reference: ObsID {candidate_obsid}, Extension {candidate_snapshot}")
                            break

                    # Second pass: fall back to extension 1 if no match found, ON hindisght This should proably really be changed, It works for now.
                    # But there is no real reason to not replace it except for the fact That I am deathly Afraid that touching anything could shatter it.
                    if suitable_ref is None:
                        print(f" Issue 101: No extension {obs_snapshot} reference found, trying extension 1...")

                        for _, ref_candidate in ref_candidates.iterrows():
                            candidate_path = ref_candidate['Full_Path']
                            candidate_obsid = ref_candidate['ObsID']
                            candidate_snapshot = ref_candidate['Snapshot']

                            if candidate_snapshot != 1:
                                continue

                            ref_dir_check = os.path.dirname(candidate_path)
                            if not os.path.exists(ref_dir_check):
                                continue

                            actual_files = os.listdir(ref_dir_check)
                            ref_base = f"sw{candidate_obsid}{band}_sk"

                            ref_file_found = None
                            for f in actual_files:
                                if f.startswith(ref_base):
                                    ref_file_found = f
                                    break

                            if ref_file_found:
                                suitable_ref = {
                                    'obsid': candidate_obsid,
                                    'snapshot': 1,
                                    'full_path': os.path.join(ref_dir_check, ref_file_found),
                                    'dir': ref_dir_check
                                }
                                print(f"    Using fallback reference: ObsID {candidate_obsid}, "
                                      f"Extension 1 (for obs ext {obs_snapshot})")
                                break

                    # If no reference found at all, we can't correct that.
                    if suitable_ref is None:
                        print(f" ❌ No DIRECT reference found "
                              f"(tried extension {obs_snapshot} and extension 1) - skipping")
                        corrections_failed += 1
                        continue

                    ref_obsid = suitable_ref['obsid']
                    ref_snapshot = suitable_ref['snapshot']
                    ref_full_path = suitable_ref['full_path']
                    ref_dir = suitable_ref['dir']

                    # Loacate the detect files
                    # We need detect files sine they contain the source catalog 
                    # (star positions and brightnesses). We need these
                    # for both the reference and observation frames to find
                    # matching stars for the correction.
                    #
                    # CHANGE FROM clean_uvot_tiles.py:
                    #   clean_uvot_tiles assumed a single detect.fits per ObsID:
                    #       obs_detect_path = f'{filepath}/{obs_frame}/uvot/image/detect.fits'
                    #   We have to check for extension specific detect files first
                    #   (band_detect_ext1.fits), falling back to the generic
                    #   detect file if not found.
                    obs_detect_file = os.path.join(obs_dir, f"{band}_detect_ext{obs_snapshot}.fits")
                    if not os.path.exists(obs_detect_file):
                        obs_detect_file = os.path.join(obs_dir, f"{band}_detect.fits")

                    ref_detect_file = os.path.join(ref_dir, f"{band}_detect_ext{ref_snapshot}.fits")
                    if not os.path.exists(ref_detect_file):
                        ref_detect_file = os.path.join(ref_dir, f"{band}_detect.fits")

                    if not os.path.exists(obs_detect_file):
                        print(f" ❌ No detect file found "
                              f"(tried {band}_detect_ext{obs_snapshot}.fits and {band}_detect.fits) - skipping")
                        corrections_failed += 1
                        continue

                    if not os.path.exists(ref_detect_file):
                        print(f" ❌ No detect file found for reference - skipping")
                        corrections_failed += 1
                        continue


                    
                    # Matching the Bachelors and Bachelorettes  
                    try:
                        # Find bright stars near the center of both frames.
                        # side_buffer controls how far from center to look Set to 7'
                        # This is bcause of Thomas's preference But now it has to be that way. 
                        # Since thats also the way the groups are identified. On hindsght that I am having just now
                        # If you tried to change that number without making it go back up to orphan hunting that would proably 
                        # Crash the code.... Might need to fix that on a later update.
                        #
                        # CHANGE FROM clean_uvot_tiles.py:
                        #   clean_uvot_tiles found reference stars once outside
                        #   the loop and reused them. But remove_separate_stars()
                        #   modifies the list, so the reference stars
                        #   got progressively filtered with each iteration.
                        #   We now find stars fresh for each correction pair, avoiding that.
                        ref_bright_stars = find_brightest_central_stars(
                            ref_detect_file,
                            num_stars=num_stars,
                            side_buffer=side_buffer
                        )

                        obs_bright_stars = find_brightest_central_stars(
                            obs_detect_file,
                            num_stars=num_stars,
                            side_buffer=side_buffer
                        )

                        # Cross-match stars between the reference and observation.
                        # Stars that appear in only one frame are removed they're
                        # either transients, artifacts, or fell off the detector edge.
                        ref_stars_filtered, obs_stars_filtered = remove_separate_stars(
                            ref_bright_stars.copy(),
                            obs_bright_stars
                        )

                        # Need at least 3 matched stars for a decent geometric
                        # transformation (translation + rotation needs ≥3 points aparantly)
                        if len(ref_stars_filtered) < 3:
                            print(f" ❌ Not enough matching stars ({len(ref_stars_filtered)}) - skipping")
                            corrections_failed += 1
                            continue

                        print(f" Found {len(ref_stars_filtered)} matching stars")

                        # Write region files (.reg) marking the matched star
                        # positions. These will be used by uvotunicorr to get
                        # the pointing offset.
                        create_ref_obs_reg_files(
                            ref_stars_filtered,
                            obs_stars_filtered,
                            outpath=obs_dir
                        )

                        # Prepare the Bachelorettes  (References)
                        # uvotunicorr needs both the reference and observation sky images in the same directory.
                        # (Not actually true I could proably have it path the the location instead but this is easier.)
                        ref_img_name = os.path.basename(ref_full_path)

                        # Unzip if the reference is gzipped(thats a fun name)
                        if ref_full_path.endswith('.gz'):
                            ref_img_path = ref_full_path[:-3]
                            if not os.path.exists(ref_img_path):
                                print(f" Unzipping reference image...")
                                if HEASOFT_BACKEND == "wsl":
                                    wsl_path = prepare_path(ref_full_path)
                                    gunzip_cmd = f"gunzip -k '{wsl_path}'"
                                    run_heasoft_command(gunzip_cmd) # I saw Heasoft had A way to do this And I trust it more then windows.
                                else:
                                    os.system(f'gunzip -k "{ref_full_path}"') # Input you best MACOS version here, or this might work.
                                ref_img_name = os.path.basename(ref_img_path)
                        else:
                            ref_img_path = ref_full_path

                        if not os.path.exists(ref_img_path):
                            print(f" ❌ Failed to access reference image")
                            corrections_failed += 1
                            continue

                        # Copy reference image into the observation's directory
                        # so uvotunicorr can find both files together.
                        #
                        # CHANGE FROM clean_uvot_tiles.py:
                        #   clean_uvot_tiles did this identically:
                        #       shutil.copy(ref_file_path, obs_directory)
                        #   We add a check to avoid copying a file onto itself (Safty first).
                        ref_img_dest = os.path.join(obs_dir, ref_img_name)
                        try:
                            if os.path.abspath(ref_img_path) != os.path.abspath(ref_img_dest):
                                shutil.copy(ref_img_path, ref_img_dest)
                                print(f"    Copied reference image: {ref_img_name}")
                        except Exception as e:
                            print(f" ❌ Failed to copy reference: {e}")
                            corrections_failed += 1
                            continue

                        
                        # Ready the Bachelors (observation img) 
                        # Find and unzip the observation sky image
                        obs_base = f"sw{obs_obsid}{band}_sk"
                        obs_dir_files = os.listdir(obs_dir)
                        obs_file_found = None

                        for f in obs_dir_files:
                            if f.startswith(obs_base):
                                obs_file_found = f
                                break

                        if not obs_file_found:
                            print(f" ❌ Observation file not found")
                            corrections_failed += 1
                            continue

                        obs_img_path = os.path.join(obs_dir, obs_file_found)

                        # Unzip observation image if needed
                        #
                        # CHANGE FROM clean_uvot_tiles.py:
                        #   clean_uvot_tiles had a separate unzipping pass that
                        #   ran before aspect correction, unzipping ALL files
                        #   upfront. I am going to unzip on demand per frame instead,
                        #   This avoids wasting disk space on frames we won't
                        #   process (smeared, orphaned, already corrected, etc).
                        if obs_img_path.endswith('.gz'):
                            obs_img_unzipped = obs_img_path[:-3]
                            if not os.path.exists(obs_img_unzipped):
                                print(f"    Unzipping observation image...")
                                if HEASOFT_BACKEND == "wsl":
                                    wsl_path = prepare_path(obs_img_path)
                                    gunzip_cmd = f"gunzip -k '{wsl_path}'"
                                    run_heasoft_command(gunzip_cmd)
                                else:
                                    os.system(f'gunzip -k "{obs_img_path}"') # Input you best MACOS version here, or this might work.
                            obs_img_path = obs_img_unzipped
                            obs_file_found = os.path.basename(obs_img_unzipped)

                        if not os.path.exists(obs_img_path):
                            print(f" ❌ Failed to unzip observation image")
                            corrections_failed += 1
                            continue

                        # RUN UVOTUNICORR
                        #
                        # CHANGE FROM clean_uvot_tiles.py:
                        #   clean_uvot_tiles called:
                        #       unicorr_command = up.create_uvotunicorr_bash_command(
                        #           ref_frame, obs_frame, obspath=obs_directory)
                        #   Which assumed native HEASOFT. I use a WSL So that is a no-no
                        #   when running through WSL.
                        if HEASOFT_BACKEND == "wsl":
                            unicorr_command = create_uvotunicorr_full_command_wsl(
                                ref_frame=ref_obsid,
                                obs_frame=obs_obsid,
                                band=band,
                                ref_snapshot=ref_snapshot,
                                obs_snapshot=obs_snapshot,
                                obspath=obs_dir
                            )

                            print(f" Running uvotunicorr (WSL)...")

                            # Force Python to release any open file handles
                            # before calling WSL, which runs in a separate filesystem namespace
                            gc.collect()

                            corrections_attempted += 1
                            run_heasoft_command(unicorr_command)
                            
                            # Wait for WSL process to finish writing to disk
                            time.sleep(5)

                        else:
                            # Native macOS/Linux HEASOFT
                            #  not yet A thing in this, you gotta do that Thomas, This is all you.
                            print(f" Error Thomas: ⚠️  macOS support not yet implemented - skipping")
                            corrections_failed += 1
                            continue

                            
                        # Check to see if it worked
                        # After uvotunicorr runs, we will open the new(hopefully corrected) FITS file
                        # and check the ASPCORR header keyword:
                        #   'DIRECT'  = shouldn't Happen, That wouldnt be correct
                        #   'UNICORR' = We did it!
                        #   'NONE'    = Rip
                        #
                        # CHANGE FROM clean_uvot_tiles.py:
                        #   clean_uvot_tiles checked ASPCORR in a separate pass
                        #   After all corrections were done:
                        #       new_aspect_uncorrected_frames = up.check_aspect_correction(filepath)
                        #   We check immediately after each correction, which
                        #   gives me a per-frame success/failure reading, good for debuging.
                        time.sleep(2) # Gotta make sure it gets its naps in.

                        # Find the corrected output file
                        corrected_base = f"sw{obs_obsid}{band}_sk"
                        corrected_files = [
                            f for f in os.listdir(obs_dir)
                            if f.startswith(corrected_base) and not f.endswith('.gz')
                        ]

                        if not corrected_files:
                            print(f" ❌ No corrected file found after uvotunicorr")
                            corrections_failed += 1
                            continue

                        # Prefer the shortest filename (usually the main output, I think.)
                        corrected_files.sort(key=lambda x: (len(x), x))
                        corrected_file = corrected_files[0]
                        corrected_path = os.path.join(obs_dir, corrected_file)

                        # Read the ASPCORR keyword from the corrected FITS header
                        try:
                            with fits.open(corrected_path) as hdul:
                                if obs_snapshot < len(hdul):
                                    aspcorr_after = hdul[obs_snapshot].header.get('ASPCORR', 'NONE')
                                    print(f" ASPCORR value after correction: {aspcorr_after}")

                                    if aspcorr_after.strip().upper() in ['DIRECT', 'UNICORR']:
                                        print(f"✅ Correction successful - ASPCORR = {aspcorr_after}")
                                        corrections_successful += 1
                                    else:
                                        print(f" Ruh-Roh: ❌ Correction failed - ASPCORR still {aspcorr_after}")
                                        corrections_failed += 1
                                else:
                                    print(f" Ruh-Roh:❌ Extension {obs_snapshot} not found in corrected file")
                                    corrections_failed += 1
                        except Exception as e:
                            print(f" Ruh-Roh: ❌ Error checking corrected file: {e}")
                            corrections_failed += 1

                    except Exception as e:
                        print(f" Ruh-Roh: ❌ Error during correction: {e}")
                        import traceback
                        traceback.print_exc()
                        corrections_failed += 1   

                # Band summery, mostly for debuging attempts, could be removed.
                print(f"\n  Band {band} Summary:")
                print(f" Attempted: {corrections_attempted}")
                print(f" Successful: {corrections_successful}")
                print(f" Failed: {corrections_failed}")

                # Record any remaining failures for this group+band combination
                remaining = corrections_failed
                if remaining > 0:
                    key = f"{group_id}_{band}"
                    aspectnone_dict[key] = remaining
                    # Build list of failed frame identifiers for retry/manual inspection
                    failed_obsids = []
                    for idx, obs_row in corrections_needed.iterrows():
                        failed_obsids.append(f"{obs_row['ObsID']}_ext{obs_row['Snapshot']}")
                    aspectnone_tiles_dict[key] = failed_obsids[:remaining]

                    
        ######################################################################################
        # Attempt to retry 

        total_remaining = sum(aspectnone_dict.values())
        print("\n" + "=" * 70)
        if attempt_num == 0:
            print("INITIAL ATTEMPT COMPLETE")
        else:
            print(f"RETRY ATTEMPT {attempt_num} COMPLETE")
        print("=" * 70)
        print(f"Frames still needing correction: {total_remaining}")

        # If everything worked, we're done
        if total_remaining == 0:
            print("\n✅ All frames successfully corrected!")
            break

        
        # Because that wont happen, retry prompt.
        # CHANGE FROM clean_uvot_tiles.py:
        #   clean_uvot_tiles had a nearly identical prompt:
        #       go_again = input('Do you wish to change the global parameters...? [Y/N]')
        #   The key difference is that on retry, clean_uvot_tiles re-ran
        #   EVERYTHING (download, detect, smear, unzip, correct). We only
        #   re-run the correction step on the specific failed frames.
        
        print(f"\n⚠️ {total_remaining} frames failed.")
        print("\nFailed frames by group:")
        for key, count in aspectnone_dict.items():
            print(f" {key}: {count} frames")

        retry = input("\nDo you want to retry failed frames with different parameters? (yes/no): ").strip().lower()

        if retry not in ['yes', 'y']:
            print("Stopping correction process.")
            break

        # Get new parameters from the user
        try:
            new_side_buffer = input(f"Enter new side_buffer value (current: {side_buffer}, press Enter to keep): ").strip()
            if new_side_buffer:
                side_buffer = int(new_side_buffer) # Again I really need to go all the way back if this changes. I should proably get rid of this.

            new_num_stars = input(f"Enter new num_stars value (current: {num_stars}, press Enter to keep): ").strip()
            if new_num_stars:
                num_stars = int(new_num_stars)

            print(f"\nRetrying with side_buffer={side_buffer}, num_stars={num_stars}")

        except ValueError:
            print("Invalid input - keeping current parameters")

        # Store failed frames for the next attempt and increment
        failed_frames_to_retry = aspectnone_tiles_dict.copy()
        attempt_num += 1

    #######################################################################################
    # The End
    print("\n" + "=" * 70)
    print("ASPECT CORRECTION FINAL SUMMARY")
    print("=" * 70)
    final_remaining = sum(aspectnone_dict.values()) if aspectnone_dict else 0
    print(f"Total frames still needing correction: {final_remaining}")

    if final_remaining > 0:
        print("\nFailed frames by group:")
        for key, count in aspectnone_dict.items():
            print(f"  {key}: {count} frames")

    return aspectnone_dict, aspectnone_tiles_dict
