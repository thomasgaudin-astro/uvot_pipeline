# -*- coding: utf-8 -*-
import os
import subprocess

import pandas as pd
import numpy as np

import math

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
    print(f"\n[SYSTEM]: Running HEASOFT command...")
    
    if HEASOFT_BACKEND == "wsl":
        # --- This is YOUR working logic ---
        full_cmd = f"conda activate henv && {command}"
        result = subprocess.run(
            ["wsl", "bash", "-ic", full_cmd],
            text=True,
            capture_output=True,
        )
        
    elif HEASOFT_BACKEND == "native":
        # --- This is the MAC lane ---
        full_cmd = f"source $HEADAS/headas-init.sh && {command}"
        result = subprocess.run(
            ["bash", "-lc", full_cmd],
            text=True,
            capture_output=True,
        )
    else:
        raise ValueError(f"Unknown HEASOFT_BACKEND: {HEASOFT_BACKEND}")

    if result.returncode != 0:
        print("  [RESULT]: FAILED")
        print("--- Error Details ---")   # Exists just to quick test if the code is working
        print(result.stderr)
    else:
        print("  [RESULT]: SUCCESS")

    return result

# --- UTILS FOR CROSS-PLATFORM PATHS ---

def prepare_path(path):
    """
    - On Windows/WSL: Translates C:\ to /mnt/c/
    - On Mac/Linux: Returns the path exactly as it is.
    """
    if HEASOFT_BACKEND == "native":
        return path  # Do nothing for Mac users
    
    # WSL logic only executes if backend is 'wsl'
    abs_path = os.path.abspath(path)
    drive, rest = os.path.splitdrive(abs_path)
    if drive:
        drive_letter = drive[0].lower()
        return f"/mnt/{drive_letter}{rest.replace('\\', '/')}"
    return abs_path.replace('\\', '/')

def find_obs_file(base_path, obsid, band, file_type='sk'):
    """
    Finds the actual path of a file even if the folder has extra date tags.
    """
    target_filename = f"sw{obsid}{band}_{file_type}.img.gz"
    for root, dirs, files in os.walk(base_path):
        # Check if we are in the correct subfolder structure
        if obsid in root and root.endswith(os.path.join("uvot", "image")):
            if target_filename in files:
                return os.path.join(root, target_filename)
    return None

def create_uvotdetect_command(source_path, output_path, exposure_path): #This wasnt working so I left it to just make it all in one function, could delete this.
    """
    Creates heredoc-style command for interactive uvotdetect.
    """
    src = prepare_path(source_path)
    out = prepare_path(output_path)
    exp = prepare_path(exposure_path)
    
    # Get directory to cd into
    src_dir = os.path.dirname(src)
    src_file = os.path.basename(src)
    out_file = os.path.basename(out)
    exp_value = "NONE" if exp == "NONE" else os.path.basename(exp)
    
    return (
        f"cd '{src_dir}' && "
        f"uvotdetect << end\n"
        f"{src_file}\n"      # Input file
        f"{out_file}\n"      # Output file
        f"{exp_value}\n"     # Exposure map
        f"3\n"               # Threshold
        f"end"
    )

    return bash_command
###################################################################Updated above
def run_uvotdetect(uvotdetect_command):

    # Run the command
    result = run_heasoft_command(uvotdetect_command)

    # print("STDOUT:\n", result.stdout)
    # print("STDERR:\n", result.stderr)

    return result.stdout

def run_uvotdetect_verbose(uvotdetect_command):

    # Run the command
    result = run_heasoft_command(uvotdetect_command)

    print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)

    return result.stdout
#Because I couldnt get the above to work properly I am just hard codding in a way to do this forgive me
def batch_run_uvotdetect(base_path):
    """
    Walks through base_path and runs uvotdetect on all SK files.
    """
    
    BANDS = ["uvv", "uuu", "ubb", "um2", "uw1", "uw2"]
    
    def get_extension_count(filepath):
        """Get number of extensions using astropy."""
        try:
            with fits.open(filepath) as hdul:
                return len(hdul) - 1
        except Exception as e:
            print(f"  Error reading FITS: {e}")
            return 0
    
    # Walk directory tree
    for root, dirs, files in os.walk(base_path):
        if os.path.basename(root) != "image":
            continue
        
        # Convert path for HEASOFT command 
        img_dir_heasoft = prepare_path(root)
        
        print(f"\n Processing image directory:")
        print(f"   {root}")
        
        # Find all OBSID patterns in this directory
        obsid_pattern = re.compile(r"sw(\d{11})([a-z0-9]+)_sk\.img\.gz") #Learned this method from a youtube video,a way to search the entire library for a spesific patern, really neat.
        
        for file in files:
            match = obsid_pattern.match(file)
            if not match:
                continue
            
            OBSID, band = match.groups()
            
            if band not in BANDS:
                continue
            
            print(f"\n Found SK image: {file}")
            
            # Define output files
            detect_base = f"{band}_detect.fits"
            detect_path = os.path.join(root, detect_base)
            
            if os.path.exists(detect_path):
                print("   Detect file already exists — skipping")
                continue
            
            # Use find_obs_file to get the full path
            sk_file_path = find_obs_file(base_path, OBSID, band, file_type='sk')
            
            if not sk_file_path:
                print(f"   Could not find SK file for OBSID={OBSID}, band={band}")
                continue
            
            # Get extension count
            ext_count = get_extension_count(sk_file_path)
            print(f"   {ext_count} image extensions found")
            
            # Prepare paths for HEASOFT
            sk_file_heasoft = prepare_path(sk_file_path)
            detect_base_heasoft = prepare_path(detect_path)
            
            # Get just the filenames for heredoc (cd handles the directory)
            sk_filename = os.path.basename(sk_file_path)
            detect_filename = os.path.basename(detect_path)
            
            # Single-extension case
            if ext_count <= 1:
                print("  Running single-extension detect...")
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
            
            # Multi-extension case
            else:
                for ext in range(1, ext_count + 1):
                    detect_ext = f"{band}_detect_ext{ext}.fits"
                    detect_ext_path = os.path.join(root, detect_ext)
                    
                    if os.path.exists(detect_ext_path):
                        print(f"   Extension {ext} detect exists — skipping")
                        continue
                    
                    print(f"   Running detect on extension {ext}")
                    
                    detect_ext_filename = os.path.basename(detect_ext_path)
                    
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
    
    print("\n UVOT Detect processing complete!")
    
    
def create_fkeyprint_bash_command(source_path):

    fits_path = prepare_path(source_path)
    
    # print("Absolute path:", fits_path)
    # print("Exists:", os.path.exists(fits_path))  # Confirm it actually exists!
    
    keyword = "ASPCORR"
    
    return f'fkeyprint infile="{fits_path}" keyname={keyword}'

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
def _scan_header_for_aspcorr(file_path):
    """
    Scans for mixed DIRECT and NONE statuses.
    If both exist, it is a 'READYRESUM' (needs fix + re-summing).
    """
    if not file_path: return "NONE"
    try:
        with fits.open(file_path) as hdul:
            # We use a set to get only unique statuses across all sheets (extensions)
            statuses = set()
            for hdu in hdul:
                val = hdu.header.get('ASPCORR', 'NONE')
                statuses.add(str(val).strip().upper())
            
            # --- THE READYRESUM LOGIC ---
            # If it has some good and some bad, it's Ready to be fixed then Re-Summed.
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

#  ----Just a bit more cleanup---
    df = pd.DataFrame(raw_results)
    if df.empty: 
        print("DEBUG: DataFrame is empty - no data found")
        return None, None, None
        
    df = df.drop_duplicates(subset=['OBSID', 'Band'], keep='first') # This is just to 100% make sure that the same file couldnt have been scanned twice by  chekcing for duplicants, the code kept thinking there was more extensions then there was somewhere and now im paranoid.

    # --- SPATIAL GROUPING ---
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
# -------------------- ORPHAN HUNTING EXPANSION -----------------------------
def solve_orphan_frames_by_group(base_path=None, save_dir=None, return_data=False, input_df=None, input_summary=None):
    """
    1. Loads data using the existing swift_automation_mode.
    2. Identifies 'ORPHAN' frames (groups with no valid aspect correction).
    3. Identifies 'REFERENCE' frames (DIRECT or READYRESUM).
    4. For each Orphan, finds the nearest Reference in 4 directions (N, S, E, W).
    5. Saves a CSV for each Orphan with those 4 neighbors.
    """
    
    # Data Input Using existing Funtion from IAC to get the DataFrames directly
    # Note: If you want to skip the input prompts(for automation), Input a base_path and save_dir.
    if input_df is not None and input_summary is not None:
        all_frames, summary_df = input_df, input_summary
        active_save_dir = save_dir if save_dir else os.getcwd()
    else:
        # Calls the core engine from IAC code
        all_frames, summary_df, active_save_dir = _run_core_engine(base_path, save_dir)

    if all_frames is None or summary_df is None:
        print(" No data returned from core engine. Cannot solve orphan frames.")
        return None

    # DEBUG: Print what we got, just to check
    print(f"\n Data loaded:")
    print(f"   Total frames: {len(all_frames)}")
    print(f"   Summary groups: {len(summary_df)}")
    print(f"   Status breakdown:\n{summary_df['Status'].value_counts()}")

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
                    print(f"  ✓ Created: {os.path.basename(save_file)}")
                
                count += 1

    print(f"\n Directives generated for {count} individual orphan frames.")
    
    if not return_data and count > 0:
        print(f" All CSVs saved to: {orphan_save_path}")
    
    return automation_results if return_data else None


def clean_up_data(automation_mode=False, base_path=None, save_path=None):
    """  
        automation_mode (bool): If True, skips GUI and print statements, returns data
        base_path (str): Required if automation_mode=True
        save_path (str): Required if automation_mode=True
        
        If automation_mode=True, then it will return:
            - 'all_frames': DataFrame from IAC
            - 'summary': Summary DataFrame from IAC  
            - 'orphan_solutions': Dict of orphan solutions
            - 'smeared_list': List of smeared observation folders
    """
    
    if automation_mode:
        # Automation mode - no prints (except errors)
        if not base_path or not save_path:
            raise ValueError("automation_mode requires base_path and save_path")
    else:
        # Interactive mode
        root = tk.Tk()
        root.withdraw()
        
        print("Please select the data directory using the popup window...")
        base_path = filedialog.askdirectory(title="Select the Base Directory for Data Cleanup")
        
        if not base_path:
            print("No directory selected. Aborting.")
            return
        
        print(f"Selected Data Directory: {base_path}")
        
        print("\nPlease select where you want to save results...")
        save_path = filedialog.askdirectory(title="Select Save Directory for Results")
        
        if not save_path:
            print("No save directory selected. Using data directory as save location.")
            save_path = base_path
        
        print(f"Selected Save Directory: {save_path}")
    
    # Initialize results dictionary for automation mode
    results = {
        'all_frames': None,
        'summary': None,
        'orphan_solutions': None,
        'smeared_list': None
    }
    
    # 1. RUN UVOT DETECT
    if not automation_mode:
        print("\n=== Running UVOT Detect ===")
    batch_run_uvotdetect(base_path)
    
    # 2. DETECT SMEARED FRAMES
    if not automation_mode:
        print("\n=== Detecting Smeared Frames ===")
    smeared_list = detect_smeared_frames(base_path)
    results['smeared_list'] = smeared_list
    
    # 3. RUN IAC
    if not automation_mode:
        print("\n=== Running IAC Swift Automation ===")
    all_frames, summary = swift_automation_mode(base_path=base_path, save_path=save_path)
    results['all_frames'] = all_frames
    results['summary'] = summary
    
    if all_frames is None or summary is None:
        if not automation_mode:
            print(" IAC automation failed to generate data.")
    else:
        # 4. SOLVE ORPHAN FRAMES
        if not automation_mode:
            print("\n=== Solving Orphan Frames ===")
        
        # In automation mode, return the data instead of saving CSVs
        orphan_solutions = solve_orphan_frames_by_group(
            base_path=base_path, 
            save_dir=save_path, 
            return_data=automation_mode,  # Return data in automation mode
            input_df=all_frames,
            input_summary=summary
        )
        results['orphan_solutions'] = orphan_solutions
    
    # 5. REMOVE SMEARED FRAMES
    if smeared_list:
        if not automation_mode:
            print("\n=== Removing Smeared Frames ===")
        remove_smeared(base_path, smeared_list)
    else:
        if not automation_mode:
            print("\n=== No Smeared Frames to Remove ===")
    
    if not automation_mode:
        print("\n=== Clean Up Data Process Complete ===")
        print(f"Results saved to: {save_path}")
        return None
    else:
        # Return all data for downstream processing
        return results
"""
What you get in automation mode:
results['all_frames'] - DataFrame with columns:
OBSID, Band, RA, Dec, Full_Path, Filename, ASPCORR, Group_ID

results['summary'] - DataFrame with columns:
Group_ID, Band, Status, Total_Frames

results['orphan_solutions'] - Dict like:
{
    "00010056003_um2": DataFrame([
        {'OBSID': '...', 'RA': ..., 'Dec': ..., 'Full_Path': '...', 'Band': '...', 'ASPCORR': '...'},
        # ... 4 reference frames (N, S, E, W)
    ]),
    # ... more orphans
}
"""
