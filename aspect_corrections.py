#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun  5 14:39:49 2025

@author: tmg6006
"""

import os

import shutil

from tqdm import tqdm

from astropy.table import QTable, Table
from astropy.io import fits

import re

import argparse

import uvot_pipeline as up

# Set all required environment variables
os.environ['HEADAS'] = '/Users/tmg6006/heasoft-6.33.2/aarch64-apple-darwin23.5.0'
os.environ['PFILES'] = f"/tmp/pfiles;{os.environ['HEADAS']}/syspfiles"
os.environ['PLT_DEV'] = '/null'  # Avoid display device errors
os.environ['HEADASNOQUERY'] = 'YES'  # Prevent prompt errors
os.environ['CALDB'] = '/Users/tmg6006/caldb'  # Remote CALDB
os.environ['CALDBCONFIG'] = '/Users/tmg6006/caldb/software/tools/caldb.config'
os.environ['CALDBALIAS'] = '/Users/tmg6006/caldb/software/tools/alias_config.fits'

# Ensure pfiles directory exists
os.makedirs("/tmp/pfiles", exist_ok=True)

#ignore UnitsWarnings
warnings.filterwarnings("ignore", category=UnitsWarning)

# Set up arguments that need to be passed to the scripts
parser = argparse.ArgumentParser(description='Options for Clean Tiles Script.')

parser.add_argument('source_name', help="The name of the source. This will be used to name the output photometry file.")
parser.add_argument('folder', help="The folder containing the Observation ID folders that need to be aspect corrected.")
parser.add_argument('-v', '--verbose', action='store_true', help='Prints command outputs instead of surpessing them.')

args = parser.parse_args()

source_name = args.source_name
folder = args.folder

print('Welcome to the Automated Swift UVOT Aspect Correction Pipeline.\n')

#grab the list of all UVOT observations in the directory
all_filepaths = sorted(os.listdir(f"./{source_name}/{folder}"))
all_filepaths.remove('.DS_Store')

print(all_filepaths)

#all possible filters
possible_bands = ['uvv', 'ubb', 'uuu', 'uw1', 'um2', 'uw2']
aspect_uncorrected_full = {}

frames_to_correct = True

#loop the commands until the code is successful
while frames_to_correct == True:

    #change the parameters of the aspect correction process
    sb_needed = True
    ns_needed = True
        
    while sb_needed == True:
        
        side_buffer = input("\nPlease select the distance from the side of the frame that you wish to include: [7]")
        
        if side_buffer == "":
            side_buffer = 7
            sb_needed = False
        else:
            try:
                int(side_buffer)
                side_buffer = int(side_buffer)
                sb_needed = False
            except:
                print("Please pick a valid integer.")
                
    while ns_needed == True:
        
        num_stars = input("Please choose how many stars you wish to select for use in aspect correction: [50]")
        
        if num_stars == "":
            num_stars = 50
            ns_needed = False
        else:
            try:
                int(num_stars)
                num_stars=int(num_stars)
                ns_needed = False
            except:
                print("Please pick a valid integer.")

    for obsid in all_filepaths:

        detected_bands = {}
        aspect_uncorrected = {}

        print(f"Checking {obsid} for all available filters and snapshots.")
        for band in tqdm(possible_bands):
            
            path_to_img = f'./{source_name}/{folder}/{obsid}/uvot/image/sw{obsid}{band}_sk.img.gz'
            
            if os.path.exists(path_to_img) == True:
                # print(f'{band} exists for {obsid}')

                hdul = fits.open(f'./{source_name}/{folder}/{obsid}/uvot/image/sw{obsid}{band}_sk.img.gz')
                num_snapshots = len(hdul) - 1 

                detected_bands[band] = num_snapshots

        print(f"Running uvotdetect on all filters and frames in Frame {obsid}.")
        for band in tqdm(detected_bands.keys()):

            for snapshot in range(1, detected_bands[band]+1):
                
                full_sourcepath=f'./{source_name}/{folder}/{obsid}/uvot/image/sw{obsid}{band}_sk.img.gz[{snapshot}]'
                full_outpath=f'./{source_name}/{folder}/{obsid}/uvot/image/{band}_detect-{snapshot}.fits'
                full_exppath=f'./{source_name}/{folder}/{obsid}/uvot/image/sw{obsid}{band}_ex.img.gz[{snapshot}]'
                full_detectpath=f'./{source_name}/{folder}/{obsid}/uvot/image/{band}_detect-{snapshot}.reg'

                uvotdetect_command = up.create_uvotdetect_bash_command(full_sourcepath, full_outpath, full_exppath, full_detectpath)

                if args.verbose:
                    up.run_uvotdetect_verbose(uvotdetect_command)
                else:
                    up.run_uvotdetect(uvotdetect_command)

        print(f"Checking all filters and frames in Frame {obsid} for aspect correction.")
        for band in tqdm(detected_bands.keys()):
            aspect_uncorrected[band] = []
            for snapshot in range(1, detected_bands[band]+1):

                full_sourcepath=f'./{source_name}/{folder}/{obsid}/uvot/image/sw{obsid}{band}_sk.img.gz[{snapshot}]'
                fkeyprint_command = up.create_fkeyprint_bash_command(full_sourcepath)

                if args.verbose:
                    aspcorr_output = up.run_fkeyprint_verbose(fkeyprint_command)
                else:
                    aspcorr_output = up.run_fkeyprint(fkeyprint_command)

                if re.search("ASPCORR = 'DIRECT  '", aspcorr_output):
                    continue
                elif re.search("ASPCORR = 'UNICORR '", aspcorr_output):
                    continue
                else:
                    detect_path = f'./{source_name}/{folder}/{obsid}/uvot/image/{band}_detect-{snapshot}.fits'
                    len_detect = len(QTable.read(detect_path).to_pandas().index)
                    if len_detect < 5:
                        continue
                    else:
                        aspect_uncorrected[band].append(snapshot)

        aspect_uncorrected_full[obsid] = aspect_uncorrected

    num_uncorrected = len(aspect_uncorrected_full.keys())
    
    print(f"Found {num_uncorrected} Frames in need of Aspect Correction for at least one filter or frame.\n")
    
    print("Correcting Bad Frames.")
    

    #list the corrected frames
    # corrected_frames = {}

    #reference frame will be first corrected frame
    # ref_frame = corrected_frames[0]
    #generate path to the reference frame detect.fits
    # ref_detect_path = f'{directory}/{ref_frame}/uvot/image/detect.fits'
    #generate path to reference image
    # ref_file_path = f'{directory}/{ref_frame}/uvot/image/sw{ref_frame}uw1_sk.img.gz'
    #find brightest stars in the center of each reference frame
    v_ref_bright_stars = up.find_brightest_central_stars(v_detect_file, num_stars=num_stars, side_buffer=side_buffer)
    b_ref_bright_stars = up.find_brightest_central_stars(b_detect_file, num_stars=num_stars, side_buffer=side_buffer)
    u_ref_bright_stars = up.find_brightest_central_stars(u_detect_file, num_stars=num_stars, side_buffer=side_buffer)
    uvw1_ref_bright_stars = up.find_brightest_central_stars(uvw1_detect_file, num_stars=num_stars, side_buffer=side_buffer)
    uvm2_ref_bright_stars = up.find_brightest_central_stars(uvm2_detect_file, num_stars=num_stars, side_buffer=side_buffer)
    uvw2_ref_bright_stars = up.find_brightest_central_stars(uvw2_detect_file, num_stars=num_stars, side_buffer=side_buffer)

    ref_bright_stars = {'uvv': v_ref_bright_stars, 
                        'ubb': b_ref_bright_stars, 
                        'uuu': u_ref_bright_stars,
                        'uw1': uvw1_ref_bright_stars, 
                        'um2': uvm2_ref_bright_stars, 
                        'uw2': uvw2_ref_bright_stars}
    
    for obs_frame in aspect_uncorrected_full.keys():
        
        print(f"Correcting ObsID {obs_frame}.")
        needs_correcting = aspect_uncorrected_full[obs_frame]

        for band in needs_correcting.keys():
            for snapshot in needs_correcting[band]:
                #generate general directory path to obs frame folder
                obs_directory = f'./{source_name}/{folder}/{obs_frame}/uvot/image'
                #generate path to detect.fits for observation frame
                obs_detect_path = f'./{source_name}/{folder}/{obs_frame}/uvot/image/{band}_detect-{snapshot}.fits'
                #find brightest stars in the center of the observation frame
                obs_bright_stars = up.find_brightest_central_stars(obs_detect_path, num_stars=num_stars, side_buffer=side_buffer)
                
                #remove stars that do not match between frames
                ref_bright_stars[band], obs_bright_stars = up.remove_separate_stars(ref_bright_stars[band], obs_bright_stars)
                
                #create ds9 .reg files for reference and observation images
                up.create_ref_obs_reg_files(ref_bright_stars[band], obs_bright_stars, outpath=obs_directory)
                
                #copy reference image to uncorrected observation folder
                shutil.copy(re.sub(f'\[[0-9]\]', '', ref_files[band]), obs_directory)
                
                #create the command to run uvotunicorr
                unicorr_command = up.create_uvotunicorr_too_bash_command(ref_frames[band], obs_frame, band, snapshot, obspath=obs_directory)
                
                #run uvotunicorr
                if args.verbose:
                    up.run_uvotunicorr_verbose(unicorr_command)
                else:
                    up.run_uvotunicorr(unicorr_command)
        
    print("Corrections Complete. Checking how many were successful.\n")
    new_aspect_uncorrected_full = {}

    for obsid in tqdm(all_filepaths):

        detected_bands = {}
        aspect_uncorrected = {}

        for band in possible_bands:
            
            path_to_img = f'./{source_name}/{folder}/{obsid}/uvot/image/sw{obsid}{band}_sk.img.gz'
            
            if os.path.exists(path_to_img) == True:

                hdul = fits.open(f'./{source_name}/{folder}/{obsid}/uvot/image/sw{obsid}{band}_sk.img.gz')
                num_snapshots = len(hdul) - 1 

                detected_bands[band] = num_snapshots

        for band in detected_bands.keys():
            aspect_uncorrected[band] = []
            for snapshot in range(1, detected_bands[band]+1):

                full_sourcepath=f'./{source_name}/{folder}/{obsid}/uvot/image/sw{obsid}{band}_sk.img.gz[{snapshot}]'
                fkeyprint_command = up.create_fkeyprint_bash_command(full_sourcepath)
                
                if args.verbose:
                    aspcorr_output = up.run_fkeyprint_verbose(fkeyprint_command)
                else:
                    aspcorr_output = up.run_fkeyprint(fkeyprint_command)

                if re.search("ASPCORR = 'DIRECT  '", aspcorr_output):
                    continue
                elif re.search("ASPCORR = 'UNICORR '", aspcorr_output):
                    continue
                else:
                    detect_path = f'./{source_name}/{folder}/{obsid}/uvot/image/{band}_detect-{snapshot}.fits'
                    len_detect = len(QTable.read(detect_path).to_pandas().index)
                    if len_detect < 5:
                        continue
                    aspect_uncorrected[band].append(snapshot)

        new_aspect_uncorrected_full[obsid] = aspect_uncorrected
    
    new_num_uncorrected = len(new_aspect_uncorrected_full.keys())
    num_successful = num_uncorrected - new_num_uncorrected
    
    print(f"{num_successful} Frames were successfully corrected.\n")
    
    if new_num_uncorrected == 0:
        print("Aspect Corrections Complete. ")
        frames_to_correct = False
    else:
        print(f"{new_num_uncorrected} still need to be corrected.")
        
        kg = True
        
        while kg == True:
            keep_going = input("Would you like to adjust the parameters and try again? [Y]")
            
            if keep_going == "":
                kg = False
                continue
            elif keep_going.upper() == 'Y':
                kg = False
                continue
            elif keep_going.upper() == 'N':
                kg=False
                frames_to_correct = False
            else:
                print("Please select Y or N.")
        
    
