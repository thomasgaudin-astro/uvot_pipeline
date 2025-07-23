#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun  2 14:29:31 2025

@author: tmg6006
"""

import os
import pandas as pd
import shutil
import uvot_pipeline as up
import argparse
from sh import gunzip

parser = argparse.ArgumentParser(description='Options for Clean Tiles Script.')

parser.add_argument('tile_name', help="The name of the tile. This will be used to identify which files to clean.")
parser.add_argument('-nd', '--no_detect', action='store_true', help='Skips uvotdetect command for each tile.')
parser.add_argument('-rb', '--remove_bad', action='store_true', help='Removes bad aspect correction tiles instead of correcting them.')
parser.add_argument('-v', '--verbose', action='store_true', help='Prints command outputs instead of surpessing them.')

args = parser.parse_args()

# Set all required environment variables
os.environ['HEADAS'] = '/bulk/pkg/heasoft-6.35.1/aarch64-apple-darwin23.6.0'
os.environ['PFILES'] = f"/tmp/pfiles;{os.environ['HEADAS']}/syspfiles"
os.environ['PLT_DEV'] = '/null'  # Avoid display device errors
os.environ['HEADASNOQUERY'] = 'YES'  # Prevent prompt errors
os.environ['CALDB'] = '/bulk/pkg/caldb'  # Local CALDB
os.environ['CALDBCONFIG'] = '/bulk/pkg/caldb/software/tools/caldb.config'
os.environ['CALDBALIAS'] = '/bulk/pkg/caldb/software/tools/alias_config.fits'

# Ensure pfiles directory exists
os.makedirs("/tmp/pfiles", exist_ok=True)

#run the pipeline
print(f'Starting the S-CUBED UVOT Cleaning Pipeline for the tile {args.tile_name}.\n')

#read in list of tiles
tiles = pd.read_csv('scubed_tiles.csv')

#make sure each tile name matches the folder names
for val in range(len(tiles.index)):
    old_tile_name = tiles.loc[val, 'Tile Name'].strip('\xa0')
    tiles.loc[val, 'New Tile Name'] = old_tile_name.replace("_", " ")

tile_index = tiles.index[tiles['Tile Name'] == args.tile_name].tolist()[0]
tile_ra = tiles.loc[tile_index, 'RA']
tile_dec = tiles.loc[tile_index, 'DEC']
new_tile_name = tiles.loc[tile_index, 'New Tile Name']

run_pipeline = True

while run_pipeline == True:
    if args.remove_bad:
        print('Frames with no aspect correction will be removed.')
    
    else:
        print('Setting Global Parameters for Aspect Correction:\n')
                
        #change the parameters of the aspect correction process
        sb_needed = True
        ns_needed = True
            
        while sb_needed == True:
            
            side_buffer = input("Please select the distance from the center of the frame that you wish to include: [7]")
            
            if side_buffer == "":
                side_buffer = 7
                sb_needed = False
            else:
                try:
                    int(side_buffer)
                    sb_needed = False
                    side_buffer = int(side_buffer)
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
                    ns_needed = False
                    num_stars = int(num_stars)
                except:
                    print("Please pick a valid integer.")
            
    aspectnone_dict = {}
    aspectnone_tiles_dict = {}
            
    #run full cleaning pipeline for each S-CUBED tile.
    # for sc_tile in tiles['Tile Name']:
    for sc_tile in [args.tile_name]:

        print(f'Downloading new data for Tile {sc_tile}.')

        undownloaded_files = up.check_for_undownloaded_files(sc_tile, new_tile_name, tile_ra, tile_dec)

        print(f'Found {len(undownloaded_files)} that need to be downloaded.')
        if len(undownloaded_files) > 0:
            print('Downloading new files.')
            up.download_new_files(undownloaded_files, sc_tile, tile_ra, tile_dec)
            print('All new files downloaded.\n')
        else:
            print('No new files to download. Moving on.\n')

    
        print(f"Cleaning Data for Tile {sc_tile}.")
        
        filepath = f'./S-CUBED/{sc_tile}/UVOT'
        
        all_filepaths = sorted(os.listdir(filepath))
        if '.DS_Store' in all_filepaths:
            all_filepaths.remove('.DS_Store')
        
        if args.no_detect:
            print('uvotdetect was skipped.')
        else:
    
            print("Running uvotdetect.")
            
            for path in all_filepaths:
                subpath = os.path.join(filepath, path)
                
                sourcepath_fill = f'uvot/image/sw{path}uw1_sk.img.gz'
                outpath_fill = 'uvot/image/detect.fits'
                exppath_fill = f'uvot/image/sw{path}uw1_ex.img.gz'
                detectpath_fill = 'uvot/image/detect.reg'
                
                full_sourcepath = os.path.join(subpath, sourcepath_fill)
                full_outpath = os.path.join(subpath, outpath_fill)
                full_exppath = os.path.join(subpath, exppath_fill)
                full_detectpath = os.path.join(subpath, detectpath_fill)

                uvotdetect_command = up.create_uvotdetect_bash_command(full_sourcepath, full_outpath, full_exppath, full_detectpath)

                if args.verbose:
                    up.run_uvotdetect_verbose(uvotdetect_command)
                else:
                    up.run_uvotdetect(uvotdetect_command)
        
            print("uvotdetect is complete.\n")
    
        print("Detecting Smeared Frames.")
    
        smeared_frames = up.detect_smeared_frames(sc_tile)
    
        print(f"Found {len(smeared_frames)} Smeared Frames.")
        print("Removing Smeared Frames.")
    
        up.remove_smeared(sc_tile, smeared_frames)
    
        print("Smear Removal is complete.\n")

        print("Unzipping all image files.")

        for path in all_filepaths:
            subpath = os.path.join(filepath, path)
            img_path_fill = f'uvot/image/sw{path}uw1_sk.img'

            img_path = os.path.join(subpath, img_path_fill)
            #if .img frame does not exist, unzip file and keep original.
            if os.path.exists(img_path) == False:
                #unzip reference image
                os.system(f'gunzip -k {img_path}.gz')

        print("All image files unzipped.\n")
    
        print("Checking Frame Aspect Correction.")
        print("Identifying Frames with No Aspect Correction.")

        if args.verbose:
            aspect_uncorrected_frames = up.check_aspect_correction_verbose(filepath)
        else:
            aspect_uncorrected_frames = up.check_aspect_correction(filepath)
        num_uncorrected = len(aspect_uncorrected_frames)
        
        new_all_filepaths = sorted(os.listdir(filepath))
        if '.DS_Store' in new_all_filepaths:
            new_all_filepaths.remove('.DS_Store')
    
        print(f"Found {num_uncorrected} Frames in need of Aspect Correction.\n")
        
        if num_uncorrected == 0:
            continue
        
        else:
            if args.remove_bad:
                print("Removing Bad Frames.")
                
                out_filepath = filepath+'/AspectNone'
                up.remove_aspect_uncorrected(filepath, out_filepath, aspect_uncorrected_frames)
                
                
            else:
                print("Correcting Bad Frames.")
                
                #list the corrected frames
                corrected_frames = [frame for frame in new_all_filepaths if frame not in aspect_uncorrected_frames]
                
                #reference frame will be first corrected frame that exists
                ref_frame = corrected_frames[0]
                
                subpath = os.path.join(ref_frame, filepath)
                sourcepath_fill = 'uvot/image/detect.fits'
                full_sourcepath = os.path.join(subpath, sourcepath_fill)
                
                if os.path.exists(full_sourcepath) == False:
                    ref_frame = corrected_frames[1]
                    
                    subpath = os.path.join(ref_frame, filepath)
                    sourcepath_fill = 'uvot/image/detect.fits'
                    full_sourcepath = os.path.join(subpath, sourcepath_fill)
                    
                    if os.path.exists(full_sourcepath) == False:
                        
                        ref_frame = corrected_frames[2]
                        
                        subpath = os.path.join(ref_frame, filepath)
                        sourcepath_fill = 'uvot/image/detect.fits'
                        full_sourcepath = os.path.join(subpath, sourcepath_fill)
                
                print(f'The Reference Frame is {ref_frame}')
                #generate path to the reference frame detect.fits
                ref_detect_path = f'{filepath}/{ref_frame}/uvot/image/detect.fits'
                #generate path to reference image
                ref_file_path = f'{filepath}/{ref_frame}/uvot/image/sw{ref_frame}uw1_sk.img'
                #find brightest stars in the center of the reference frame
                ref_bright_stars = up.find_brightest_central_stars(ref_detect_path, num_stars=num_stars, side_buffer=side_buffer)
                
                for obs_frame in aspect_uncorrected_frames:
                    
                    print(f"Correcting ObsID {obs_frame}.")
                    
                    #generate general directory path to obs frame folder
                    obs_directory = f'{filepath}/{obs_frame}/uvot/image'
                    #generate path to detect.fits for observation frame
                    obs_detect_path = f'{filepath}/{obs_frame}/uvot/image/detect.fits'
                    #find brightest stars in the center of the observation frame
                    obs_bright_stars = up.find_brightest_central_stars(obs_detect_path, num_stars=num_stars, side_buffer=side_buffer)
                    
                    #remove stars that do not match between frames
                    ref_bright_stars, obs_bright_stars = up.remove_separate_stars(ref_bright_stars, obs_bright_stars)
                    
                    #create ds9 .reg files for reference and observation images
                    up.create_ref_obs_reg_files(ref_bright_stars, obs_bright_stars, outpath=obs_directory)
                    
                    #copy reference image to uncorrected observation folder
                    shutil.copy(ref_file_path, obs_directory)
                    
                    
                    #create the command to run uvotunicorr
                    unicorr_command = up.create_uvotunicorr_bash_command(ref_frame, obs_frame, obspath=obs_directory)
                    
                    #run uvotunicorr
                    if args.verbose:
                        up.run_uvotunicorr_verbose(unicorr_command)
                    else:
                        up.run_uvotunicorr(unicorr_command)
                    
                print("Corrections Complete. Checking how many were successful.\n")
                
                #check again for bad frames. Count how many are left to correct
                new_aspect_uncorrected_frames = up.check_aspect_correction(filepath)
                new_num_uncorrected = len(new_aspect_uncorrected_frames)
                
                num_successful = num_uncorrected - new_num_uncorrected
                
                print(f"{num_successful} Frames were successfully corrected for tile {sc_tile}.\n")
                
                # If there are no frames left to correct, no more work is needed.
                if new_num_uncorrected == 0:
                    print("Aspect Corrections Complete. ")
                    aspectnone_dict[sc_tile] = 0
                
                #If there are frames to correct, append number to one dictionary for counting
                #Append tile names to a second dictionary for output and manual inspection
                else:
                    print(f"{new_num_uncorrected} still need to be corrected.")
                    aspectnone_dict[sc_tile] = new_num_uncorrected
                    aspectnone_tiles_dict[sc_tile] = new_aspect_uncorrected_frames
        
        print("Aspect Correction Check is complete.\n")
        
    print('First Pass finished.')
    if args.remove_bad:
        print('No more actions needed.')
        print('Exiting Cleaning Pipeline')
        
        run_pipeline = False
    else:
        
        if sum(aspectnone_dict.values()) == 0:
            
            print('No more tiles to aspect correct. No more actions needed.')
            print('Exiting Cleaning Pipeline.')
            
            run_pipeline = False
        
        else:
            
            print(f'Found {sum(aspectnone_dict.values())} frames that still need correcting.')
            
            ga = False
            
            while ga == False:
                go_again = input('Do you wish to change the global parameters and try another round of aspect correction? [Y/N]')
                
                if go_again.upper() == 'Y':
                    print('Starting Next Pass.')
                    ga=True
                    
                elif go_again.upper() == 'N':
                    print('Exiting Cleaning Pipeline. Please manually check remaining bad frames.')
                    print('Outputting bad_frames.csv for manual inspection.')
                    
                    bad_frames = pd.DataFrame.from_dict(aspectnone_tiles_dict, orient='index')
                    bad_frames.to_csv('bad_frames.csv')
                    
                    run_pipeline = False
                    ga=True
                    
                else:
                    print("Please pick a valid option. [Y/N]")