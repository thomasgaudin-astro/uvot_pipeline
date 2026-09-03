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
import warnings

from tqdm import tqdm
from sh import gunzip
from astropy.units import UnitsWarning
from astropy.table import QTable, Table

from sc_uvot import DownloadUVOT, FTOOLSCommands, RemoveSmeared, AspectCorrections

def CleanUVOT(args):

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

    pass_counter = 0

    while run_pipeline == True:

        #sets remove bad parameter for multiple runs
        if (args.remove_bad) | (pass_counter > 2):
            removing_bad = True
        else:
            removing_bad = False

        if removing_bad == True:
            print('Frames with no aspect correction will be removed.')
        
        else:
            print('Setting Global Parameters for Aspect Correction:\n')
            #depending on number of runs performed, reduce area and number of star matches used for aspect corrections
            if args.batch:
                side_buffers = [10, 9, 7, 5, 4]
                num_star_choices = [200, 100, 50, 30, 15]

                side_buffer = side_buffers[pass_counter]
                num_stars = num_star_choices[pass_counter]    

                print(f'Distance from the center of the frame included: {side_buffer}')  
                print(f'Number of stars used in aspect correction: {num_stars}')          

            else:
                #change the parameters of the aspect correction process
                sb_needed = True
                ns_needed = True
                    
                while sb_needed == True:
                    
                    side_buffer = input("Please select the distance from the center of the frame that you wish to include: [10]")
                    
                    if side_buffer == "":
                        side_buffer = 10
                        sb_needed = False
                    else:
                        try:
                            int(side_buffer)
                            sb_needed = False
                            side_buffer = int(side_buffer)
                        except:
                            print("Please pick a valid integer.")
                            
                while ns_needed == True:
                    
                    num_stars = input("Please choose how many stars you wish to select for use in aspect correction: [200]")
                    
                    if num_stars == "":
                        num_stars = 200
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
        sc_tile = args.tile_name
        print(f'Downloading new data for Tile {sc_tile}.')

        download = DownloadUVOT(sc_tile, new_tile_name, tile_ra, tile_dec)

        print(f"Cleaning Data for Tile {sc_tile}.")
        
        filepath = f'./S-CUBED/{sc_tile}/UVOT'
        
        all_filepaths = sorted(os.listdir(filepath))
        if '.DS_Store' in all_filepaths:
            all_filepaths.remove('.DS_Store')
        
        if args.no_detect:
            print('uvotdetect was skipped.')
        else:
            print("Running uvotdetect.")

            for path in tqdm(all_filepaths):

                detect = FTOOLSCommands(sc_tile, obsid=path, source_name=None, detect=True)

                uvotdetect_command = detect.uvotdetect_command

                if args.verbose:
                    detect.run_uvotdetect_verbose(uvotdetect_command)
                else:
                    detect.run_uvotdetect(uvotdetect_command)
        
            print("uvotdetect is complete.\n")

        print("Detecting Smeared Frames.")

        RemoveSmeared(sc_tile)

        print("Smear Removal is complete.\n")

        print("Unzipping all image files.")

        for path in all_filepaths:
            subpath = os.path.join(filepath, path)
            img_path_fill = f'uvot/image/sw{path}uw1_sk.img'
            unzipped_img_path_fill = f'uvot/image/sw{path}uw1_sk.img.gz'

            img_path = os.path.join(subpath, img_path_fill)
            unzipped_img_path = os.path.join(subpath, unzipped_img_path_fill)

            #if .img frame does not exist, unzip file and keep original.
            if os.path.exists(img_path) == False:
                #unzip reference image if it exists
                if os.path.exists(unzipped_img_path) == True:
                    os.system(f'gunzip -k {img_path}.gz')

        print("All image files unzipped.\n")

        print("Checking Frame Aspect Correction.")

        aspect_correction = AspectCorrections(sc_tile, verbose=args.verbose, side_buffer=side_buffer, num_stars=num_stars)
        
        print("Aspect Correction Check is complete.\n")

    if args.batch:    
        print(f'Pass {pass_counter} finished.')

    if args.remove_bad:
        print('No more actions needed.')
        print('Exiting Cleaning Pipeline and Removing Unnecessary Files.')

        if args.clean:
            #loop through all filepaths and remove source.fits & source.reg files
            for path in tqdm(all_filepaths):

                #file names
                detect_fitsfile = f'{filepath}/uvot/image/detect.fits'
                detect_regfile = f'{filepath}/uvot/image/detect.reg'
                ref_regfile = f'{filepath}/uvot/image/ref.reg'
                obs_regfile = f'{filepath}/uvot/image/obs.reg'

                # #remove source.fits if it exists
                if os.path.exists(detect_fitsfile) == True:
                    os.remove(detect_fitsfile)

                # #remove source.reg if it exists
                if os.path.exists(detect_regfile) == True:
                    os.remove(detect_regfile)

                #remove source.reg if it exists
                if os.path.exists(ref_regfile) == True:
                    os.remove(ref_regfile)

                #remove source.reg if it exists
                if os.path.exists(obs_regfile) == True:
                    os.remove(obs_regfile)
        
        run_pipeline = False
    else:
        
        if aspect_correction.frames_to_correct == False:
            
            print('No more tiles to aspect correct. No more actions needed.')

            if args.clean:
                print('Exiting Cleaning Pipeline and Removing Unnecessary Files.')
                #loop through all filepaths and remove source.fits & source.reg files
                for path in tqdm(all_filepaths):

                    #file names
                    detect_fitsfile = f'{filepath}/{path}/uvot/image/detect.fits'
                    detect_regfile = f'{filepath}/{path}/uvot/image/detect.reg'
                    ref_regfile = f'{filepath}/{path}/uvot/image/ref.reg'
                    obs_regfile = f'{filepath}/{path}/uvot/image/obs.reg'

                    # #remove source.fits if it exists
                    if os.path.exists(detect_fitsfile) == True:
                        os.remove(detect_fitsfile)

                    # #remove source.reg if it exists
                    if os.path.exists(detect_regfile) == True:
                        os.remove(detect_regfile)

                    # #remove source.reg if it exists
                    if os.path.exists(ref_regfile) == True:
                        os.remove(ref_regfile)

                    # #remove source.reg if it exists
                    if os.path.exists(obs_regfile) == True:
                        os.remove(obs_regfile)

                run_pipeline = False
        
        else:
            
            if args.batch:
                print(f'Found {aspect_correction.new_num_uncorrected} frames that still need correcting.')

                pass_counter += 1
                
                ga = True
            
            else:

                print(f'Found {aspect_correction.new_num_uncorrected} frames that still need correcting.')
                
                ga = False
                
                while ga == False:
                    go_again = input('Do you wish to change the global parameters and try another round of aspect correction? [Y/N]')
                    
                    if go_again.upper() == 'Y':
                        print('Starting Next Pass.')
                        ga=True
                        
                    elif go_again.upper() == 'N':
                        print('Exiting Cleaning Pipeline. Please manually check remaining bad frames.')
                        print('Outputting bad_frames.csv for manual inspection.')
                        
                        bad_frames = pd.DataFrame(aspect_correction.new_aspect_uncorrected_frames, columns=['Frame_ID'])
                        bad_frames.to_csv('bad_frames.csv', index=False)
                        
                        run_pipeline = False
                        ga=True
                        
                    else:
                        print("Please pick a valid option. [Y/N]")

def main():

    parser = argparse.ArgumentParser(description='Options for Clean Tiles Script.')

    parser.add_argument('tile_name', 
                        help="The name of the tile. This will be used to identify which files to clean."
                        )
    parser.add_argument('-nd', 
                        '--no_detect', 
                        action='store_true', 
                        help='Skips uvotdetect command for each tile.'
                        )
    parser.add_argument('-rb', 
                        '--remove_bad', 
                        action='store_true', 
                        help='Removes bad aspect correction tiles instead of correcting them.'
                        )
    parser.add_argument('-v', 
                        '--verbose', 
                        action='store_true', 
                        help='Prints command outputs instead of surpessing them.'
                        )
    parser.add_argument('-b', 
                        '--batch', 
                        action='store_true', 
                        help='Removes prompts that are unnecessary for batch processing version of code.'
                        )
    parser.add_argument('-c', 
                        '--clean', 
                        action='store_true', 
                        help='Removes unnecessary .fits and .reg files generated for an observation by the cleaning process.'
                        )

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

    #ignore UnitsWarnings
    warnings.filterwarnings("ignore", category=UnitsWarning)

    #run the pipeline
    CleanUVOT(args)

if __name__ == "__main__":
    main() 