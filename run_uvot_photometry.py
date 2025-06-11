#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun  2 12:08:00 2025

@author: tmg6006
"""

import argparse

import os

from astropy.io import fits
import astropy.units as u
from astropy.coordinates import SkyCoord

import sys

import pandas as pd

import uvot_pipeline as up

from swifttools.swift_too import Clock


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

# Set up arguments that need to be passed to the scripts
parser = argparse.ArgumentParser(description='Options for Clean Tiles Script.')

parser.add_argument('source_name', help="The name of the source. This will be used to name the output photometry file.")
parser.add_argument('source_reg', help="The path to 5 arcsecond diameter source region file that contains the star for which you wish to perform photometry.")
parser.add_argument('bkg_reg', help="The path to background region file that contains the empty sky area that is used to calibrate photometry.")
parser.add_argument('source_ra', help="The Right Ascension coordinate of the source in decimal degrees.", type=float)
parser.add_argument('source_dec', help="The Right Ascension coordinate of the source in decimal degrees.", type=float)
parser.add_argument('-v', '--verbose', action='store_true', help='Prints command outputs instead of surpessing them.')

args = parser.parse_args()

#read in list of tiles
tiles = pd.read_csv('scubed_tiles.csv')

#run the pipeline
print('Starting the S-CUBED UVOT Photometry Pipeline.\n')

#check that input folder and source/background regions exist.
print('Checking existence of files and directories.')

files_exist = False

while files_exist == False:

    if (os.path.exists(args.source_reg) == True) & (os.path.exists(args.bkg_reg) == True):
        print("Source Region and Background Region exist.")
        files_exist = True
    else:
        sys.exit("ERROR: Source or Background Region not found.")
    
print("Checking which S-CUBED Tile is closest to your photometry target.")

# Create SkyCoord object from input RA and DEC.
source_coords = SkyCoord(args.source_ra, args.source_dec, frame='icrs', unit=u.deg)

# Start code to check closest tile to the source position.
# Loop through tiles to calcualte separation for each tile center to source RA and DEC. 
for ind in tiles.index:
    tiles.loc[ind, 'Tile Name'] = tiles.loc[ind, 'Tile Name'].rstrip()
    
    # Create SkyCoord object for tile central RA and DEC
    tile_ra = tiles.loc[ind, 'RA']
    tile_dec = tiles.loc[ind, 'DEC']
    tile_coords = SkyCoord(tile_ra, tile_dec, frame='icrs', unit=u.deg)

    # Calc separation and append to tiles DataFrame
    sep = source_coords.separation(tile_coords).deg
    tiles.loc[ind, 'Sep'] = sep

# Sort tiles by distance so that cleses target is on top.
minimized_tiles = tiles.sort_values('Sep', ascending=True).reset_index(drop=True)
min_dist = minimized_tiles.loc[0, 'Sep']
closest_tile = minimized_tiles.loc[0, 'Tile Name']

print(f'The Closest Tile is: {closest_tile}')
print(f'Distance to Closest Tile is: {min_dist} deg')

use_tile = False
valid_tile = False

# Check to see if this tile is 
while use_tile == False:

    uf = input(f'Do you wish to use {closest_tile}? [Y]')

    if uf == "":
        print(f"Using {closest_tile}")
        use_tile = True
    elif uf.upper == "Y":
        use_tile = True
    elif uf.upper == "N":
        closest_tile = input(f'Which tile do you wish to use instead? ')
        
        # Check to make sure new tile is real.
        while valid_tile == False:
            filepath = f'./S-CUBED'
        
            all_filepaths = sorted(os.listdir(filepath))
            if '.DS_Store' in all_filepaths:
                all_filepaths.remove('.DS_Store')

            if closest_tile in all_filepaths:
                print(f"Using {closest_tile}")
                valid_tile = True
            else:
                print("Tile not found. Please input a valid title.")
            
        use_tile = True
    else:
        print("Please pick a valid option [Y/N]")

print("\nStarting aperture photometry.")

# Define filepath for individual UVOT observations
tile_filepath = f"./S-CUBED/{closest_tile}/UVOT"

# Generate a list of observation ids. Remove ".DS_Store" if in list
all_target_filepaths = sorted(os.listdir(tile_filepath))
    
if '.DS_Store' in all_target_filepaths:
    all_target_filepaths.remove('.DS_Store')

# Loop through filepaths and run uvotsource
for obs in all_target_filepaths:

    # Write command for uvotsource
    uvotsource_command = up.create_uvotsource_bash_command(closest_tile, obs, args.source_reg, args.bkg_reg, args.source_name)
    
    if "-v" == True:
        up.run_uvotsource_verbose(uvotsource_command)
    else:
        up.run_uvotsource(uvotsource_command)

print("Aperture photometry complete.\n")

print("Grabbing Data for Output.")

# Generate blank data used for general data storage
source_data = pd.DataFrame()

# Loop through all filepaths and grab fits data from photometry source.fits output
for obs in all_target_filepaths:

    filename = f'./S-CUBED/{closest_tile}/UVOT/{obs}/uvot/image/{args.source_name}_source.fits'

    if os.path.exists(filename) == True:
        # Open fits file and grab data from it. Turn it into an array
        with fits.open(filename) as hdul:
            head = hdul[0].header
            data = hdul[1].data

            data_array = np.array(data[0])

        # Re-shape data array into DataFrame
        data_array = pd.DataFrame(data_array.reshape(1, 126), columns=data.names)
        
        # If this is first observation, create source_data DataFrame from data_array DataFrame. 
        # If this is not the first observation, tack the data_array values onto the end of the source_data DataFrame
        if source_data.empty == False:
            source_data = pd.concat([source_data, data_array])
        else:
            source_data = data_array

# Sort DataFrame by Time    
source_data = source_data.sort_values('MET', ascending=True)
source_data = source_data.reset_index(drop=True)

# Make sure important values are floats
source_data['AB_MAG'] = source_data['AB_MAG'].astype(float)
source_data['AB_MAG_ERR'] = source_data['AB_MAG_ERR'].astype(float)
source_data['MAG'] = source_data['MAG'].astype(float)
source_data['MAG_ERR'] = source_data['MAG_ERR'].astype(float)
source_data['MET'] = source_data['MET'].astype(float)
source_data['EXPOSURE'] = source_data['EXPOSURE'].astype(float)

# Convert MET values into MJD values
cc = Clock()

cc.met = list(source_data['MET'])
cc.submit()
times = Time(cc.utc, scale='utc').mjd

source_data['MJD'] = times

#copy just the values that we want to a sliced DataFrame
uvot_data_slice = source_data[['MJD', 'MAG', 'MAG_ERR', 'FLUX_AA', 'FLUX_AA_ERR']].copy()

outpath = f'./UVOT_Outputs/{args.source_name}_uvot_data.txt'

#export DataFrame to text file
with open(outpath, 'w') as f:
    df_string = uvot_data_slice.to_string(header=False, index=False)
    f.write(df_string)
    
print('Task Complete.')
print(f'Outputting new file: {args.source_name}_uvot_data.txt')

print("\nDeleting unnecessary files.")

for obs in all_target_filepaths:

    filename = f'./S-CUBED/{closest_tile}/UVOT/{obs}/uvot/image/{args.source_name}_source.fits'

    if os.path.exists(filename) == True:
        os.remove(filename)

print("All source.fits files deleted.")

print("\nUVOT Photometry has been generated.")
print("\nExiting Photometry Pipeline.")