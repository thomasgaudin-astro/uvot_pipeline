#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun  2 12:08:00 2025

@author: tmg6006
"""

import argparses
import os
import pandas as pd
import uvot_pipeline as up

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
parser.add_argument('source_ra', help="The Right Ascension coordinate of the source in decimal degrees.", type=int)
parser.add_argument('source_dec', help="The Right Ascension coordinate of the source in decimal degrees.", type=int)
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
while use_tile = False:

    uf = input(f'Do you wish to use {closest_tile}? [Y]')

    if uf == "":
        use_tile = True
    elif uf.upper == "Y":
        use_tile = True
    elif uf.upper == "N":
        closest_tile = input(f'')