#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thurs Jul 24 10:33:00 2025

@author: tmg6006
"""

import numpy as np
import pandas as pd

from astropy.coordinates import SkyCoord
import astropy.units as u

tiles_list = 'scubed_tiles.csv'

tiles = pd.read_csv(tiles_list, header=0)

print('Welcome to Closest Tile Finder.')
print('Please enter the coordinates of your target so that we can find the proper S-CUBED tile to clean.\n')

try:
    source_ra = float(input('Source RA (degrees): '))
    source_dec = float(input('Source Dec (degrees): '))
except ValueError:
    print('Error. Source RA and Dec must be in units of degrees.')

source_coords = SkyCoord(source_ra, source_dec, frame='icrs', unit=u.deg)

for ind in tiles.index:
    tiles.loc[ind, 'Tile Name'] = tiles.loc[ind, 'Tile Name'].rstrip()

    tile_ra = tiles.loc[ind, 'RA']
    tile_dec = tiles.loc[ind, 'DEC']
    tile_coords = SkyCoord(tile_ra, tile_dec, frame='icrs', unit=u.deg)

    sep = source_coords.separation(tile_coords).deg
    tiles.loc[ind, 'Sep'] = sep

minimized_tiles = tiles.sort_values('Sep', ascending=True).reset_index(drop=True)
min_dist = minimized_tiles.loc[0, 'Sep']
closest_tile = minimized_tiles.loc[0, 'Tile Name']

print(f'The Closest Tile is: {closest_tile}')
print(f'Distance to Closest Tile is: {min_dist} deg')

if min_dist >= 0.15:    
    print('WARNING. Target is near the edge of the Closest Tile or off the frame. Light curve may be sparse.')