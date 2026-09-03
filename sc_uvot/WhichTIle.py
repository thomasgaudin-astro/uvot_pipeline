# -*- coding: utf-8 -*-

import pandas as pd

import astropy.units as u
from astropy.coordinates import SkyCoord

class WhichTile:
    def __init__(self, source_ra, source_dec):
        self.source_ra = source_ra
        self.source_dec = source_dec

        # Create SkyCoord object from input RA and DEC.
        self.source_coords = SkyCoord(self.source_ra, self.source_dec, frame='icrs', unit=u.deg)

        #read in list of tiles
        self.tiles = pd.read_csv('scubed_tiles.csv')

        #caclulate closest tile to source position
        self.min_dist, self.closest_tile = self.closest_tile(self.tiles, self.source_coords)

        def closest_tile(self, tiles, source_coords): 
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

            if min_dist >= 0.15:    
                print('WARNING. Target is near the edge of the Closest Tile or off the frame. Light curve may be sparse.')

            return min_dist, closest_tile

        