#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import subprocess
import pandas as pd
import argparse

from collections import Counter

parser = argparse.ArgumentParser(description='Options for Batch Photometry Script.')

parser.add_argument('filename', help="The name of the file containing a list of sources, S-CUBED tiles, and their cleaning status.")
parser.add_argument('-v', '--verbose', action='store_true', help='Prints command outputs instead of surpessing them.')

args = parser.parse_args()

targets = pd.read_csv(args.filename, header=None, names=['ID', 'UVOT RA', 'UVOT Dec', 'S-CUBED Tile', 'Cleaned?', 'Problems?'], sep=r'\s+')

tiles_to_clean = Counter(targets['S-CUBED Tile']).keys()

print('Starting batch processing photometry of S-CUBED sources.\n')
print(f'Cleaning {len(tiles_to_clean)} tiles.')

for tile in tiles_to_clean:

    print(f'Begin cleaning for Tile {tile}.')

    if args.verbose:
        subprocess.run(['python', 'clean_uvot_tiles.py', f'{tile}', '-v', '-b'])
    else:
        subprocess.run(['python', 'clean_uvot_tiles.py', f'{tile}', '-b'])

print('Batch cleaning complete. Please proceed to running photometry.')