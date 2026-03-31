#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import subprocess
import pandas as pd
import argparse

from collections import Counter

parser = argparse.ArgumentParser(description='Options for Batch Photometry Script.')

parser.add_argument('filename', help="The name of the file containing a list of sources, S-CUBED tiles, and their cleaning status.")

args = parser.parse_args()

targets = pd.read_csv(args.filename, header=None, names=['ID', 'UVOT RA', 'UVOT Dec', 'S-CUBED Tile', 'Cleaned?', 'Problems?', 'OGLE Name', 'SC Num'], sep=r'\s+')

print('Starting batch plotting of S-CUBED photometry.\n')
print(f'Cleaning {len(tiles_to_clean)} tiles.')

for targind in targets.index:

    targname = targets.loc[targind, 'ID']
    ogle_name = targets.loc[targind, 'OGLE Name']
    sc_name = targets.loc[targind, 'SC num']

    print(f'Plotting data for {targname}.')

    subprocess.run(['python', 'plot_mw_lcs.py', f'{targname}', f'{ogle_name}', f'{sc_name}', '-b'])

print('Batch plotting complete. Please check output folders for results.')