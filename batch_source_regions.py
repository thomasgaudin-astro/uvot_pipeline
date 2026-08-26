#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
import argparse

parser = argparse.ArgumentParser(description='Options for Batch Photometry Script.')

parser.add_argument('filename', help="The name of the file containing a list of sources, S-CUBED tiles, and their cleaning status.")

args = parser.parse_args()

targets = pd.read_csv(args.filename, header=None, names=['ID', 'UVOT RA', 'UVOT Dec', 'S-CUBED Tile', 'Cleaned?', 'Problems?', 'OGLE Name', 'SC Num'], sep=r'\s+')

print('Generating batch source regions for S-CUBED sources.\n')

for targind in targets.index:

    targname = targets.loc[targind, 'ID']
    targra = targets.loc[targind, 'UVOT RA']
    targdec = targets.loc[targind, 'UVOT Dec']

    print(f'Generating source region for {targname} at RA: {targra}, Dec: {targdec}.')

    reg_filename = f'{targname}_source.reg'

    #generate new region text and write out file
    new_reg_text = f'# Region file format: DS9 version 4.1\nfk5\ncircle({targra},{targdec},5.000")'

    with open(reg_filename, mode='w', encoding='utf-8') as regfile:
        regfile.write(new_reg_text)