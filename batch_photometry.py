import subprocess
import pandas as pd
import argparse

parser = argparse.ArgumentParser(description='Options for Batch Photometry Script.')

parser.add_argument('filename', help="The name of the file containing a list of sources, S-CUBED tiles, and their cleaning status.")
parser.add_argument('-v', '--verbose', action='store_true', help='Prints command outputs instead of surpessing them.')

args = parser.parse_args()

targets = pd.read_csv(args.filename, header=None, names=['ID', 'UVOT RA', 'UVOT Dec', 'S-CUBED Tile', 'Cleaned?', 'Problems?'], sep=r'\s+')

print('Starting batch processing photometry of S-CUBED sources.\n')
print(f'Producing light curves for {len(targets.index)} sources.')

for ind, target in enumerate(targets.index):

    targname = targets.loc[ind, 'ID']
    targ_ra = target[ind, 'UVOT RA']
    targ_dec = target[ind, 'UVOT DEC']

    print(f'Begin photometry for Source {targname}.')

    subprocess.run(['python', 
                    f'./run_uvot_photometry.py {targname} {targname}_source.reg {targname}_bkg.reg {targ_ra} {targ_dec}'])

print('Batch processing complete. Check UVOT Outputs folder for completed light curves.')