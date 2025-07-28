#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 28 12:10:00 2025

@author: tmg6006
"""

import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt

mpl.rcParams['pdf.fonttype'] = 42
mpl.rcParams['ps.fonttype'] = 42
mpl.rcParams['font.family'] = 'Serif'

parser.add_argument('source_name', help="The name of the source. This will be used to identify which uvot file to plot as a light curve.")

print('Welcome to the UVOT Light Curve Plotter.')
print(f'Plotting a light curve for the source {args.source_name}.\n')

print('Grabbing data for source.\n')

test_data = pd.read_csv(f'./UVOT_Outputs/{args.source_name}_uvot_data.txt', header=None, sep='\s+')

print('Plotting data and saving output.\n')

fig, ax = plt.subplots(figsize=(10,7), facecolor='white')

ax.scatter(test_data[0], test_data[1], marker='D', s=20, c='k', zorder=5)
ax.errorbar(test_data[0], test_data[1], yerr=test_data[2], lw=1, capsize=2, fmt='none', c='r')

ax.invert_yaxis()

ax.set_ylabel('UVW1-band Mag', fontsize=14)
ax.set_xlabel('MJD', fontsize=14)
ax.tick_params(labelsize=14)

plt.savefig(f'./UVOT_plots/{args.source_name}_uvot_lc.pdf', bbox_inches='tight')

print(f'Process complete. File will be in the UVOT plots folder under the name {args.source_name}_uvot_lc.pdf')