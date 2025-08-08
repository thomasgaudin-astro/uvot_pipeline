#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Aug 8 12:23:00 2025

@author: tmg6006
"""

import matplotlib.pyplot as plt

def plot_ogle_lc(source_name, uvot_data):

    fig, ax = plt.subplots(figsize=(10,7), facecolor='white')

    ax.scatter(ogle_data['MJD'], ogle_data['Mag'], marker='D', s=20, c='k', zorder=5)
    ax.errorbar(ogle_data['MJD'], ogle_data['Mag'], yerr=ogle_data['Mag_Err'], lw=1, capsize=2, fmt='none', c='k')

    ax.invert_yaxis()

    ax.set_ylabel('I-band Mag', fontsize=14)
    ax.set_xlabel('MJD', fontsize=14)
    ax.set_title(f'{source_name}', fontsize=16)
    ax.tick_params(labelsize=14)

    plt.savefig(f'./Plots/OGLE_plots/{source_name}_ogle_lc.pdf', bbox_inches='tight')

def plot_uvot_lc(source_name, uvot_data):

    fig, ax = plt.subplots(figsize=(10,7), facecolor='white')

    ax.scatter(uvot_data['MJD'], uvot_data['Mag'], marker='D', s=20, c='k', zorder=5)
    ax.errorbar(uvot_data['MJD'], uvot_data['Mag'], yerr=uvot_data['Mag_Err'], lw=1, capsize=2, fmt='none', c='r')

    ax.invert_yaxis()

    ax.set_ylabel('UVW1-band Mag', fontsize=14)
    ax.set_xlabel('MJD', fontsize=14)
    ax.set_title(f'{source_name}', fontsize=16)
    ax.tick_params(labelsize=14)

    plt.savefig(f'./Plots/UVOT_plots/{source_name}_uvot_lc.pdf', bbox_inches='tight')

def plot_xrt_lc_ul(source_name, xrt_data, xrt_ul_data, ymin=0):
    
    max_ul_val = max(xrt_ul_data['CR'])

    max_xrt_val = max(xrt_data['CR_perr']+xrt_data['CR'])
    min_xrt_val = min(xrt_data['CR']-xrt_data['CR_nerr'])

    max_val = max(max_ul_val, max_xrt_val)

    fig, ax = plt.subplots(figsize=(10,7), facecolor='white')

    fig.subplots_adjust(hspace=0)

    ax.scatter(xrt_data['MJD'], xrt_data['CR'], marker='D', s=20, c='k', zorder=10)
    ax.errorbar(xrt_data['MJD'], xrt_data['CR'], 
                   yerr=[xrt_data['CR_nerr'], xrt_data['CR_perr']],
                   capsize = 2, lw=1, fmt='none', c='b', zorder=5)

    for date, rate in zip(xrt_ul_data['MJD'], xrt_ul_data['CR']):

        ax.arrow(date, rate, 0, -0.03*max_xrt_val, color='plum', width=0.5, 
                    head_width=30, head_length=0.03*max_xrt_val, zorder=1)

    ax.scatter(xrt_ul_data['MJD'], xrt_ul_data['CR'], marker='_', c='plum', zorder=1)

    ax.set_ylabel('XRT Count Rate (counts/s)', fontsize=16)
    ax.set_ylim(min_xrt_val-0.03, max_val+0.03)
    ax.set_xlabel('MJD', fontsize=16)
    ax.set_title(f'{source_name}', fontsize=16)
    ax.tick_params(labelsize=16)

    plt.savefig(f'./Plots/XRT_plots/{source_name}_xrt_lc.pdf', bbox_inches="tight")