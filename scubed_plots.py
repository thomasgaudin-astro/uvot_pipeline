#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Aug 8 12:23:00 2025

@author: tmg6006
"""

import matplotlib.pyplot as plt

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

def plot_ogle_lc(source_name, ogle_data):

    fig, ax = plt.subplots(figsize=(10,7), facecolor='white')

    ax.scatter(ogle_data['MJD'], ogle_data['I'], marker='D', s=20, c='k', zorder=5)
    ax.errorbar(ogle_data['MJD'], ogle_data['I'], yerr=ogle_data['I_Err'], lw=1, capsize=2, fmt='none', c='k')

    ax.invert_yaxis()

    ax.set_ylabel('I-band Mag', fontsize=14)
    ax.set_xlabel('MJD', fontsize=14)
    ax.set_title(f'{source_name}', fontsize=16)
    ax.tick_params(labelsize=14)

    plt.savefig(f'./Plots/OGLE_plots/{source_name}_ogle_lc.pdf', bbox_inches='tight')

def plot_ogle_uvot_lc(source_name, ogle_data, uvot_data):

    fig, ax = plt.subplots(2, 1, figsize=(10,7), facecolor='white', sharex=True)

    fig.subplots_adjust(hspace=0)

    ax[0].scatter(ogle_data['MJD'], ogle_data['I'], marker='D', s=20, c='k', zorder=5)
    ax[0].errorbar(ogle_data['MJD'], ogle_data['I'], yerr=ogle_data['I_Err'], lw=1, capsize=2, fmt='none', c='k')

    ax[0].invert_yaxis()

    ax[0].set_ylabel('I-band Mag', fontsize=14)
    ax[0].set_xlabel('MJD', fontsize=14)
    ax[0].set_title(f'{source_name}', fontsize=16)
    ax[0].tick_params(labelsize=14)

    ax[1].scatter(uvot_data['MJD'], uvot_data['Mag'], marker='D', s=20, c='k', zorder=5)
    ax[1].errorbar(uvot_data['MJD'], uvot_data['Mag'], yerr=uvot_data['Mag_Err'], lw=1, capsize=2, fmt='none', c='r')

    ax[1].invert_yaxis()

    ax[1].set_ylabel('UVW1-band Mag', fontsize=14)
    ax[1].set_xlabel('MJD', fontsize=14)
    ax[1].tick_params(labelsize=14)

    plt.savefig(f'./Plots/OGLE_UVOT_plots/{source_name}_ogle_uvot_lc.pdf', bbox_inches='tight')

def plot_trunc_ogle_uvot_lc(source_name, truncated_ogle_data, uvot_data):

    fig, ax = plt.subplots(2, 1, figsize=(10,7), facecolor='white', sharex=True)

    fig.subplots_adjust(hspace=0)

    ax[0].scatter(truncated_ogle_data['MJD'], truncated_ogle_data['I'], marker='D', s=20, c='k', zorder=5)
    ax[0].errorbar(truncated_ogle_data['MJD'], truncated_ogle_data['I'], yerr=truncated_ogle_data['I_Err'], lw=1, capsize=2, fmt='none', c='k')

    ax[0].invert_yaxis()

    ax[0].set_ylabel('I-band Mag', fontsize=14)
    ax[0].set_xlabel('MJD', fontsize=14)
    ax[0].set_title(f'{source_name}', fontsize=16)
    ax[0].tick_params(labelsize=14)

    ax[1].scatter(uvot_data['MJD'], uvot_data['Mag'], marker='D', s=20, c='k', zorder=5)
    ax[1].errorbar(uvot_data['MJD'], uvot_data['Mag'], yerr=uvot_data['Mag_Err'], lw=1, capsize=2, fmt='none', c='r')

    ax[1].invert_yaxis()

    ax[1].set_ylabel('UVW1-band Mag', fontsize=14)
    ax[1].set_xlabel('MJD', fontsize=14)
    ax[1].tick_params(labelsize=14)

    plt.savefig(f'./Plots/Trunc_OGLE_UVOT_plots/{source_name}_trunc_ogle_uvot_lc.pdf', bbox_inches='tight')

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

    ax.set_ylabel('XRT Count Rate (counts/s)', fontsize=14)
    ax.set_ylim(min_xrt_val-0.03, max_val+0.03)
    ax.set_xlabel('MJD', fontsize=14)
    ax.set_title(f'{source_name}', fontsize=16)
    ax.tick_params(labelsize=14)

    plt.savefig(f'./Plots/XRT_plots/{source_name}_xrt_lc.pdf', bbox_inches="tight")

def plot_uvot_xrt_lc_ul(source_name, uvot_data, xrt_data, xrt_ul_data, ymin=0):
    
    max_ul_val = max(xrt_ul_data['CR'])

    max_xrt_val = max(xrt_data['CR_perr']+xrt_data['CR'])
    min_xrt_val = min(xrt_data['CR']-xrt_data['CR_nerr'])

    max_val = max(max_ul_val, max_xrt_val)

    fig, ax = plt.subplots(2, 1, figsize=(10,7), facecolor='white', sharex=True)

    fig.subplots_adjust(hspace=0)

    ax[0].scatter(uvot_data['MJD'], uvot_data['Mag'], marker='D', s=20, c='k', zorder=5)
    ax[0].errorbar(uvot_data['MJD'], uvot_data['Mag'], yerr=uvot_data['Mag_Err'], lw=1, capsize=2, fmt='none', c='r')

    ax[0].invert_yaxis()

    ax[0].set_ylabel('UVW1-band Mag', fontsize=14)
    ax[0].set_xlabel('MJD', fontsize=14)
    ax[0].set_title(f'{source_name}', fontsize=16)
    ax[0].tick_params(labelsize=14)

    ax[1].scatter(xrt_data['MJD'], xrt_data['CR'], marker='D', s=20, c='k', zorder=10)
    ax[1].errorbar(xrt_data['MJD'], xrt_data['CR'], 
                   yerr=[xrt_data['CR_nerr'], xrt_data['CR_perr']],
                   capsize = 2, lw=1, fmt='none', c='b', zorder=5)

    for date, rate in zip(xrt_ul_data['MJD'], xrt_ul_data['CR']):

        ax[1].arrow(date, rate, 0, -0.03*max_xrt_val, color='plum', width=0.5, 
                    head_width=30, head_length=0.03*max_xrt_val, zorder=1)

    ax[1].scatter(xrt_ul_data['MJD'], xrt_ul_data['CR'], marker='_', c='plum', zorder=1)

    ax[1].set_ylabel('XRT Count Rate (counts/s)', fontsize=14)
    ax[1].set_ylim(min_xrt_val-0.03, max_val+0.03)
    ax[1].set_xlabel('MJD', fontsize=14)
    ax[1].tick_params(labelsize=14)

    plt.savefig(f'./Plots/UVOT_XRT_plots/{source_name}_uvot_xrt_lc.pdf', bbox_inches="tight")

def plot_ogle_uvot_xrt_lc_ul(source_name, ogle_data, uvot_data, xrt_data, xrt_ul_data, ymin=0):
    
    max_ul_val = max(xrt_ul_data['CR'])

    max_xrt_val = max(xrt_data['CR_perr']+xrt_data['CR'])
    min_xrt_val = min(xrt_data['CR']-xrt_data['CR_nerr'])

    max_val = max(max_ul_val, max_xrt_val)

    fig, ax = plt.subplots(3, 1, figsize=(15,7), facecolor='white', sharex=True)

    fig.subplots_adjust(hspace=0)

    ax[0].scatter(ogle_data['MJD'], ogle_data['I'], marker='D', s=20, c='k', zorder=5)
    ax[0].errorbar(ogle_data['MJD'], ogle_data['I'], yerr=ogle_data['I_Err'], lw=1, capsize=2, fmt='none', c='k')

    ax[0].invert_yaxis()

    ax[0].set_ylabel('I-band Mag', fontsize=14)
    ax[0].set_xlabel('MJD', fontsize=14)
    ax[0].set_title(f'{source_name}', fontsize=16)
    ax[0].tick_params(labelsize=14)

    ax[1].scatter(uvot_data['MJD'], uvot_data['Mag'], marker='D', s=20, c='k', zorder=5)
    ax[1].errorbar(uvot_data['MJD'], uvot_data['Mag'], yerr=uvot_data['Mag_Err'], lw=1, capsize=2, fmt='none', c='r')

    ax[1].invert_yaxis()

    ax[1].set_ylabel('UVW1-band Mag', fontsize=14)
    ax[1].set_xlabel('MJD', fontsize=14)
    ax[1].tick_params(labelsize=14)

    ax[2].scatter(xrt_data['MJD'], xrt_data['CR'], marker='D', s=20, c='k', zorder=10)
    ax[2].errorbar(xrt_data['MJD'], xrt_data['CR'], 
                   yerr=[xrt_data['CR_nerr'], xrt_data['CR_perr']],
                   capsize = 2, lw=1, fmt='none', c='b', zorder=5)

    for date, rate in zip(xrt_ul_data['MJD'], xrt_ul_data['CR']):

        ax[2].arrow(date, rate, 0, -0.03*max_xrt_val, color='plum', width=0.5, 
                    head_width=30, head_length=0.03*max_xrt_val, zorder=1)

    ax[2].scatter(xrt_ul_data['MJD'], xrt_ul_data['CR'], marker='_', c='plum', zorder=1)

    ax[2].set_ylabel('XRT Count Rate (counts/s)', fontsize=14)
    ax[2].set_ylim(min_xrt_val-0.03, max_val+0.03)
    ax[2].set_xlabel('MJD', fontsize=14)
    ax[2].tick_params(labelsize=14)

    plt.savefig(f'./Plots/OGLE_UVOT_XRT_plots/{source_name}_ogle_uvot_xrt_lc.pdf', bbox_inches="tight")

def plot_trunc_ogle_uvot_xrt_lc_ul(source_name, truncated_ogle_data, uvot_data, xrt_data, xrt_ul_data, ymin=0):
    
    max_ul_val = max(xrt_ul_data['CR'])

    max_xrt_val = max(xrt_data['CR_perr']+xrt_data['CR'])
    min_xrt_val = min(xrt_data['CR']-xrt_data['CR_nerr'])

    max_val = max(max_ul_val, max_xrt_val)

    fig, ax = plt.subplots(3, 1, figsize=(15,7), facecolor='white', sharex=True)

    fig.subplots_adjust(hspace=0)

    ax[0].scatter(truncated_ogle_data['MJD'], truncated_ogle_data['I'], marker='D', s=20, c='k', zorder=5)
    ax[0].errorbar(truncated_ogle_data['MJD'], truncated_ogle_data['I'], yerr=truncated_ogle_data['I_Err'], lw=1, capsize=2, fmt='none', c='k')

    ax[0].invert_yaxis()

    ax[0].set_ylabel('I-band Mag', fontsize=14)
    ax[0].set_xlabel('MJD', fontsize=14)
    ax[0].set_title(f'{source_name}', fontsize=16)
    ax[0].tick_params(labelsize=14)

    ax[1].scatter(uvot_data['MJD'], uvot_data['Mag'], marker='D', s=20, c='k', zorder=5)
    ax[1].errorbar(uvot_data['MJD'], uvot_data['Mag'], yerr=uvot_data['Mag_Err'], lw=1, capsize=2, fmt='none', c='r')

    ax[1].invert_yaxis()

    ax[1].set_ylabel('UVW1-band Mag', fontsize=14)
    ax[1].set_xlabel('MJD', fontsize=14)
    ax[1].tick_params(labelsize=14)

    ax[2].scatter(xrt_data['MJD'], xrt_data['CR'], marker='D', s=20, c='k', zorder=10)
    ax[2].errorbar(xrt_data['MJD'], xrt_data['CR'], 
                   yerr=[xrt_data['CR_nerr'], xrt_data['CR_perr']],
                   capsize = 2, lw=1, fmt='none', c='b', zorder=5)

    for date, rate in zip(xrt_ul_data['MJD'], xrt_ul_data['CR']):

        ax[2].arrow(date, rate, 0, -0.03*max_xrt_val, color='plum', width=0.5, 
                    head_width=30, head_length=0.03*max_xrt_val, zorder=1)

    ax[2].scatter(xrt_ul_data['MJD'], xrt_ul_data['CR'], marker='_', c='plum', zorder=1)

    ax[2].set_ylabel('XRT Count Rate (counts/s)', fontsize=14)
    ax[2].set_ylim(min_xrt_val-0.03, max_val+0.03)
    ax[2].set_xlabel('MJD', fontsize=14)
    ax[2].tick_params(labelsize=14)

    plt.savefig(f'./Plots/Trunc_OGLE_UVOT_XRT_plots/{source_name}_trunc_ogle_uvot_xrt_lc.pdf', bbox_inches="tight")