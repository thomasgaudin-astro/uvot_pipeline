#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Aug 8 10:42:00 2025

@author: tmg6006
"""

import pandas as pd

import argparse

import matplotlib as mpl

from astropy.table import QTable, Table

import requests
from requests.auth import HTTPBasicAuth

import uvot_pipeline as up
import scubed_plots as sc_plots

mpl.rcParams['pdf.fonttype'] = 42
mpl.rcParams['ps.fonttype'] = 42
mpl.rcParams['font.family'] = 'Serif'

parser = argparse.ArgumentParser(description='Options for Plot Light Curves Script.')

parser.add_argument('source_name', help="The name of the source. This will be used to identify which files to plot as a light curve.")
parser.add_argument('--ogle_name', help="The name of the source in OGLE. This will be used to identify which files to plot as a light curve.")
parser.add_argument('--sc_name', help="The name of the source in S-CUBED. This will be used to identify which files to plot as a light curve.")
parser.add_argument('-b', '--batch', action='store_true', help='Removes prompts that are unnecessary for batch processing version of code.')

args = parser.parse_args()

print('Welcome to the Multi-wavelength Light Curve Plotter.')
print(f'Plotting light curves for the source {args.source_name}.\n')

if args.batch:
    if args.ogle_name == 'N':
        is_ogle = False
    elif args.ogle_name == '-':
        is_ogle = True
        ogle_name = args.source_name
    else:
        is_ogle = True
        ogle_name = args.ogle_name

        try:
            print("Downloading Data.")
            up.download_ogle_data(ogle_name, args.source_name)
            print("Download Successful.")
        except up.DownloadError:
            print("An Error occurred when downloading the file. Please check the name of the OGLE Source and try again.")
    
    if (args.sc_name == 'N') | (args.sc_name == '-'):
        is_xrt = False
    else:
        is_xrt = True
        xrt_num = args.sc_name

        try:
            print("Downloading Data.")
            up.download_xrt_data(xrt_num, args.source_name)
            print("Download Successful.")
        except up.DownloadError:
            print("An Error occurred when downloading the file. Please check the name of the XRT Source and try again.")


else:

    prompt_ogle = True
    is_ogle = True
    ogle_dl_prompt = True

    while prompt_ogle == True:

        ogle = input(f'Is there an OGLE IV Light Curve associated with this UV source? [Y/N] ')

        if ogle.upper() == "Y":

            ogle_name = input("Please provide the OGLE IV name of the source: ")

            while ogle_dl_prompt == True:
                download = input(f'Do you wish to download the newest version of the OGLE IV Light Curve for this source before plotting? [Y/N] ')
                
                if download.upper() == "Y":

                    try:
                        print("Downloading Data.")
                        up.download_ogle_data(ogle_name, args.source_name)
                        ogle_dl_prompt = False
                        print("Download Successful.")
                    except up.DownloadError:
                        print("An Error occurred when downloading the file. Please check the name of the OGLE Source and try again.")

                elif download.upper() == "N":
                    ogle_dl_prompt = False
                else:
                    print("Please pick a valid option [Y/N]")
            
            prompt_ogle = False

        elif ogle.upper() == "N":
            is_ogle = False
            prompt_ogle = False
        else:
            print("Please pick a valid option [Y/N]")

    prompt_xrt = True
    is_xrt = True
    xrt_dl_prompt = True

    while prompt_xrt == True:

        xrt = input(f'Is there a S-CUBED XRT Source associated with this UV source? [Y/N]')

        if xrt.upper() == "Y":

            xrt_num = input("Please provide the S-CUBED Number of the source: ")

            while xrt_dl_prompt == True:
                download_xrt = input(f'Do you wish to download the newest version of the XRT Light Curve for this source before plotting? [Y/N]')
                
                if download_xrt.upper() == "Y":

                    try:
                        print("Downloading Data.")
                        up.download_xrt_data(xrt_num, args.source_name)
                        xrt_dl_prompt = False
                        print("Download Successful.")
                    except up.DownloadError:
                        print("An Error occurred when downloading the file. Please check the name of the XRT Source and try again.")

                elif download_xrt.upper() == "N":
                    xrt_dl_prompt = False
                else:
                    print("Please pick a valid option [Y/N]")
            
            prompt_xrt = False

        elif xrt.upper() == "N":
            is_xrt = False
            prompt_xrt = False
        else:
            print("Please pick a valid option [Y/N]")

print('Grabbing data for source.\n')

if is_ogle == True:

    ogle_data = up.read_ogle_data(args.source_name)
    truncated_ogle_data = ogle_data[ogle_data['MJD'] >= 57563]

if is_xrt == True:

    xrt_data, xrt_ul_data = up.read_xrt_data(args.source_name)

uvot_data = up.read_uvot_data(args.source_name)

print('Plotting data and saving outputs.\n')

if is_ogle == True:

    sc_plots.plot_ogle_lc(args.source_name, ogle_data)

    sc_plots.plot_ogle_uvot_lc(args.source_name, ogle_data, uvot_data)

    sc_plots.plot_trunc_ogle_uvot_lc(args.source_name, truncated_ogle_data, uvot_data)

if is_xrt == True:
    
    if args.batch:
        ymin = 0
        
    else:
        ymin = input("Please set the minimum y-axis value for XRT Count Rate plots: [0]")

        if ymin == "":
            ymin = 0
        else:
            ymin = int(ymin)

    sc_plots.plot_xrt_lc_ul(args.source_name, xrt_data, xrt_ul_data, ymin)

    sc_plots.plot_uvot_xrt_lc_ul(args.source_name, uvot_data, xrt_data, xrt_ul_data, ymin)

if (is_ogle == True) & (is_xrt == True):

    sc_plots.plot_ogle_xrt_lc_ul(args.source_name, ogle_data, xrt_data, xrt_ul_data, ymin=0)
    sc_plots.plot_ogle_uvot_xrt_lc_ul(args.source_name, ogle_data, uvot_data, xrt_data, xrt_ul_data, ymin=0)
    sc_plots.plot_trunc_ogle_uvot_xrt_lc_ul(args.source_name, truncated_ogle_data, uvot_data, xrt_data, xrt_ul_data, ymin=0)

sc_plots.plot_uvot_lc(args.source_name, uvot_data)


print(f'Process complete. File will be in the Plots folders under various file names.')