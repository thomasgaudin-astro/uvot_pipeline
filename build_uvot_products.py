#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys

import pandas as pd
import numpy as np

import shutil
import uvot_pipeline as up
import argparse
import warnings

from tqdm import tqdm
from sh import gunzip

from astropy.units import UnitsWarning
from astropy.io import fits
from astropy.coordinates import SkyCoord
from astropy.time import Time

from swifttools.swift_too import TOO, Resolve, ObsQuery, Data, Clock

# Set all required environment variables
# needed for MacOS
os.environ['HEADAS'] = '/bulk/pkg/heasoft-6.35.1/aarch64-apple-darwin23.6.0'
os.environ['PFILES'] = f"/tmp/pfiles;{os.environ['HEADAS']}/syspfiles"
os.environ['PLT_DEV'] = '/null'  # Avoid display device errors
os.environ['HEADASNOQUERY'] = 'YES'  # Prevent prompt errors
os.environ['CALDB'] = '/bulk/pkg/caldb'  # Local CALDB
os.environ['CALDBCONFIG'] = '/bulk/pkg/caldb/software/tools/caldb.config'
os.environ['CALDBALIAS'] = '/bulk/pkg/caldb/software/tools/alias_config.fits'

# Ensure pfiles directory exists
# needed for MacOS
os.makedirs("/tmp/pfiles", exist_ok=True)

# Set up arguments that need to be passed to the scripts
parser = argparse.ArgumentParser(description='Options for Clean Tiles Script.')

parser.add_argument('source_name', help="The name of the source. This will be used to name the output photometry file.")
parser.add_argument('source_ra', help="The Right Ascension coordinate of the source in decimal degrees.", type=float)
parser.add_argument('source_dec', help="The Right Ascension coordinate of the source in decimal degrees.", type=float)
parser.add_argument('-v', '--verbose', action='store_true', help='Prints command outputs instead of surpessing them.')
parser.add_argument('-wsl', '--use_wsl', action='store_true', help='Prints command outputs instead of surpessing them.')
parser.add_argument('-mp', '--make_plots', action='store_true', help='Prints command outputs instead of surpessing them.')

args = parser.parse_args()

source_name = args.source_name
source_ra = args.source_ra
source_dec = args.source_dec

#initialize bands
possible_bands = ['uvv', 'ubb', 'uuu', 'uw1', 'um2', 'uw2']

#loop variable initialization
files_located = False
need_rad = True
folder_found = False

#either download files to known folder or point to a known location
while files_located == False:
    download_files = input("Do you wish to download a new copy of the data for this source? [Y/n]")

    #if yes, download data to known folder. Default is yes.
    if (download_files == "") | (download_files.upper == "Y"):
        print("\nDownloading data for all Swift observations with a duration >30s.")
        #grab radius for Swift pointings to download
        while need_rad == True:
            #make sure a valid radius is provided
            try:
                source_radius = input("Please input a download radius for your observations in units of decimal degrees. [0.18] ")
                if source_radius == "":
                    source_rad = 0.18 #degrees
                    need_rad = False
                else:
                    source_rad = float(source_radius)
                    need_rad = False
            except ValueError:
                print("Please input a valid decimal degrees value")
        
        #download data and escape loop
        print(f"Downloading observations within {source_rad} degrees of source position.")
        up.download_uvot_data(source_name, source_ra, source_dec, rad)
        print(f"Download Complete. Data is stored in ./{source_name}/UVOT")
        files_located = True
        main_path = f"./{source_name}/UVOT"

    #if no, make sure data is stored in a location that exists.
    elif download_files.upper == "N":
        #subpath idenfitication code goes here.
        print("Data will not be downloaded. Please ensure that data is stored in a readable location.")

        while folder_found = False:
            path_given = input("Please prodive the filepath of the folder containing all of your data.[.\] ")

            if path_given == "":
                main_path = "./"
                folder_found = True
            else:
                main_path = path_given
                
                if os.path.isdir(main_path) == True:
                    print("Path exists.")
                    folder_found = True
                else:
                    print("Path not found. Please provide a valid path")

        files_located = True
        
    else:
        print("Please pick a valid option. [Y/N]")

#insert aspect correction parameter initialization here
side_buffer, num_stars = initialize_aspect_corrections()

#initialize master table
master_table = pd.DataFrame(columns=['ObsID', 'Filter', 'Snapshot', 'Group Type', 'Group Num', 'Smeared Flag', 'SSS Flag', 'AspCorr Flag'])




