#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun  2 14:29:31 2025

@author: tmg6006
"""

import os
import subprocess

import pandas as pd
import numpy as np

import math

import re

import sys

import shutil

from astropy.io import fits

import uvot_pipeline as up

# Set all required environment variables
os.environ['HEADAS'] = '/bulk/pkg/heasoft-6.35.1/aarch64-apple-darwin23.6.0'
os.environ['PFILES'] = f"/tmp/pfiles;{os.environ['HEADAS']}/syspfiles"
os.environ['PLT_DEV'] = '/null'  # Avoid display device errors
os.environ['HEADASNOQUERY'] = 'YES'  # Prevent prompt errors
os.environ['CALDB'] = 'http://heasarc.gsfc.nasa.gov/FTP/caldb'  # Remote CALDB
os.environ['CALDBCONFIG'] = os.path.expanduser('caldb.config')

# Ensure pfiles directory exists
os.makedirs("/tmp/pfiles", exist_ok=True)

#read in list of tiles
tiles = pd.read_csv('scubed_tiles.csv')

#run the pipeline
print('Starting the S-CUBED UVOT Cleaning Pipeline.\n')

#check for preference on aspect correction of frames.
#frames can be automatically corrected or removed if they do not pass the fkeyprint check.
correct_frames = False

while correct_frames == False:
    cf = input("Some frames will not be aspect corrected.\nDo you wish to have the frames automatically corrected? [Y/n] ")

    if cf == 'Y':
        correct_frames = True
        print("\nBad Frames will be corrected.\n")

    elif cf == 'N':
        correct_frames = True
        print("\nBad Frames will be removed.\n")

    else:
        print("Please provide a valid input. [Y/N]")
        
#run full cleaning pipeline for each S-CUBED tile.
for sc_tile in tiles['Tile Name']:

    print(f"Cleaning Data for Tile {sc_tile}.")
    
    filepath = f'./S-CUBED/{sc_tile}/UVOT'

    print("Running uvotdetect.")
    
    for path in sorted(os.listdir(filepath)):
        if path == '.DS_STORE':
            continue
        else:
            subpath = os.path.join(filepath, path)
            
            sourcepath_fill = f'uvot/image/sw{path}uw1_sk.img'
            outpath_fill = 'uvot/image/detect.fits'
            exppath_fill = f'uvot/image/sw{path}uw1_ex.img'
            
            full_sourcepath = os.path.join(subpath, sourcepath_fill)
            full_outpath = os.path.join(subpath, outpath_fill)
            full_exppath = os.path.join(subpath, exppath_fill)

            uvotdetect_command = up.create_uvotdetect_bash_command(full_sourcepath, full_outpath, full_exppath)

            # run_uvotdetect(uvotdetect_command)

    print("uvotdetect is complete.\n")

    print("Detecting Smeared Frames.")

    smeared_frames = up.detect_smeared_frames(sc_tile)

    print(f"Found {len(smeared_frames)} Smeared Frames.")
    print("Removing Smeared Frames.")

    up.remove_smeared(sc_tile, smeared_frames)

    print("Smear Removal is complete.\n")

    print("Checking Frame Aspect Correction.")
    print("Identifying Frames with No Aspect Correction.")

    aspect_uncorrected_frames = up.check_aspect_correction(sc_tile)

    print(f"Found {len(aspect_uncorrected_frames)} Frames in need of Aspect Correction.\n")

    if cf == 'Y':
        print("Correcting Bad Frames.")

    elif cf == 'N':
        print("Removing Bad Frames.")
        # up.remove_aspect_uncorrected(sc_tile, aspect_uncorrected_frames)
    
    print("Aspect Correction Check is complete.\n")