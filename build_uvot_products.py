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

from swifttools.swift_too import Clock

# Set all required environment variables
os.environ['HEADAS'] = '/bulk/pkg/heasoft-6.35.1/aarch64-apple-darwin23.6.0'
os.environ['PFILES'] = f"/tmp/pfiles;{os.environ['HEADAS']}/syspfiles"
os.environ['PLT_DEV'] = '/null'  # Avoid display device errors
os.environ['HEADASNOQUERY'] = 'YES'  # Prevent prompt errors
os.environ['CALDB'] = '/bulk/pkg/caldb'  # Local CALDB
os.environ['CALDBCONFIG'] = '/bulk/pkg/caldb/software/tools/caldb.config'
os.environ['CALDBALIAS'] = '/bulk/pkg/caldb/software/tools/alias_config.fits'

# Ensure pfiles directory exists
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

#insert download products code here

#insert subfolder idenfitication code here

#insert aspect correction parameter initialization here

master_table = pd.DataFrame(columns=['ObsID', 'Filter', 'Snapshot', 'Group Type', 'Group Num', 'Smeared Flag', 'SSS Flag', 'AspCorr Flag'])