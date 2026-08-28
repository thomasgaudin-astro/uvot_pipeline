# -*- coding: utf-8 -*-
import os
import subprocess

import pandas as pd
import numpy as np

import math

import re

import shutil

from astropy.io import fits
from astropy.table import QTable, Table
import astropy.units as u
from astropy.coordinates import SkyCoord

from concurrent.futures import ThreadPoolExecutor, as_completed

from tqdm import tqdm

import requests
from requests.auth import HTTPBasicAuth

class DownloadError(Exception):
    """Raise when requests status quo does not return 200."""
    pass
        
def single_uvotdetect(filepath, path, verbose=False):
    subpath = os.path.join(filepath, path)
    
    sourcepath_fill = f'uvot/image/sw{path}uw1_sk.img.gz'
    outpath_fill = 'uvot/image/detect.fits'
    exppath_fill = f'uvot/image/sw{path}uw1_ex.img.gz'
    detectpath_fill = 'uvot/image/detect.reg'
    
    full_sourcepath = os.path.join(subpath, sourcepath_fill)
    full_outpath = os.path.join(subpath, outpath_fill)
    full_exppath = os.path.join(subpath, exppath_fill)
    full_detectpath = os.path.join(subpath, detectpath_fill)

    uvotdetect_command = create_uvotdetect_bash_command(full_sourcepath, full_outpath, full_exppath, full_detectpath)

    if verbose==True:
        run_uvotdetect_verbose(uvotdetect_command)
    else:
        run_uvotdetect(uvotdetect_command)

def single_alternate_uvotdetect(filepath, path, verbose=False):
    subpath = os.path.join(filepath, path)
    
    sourcepath_fill = f'uvot/image/sw{path}uw1_sk.img.gz'
    outpath_fill = 'uvot/image/detect.fits'
    exppath_fill = f'uvot/image/sw{path}uw1_ex.img.gz'
    detectpath_fill = 'uvot/image/detect1.reg'
    
    full_sourcepath = os.path.join(subpath, sourcepath_fill)
    full_outpath = os.path.join(subpath, outpath_fill)
    full_exppath = os.path.join(subpath, exppath_fill)
    full_detectpath = os.path.join(subpath, detectpath_fill)

    uvotdetect_command = create_uvotdetect_bash_command(full_sourcepath, full_outpath, full_exppath, full_detectpath)

    if verbose==True:
        run_uvotdetect_verbose(uvotdetect_command)
    else:
        run_uvotdetect(uvotdetect_command)

def single_uvotsource(tile, obsid, source_name, source_reg, bkg_reg, verbose=False):
    #path to improved source region files
    reg_filepath = f'./S-CUBED/{tile}/UVOT/{obsid}/uvot/image/{source_name}_source.reg'

    #check to make sure new region file exists
    if os.path.exists(reg_filepath) == True:
        # Write command for uvotsource using new region file
        uvotsource_command = create_uvotsource_bash_command(tile, obsid, reg_filepath, bkg_reg, source_name)
    else:
        # Write command for uvotsource using old region file if new one cannot be found
        uvotsource_command = create_uvotsource_bash_command(tile, obsid, source_reg, bkg_reg, source_name)
    
    if verbose == True:
        run_uvotsource_verbose(uvotsource_command)
    else:
        run_uvotsource(uvotsource_command)

def download_ogle_data(ogle_name, source_name):

    ogle = requests.get(f'https://www.astrouw.edu.pl/ogle/ogle4/xrom/{ogle_name}/phot.dat')

    if ogle.status_code != 200:
        raise DownloadError("An Error occurred when downloading the file. Please check the name of the OGLE Source and try again.")
    else:
        ogle_local_filename = f"./OGLE_Outputs/{source_name}.dat"
        with open(ogle_local_filename, 'wb') as f:
            for chunk in ogle.iter_content(chunk_size=8192):
                f.write(chunk)

def download_xrt_data(xrt_num, source_name):

    xrt = requests.get(f'https://www.swift.ac.uk/SMC/data/source{xrt_num}/curve/PC_incbad.qdp', auth=HTTPBasicAuth('smc', 'T1le_th3_$MC'))

    if xrt.status_code != 200:
        raise DownloadError("An Error occurred when downloading the file. Please check the number of the XRT Source and try again.")
    else:
        xrt_local_filename = f"./XRT_Outputs/{source_name}.qdp"
        with open(xrt_local_filename, 'wb') as f:
            for chunk in xrt.iter_content(chunk_size=8192):
                f.write(chunk)

def read_ogle_data(source_name):

    ogle_data = pd.read_csv(f'./OGLE_Outputs/{source_name}.dat', sep=r'\s+', header=None, names=['Time', 'I', 'I_Err', 'Seeing', 'Sky'])
    
    ogle_data['MJD'] = ogle_data['Time'] - 2400000

    return ogle_data

def read_uvot_data(source_name):

    uvot_data = pd.read_csv(f'./UVOT_Outputs/{source_name}_uvot_data.txt', header=None, sep=r'\s+', names=['MJD', 'Mag', 'Mag_Err', 'F_lam', 'F_lam_err', 'F_lam_coin_lim', 'OBSID'])

    return uvot_data

def read_xrt_data(source_name):

    xrt_data = Table.read(f"./XRT_Outputs/{source_name}.qdp", format='ascii.qdp', table_id=0, names=['MJD', 'CR'])
    xrt_ul_data = Table.read(f"./XRT_Outputs/{source_name}.qdp", format='ascii.qdp', table_id=1, names=['MJD', 'CR'])

    xrt_data['MJD_nerr'] = -1*xrt_data['MJD_nerr']
    xrt_data['CR_nerr'] = -1*xrt_data['CR_nerr']

    return xrt_data, xrt_ul_data


    
    