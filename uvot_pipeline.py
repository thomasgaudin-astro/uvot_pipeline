# -*- coding: utf-8 -*-
import os
import subprocess

import pandas as pd
import numpy as np

import math

import re

import sys

import shutil

from astropy.io import fits

def create_uvotdetect_bash_command(source_path, output_path, exposure_path):

    # Construct bash command
    bash_command = f"""
    bash -c '
    source {os.environ['HEADAS']}/headas-init.sh
    uvotdetect \\
        infile={source_path} \\
        outfile={output_path} \\
        expfile={exposure_path} \\
        threshold=3 \\
        sexfile=DEFAULT \\
        plotsrc=NO \\
        zerobkg=0.03 \\
        expopt=BETA \\
        calibrate=YES \\
        clobber=YES
    '
    """

    return bash_command

def run_uvotdetect(uvotdetect_command):

    # Run the command
    os.system(uvotdetect_command)
    
def create_fkeyprint_bash_command(source_path):

    fits_path = os.path.abspath(source_path)
    
    # print("Absolute path:", fits_path)
    # print("Exists:", os.path.exists(fits_path))  # Confirm it actually exists!
    
    keyword = "ASPCORR"
    
    command = f"""
    source {os.environ['HEADAS']}/headas-init.sh
    fkeyprint "{fits_path}" {keyword}
    """
    
    return command

def run_fkeyprint(fkeyprint_command):

    result = subprocess.run(
        ['bash', '-i', '-c', fkeyprint_command],
        capture_output=True,
        text=True
    )
    
    # print("STDOUT:\n", result.stdout)
    # print("STDERR:\n", result.stderr)

    return result.stdout

def detect_smeared_frames(tile_name):

    filepath = f'./S-CUBED/{tile_name}/UVOT'

    smeared = []
    
    for path in os.listdir(filepath):
        if path == '.DS_Store':
            continue
        else:
            #print(path)
            subpath = os.path.join(filepath, path)
            for new_path in os.listdir(subpath):
                if new_path == 'uvot':
                    subpath2 = os.path.join(subpath, new_path)
                    for impath in os.listdir(subpath2):
                        if impath == 'image':
                            subpath3 = os.path.join(subpath2, impath)
                            for file in os.listdir(subpath3):
                                if file == 'detect.fits':
    
                                    filename = os.path.join(subpath3, file)
    
    #                                     print(f'Opening File {filename}')
    
                                    with fits.open(filename) as hdul:
                                        head = hdul[0].header
                                        data = hdul[1].data
                    
                                    a = np.mean(data['PROF_MAJOR'])
                                    b = np.mean(data['PROF_MINOR'])
                        
                                    c = math.sqrt(a**2 - b**2)
                                    e = c/a
                                    #print(e)
                                
                                    if e >= 0.5:
                                        smeared.append(path)
    
                                else:
                                    continue
                        else:
                            continue
                else:
                    continue

    return smeared

def remove_smeared(tile_name, smeared_tiles):

    filepath = f'./S-CUBED/{tile_name}/UVOT'

    for smear in smeared_tiles:
    
        source = os.path.join(filepath, smear)
        destination = f'./S-CUBED/{tile_name}/Smeared'
    
        shutil.move(source, destination)
        
def check_aspect_correction(tile_name):

    filepath = f'./S-CUBED/{tile_name}/UVOT'

    aspect_uncorrected = []

    for path in sorted(os.listdir(filepath)):
        if path == '.DS_STORE':
            continue
        else:
            subpath = os.path.join(filepath, path)
            
            sourcepath_fill = f'uvot/image/sw{path}uw1_sk.img'
            full_sourcepath = os.path.join(subpath, sourcepath_fill)

            fkeyprint_command = create_fkeyprint_bash_command(full_sourcepath)

            aspcorr_output = run_fkeyprint(fkeyprint_command)

            if re.search("ASPCORR = 'DIRECT  '", aspcorr_output):
                continue
            
            else:
                aspect_uncorrected.append(subpath)

    return aspect_uncorrected

def remove_aspect_uncorrected(tile_name, aspect_uncorrected_tiles):

    filepath = f'./S-CUBED/{tile_name}/UVOT'

    for auct in aspect_uncorrected_tiles:
    
        source = os.path.join(filepath, auct)
        destination = f'./S-CUBED/{tile_name}/AspectNone'
    
        shutil.move(source, destination)