# -*- coding: utf-8 -*-
import os
import subprocess

import pandas as pd
import numpy as np

import math

import re

import shutil

from astropy.io import fits
from astropy.table import QTable
import astropy.units as u
from astropy.coordinates import SkyCoord

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
    result = subprocess.run(
        ['bash', '-i', '-c', uvotdetect_command],
        capture_output=True,
        text=True
    )

    # print("STDOUT:\n", result.stdout)
    # print("STDERR:\n", result.stderr)

    return result.stdout

def run_uvotdetect_verbose(uvotdetect_command):

    # Run the command
    result = subprocess.run(
        ['bash', '-i', '-c', uvotdetect_command],
        capture_output=True,
        text=True
    )

    print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)

    return result.stdout
    
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

def run_fkeyprint_verbose(fkeyprint_command):

    result = subprocess.run(
        ['bash', '-i', '-c', fkeyprint_command],
        capture_output=True,
        text=True
    )
    
    print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)

    return result.stdout

def create_uvotunicorr_bash_command(ref_frame, obs_frame, obspath=None):

    if obspath:
        ref_filepath = obspath+f'/sw{ref_frame}uw1_sk.img.gz[1]'
        obs_filepath = obspath+f'/sw{obs_frame}uw1_sk.img.gz[1]'
        ref_reg_filepath = obspath+'/ref.reg'
        obs_reg_filepath = obspath+'/obs.reg'
    else:
        ref_filepath = f'sw{ref_frame}uw1_sk.img.gz[1]'
        obs_filepath = f'sw{obs_frame}uw1_sk.img.gz[1]'
        ref_reg_filepath = 'ref.reg'
        obs_reg_filepath = 'obs.reg'
    
    bash_command = f"""
        bash -c '
        source {os.environ['HEADAS']}/headas-init.sh
        uvotunicorr obsfile={obs_filepath} reffile={ref_filepath} obsreg={obs_reg_filepath} refreg={ref_reg_filepath}
        '
        """

    return bash_command

def run_uvotunicorr(uvotunicorr_command):

    # Run the command
    result = subprocess.run(
        ['bash', '-i', '-c', uvotunicorr_command],
        capture_output=True,
        text=True
    )

    # print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)

    return result.stdout

def run_uvotunicorr_verbose(uvotunicorr_command):

    # Run the command
    result = subprocess.run(
        ['bash', '-i', '-c', uvotunicorr_command],
        capture_output=True,
        text=True
    )

    print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)

    return result.stdout

def create_uvotsource_bash_command(tile_name, obsid, source_reg_file, bkg_reg_file, target_name):

    trunc_obs_filepath = f'./{tile_name}/UVOT/{obsid}/uvot/image/'
    obs_filepath = f'./{tile_name}/UVOT/{obsid}/uvot/image/sw{obsid}uw1_sk.img.gz'
    
    bash_command = f"""
        bash -c '
        source {os.environ['HEADAS']}/headas-init.sh
        uvotsource image="{obs_filepath}" srcreg="{source_reg_file}" bkgreg="{bkg_reg_file}" sigma=5 zerofile=CALDB coinfile=CALDB psffile=CALDB lssfile=CALDB syserr=NO frametime=DEFAULT apercorr=NONE output=ALL outfile="{trunc_obs_filepath + target_name}_source.fits" cleanup=YES clobber=YES chatter=1

        '
        """

    return bash_command

def run_uvotsource(uvotsource_command):

    # Run the command
    result = subprocess.run(
        ['bash', '-i', '-c', uvotsource_command],
        capture_output=True,
        text=True
    )

    # print("STDOUT:\n", result.stdout)
    # print("STDERR:\n", result.stderr)

    return result.stdout

def run_uvotsource_verbose(uvotsource_command):

    # Run the command
    result = subprocess.run(
        ['bash', '-i', '-c', uvotsource_command],
        capture_output=True,
        text=True
    )

    print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)

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
                                
                                    if e >= 0.55:
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
        
def check_aspect_correction(filepath):

    aspect_uncorrected = []

    for path in sorted(os.listdir(filepath)):
        if path == '.DS_Store':
            continue
        else:
            subpath = os.path.join(filepath, path)

            sourcepath_fill = f'uvot/image/sw{path}uw1_sk.img.gz'
            full_sourcepath = os.path.join(subpath, sourcepath_fill)
            
            exists = os.path.exists(full_sourcepath)
            
            if exists == True:
                fkeyprint_command = create_fkeyprint_bash_command(full_sourcepath)

                aspcorr_output = run_fkeyprint(fkeyprint_command)

                if re.search("ASPCORR = 'DIRECT  '", aspcorr_output):
                    continue
                elif re.search("ASPCORR = 'UNICORR '", aspcorr_output):
                    continue
                else:
                    aspect_uncorrected.append(path)
                
            elif exists == False:
                continue

    return aspect_uncorrected

def check_aspect_correction_verbose(filepath):

    aspect_uncorrected = []

    for path in sorted(os.listdir(filepath)):
        if path == '.DS_Store':
            continue
        else:
            subpath = os.path.join(filepath, path)

            sourcepath_fill = f'uvot/image/sw{path}uw1_sk.img.gz'
            full_sourcepath = os.path.join(subpath, sourcepath_fill)
            
            exists = os.path.exists(full_sourcepath)
            
            if exists == True:
                fkeyprint_command = create_fkeyprint_bash_command(full_sourcepath)

                aspcorr_output = run_fkeyprint_verbose(fkeyprint_command)

                if re.search("ASPCORR = 'DIRECT  '", aspcorr_output):
                    continue
                elif re.search("ASPCORR = 'UNICORR '", aspcorr_output):
                    continue
                else:
                    aspect_uncorrected.append(path)
                
            elif exists == False:
                continue

    return aspect_uncorrected

def remove_aspect_uncorrected(in_filepath, out_filepath, aspect_uncorrected_tiles):

    for auct in aspect_uncorrected_tiles:
    
        source = os.path.join(in_filepath, auct)
        destination = out_filepath+'AspectNone'
    
        shutil.move(source, destination)
        
def find_brightest_central_stars(detect_path, num_stars=15, side_buffer=5):

    #open detect.fits and read header into dataframe
    with fits.open(detect_path) as hdul:
        detect_header = hdul[0].header
        
    #read header to find central pointing position
    center_ra = detect_header['RA_PNT'] * u.deg
    center_dec = detect_header['DEC_PNT']* u.deg

    #set up buffers
    center_coords = SkyCoord(ra=center_ra, dec=center_dec, frame='fk5')
    position_angle1 = 0 * u.deg
    position_angle2 = 90 * u.deg
    position_angle3 = 180 * u.deg
    position_angle4 = 270 * u.deg
    sep = side_buffer * u.arcmin

    #create upper and lower ra/dec bounds
    dec_max = center_coords.directional_offset_by(position_angle1, sep).dec.degree
    dec_min = center_coords.directional_offset_by(position_angle3, sep).dec.degree
    
    ra_max = center_coords.directional_offset_by(position_angle2, sep).ra.degree
    ra_min = center_coords.directional_offset_by(position_angle4, sep).ra.degree

    #extract sources from detect.fits
    stars = QTable.read(detect_path).to_pandas()
    stars = stars[(stars['RA'] >= ra_min) & (stars['RA'] <= ra_max)]
    stars = stars[(stars['DEC'] >= dec_min) & (stars['DEC'] <= dec_max)]

    #keep only the 15 brightest sources
    bright_stars = stars.sort_values('MAG', ascending=True)
    bright_stars = bright_stars.iloc[:num_stars+1, :]

    nearby_stars = []

    #loop over all bright central stars
    #use positions to calculate separation between each star
    #remove stars closer together than 1 arcminute
    for i in range(16):
        for j in range(16):
    
            if i != j:
                star1_ra = bright_stars.iloc[i, 0]
                star1_dec = bright_stars.iloc[i, 1]
                star1_coords  = SkyCoord(star1_ra, star1_dec, unit='deg', frame='fk5')
        
                star2_ra = bright_stars.iloc[j, 0]
                star2_dec = bright_stars.iloc[j, 1]
                star2_coords  = SkyCoord(star2_ra, star2_dec, unit='deg', frame='fk5')
        
                sep = star1_coords.separation(star2_coords).to(u.arcsecond) / u.arcsecond
        
                if sep <= 60:
                    nearby_stars.append(j)
                
            else:
                continue
    
    star_indices = [star for star in range(16) if star not in nearby_stars]
    bright_stars = bright_stars.iloc[star_indices, :]

    return bright_stars

def remove_separate_stars(ref_bright_stars, obs_bright_stars):

    sep_frame = pd.DataFrame(columns=obs_bright_stars.index, index=ref_bright_stars.index)

    ref_coords = []
    obs_coords = []
    
    for ind in ref_bright_stars.index:
        ref_star_ra = ref_bright_stars.loc[ind, 'RA']
        ref_star_dec = ref_bright_stars.loc[ind, 'DEC']
    
        ref_star_coords = SkyCoord(ref_star_ra, ref_star_dec, unit='deg', frame='fk5')
        ref_coords.append(ref_star_coords)
    
    for ind in obs_bright_stars.index:
        obs_star_ra = obs_bright_stars.loc[ind, 'RA']
        obs_star_dec = obs_bright_stars.loc[ind, 'DEC']
    
        obs_star_coords = SkyCoord(obs_star_ra, obs_star_dec, unit='deg', frame='fk5')
        obs_coords.append(obs_star_coords)
    
    for obs_ind, obs_star in zip(obs_bright_stars.index, obs_coords):
        for ref_ind, ref_star in zip(ref_bright_stars.index, ref_coords):
    
            sep_frame.loc[ref_ind, obs_ind] = obs_star.separation(ref_star).to(u.arcsecond) / u.arcsecond
    
    sep_frame = sep_frame.where(sep_frame<(30.0*u.arcsecond)).dropna(axis=1, how='all').dropna(axis=0, how='all')

    if len(sep_frame.index) == len(sep_frame.columns):
        
        ref_bright_stars = ref_bright_stars.loc[list(sep_frame.index), :]
        obs_bright_stars = obs_bright_stars.loc[list(sep_frame.columns), :]

    else:
        print("Bright Stars Did Not Match. Please Adjust Filter Parameters and Try Again.")

    return ref_bright_stars, obs_bright_stars

def create_ref_obs_reg_files(ref_bright_stars, obs_bright_stars, outpath=None):

    ref_circles = []
    ref_coords = []
    
    for ind in ref_bright_stars.index:
        ref_ra = ref_bright_stars.loc[ind, 'RA']
        ref_dec = ref_bright_stars.loc[ind, 'DEC']
    
        ref_star_coords = SkyCoord(ref_ra, ref_dec, unit='deg', frame='fk5')
        # region = CircleSkyRegion(star_coords, radius=5*u.arcsecond)
        # region.write('ref.reg', format='ds9')
        ref_circle = f'circle({ref_ra},{ref_dec},5.000")\n'
        ref_circles.append(ref_circle)
        ref_coords.append(ref_star_coords)
        
    if outpath:
        ref_filename = outpath+'/ref.reg'
    else:
        ref_filename = 'ref.reg'
        
    reg_header = '# Region file format: DS9 version 4.1\nfk5\n'

    ref_circles_sum = "".join(ref_circles)
    ref_reg_text = reg_header + ref_circles_sum
    
    with open(ref_filename, mode='w', encoding='utf-8') as reffile:
        reffile.write(ref_reg_text)

    obs_circles = []
    obs_coords = []
    
    for ind in obs_bright_stars.index:
        obs_ra = obs_bright_stars.loc[ind, 'RA']
        obs_dec = obs_bright_stars.loc[ind, 'DEC']
    
        obs_star_coords = SkyCoord(obs_ra, obs_dec, unit='deg', frame='fk5')
        # region = CircleSkyRegion(star_coords, radius=5*u.arcsecond)
        # region.write('ref.reg', format='ds9')
        obs_circle = f'circle({obs_ra},{obs_dec},5.000")\n'
        obs_circles.append(obs_circle)
        obs_coords.append(obs_star_coords)
        
    if outpath:
        obs_filename = outpath+'/obs.reg'
    else:
        obs_filename = 'obs.reg'
    
    obs_circles_sum = "".join(obs_circles)
    obs_reg_text = reg_header + obs_circles_sum
    
    with open(obs_filename, mode='w', encoding='utf-8') as obsfile:
        obsfile.write(obs_reg_text)