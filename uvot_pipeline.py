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

from swifttools.swift_too import TOO, Resolve, ObsQuery, Data

from tqdm import tqdm

import requests
from requests.auth import HTTPBasicAuth

class DownloadError(Exception):
    """Raise when requests status quo does not return 200."""
    pass

def create_uvotdetect_bash_command(source_path, output_path, exposure_path, reg_path):

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
        regfile={reg_path} \\
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

def create_fappend_bash_command(file1_name, file2_name):

    command = f"""
    source {os.environ['HEADAS']}/headas-init.sh
    fappend {file1_name} {file2_name}
    """

    return command

def run_fappend(fappend_command):

    result = subprocess.run(
        ['bash', '-i', '-c', fappend_command],
        capture_output=True,
        text=True
    )
    
    # print("STDOUT:\n", result.stdout)
    # print("STDERR:\n", result.stderr)

    return result.stdout

def run_fappend_verbose(fappend_command):

    result = subprocess.run(
        ['bash', '-i', '-c', fappend_command],
        capture_output=True,
        text=True
    )
    
    print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)

    return result.stdout

def create_uvotunicorr_bash_command(ref_frame, obs_frame, obspath=None):

    if obspath:
        ref_filepath = obspath+f'/sw{ref_frame}uw1_sk.img[1]'
        obs_filepath = obspath+f'/sw{obs_frame}uw1_sk.img[1]'
        ref_reg_filepath = obspath+'/ref.reg'
        obs_reg_filepath = obspath+'/obs.reg'
    else:
        ref_filepath = f'sw{ref_frame}uw1_sk.img[1]'
        obs_filepath = f'sw{obs_frame}uw1_sk.img[1]'
        ref_reg_filepath = 'ref.reg'
        obs_reg_filepath = 'obs.reg'
    
    bash_command = f"""
        bash -c '
        source {os.environ['HEADAS']}/headas-init.sh
        uvotunicorr obsfile={obs_filepath} reffile={ref_filepath} obsreg={obs_reg_filepath} refreg={ref_reg_filepath}
        '
        """

    return bash_command

def create_uvotunicorr_too_bash_command(ref_frame, obs_frame, band, snapshot, obspath=None):

    if obspath:
        ref_filepath = obspath+f'/sw{ref_frame}{band}_sk.img[{snapshot}]'
        obs_filepath = obspath+f'/sw{obs_frame}{band}_sk.img[{snapshot}]'
        ref_reg_filepath = obspath+'/ref.reg'
        obs_reg_filepath = obspath+'/obs.reg'
    else:
        ref_filepath = f'sw{ref_frame}{band}_sk.img[{snapshot}]'
        obs_filepath = f'sw{obs_frame}{band}_sk.img[{snapshot}]'
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
    # print("STDERR:\n", result.stderr)

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

def create_uvotimsum_too_bash_command(source_name, obsid, band, file_type, exclude=None, ref_frame=False):
    
    infile_path = f'./{source_name}/TOO/{obsid}/uvot/image/sw{obsid}{band}_{file_type}.img.gz'

    if ref_frame == True:
        outfile_path = f'./{source_name}/Ref_Frames/{obsid}_{band}_summed.fits'
    else:
        if file_type == 'sk':
            outfile_path = f'./{source_name}/TOO/{obsid}/uvot/image/{band}_summed.fits'
        
        elif file_type == 'ex':
            outfile_path = f'./{source_name}/TOO/{obsid}/uvot/image/{band}_ex_summed.fits'
    
    if exclude == None:
        bash_command = f"""
            bash -c '
            source {os.environ['HEADAS']}/headas-init.sh
            uvotimsum infile="{infile_path}" outfile="{outfile_path}"
            '
            """
    else:
        bash_command = f"""
            bash -c '
            source {os.environ['HEADAS']}/headas-init.sh
            uvotimsum infile="{infile_path}" outfile="{outfile_path}" exclude={exclude}
            '
            """

    return bash_command

def create_uvotimsum_master_ref_bash_command(source_name, group_name):

    infile_path = f'./{source_name}/Ref_Frames/{group_name}_summed.fits'
    outfile_path = f'./{source_name}/Ref_Frames/{group_name}_master.fits'

    bash_command = f"""
            bash -c '
            source {os.environ['HEADAS']}/headas-init.sh
            uvotimsum infile="{infile_path}" outfile="{outfile_path}" exclude=0
            '
            """

    return bash_command

def run_uvotimsum(uvotimsum_command):

    # Run the command
    result = subprocess.run(
        ['bash', '-i', '-c', uvotimsum_command],
        capture_output=True,
        text=True
    )

    # print("STDOUT:\n", result.stdout)
    # print("STDERR:\n", result.stderr)

    return result.stdout

def run_uvotimsum_verbose(uvotimsum_command):

    # Run the command
    result = subprocess.run(
        ['bash', '-i', '-c', uvotimsum_command],
        capture_output=True,
        text=True
    )

    print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)

    return result.stdout

def create_uvotsource_bash_command(tile_name, obsid, source_reg_file, bkg_reg_file, target_name):

    trunc_obs_filepath = f'./S-CUBED/{tile_name}/UVOT/{obsid}/uvot/image/'
    obs_filepath = f'./S-CUBED/{tile_name}/UVOT/{obsid}/uvot/image/sw{obsid}uw1_sk.img'
    exp_filepath  = f'./S-CUBED/{tile_name}/UVOT/{obsid}/uvot/image/sw{obsid}uw1_ex.img.gz'
    
    bash_command = f"""
        bash -c '
        source {os.environ['HEADAS']}/headas-init.sh
        uvotsource image="{obs_filepath}" srcreg="{source_reg_file}" bkgreg="{bkg_reg_file}" sigma=5 zerofile=CALDB coinfile=CALDB psffile=CALDB lssfile=CALDB expfile="{exp_filepath}" syserr=NO frametime=DEFAULT apercorr=NONE output=ALL outfile="{trunc_obs_filepath + target_name}_source.fits" cleanup=YES clobber=YES chatter=1

        '
        """

    return bash_command

def create_uvotsource_too_bash_command(source_name, obsid, band, snapshot, source_reg_file, bkg_reg_file):

    trunc_obs_filepath = f'./{source_name}/TOO/{obsid}/uvot/image/'
    obs_filepath = f'./{source_name}/TOO/{obsid}/uvot/image/sw{obsid}{band}_sk.img[{snapshot}]'
    exp_filepath  = f'./{source_name}/TOO/{obsid}/uvot/image/sw{obsid}{band}_ex.img.gz[{snapshot}]'

    if snapshot == 1:
    
        bash_command = f"""
            bash -c '
            source {os.environ['HEADAS']}/headas-init.sh
            uvotsource image="{obs_filepath}" srcreg="{source_reg_file}" bkgreg="{bkg_reg_file}" sigma=5 zerofile=CALDB coinfile=CALDB psffile=CALDB lssfile=CALDB expfile="{exp_filepath}" syserr=NO frametime=DEFAULT apercorr=NONE output=ALL outfile="{trunc_obs_filepath}{band}_source.fits" cleanup=YES clobber=YES chatter=1

            '
            """
    else:
        bash_command = f"""
            bash -c '
            source {os.environ['HEADAS']}/headas-init.sh
            uvotsource image="{obs_filepath}" srcreg="{source_reg_file}" bkgreg="{bkg_reg_file}" sigma=5 zerofile=CALDB coinfile=CALDB psffile=CALDB lssfile=CALDB expfile="{exp_filepath}" syserr=NO frametime=DEFAULT apercorr=NONE output=ALL outfile="{trunc_obs_filepath}{band}_source{snapshot}.fits" cleanup=YES clobber=YES chatter=1

            '
            """

    return bash_command

def create_uvotsource_summed_bash_command(source_name, obsid, band, source_reg_file, bkg_reg_file):

    trunc_obs_filepath = f'./{source_name}/TOO/{obsid}/uvot/image/'
    obs_filepath = f'./{source_name}/TOO/{obsid}/uvot/image/{band}_summed.fits'
    exp_filepath  = f'./{source_name}/TOO/{obsid}/uvot/image/{band}_ex_summed.fits'
    
    bash_command = f"""
        bash -c '
        source {os.environ['HEADAS']}/headas-init.sh
        uvotsource image="{obs_filepath}" srcreg="{source_reg_file}" bkgreg="{bkg_reg_file}" sigma=5 zerofile=CALDB coinfile=CALDB psffile=CALDB lssfile=CALDB expfile="{exp_filepath}" syserr=NO frametime=DEFAULT apercorr=NONE output=ALL outfile="{trunc_obs_filepath}{band}_source.fits" cleanup=YES clobber=YES chatter=1

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

def check_for_undownloaded_files(tile_name, new_tile_name, tile_ra, tile_dec):

    undownloaded_files = []

    #Run ObsQuery for all files in the region of the sky that we are interested in
    query = ObsQuery(ra=tile_ra, dec=tile_dec, radius = 0.18)

    #loop through all queried observations
    #only check observations where file name is desired S-CUBED tile
    #if directory doesn't exist for observation, append to undownloaded files  
    for ind, q in enumerate(query):
        if (q.targname == new_tile_name) & (q.exposure.total_seconds() > 30):
            obsid = query[ind].obsid
            dirpath = f'./S-CUBED/{tile_name}/UVOT/{obsid}'
            smeared_dirpath = f'./S-CUBED/{tile_name}/Smeared/{obsid}'
            if (os.path.isdir(dirpath) == False) & (os.path.isdir(smeared_dirpath) == False):
                undownloaded_files.append(obsid)

    return undownloaded_files

def download_new_files(undownloaded_files, tile_name, tile_ra, tile_dec):

    #Run ObsQuery for all files in the region of the sky that we are interested in
    query = ObsQuery(ra=tile_ra, dec=tile_dec, radius = 0.18)

    #loop through all queried observations
    #if obsid is in undownloaded_files, download the UVOT data for the observation
    for ind, q in enumerate(query):
        if query[ind].obsid in undownloaded_files:
            Data(obsid=query[ind].obsid, uvot=True, uksdc=True, outdir=f"~/S-CUBED/{tile_name}/UVOT")

def download_uvot_data(source_name, source_ra, source_dec, rad):

    #Run ObsQuery for all files in the region of the sky that we are interested in
    query = ObsQuery(ra=source_ra, dec=source_dec, radius = rad)

    #loop through all queried observations
    #if obsid is in undownloaded_files, download the UVOT data for the observation
    for ind, q in enumerate(query):
        if q.exposure.total_seconds() > 30:
            Data(obsid=query[ind].obsid, uvot=True, outdir=f"~/{source_name}/UVOT")

def initialize_aspect_corrections():

    print('Setting Global Parameters for Aspect Correction:\n')
                
    #change the parameters of the aspect correction process
    sb_needed = True
    ns_needed = True
        
    while sb_needed == True:
        
        side_buffer = input("Please select the distance from the center of the frame that you wish to include: [7]")
        
        if side_buffer == "":
            side_buffer = 7
            sb_needed = False
        else:
            try:
                int(side_buffer)
                sb_needed = False
                side_buffer = int(side_buffer)
            except:
                print("Please pick a valid integer.")
                
    while ns_needed == True:
        
        num_stars = input("Please choose how many stars you wish to select for use in aspect correction: [50]")
        
        if num_stars == "":
            num_stars = 50
            ns_needed = False
        else:
            try:
                int(num_stars)
                ns_needed = False
                num_stars = int(num_stars)
            except:
                print("Please pick a valid integer.")

    return side_buffer, num_stars

def populate_observations_table(path_to_files, obs_table):

    possible_bands = ['uvv', 'ubb', 'uuu', 'uw1', 'um2', 'uw2']

    #get list of obsids. Remove the '.DS_Store' file that gets downloaded with data
    all_filepaths = sorted(os.listdir(path_to_files))
    if '.DS_Store' in all_filepaths:
        all_filepaths.remove('.DS_Store')

    counter = 0
    for obsid in all_filepaths:
        for band in possible_bands:

            full_path = os.path.join(path_to_files, obsid, 'uvot', 'image', f'sw{obsid}{band}_sk.img.gz')
            
            if os.path.exists(full_path) == True:
                hdul = fits.open(full_path)
                num_snapshots = len(hdul) - 1

                for ext in range(1, num_snapshots+1):
                    obs_table.loc[counter, 'ObsID'] = obsid
                    obs_table.loc[counter, 'Filter'] = band
                    obs_table.loc[counter, 'Snapshot'] = ext
                    obs_table.loc[counter, 'Smeared Flag'] = False
                    obs_table.loc[counter, 'SSS Flag'] = False
                    obs_table.loc[counter, 'AspCorr Flag'] = False

                    counter += 1

    print(f'Found {counter} snapshots that will be included in analysis.')
    return obs_table

    

def detect_smeared_frames(tile_name):

    filepath = f'./S-CUBED/{tile_name}/UVOT'

    smeared = []
    
    for path in tqdm(os.listdir(filepath)):
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

                                    detected_frame = pd.DataFrame(columns=['PROF_MAJOR', 'PROF_MINOR', 'FLAGS'])

                                    for ind, val in enumerate(data):
                                        detected_frame.loc[ind, 'PROF_MAJOR'] = val['PROF_MAJOR']
                                        detected_frame.loc[ind, 'PROF_MINOR'] = val['PROF_MINOR']
                                        detected_frame.loc[ind, 'FLAGS'] = val['FLAGS']

                                    detected_frame = detected_frame[detected_frame['FLAGS'] == 0]

                                    a = np.mean(detected_frame['PROF_MAJOR'])
                                    b = np.mean(detected_frame['PROF_MINOR'])
                        
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
        
def check_aspect_correction(filepath):

    aspect_uncorrected = []

    for path in tqdm(sorted(os.listdir(filepath))):
        if path == '.DS_Store':
            continue
        else:
            subpath = os.path.join(filepath, path)

            sourcepath_fill = f'uvot/image/sw{path}uw1_sk.img'
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

    for path in tqdm(sorted(os.listdir(filepath))):
        if path == '.DS_Store':
            continue
        else:
            subpath = os.path.join(filepath, path)

            sourcepath_fill = f'uvot/image/sw{path}uw1_sk.img'
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

def check_direct_corrections(filepath):

    aspect_direct = []

    for path in tqdm(sorted(os.listdir(filepath))):
        if path == '.DS_Store':
            continue
        else:
            subpath = os.path.join(filepath, path)

            sourcepath_fill = f'uvot/image/sw{path}uw1_sk.img'
            full_sourcepath = os.path.join(subpath, sourcepath_fill)
            
            exists = os.path.exists(full_sourcepath)
            
            if exists == True:
                fkeyprint_command = create_fkeyprint_bash_command(full_sourcepath)

                aspcorr_output = run_fkeyprint(fkeyprint_command)

                if re.search("ASPCORR = 'DIRECT  '", aspcorr_output):
                    continue
                else:
                    aspect_direct.append(path)
                
            elif exists == False:
                continue

    return aspect_direct

def check_direct_corrections_verbose(filepath):

    aspect_direct = []

    for path in tqdm(sorted(os.listdir(filepath))):
        if path == '.DS_Store':
            continue
        else:
            subpath = os.path.join(filepath, path)

            sourcepath_fill = f'uvot/image/sw{path}uw1_sk.img'
            full_sourcepath = os.path.join(subpath, sourcepath_fill)
            
            exists = os.path.exists(full_sourcepath)
            
            if exists == True:
                fkeyprint_command = create_fkeyprint_bash_command(full_sourcepath)

                aspcorr_output = run_fkeyprint_verbose(fkeyprint_command)

                if re.search("ASPCORR = 'DIRECT  '", aspcorr_output):
                    continue
                else:
                    aspect_direct.append(path)
                
            elif exists == False:
                continue

    return aspect_direct

def remove_aspect_uncorrected(in_filepath, out_filepath, aspect_uncorrected_tiles):

    for auct in tqdm(aspect_uncorrected_tiles):
    
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
    for i in range(num_stars+1):
        for j in range(num_stars+1):
    
            if i != j:
                star1_ra = bright_stars.iloc[i, 0]
                star1_dec = bright_stars.iloc[i, 1]
                star1_coords  = SkyCoord(star1_ra, star1_dec, unit='deg', frame='fk5')
        
                star2_ra = bright_stars.iloc[j, 0]
                star2_dec = bright_stars.iloc[j, 1]
                star2_coords  = SkyCoord(star2_ra, star2_dec, unit='deg', frame='fk5')
        
                sep = star1_coords.separation(star2_coords).to(u.arcsecond) / u.arcsecond
        
                if sep <= 31:
                    nearby_stars.append(j)
                
            else:
                continue
    
    star_indices = [star for star in range(num_stars) if star not in nearby_stars]
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
    
    sep_frame = sep_frame.where(sep_frame<(30.0)).dropna(axis=1, how='all').dropna(axis=0, how='all')

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

def write_source_reg_files(tile_name, obsid, source_name, source_ra, source_dec):

    #generate source coordinates
    source_coords = SkyCoord(source_ra, source_dec, unit='deg', frame='icrs')
    
    trunc_obs_filepath = f'./S-CUBED/{tile_name}/UVOT/{obsid}/uvot/image/'
    detect_filepath = f'./S-CUBED/{tile_name}/UVOT/{obsid}/uvot/image/detect.fits'
    
    #generate  blank dataframe
    detected_frame = pd.DataFrame(columns=['RA', 'DEC', 'SEP'])
    
    #open detect.fits
    with fits.open(detect_filepath) as hdul:
        head = hdul[0].header
        data = hdul[1].data

    #loop through all the sources in detect.fits, append coordinates to dataframe
    for ind, val in enumerate(data):
        detected_frame.loc[ind, 'RA'] = val['RA']
        detected_frame.loc[ind, 'DEC'] = val['DEC']

    #new .reg filename 
    reg_filename = f'{trunc_obs_filepath + source_name}_source.reg'

    #loop through all the stars in detect.fits
    #find the closest one to coordinate positions of source
    #use those coords to write a new .reg file out in the obs filder
    if len(detected_frame.index) >= 1:
        for ind in detected_frame.index:
            
            #generate a SkyCoord object for each star
            ra = detected_frame.loc[ind, 'RA']
            dec = detected_frame.loc[ind, 'DEC']
            
            star_coords = SkyCoord(ra, dec, unit='deg', frame='fk5')

            #calculate separation to source and append to dataframe
            sep = star_coords.separation(source_coords).to(u.arcsecond)
            detected_frame.loc[ind, 'SEP'] = sep

        #look for star with min separation and grab coordinates of that star
        min_sep = detected_frame['SEP'].idxmin()

        min_ra = detected_frame.loc[min_sep, 'RA']
        min_dec = detected_frame.loc[min_sep, 'DEC']

        #check to see how far away the nearest star is before writing a region file
        #if distance is > 5 arcseconds, no new region file is created.
        if detected_frame.loc[min_sep, 'SEP'] <= (10 * u.arcsecond):
            #generate new region text and write out file
            new_reg_text = f'# Region file format: DS9 version 4.1\nfk5\ncircle({min_ra},{min_dec},5.000")'
        
            with open(reg_filename, mode='w', encoding='utf-8') as regfile:
                regfile.write(new_reg_text)

def find_aspect_none_snapshots(path_to_frame):

    fkeyprint_command = up.create_fkeyprint_bash_command(path_to_frame)

    fkeyprint_out = up.run_fkeyprint(fkeyprint_command)

    corrected = re.findall("# EXTENSION:    [0-9]\nASPCORR = 'DIRECT  '", fkeyprint_out)
    uncorrected = re.findall("# EXTENSION:    [0-9]\nASPCORR = 'NONE    '", fkeyprint_out)
    
    exclude=[]
    
    for frame in uncorrected:
        exclude_frame = re.findall("[0-9]", frame)[0]
        exclude.append(exclude_frame)
    
    if len(exclude) > 0:
        exclude_string = ','.join(exclude)
        return exclude_string
    else:
        print('No snapshots need aspect correction. Excluding no frames from master ref.')
        return None

def create_master_ref_file(source_name, band, ref_files, group_name):
    """
    Takes uvot reference frames and sums them together. 
    INPUT:
        ref_files (list): list of obsids of the relevant reference files.
    """

    #sum first file
    primary_file = ref_files[0]
    primary_imsum_command = up.create_uvotimsum_too_bash_command(source_name, primary_file, band, 'sk', ref_frame=True)

    up.run_uvotimsum(primary_imsum_command)
    os.rename(f'./{source_name}/Ref_Frames/{primary_file}_{band}_summed.fits', 
              f'./{source_name}/Ref_Frames/{group_name}_summed.fits')

    for ref_id in ref_files[1:]:
        imsum_command = up.create_uvotimsum_too_bash_command(source_name, ref_id, band, 'sk', ref_frame=True)
        up.run_uvotimsum(imsum_command)

        outfilename = f'./{source_name}/Ref_Frames/{ref_id}_{band}_summed.fits'

        fappend_command = create_fappend_bash_command(outfilename, 
                                                      f'./{source_name}/Ref_Frames/{group_name}_summed.fits')
        run_fappend(fappend_command)

    
    mastersum_command = create_uvotimsum_master_ref_bash_command(source_name, group_name)
    up.run_uvotimsum(mastersum_command)

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

    uvot_data = pd.read_csv(f'./UVOT_Outputs/{source_name}_uvot_data.txt', header=None, sep=r'\s+', names=['MJD', 'Mag', 'Mag_Err', 'F_lam', 'F_lam_err'])

    return uvot_data

def read_xrt_data(source_name):

    xrt_data = Table.read(f"./XRT_Outputs/{source_name}.qdp", format='ascii.qdp', table_id=0, names=['MJD', 'CR'])
    xrt_ul_data = Table.read(f"./XRT_Outputs/{source_name}.qdp", format='ascii.qdp', table_id=1, names=['MJD', 'CR'])

    xrt_data['MJD_nerr'] = -1*xrt_data['MJD_nerr']
    xrt_data['CR_nerr'] = -1*xrt_data['CR_nerr']

    return xrt_data, xrt_ul_data

# -------------------- The hunt for Red ASPCORR -----------------------------
# This has given me some pause for some time as what I did in the past was a very basic bit of code that used existing fkeyprint code and read the extension
# That on hindsight didnt work to well for two reasons, 1: I was only reading the first extension and not the whole list(whops) 2: Meant that code only worked for WSL, this needs to be universal.

#So what we now do instead is a scan the FITS file itself for the extension and read the proper sheet. I.E. the fits files themselves have extension as the photos do so they may have-- Sheet 0 (Primary) Sheet 1(Image) Sheet 2(Image), etc.
# So we will have to Loop through all sheets for our hunt
def _scan_header_for_aspcorr(file_path):
    """
    Scans for mixed DIRECT and NONE statuses.
    If both exist, it is a 'READYRESUM' (needs fix + re-summing).
    """
    if not file_path: return "NONE"
    try:
        with fits.open(file_path) as hdul:
            # We use a set to get only unique statuses across all sheets (extensions)
            statuses = set()
            for hdu in hdul:
                val = hdu.header.get('ASPCORR', 'NONE')
                statuses.add(str(val).strip().upper())
            
            # --- THE READYRESUM LOGIC ---
            # If it has some good and some bad, it's Ready to be fixed then Re-Summed.
            if 'DIRECT' in statuses and 'NONE' in statuses:
                return 'READYRESUM'
            if 'NONE' in statuses: 
                return 'NONE'
            if 'DIRECT' in statuses: 
                return 'DIRECT'
            return "NONE"
    except:
        return "NONE"

# Essentially the same as above,we are grabing the RA/DEC from the image header. The only funny little thing about this is NAXIS
# A bit of a problem you may run into if you make a few misteps is not all FITS extensions are images, we need to find which ones are. The best way I would find is to look for NAXIS I.E. does the file have Height and Width.
def _get_coords(file_path):
    if not file_path: return None, None
    try:
        with fits.open(file_path) as hdul:
            for hdu in hdul:
                if hdu.header.get('NAXIS', 0) >= 2: #Asking if two NAXIS or more exist in the files, I.E. high and width for a 2d img. Typically called NAXIS1, NAXIS2. if you looked in the file you would find something lik NAXIS=2, NAXIS1=500, NAXIS2=400. or the like.
                    w = WCS(hdu.header) # Thought this would be more important then it was, could get rid of this and change bellow rather easily but eh.
                    cx, cy = hdu.header['NAXIS1']/2.0, hdu.header['NAXIS2']/2.0 #Of course since we know the Width and Height thanks to NAXIS just /2 to get the center of the IMG. In pixels typically.
                    ra, dec = w.all_pix2world(cx, cy, 0) #Convert that pixel data into celestial coordinates
                    return float(ra), float(dec)
            return None, None
    except:
        return None, None


# The Engine of the operation
def _run_core_engine(base_folder=None, save_dir=None):
    if not base_folder:
        base_folder = os.path.normpath(input("1. Path to UVOT raw data: ").strip().strip('"').strip("'"))
    if not save_dir:
        save_dir = os.path.normpath(input("2. Save directory: ").strip().strip('"').strip("'"))
    
    if not os.path.exists(save_dir): os.makedirs(save_dir)

    bands_list = ["uvv", "uuu", "ubb", "um2", "uw1", "uw2"]
    raw_results = []
    
    try:
        top_folders = [f for f in os.listdir(base_folder) if os.path.isdir(os.path.join(base_folder, f))] # Filter to get only Directories. 
    except Exception as e: 
        print(f"Error: {e}"); return None, None, None

    folder_pattern = re.compile(r"(\d{11})")

    for folder in top_folders:
        match = folder_pattern.search(folder)
        if not match: continue
        obsid = match.group(1).zfill(11) 
        
        band_files = {b: {'sum': None, 'sky': None} for b in bands_list}

        for root, _, filenames in os.walk(os.path.join(base_folder, folder)): 
            for f in filenames:
                f_low = f.lower()
                if not any(ext in f_low for ext in ['.img', '.fits', '.gz']): continue
                for band in bands_list:
                    if band in f_low:
                        if "summed" in f_low: 
                            band_files[band]['sum'] = os.path.join(root, f) 
                        elif "_sk" in f_low: 
                            band_files[band]['sky'] = os.path.join(root, f)

        for band, files in band_files.items():
            sum_f, sky_f = files['sum'], files['sky']
            if not sum_f and not sky_f: continue
            
            target_file = sum_f if sum_f else sky_f  
            status_source_file = sky_f if sky_f else sum_f 

            ra, dec = _get_coords(target_file) 
            if ra is None: continue 
            
            raw_results.append({
                "OBSID": obsid, "Band": band, "RA": ra, "Dec": dec,
                "Full_Path": target_file, 
                "Filename": os.path.basename(target_file),
                "ASPCORR": _scan_header_for_aspcorr(status_source_file)
            })

    df = pd.DataFrame(raw_results)
    if df.empty: return None, None, None
    df = df.drop_duplicates(subset=['OBSID', 'Band'], keep='first')

    # --- SPATIAL GROUPING ---
    merged = df.copy()
    merged['Group_ID'] = -1
    group_cnt = 0
    for i in range(len(merged)):
        if merged.iloc[i]['Group_ID'] != -1: continue
        mask = (np.abs(merged['RA']-merged.iloc[i]['RA']) <= 240/3600) & \
               (np.abs(merged['Dec']-merged.iloc[i]['Dec']) <= 240/3600) & \
               (merged['Group_ID'] == -1)
        merged.loc[mask, 'Group_ID'] = group_cnt
        group_cnt += 1

    # --- STATUS REPORTING ---
    def check_status(g):
        # A Reference can be a fully DIRECT file OR a READYRESUM file (since it has DIRECT parts).
        # A group needs work if there is a NONE or a READYRESUM present.
        has_ref = (g['ASPCORR'].isin(['DIRECT', 'READYRESUM'])).any()
        needs_work = (g['ASPCORR'].isin(['NONE', 'READYRESUM'])).any()
        
        status = 'COMPLETED' if not needs_work else ('READY' if has_ref else 'ORPHAN')
        return pd.Series({'Status': status, 'Total_Frames': len(g)})

    summary = merged.groupby(['Group_ID', 'Band']).apply(check_status, include_groups=False).reset_index()
    return merged, summary, save_dir


# --- MODES ---
def swift_automation_mode(base_path=None, save_path=None):
    all_frames, summary, _ = _run_core_engine(base_path, save_path)
    return all_frames, summary

def swift_interactive_mode():
    print("\n=== Swift UVOT Interactive Mode ===")
    all_frames, summary, save_dir = _run_core_engine()
    if all_frames is None: return

    summary_path = _get_unique_filename(os.path.join(save_dir, "workload_summary.csv"))
    summary.to_csv(summary_path, index=False)
    
    print(f"\nSummary saved to: {os.path.basename(summary_path)}")
    print("\n--- WORKLOAD SUMMARY ---")
    print(summary['Status'].value_counts().to_frame())
    
    while True:
        choice = input("\nEnter Group_ID to export (or 'q' to quit): ").lower()
        if choice == 'q': break
        try:
            g_id = int(choice)
            g_data = all_frames[all_frames['Group_ID'] == g_id].sort_values(by='ASPCORR', ascending=False)
            if not g_data.empty:
                detail_path = _get_unique_filename(os.path.join(save_dir, f"Group_{g_id}_Details.csv"))
                g_data.to_csv(detail_path, index=False)
                print(f"Exported: {os.path.basename(detail_path)}")
                print(g_data[['OBSID', 'Band', 'ASPCORR', 'Filename']].to_string(index=False))
        except: print("Invalid ID.")
    
    