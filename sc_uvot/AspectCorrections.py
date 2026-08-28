# -*- coding: utf-8 -*-
import os
import re
import shutil
import tqdm

from astropy.coordinates import SkyCoord
from astropy.io import fits
from astropy.table import QTable
import astropy.units as u

from .FTOOLSCommands import create_fkeyprint_bash_command, run_fkeyprint, run_fkeyprint_verbose

class AspectCorrections():
    def __init__(self, tile_name, verbose=False, remove_bad=False, num_stars=200,  side_buffer=10):
        self.tile_name = tile_name
        self.verbose = verbose
        self.remove_bad = remove_bad
        self.num_stars = num_stars
        self.side_buffer = side_buffer

        self.filepath = f'./S-CUBED/{tile_name}/UVOT' 

        # check the aspect correction status of all the observations in the tile 
        # return a list of uncorrected observations and a list of direct corrected observations
        # return the number of uncorrected observations
        if self.verbose == True:
            self.aspect_uncorrected = self.check_aspect_correction_verbose(self.filepath)
            self.aspect_direct = self.check_direct_corrections_verbose(self.filepath)
        else:
            self.aspect_uncorrected = self.check_aspect_correction(self.filepath)
            self.aspect_direct = self.check_direct_corrections(self.filepath)
        self.num_uncorrected = len(self.aspect_uncorrected)

        # honestly can't remember why these lines are needed, but they are
        self.new_all_filepaths = sorted(os.listdir(self.filepath))
        if '.DS_Store' in self.new_all_filepaths:
            self.new_all_filepaths.remove('.DS_Store')

        print(f"Found {self.num_uncorrected} Frames in need of Aspect Correction.\n")

        # if there are no frames in need of aspect correction, then we are done
        if self.num_uncorrected == 0:
            print("No frames to correct.\n")

        else:
            # if there are frames to correct, but we want them removed, then remove them and move on      
            if self.remove_bad == True:
                print("Removing Bad Frames.")
                
                self.out_filepath = f'./S-CUBED/{self.tile_name}/AspectNone'
                self.remove_aspect_uncorrected(self.filepath, self.out_filepath, self.aspect_uncorrected_frames)

            # most of the time we want them corrected, and there is a lot needed to make that happen
            else:
                print("Correcting Bad Frames.")
                
                #list the direct frames
                direct_frames = [frame for frame in self.new_all_filepaths if frame in self.aspect_direct]
                
                #reference frame will be first corrected frame that exists
                self.ref_frame = self.aspect_direct[0]
                self.ref_path = os.path.join(self.filepath, self.ref_frame, 'uvot/image/detect.fits')
    
                self.detect_frame_exists = os.path.exists(self.ref_path)

                #define a counter in case we need to go searching for a reference frame 
                counter = 0

                #if first reference frame didn't exist, then we go grab the first one that does 
                while self.detect_frame_exists == False:
    
                    counter += 1
                    
                    print(len(self.aspect_direct))
                    print(counter)
    
                    self.ref_frame = self.aspect_direct[counter]
                    self.ref_path = os.path.join(self.filepath, self.ref_frame, 'uvot/image/detect.fits')

                    #when we find a refence frame that exists, break the loop
                    self.detect_frame_exists = os.path.exists(self.ref_path)

                print(f'The Reference Frame is {self.ref_frame}')
                #generate path to the reference frame detect.fits
                self.ref_detect_path = f'{filepath}/{self.ref_frame}/uvot/image/detect.fits'
                #generate path to reference image
                self.ref_file_path = f'{filepath}/{self.ref_frame}/uvot/image/sw{self.ref_frame}uw1_sk.img'
                #find brightest stars in the center of the reference frame
                self.ref_bright_stars = self.find_brightest_central_stars(self.ref_detect_path, num_stars=self.num_stars, side_buffer=self.side_buffer)

                





    def check_aspect_correction(self, filepath):

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

    def check_aspect_correction_verbose(self, filepath):

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

    def check_direct_corrections(self, filepath):

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
                        aspect_direct.append(path)
                    else:
                        continue
                    
                elif exists == False:
                    continue

        return aspect_direct

    def check_direct_corrections_verbose(self, filepath):

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
                        aspect_direct.append(path)
                    else:
                        continue
                    
                elif exists == False:
                    continue

        return aspect_direct

    def remove_aspect_uncorrected(self, in_filepath, out_filepath, aspect_uncorrected_tiles):

        for auct in tqdm(aspect_uncorrected_tiles):
        
            source = f'{in_filepath}/{auct}'
            destination = out_filepath

            if os.path.isdir(f'{out_filepath}/{auct}'):
                shutil.rmtree(source)
            else:
                shutil.move(source, destination)
            
    def find_brightest_central_stars(self, detect_path, num_stars=200, side_buffer=10):

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
