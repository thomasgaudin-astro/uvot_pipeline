# -*- coding: utf-8 -*-
import os
import math
import shutil
import tqdm

import numpy as np
import pandas as pd

from astropy.io import fits

class RemoveSmeared():
    def __init__(self, tile_name):
        self.filepath = f'./S-CUBED/{tile_name}/UVOT'
        self.smeared = self.detect_smeared_frames(self.filepath)
        self.remove_smeared(self.filepath, self.smeared)

    def detect_smeared_frames(self, filepath):

        smeared = []
        
        for path in tqdm(os.listdir(filepath)):
            if path == '.DS_Store':
                continue
            else:
                filename = f'{filepath}/{path}/uvot/image/detect.fits'
        
                with fits.open(filename) as hdul:
                    data = hdul[1].data

                detected_frame = pd.DataFrame(columns=['PROF_MAJOR', 'PROF_MINOR', 'FLAGS'])

                for ind, val in enumerate(data):
                    detected_frame.loc[ind, 'PROF_MAJOR'] = val['PROF_MAJOR']
                    detected_frame.loc[ind, 'PROF_MINOR'] = val['PROF_MINOR']
                    detected_frame.loc[ind, 'FLAGS'] = val['FLAGS']

                detected_frame = detected_frame[detected_frame['FLAGS'] == 0]

                a = np.median(detected_frame['PROF_MAJOR'])
                b = np.median(detected_frame['PROF_MINOR'])
    
                c = math.sqrt(a**2 - b**2)
                e = c/a
            
                if e >= 0.5:
                    smeared.append(path)

        return smeared

    def remove_smeared(self, filepath, smeared_obs):

        for smear in smeared_obs:
        
            source = os.path.join(filepath, smear)
            destination = f'./S-CUBED/{tile_name}/Smeared'
        
            shutil.move(source, destination)