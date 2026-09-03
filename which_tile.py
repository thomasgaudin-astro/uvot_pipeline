#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thurs Jul 24 10:33:00 2025

@author: tmg6006
"""

import numpy as np
import pandas as pd

from astropy.coordinates import SkyCoord
import astropy.units as u

from sc_uvot import WhichTile

print('Welcome to Closest Tile Finder.')
print('Please enter the coordinates of your target so that we can find the proper S-CUBED tile to clean.\n')

try:
    source_ra = float(input('Source RA (degrees): '))
    source_dec = float(input('Source Dec (degrees): '))
except ValueError:
    print('Error. Source RA and Dec must be in units of degrees.')

WhichTile(source_ra, source_dec)