import os
import subprocess
from swifttools.swift_too import TOO, Resolve, ObsQuery, Data

class DownloadUVOT():

    def __init__(self, tile_name, new_tile_name, tile_ra, tile_dec):
        self.tile_name = tile_name
        self.new_tile_name = new_tile_name
        self.tile_ra = tile_ra
        self.tile_dec = tile_dec

        self.undownloaded_files = self.check_for_undownloaded_files(tile_name, new_tile_name, tile_ra, tile_dec)

        if len(self.undownloaded_files) > 0:
            self.download_new_files(self.undownloaded_files, tile_name, tile_ra, tile_dec)

    def check_for_undownloaded_files(self, tile_name, new_tile_name, tile_ra, tile_dec):

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

    def download_new_files(self, undownloaded_files, tile_name, tile_ra, tile_dec):

        #Run ObsQuery for all files in the region of the sky that we are interested in
        query = ObsQuery(ra=tile_ra, dec=tile_dec, radius = 0.18)

        #loop through all queried observations
        #if obsid is in undownloaded_files, download the UVOT data for the observation
        for ind, q in enumerate(query):
            if query[ind].obsid in undownloaded_files:
                Data(obsid=query[ind].obsid, uvot=True, uksdc=True, outdir=f"~/S-CUBED/{tile_name}/UVOT")