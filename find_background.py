#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse

from .BackgroundGenerator import BackgroundGenerator

def main():

    parser = argparse.ArgumentParser()

    parser.add_argument(tile_name, 
                        type=str,
                        help="The S-CUBED Tile containing the source that needs a background region"
    )
    parser.add_argument(obsid,
                        type=int,
                        help="The reference observation ID to use for detecting sources that need to be avoided."
    )
    parser.add_argument(source_ra,
                        type=float,
                        help="The Right Ascension of the target source in decimal degrees."
    )
    parser.add_argument(source_dec,
                        type=float,
                        help="The Declination of the target source in decimal degrees."
    )
    