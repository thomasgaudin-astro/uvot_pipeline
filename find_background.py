#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse

from BackgroundGenerator import BackgroundGenerator

def main():

    parser = argparse.ArgumentParser()

    parser.add_argument('tile_name', 
                        type=str,
                        help="The S-CUBED Tile containing the source that needs a background region"
    )
    parser.add_argument('obsid',
                        type=int,
                        help="The reference observation ID to use for detecting sources that need to be avoided."
    )
    parser.add_argument('source_name',
                        type=str,
                        help="The name of the source that needs a background region."
    )
    parser.add_argument('source_ra',
                        type=float,
                        help="The Right Ascension of the target source in decimal degrees."
    )
    parser.add_argument('source_dec',
                        type=float,
                        help="The Declination of the target source in decimal degrees."
    )
    parser.add_argument('-v', '--verbose',
                        action='store_true',
                        help="Prints out additional information about the background region search."
    )
    parser.add_argument('-c', '--clobber',
                        action='store_true',
                        help="Overwrites any existing background region files."
    )
    parser.add_argument('-t', '--threshold', 
                        type=float,
                        default=1,
                        help="The sigma threshold for source detection in the background region search."
    )
    parser.add_argument('-o', '--output',
                        action='store_true',
                        help="Saves the excess source regions to a file."
    )
    parser.add_argument('-s', '--shape',
                        type=str,
                        default="circle",
                        help="The shape of the source detection region. Options are 'circle' or 'ellipse'."
    )
    parser.add_argument('-l', '--logscale',
                        action='store_true',
                        help="Uses a logarithmic scale for the source detection image."
    )
    parser.add_argument('-p', '--plotsrc',
                        action='store_true',
                        help="Plots the source detection image with the detected sources overlaid."
    )

    args = parser.parse_args()

    bg = BackgroundGenerator(tile_name=args.tile_name,
                             obsid=args.obsid,
                             source_name=args.source_name,
                             source_ra=args.source_ra,
                             source_dec=args.source_dec,
                             verbose=args.verbose,
                             clobber=args.clobber,
                             threshold=args.threshold,
                             output=args.output,
                             shape=args.shape,
                             logscale=args.logscale,
                             plotsrc=args.plotsrc
    )

if __name__ == "__main__":
    main()