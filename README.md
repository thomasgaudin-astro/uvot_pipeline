# S-CUBED UVOT Photometry Pipeline

This is the homepage for the S-CUBED UVOT photometry pipeline. 
The code in this pipeline is meant to produce 10 year UVW1-Band light curves for point sources in the Small Magellanic Cloud from the 60s exposures that are taken weekly by the Swift SMC Survey (S-CUBED).
Inside this module, there are four separate pieces of code that all have different purposes. 
Running them in order will take you from the RA/Dec coordinates of your source to a plotted light curve.

## Which Tile

The file `which_tile.py` can be run from the terminal to give you the closest S-CUBED tile to the coordinates of your source. 
There are no mandatory arguments that need to be passed to this code for it to work.
Once the file is run, it will prompt you for the RA and Dec of your source in decimal degrees. 
Using those coordinates, it will tell you what the name of the closest S-CUBED tile is to your source and how far from the center of the frame that it is.

Due to the smaller field of view of UVOT when compared to XRT, the UVOT tiles of S-CUBED do not overlap.
There are gaps between the tiles where no data is collected. 
Which Tile will warn you if your source is likely to be found in a gap between tiles.
If you get this warning, you may proceed with photometry, but your results may be sparse. 

## Clean UVOT Data

