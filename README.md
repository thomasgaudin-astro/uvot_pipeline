# S-CUBED UVOT Photometry Pipeline

This is the homepage for the S-CUBED UVOT photometry pipeline. 
The code in this pipeline is meant to produce 10 year UVW1-Band light curves for point sources in the Small Magellanic Cloud from the 60s exposures that are taken weekly by the Swift SMC Survey (S-CUBED).
Inside this module, there are four separate pieces of code that all have different purposes. 
Running them in order will take you from the RA/Dec coordinates of your source to a plotted light curve.

## Which Tile

The file `which_tile.py` can be run from the terminal to give you the closest S-CUBED tile to the coordinates of your source. 
There are no mandatory arguments that need to be passed to this code for it to work.
Once the file is run, it will prompt you for the RA and Dec of your source in decimal degrees. 
Using those coordinates, it will print the name of the closest S-CUBED tile is to your source and its distance from the center of this closest frame.

Due to the smaller field of view of UVOT when compared to XRT, the UVOT tiles of S-CUBED do not overlap.
There are gaps between the tiles where no data is collected. 
Which Tile will warn you if your source is likely to be found in a gap between tiles.
If you get this warning, you may proceed with photometry, but your results may be sparse. 

## Clean UVOT Data

The file `clean_uvot_tiles.py` is used to ensure that the frames observed by S-CUBED are ready for aperture photometry before proceeding.
S-CUBED frames can have two very costly defects that need to be accounted for.
There are some frames where all stars are smeared due to a failure of the spacecraft to settle during the exposure. Other frames suffer from failed aperture corrections that assign the wrong sky coordinates to each pixel. 
The primary three functions of this code are to download the latest frames taken by the survey, remove any frames that suffer from excessive smearing, and automatically aspect correct any frames that need it.
There is one mandatory argument that is needed for the code to run: an S-CUBED tile name of the format SMC_Jxxxx.x-xxxx. 

Once the file is run, you will be prompted to provide the necessary two parameters that are needed for aspect correction:
- The first parameter is how large you want the radius of your aspect correction region to be. 
  This will set the outer edge where the code will stop looking for stars to use for its aspect correction algorithm. 
  The default value is 7 arcminutes.
- The second parameter is the number of stars that are used to match each frame to a reference image.
  The default value is the 50 brightest stars inside your aspect correction radius.

At the end of the script, it will automatically check to see if all frames that need aspect correction were successfully aspect corrected. 
If all frames were successful, the code will stop running.
However, if there are frames that still need to be corrected, you will be asked if you want to ty again with new correction parameters. 
Typically, reducing the size of the field and decreasing the number of stars used will lead to successful aspect corrections on a second pass.
If you decide that you wish to stop the script instead of proceeding to a second pass of corrections, a file called `bad_frames.csv` will be automatically outputted to your home directory containing a list of all frames that still need aspect corrections so that you can manually inspect them.

Several optional arguments exist to help you reduce the time that it takes for the code to run or to gain more details about the underlying uvot commands being run by FTOOLS:
- *-nd*: This is the "No Detect" option. If you are sure that all frames for the proper S-CUBED tile have had the FTOOLS function `uvotdetect` run on them to identify all stars in the field, you may skip the step that re-runs this function again for all frames.
- *-rb*: This is the "Remove Bad" option. Instead of trying to aspect correct each frame, this option will simply remove frames that are not aspect corrected so that photometry cannot be run for these bad frames.
- *-v*: This is the "Verbose" option. This prints the output of all FTOOLS functions that are run on each frame. 
