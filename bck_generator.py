# ============================================================================
# bck_generator.py
#
# Tool for locating "clean" background regions near UV source targets in
# Swift/UVOT-style FITS images. Workflow:
#   1. find_sources()          -> detect excess/UV sources above background
#                                  in an image (via sep) and write a DS9
#                                  region file of the detected excess.
#   2. find_valid_background() -> search outward from a target coordinate
#                                  for a background circle that doesn't
#                                  overlap any detected excess source.
#   3. image_plotter()         -> make a diagnostic plot showing the target,
#                                  chosen background circle, and excess
#                                  regions overlaid on the image.
# The bottom of the file is a command-line entry point (argparse) that wires
# these pieces together for either a single image or a whole directory tree
# of images/filters ("multi" mode).
# ============================================================================

# Standard library
import argparse
from pathlib import Path

# Numerical / data science
import numpy as np
import pandas as pd

# Plotting
import matplotlib.pyplot as plt
from matplotlib import rcParams
from matplotlib.patches import Circle, Ellipse

# Astronomy / FITS / imaging
import fitsio
import sep
from astropy.io import fits
from astropy.table import Table
from astropy.wcs import WCS
from astropy.coordinates import SkyCoord  # High-level coordinates
import astropy.units as u
from astropy.coordinates import Angle

import warnings
from astropy.wcs import FITSFixedWarning
warnings.simplefilter('ignore', FITSFixedWarning)

# Regions
from regions import EllipseSkyRegion, CircleSkyRegion

def index_to_percentage(i, n, steps=10, phrase="% Complete"):
    """
    Emit a progress percentage at evenly spaced index positions.

    The function divides the index range [0, n-1] into `steps` evenly spaced
    checkpoints. When the current index `i` matches one of those checkpoints,
    a formatted percentage string is printed. Otherwise, nothing happens.

    Parameters
    ----------
    i : int
        Current index (must satisfy 0 <= i < n).
    n : int
        Total number of indices.
    steps : int, optional
        Number of progress updates to emit (default is 10).
        If steps > n, it is clamped to n.
    phrase : str, optional
        Suffix appended to the percentage output
        (default is "% Complete").

    Raises
    ------
    ValueError
        If `i` is outside the valid range or `steps` is not positive.

    Example
    -------
    >>> for i in range(10):
    ...     index_to_percentage(i, 10, steps=4)
    0% Complete
    20% Complete
    50% Complete
    70% Complete
    """
    if not (0 <= i < n):
        raise ValueError(f"i must be in range [0, {n - 1}]")
    if steps <= 0:
        raise ValueError("steps must be positive")

    steps = min(steps, n)

    # Indices at which progress updates should be emitted
    step_indices = np.linspace(0, n - 1, num=steps, dtype=int)

    # Corresponding percentage values (never reaches 100%)
    percentages = np.linspace(0, 100 * (n - 1) / n, num=steps)

    # Emit progress only at step boundaries
    for idx, step_i in enumerate(step_indices):
        if i == step_i:
            print(f"{int(percentages[idx])}{phrase}")
            break

def find_sources(filename, outreg="excess.reg", threshold=1,
                 verbose=True, output=True,
                 shape="circle", logscale=True, plotsrc=True):
    
    """
    Finds all excess UV sources above the background.

    Parameters
    ----------
    filename : str
        Name of fits UV image file 
    outreg : str
        Output region name for UV excess (default is 'excess.reg')
    threshold : float
        Threshold sigma for source detection (default is 1.0)
    verbose : bool
        Give outputs when things go wrong (default is True)
    output : bool
        Output region files? (default is True)
    shape : str
        Return shape of sources with 'circle' or 'ellipse'. (default is ';circle')
    logscale : bool
        Search for sources using a logarithmic [True] or linear [False] method (default is True)
    plotsrc : bool
        Output matplot images of source (default is True)

    Return
    ------
    excess,excess_pxl : list,list 
        Two output structured arrays of circles/ellipses of the sources. Excess is in fk5 degree format with RA,DEC,R for circle and 
        RA,DEC,SMaj,SMin,Angl for ellipse, and excess_pxl is in pixel format with X,Y,a,b,theta.
        RA : float
            Right Ascension in J2000 with units of degrees
        DEC : float
            Declination in J2000 with units of degrees
        R : float
            Radius with units of degrees
        SMaj : float
            Semi-Major axis at 3-sig in units of degrees
        SMin : float
            Semi-Minor axis at 3-sig in units of degrees
        Angl : float
            Position Angle East of North in units of degrees
        X : float
            X-axis coordinate
        Y : float
            Y-axis coordinate
        a : float
            Semi-major axis at 1-sig
        b : float
            Semi-minor axis at 1-sig
        theta : float
            Position angle in degrees

    """

    if not Path(filename).is_file():
        raise ValueError(f'{filename} could not be found.')
    if threshold <= 0:
        raise ValueError('Threshold must be > 0.')

    data = fitsio.read(filename)
    hdu = fits.open(filename)[1]
    w = WCS(hdu.header)

    # --- Log or linear ---
    if logscale:
        lindata = data
        with np.errstate(divide='ignore'):
            data = np.log10(np.array(data))
    else:
        lindata = data

    # --- Background estimation ---
    bkg = sep.Background(data)

    if verbose:
        # get a "global" mean and noise of the image background:
        print(f'{bkg.globalback:.5f} background mean value')
        print(f'{bkg.globalrms:.5f} background noise')

    # subtract the background
    data_sub = data - bkg
    nonSUB = [d for d in np.array(data_sub).flatten() if not np.isneginf(d)]

    # total objects detected
    objects = sep.extract(data_sub, threshold, err=bkg.globalrms)

    if verbose:
        print(f'{len(objects)} objects found above {threshold}σ.')

    # Plot the sources
    if plotsrc:
        minSUB = np.min(nonSUB)
        for i in range(np.shape(data_sub)[0]):
            for j in range(np.shape(data_sub)[1]):
                if bool(np.isneginf(data_sub[i][j])): 
                    data_sub[i][j] = minSUB
                    
        # plot background-subtracted image
        fig, ax = plt.subplots()
        m, s = np.mean(nonSUB), np.std(nonSUB)
        im = ax.imshow(data_sub, interpolation='nearest', cmap='gray',
                    vmin=m-2*s, vmax=m+2*s, origin='lower')
        
        fig.colorbar(im, ax=ax)
        np.random.shuffle(objects)
        
        for i in range(len(objects)):
            e = Ellipse(xy=(objects['x'][i], objects['y'][i]),
                        width=6*objects['a'][i],
                        height=6*objects['b'][i],
                        angle=objects['theta'][i] * 180. / np.pi)
            e.set_facecolor('none')
            e.set_edgecolor('red')
            e.set_lw(2)
            ax.add_artist(e)

            # if i > 50: break

        plt.show()

    # --- Allocate structured arrays ---
    n = len(objects)

    if shape == "circle":

        sky_dtype = np.dtype([
            ("RA", "f8"),
            ("DEC", "f8"),
            ("R", "f8")      # degrees (3σ)
        ])

    else:

        sky_dtype = np.dtype([
            ("RA", "f8"),
            ("DEC", "f8"),
            ("SMaj", "f8"),     # degrees (3σ)
            ("SMin", "f8"),
            ("Angl", "f8")  # degrees
        ])

    pixel_dtype = np.dtype([
        ("X", "f8"),
        ("Y", "f8"),
        ("a", "f8"),        # 1σ semi-major (pixels)
        ("b", "f8"),
        ("theta", "f8")     # degrees
    ])

    excess = np.zeros(n, dtype=sky_dtype)
    excess_pxl = np.zeros(n, dtype=pixel_dtype)

    reg_lines = (
        '# Region file format: DS9 version 4.1\n'
        'global color=red dashlist=8 3 width=1 font="helvetica 10 normal roman" '
        'select=1 highlite=1 dash=0 fixed=0 edit=1 move=1 delete=1 include=1 source=1\n'
        'fk5\n'
    )

    # --- Fill arrays ---
    for i, obj in enumerate(objects):

        x = float(obj["x"])
        y = float(obj["y"])

        c0 = w.pixel_to_world(x, y)
        ra = c0.fk5.ra.deg
        dec = c0.fk5.dec.deg

        a3 = float(3 * obj["a"])   # 3σ semi-major (pixels)
        b3 = float(3 * obj["b"])
        theta_deg = obj["theta"] * 180 / np.pi

        # --- Pixel array (store 1σ) ---
        excess_pxl[i] = (
            x,
            y,
            float(obj["a"]),
            float(obj["b"]),
            theta_deg
        )

        if shape == "circle":
            r_deg = (a3 / 3600.0)
            excess[i] = (ra, dec, r_deg)

        else:
            a_deg = a3 / 3600.0
            b_deg = b3 / 3600.0
            excess[i] = (ra, dec, a_deg, b_deg, theta_deg)

        # --- DS9 output ---
        reg_lines += f'ellipse({ra},{dec},{a3}",{b3}",{theta_deg})\n'

    if output:
        with open(outreg, "w") as outfile:
            outfile.write(reg_lines)

    return excess, excess_pxl

def Make_Ellipse(RA, DEC, SMaj, SMin, Angl,
                 sky_frame="fk5",
                 ra_unit=u.deg,
                 dec_unit=u.deg,
                 size_unit=u.arcsec,
                 angle_unit=u.deg):
    """
    Generate a list of EllipseSkyRegion objects in RA/Dec.

    Parameters
    ----------
    RA, DEC : float, Quantity, or array-like
        Right ascension and declination.
    SMaj, SMin : float, Quantity, or array-like
        Semi-major and semi-minor axes.
    Angl : float, Quantity, or array-like
        Position angle (East of North).
    sky_frame : str
        Coordinate frame (default: 'fk5').

    Returns
    -------
    list of EllipseSkyRegion
    """

    # Convert inputs to arrays
    RA = np.atleast_1d(RA)
    DEC = np.atleast_1d(DEC)
    SMaj = np.atleast_1d(SMaj)
    SMin = np.atleast_1d(SMin)
    Angl = np.atleast_1d(Angl)

    # Broadcast length
    n = max(len(RA), len(DEC), len(SMaj), len(SMin), len(Angl))

    def broadcast(arr):
        if len(arr) == 1:
            return np.repeat(arr, n)
        return arr

    RA = broadcast(RA)
    DEC = broadcast(DEC)
    SMaj = broadcast(SMaj)
    SMin = broadcast(SMin)
    Angl = broadcast(Angl)

    # Attach units if needed
    if not isinstance(RA[0], u.Quantity):
        RA = RA * ra_unit
    if not isinstance(DEC[0], u.Quantity):
        DEC = DEC * dec_unit
    if not isinstance(SMaj[0], u.Quantity):
        SMaj = SMaj * size_unit
    if not isinstance(SMin[0], u.Quantity):
        SMin = SMin * size_unit
    if not isinstance(Angl[0], u.Quantity):
        Angl = Angl * angle_unit

    regions = []

    for ra, dec, sma, smi, ang in zip(RA, DEC, SMaj, SMin, Angl):

        center = SkyCoord(ra, dec, frame=sky_frame)

        ellipse = EllipseSkyRegion(
            center=center,
            width=2 * sma,     # full axis
            height=2 * smi,    # full axis
            angle=ang
        )

        regions.append(ellipse)

    return regions

def Make_Circle(RA, DEC, R,
                 sky_frame="fk5",
                 ra_unit=u.deg,
                 dec_unit=u.deg,
                 size_unit=u.arcsec):
    """
    Generate a list of EllipseSkyRegion objects in RA/Dec.

    Parameters
    ----------
    RA, DEC : float, Quantity, or array-like
        Right ascension and declination.
    R : float, Quantity, or array-like
        Radius.
    sky_frame : str
        Coordinate frame (default: 'fk5').

    Returns
    -------
    list of EllipseSkyRegion
    """

    # Convert inputs to arrays
    RA = np.atleast_1d(RA)
    DEC = np.atleast_1d(DEC)
    R = np.atleast_1d(R)

    # Broadcast length
    n = max(len(RA), len(DEC), len(R))

    def broadcast(arr):
        if len(arr) == 1:
            return np.repeat(arr, n)
        return arr

    RA = broadcast(RA)
    DEC = broadcast(DEC)
    R = broadcast(R)

    # Attach units if needed
    if not isinstance(RA[0], u.Quantity):
        RA = RA * ra_unit
    if not isinstance(DEC[0], u.Quantity):
        DEC = DEC * dec_unit
    if not isinstance(R[0], u.Quantity):
        R = R * size_unit

    regions = []

    for ra, dec, r in zip(RA, DEC, R):

        center = SkyCoord(ra, dec, frame=sky_frame)

        circle = CircleSkyRegion(
            center=center,
            radius=2 * r
        )

        regions.append(circle)

    return regions


def angles_to_ellipses(target_center, ellipse_regions):
    """
    Returns array of position angles (east of north)
    from target_center to each ellipse center.
    """
    # Gather all ellipse centers into a single SkyCoord array so the
    # position angle from target_center to every ellipse can be computed
    # in one vectorized call.
    centers = SkyCoord([e.center.ra for e in ellipse_regions],
                       [e.center.dec for e in ellipse_regions],
                       frame=target_center.frame)

    return target_center.position_angle(centers)

def circle_intersects_ellipse(circle_center, circle_radius, ellipse, wcs, n_samples=72):
    """
    Returns True if circle intersects ellipse.
    Uses boundary sampling + ellipse.contains().
    """

    # Sample points along circle boundary
    thetas = np.linspace(0, 2*np.pi, n_samples, endpoint=False)

    # Project each sampled angle outward from the circle's center by the
    # circle's radius to get actual sky coordinates on the circle boundary.
    boundary_points = circle_center.directional_offset_by(
        thetas * u.rad,
        circle_radius
    )

    # Check if any boundary point lies inside ellipse
    # (approximate test: true intersection would also need to check points
    # along the ellipse boundary, but this is sufficient when the circle is
    # small relative to the ellipse / search step size).
    contained = ellipse.contains(boundary_points,wcs)

    return np.any(contained)

def circle_intersects_circle(c1, r1, c2, r2):
    # Two circles intersect (or overlap) if the angular separation between
    # their centers is less than the sum of their radii.
    return c1.separation(c2) < (r1 + r2)

def find_valid_background(excess, target_center,
                          target_radius=10*u.arcsec,
                          bck_radius=10*u.arcsec,
                          step_size=1*u.arcsec,
                          dist_limit=100*u.arcsec,
                          max_iter=None,
                          suffix="",
                          filter="",
                          output=True,
                          verbose=True,
                          clobber=True,
                          directory=Path(".")):
    """
    Determine a valid background circle location around a target source.

    This function searches radially outward from a target sky position to
    identify the nearest location where a background circle of radius
    ``bck_radius`` does not intersect any detected excess sources and does
    not overlap the target source region.

    The search proceeds by stepping outward in distance from
    ``target_radius + bck_radius`` up to ``dist_limit``, testing candidate
    positions along position angles defined by the relative angles between
    the target and detected excess sources.

    If a valid location is found, its coordinates are optionally written
    to a DS9 region file and returned.

    Parameters
    ----------
    excess : numpy structured array
        Structured array containing detected excess sources. Required columns:

        - For circular sources:
            'RA', 'DEC', 'R'
        - For elliptical sources:
            'RA', 'DEC', 'SMaj', 'SMin', 'Angl'

        RA and DEC must be in degrees. Sizes must be in degrees.

    target_center : array-like or SkyCoord
        Target sky coordinates as (RA, DEC). Values may be floats (degrees)
        or `astropy.units.Quantity`.

    target_radius : `~astropy.units.Quantity`, optional
        Radius of the target source region. Default is 10 arcsec.

    bck_radius : `~astropy.units.Quantity`, optional
        Radius of the background circle to place. Default is 10 arcsec.

    step_size : `~astropy.units.Quantity`, optional
        Radial increment used when searching outward. Default is 1 arcsec.

    dist_limit : `~astropy.units.Quantity`, optional
        Maximum search distance from the target center. Default is 100 arcsec.

    max_iter : int
        Maximum number of allowed theta steps. Default is None
        
    suffix : str, optional
        Optional suffix appended to the output region filename.

    filter : str, optional
        Optional filter label appended to the output region filename.

    output : bool, optional
        If True, write a DS9 region file containing the selected
        background circle.

    verbose : bool, optional
        If True, print diagnostic information.

    clobber : bool, optional
        If False and the output region file already exists, the background
        position is read from file and returned without recomputation.

    directory : str or Path, optional
        Absolute directory in which to look for/write the output region
        file. Default is the current directory.

    Returns
    -------
    tuple or None
        (RA, DEC) of the valid background circle center in degrees,
        or None if no valid location is found within ``dist_limit``.

    Notes
    -----
    - Position angles are computed east of north using spherical geometry.
    - Elliptical excess sources are converted to `EllipseSkyRegion`
      objects and tested for intersection with the candidate circle.
    - Intersection testing uses boundary sampling for geometric accuracy.
    - The algorithm reduces angular search space by evaluating only
      directions corresponding to excess source position angles.
    - All computations are performed in the FK5 coordinate frame.

    Raises
    ------
    TypeError
        If the input structured array does not contain the required columns.

    """
    # Normalize suffix/filter labels so the output filename gets an
    # underscore separator only when a non-empty label is actually given.
    if not "_" in suffix and suffix != "": suffix = f'_{suffix}'
    if not "_" in filter and filter != "": filter = f'_{filter}'

    outfile = Path(directory) / f"bck{suffix}{filter}.reg"

    # If a region file already exists and we're not overwriting (clobber),
    # just read the previously computed background center back out instead
    # of redoing the search.
    if Path(outfile).is_file() and not clobber: 
        with open(outfile,"r") as file:
            openlines = file.readlines()
            for line in openlines:
                if "circle" in line:
                    ra = float(line.split(",")[0].replace("circle(",""))
                    dec = float(line.split(",")[1])
                    if verbose:
                        if filter != "":
                            print(f"Background center {filter.replace("_","")}: ({ra},{dec})")
                        else:
                            print(f"Background center: ({ra},{dec})")
                    return (ra,dec)
    # if type(target_center) == SkyCoord: 
    #     c0 = target_center
    # elif not isinstance(target_center[0],u.Quantity):
    #     print(target_center)
    #     # target_center = np.array(target_center)*u.deg
    #     c0 = SkyCoord(target_center[0],target_center[1],frame="fk5")
    #     raise ValueError("BREAK")

    # --- Normalize target_center into both a SkyCoord (c0) and a plain
    # (RA, Dec) Quantity array (target_center), since both forms are used
    # later (SkyCoord for angular math, array for the WCS crval below).
    if isinstance(target_center,SkyCoord):
        if verbose:
            print(target_center)
        c0 = target_center
        target_center = np.array((c0.ra.deg,c0.dec.deg))*u.deg
    else:
        if not isinstance(target_center[0],u.Quantity):
            if verbose:
                print(target_center)
            target_center = np.array(target_center)*u.deg
        c0 = SkyCoord(target_center[0],target_center[1],frame="fk5")
        # raise ValueError("BREAK")

    # --- Ensure all distance/radius/step parameters carry angular units
    # (arcsec) even if the caller passed plain floats.
    if not isinstance(target_radius,u.Quantity):
        target_radius = np.array(target_radius)*u.arcsec
    if not isinstance(dist_limit,u.Quantity):
        dist_limit = np.array(dist_limit)*u.arcsec
    if not isinstance(bck_radius,u.Quantity):
        bck_radius = np.array(bck_radius)*u.arcsec
    if not isinstance(step_size,u.Quantity):
        step_size = np.array(step_size)*u.arcsec
        

    # target_region = CircleSkyRegion(center=c0,radius=target_radius)
    # Build a local tangent-plane WCS (Hammer-Aitoff-ish AIT projection)
    # centered on the target, at 1 arcsec/pixel. This gives a coordinate
    # system to test region "contains" queries against later.
    w = WCS(naxis=2)
    w.wcs.crpix = [0, 0]
    w.wcs.cdelt = np.array([-1/3600, 1/3600])
    w.wcs.crval = target_center
    w.wcs.ctype = ["RA---AIT", "DEC--AIT"]
    w.wcs.set_pv([(0, 0, 0)])

    # Build the radial grid of candidate distances to search, from just
    # outside the target+background radii out to dist_limit, in step_size
    # increments.
    n_steps = int(((dist_limit - (target_radius+bck_radius))/step_size).decompose())
    dist_arr = np.linspace((target_radius+bck_radius).to(u.arcsec),dist_limit,n_steps)

    # Shuffle so that, when max_iter caps the number of excess sources
    # considered, we don't always test the same subset of them.
    np.random.shuffle(excess)

    # Separation and position angle from the target to each excess source;
    # these position angles double as the candidate search directions
    # (testing behind/around known sources rather than every direction).
    ex_center = SkyCoord(excess["RA"],excess["DEC"],unit=u.deg,frame="fk5")
    sep_arr = c0.separation(ex_center).to(u.arcsec)
    angl_arr = c0.position_angle(ex_center).degree
    n_excess = len(sep_arr)
    if max_iter is None or max_iter > n_excess: max_iter = n_excess

    # With very few excess sources, fall back to a coarse uniform angular
    # grid (and, with fewer than 3, a uniform distance grid too) since the
    # source-derived angles/separations aren't a reliable sampling of the
    # search space.
    if n_excess < 5:
        angl_arr = np.linspace(0,360,20)*u.deg
    if n_excess < 3:
        sep_arr = np.linspace((target_radius+bck_radius).to(u.arcsec),dist_limit/2,len(sep_arr))*u.arcsec

    # Cap the number of angles tested to max_iter.
    angl_arr = angl_arr[:int(max_iter)]

    # Determine whether the excess sources are stored as circles ('R') or
    # ellipses ('SMaj'/'SMin'/'Angl') based on which columns are present.
    cols = excess.dtype.names
    if "R" in cols:
        circle = True
    elif "SMaj" in cols:
        circle = False
    else:
        raise TypeError(f"excess, {cols}, doesn't match required columns.")
    
    if circle:
        # --- Circular excess sources ---
        circles = Make_Circle(excess["RA"],excess["DEC"],excess["R"],sky_frame="fk5",size_unit=u.deg)
        if verbose:
            print(f"Checking {len(dist_arr)*len(angl_arr)} permutations against {n_excess} excess sources.")
        # Try every (distance, angle) candidate position, nearest first,
        # and return as soon as one doesn't intersect the target or any
        # excess source.
        for i, dist in enumerate(dist_arr):
            for j,angl in enumerate(angl_arr):
                candidate = c0.directional_offset_by(angl,dist)
                intersects = False

                # Quick reject: if this candidate is farther from the
                # target than the excess source that defined this angle,
                # it's likely on/behind that source, so skip it as an
                # intersection without doing the full geometric test.
                if n_excess > 4:
                    c_sep = sep_arr[j]
                    if c_sep > candidate.separation(c0): 
                        intersects = True      

                # Full test: does the candidate background circle overlap
                # any detected excess circle?
                if circle_intersects_circle(candidate,bck_radius,ex_center,excess["R"]*u.degree).any():
                    intersects = True

                if not intersects:
                    # Found a clean spot — write it out as a DS9 region
                    # file and return immediately (no need to keep
                    # searching).
                    ra,dec = float(candidate.ra.deg),float(candidate.dec.deg)
                    if output:
                        reg_lines = (
                        '# Region file format: DS9 version 4.1\n'
                        'global color=green dashlist=8 3 width=1 font="helvetica 10 normal roman" '
                        'select=1 highlite=1 dash=0 fixed=0 edit=1 move=1 delete=1 include=1 source=1\n'
                        'fk5\n'
                        )
                        reg_lines += f'circle({ra},{dec},8")\n'
                        with open(outfile,"w") as file:
                            file.write(reg_lines)
                    if verbose: print(f"Background found: ({round(ra,3)*u.deg},{round(dec,3)*u.deg})")
                    return (ra,dec)
                if verbose:
                    # Print periodic progress through the (distance, angle) grid.
                    index_to_percentage(j+i*len(angl_arr),len(dist_arr)*len(angl_arr),phrase="% Scanned")
    else:
        # --- Elliptical excess sources ---
        ellipses = Make_Ellipse(excess["RA"],excess["DEC"],SMaj=excess["SMaj"],SMin=excess["SMin"],Angl=excess["Angl"],sky_frame="fk5",size_unit=u.deg)

        if verbose:
            print(f"Checking {len(dist_arr)*len(angl_arr)} permutations against {n_excess} excess sources.")
        for i, dist in enumerate(dist_arr):
            for j,angl in enumerate(angl_arr):
                candidate = c0.directional_offset_by(angl,dist)
                intersects = False

                # Same "behind a known source" quick reject as the circle case.
                if n_excess > 4:
                    c_sep = sep_arr[j]
                    if c_sep > candidate.separation(c0): 
                        intersects = True
                    
                # Cheap pre-filter using each ellipse's minor-axis circle
                # before doing the more expensive true ellipse containment
                # check below.
                if circle_intersects_circle(candidate,bck_radius,ex_center,excess["SMin"]*u.degree).any():
                    intersects = True

                # Full geometric test against every ellipse, stopping at
                # the first intersection found.
                for k, ellipse in enumerate(ellipses):
                    if intersects:
                        break
               
                    elif circle_intersects_ellipse(candidate,bck_radius,ellipse,n_samples=30,wcs=w):
                        intersects = True
                        break
                    else:
                        continue
                
                if not intersects:
                    # Found a clean spot — write it out and return.
                    ra,dec = float(candidate.ra.deg),float(candidate.dec.deg)
                    if output:
                        reg_lines = (
                        '# Region file format: DS9 version 4.1\n'
                        'global color=green dashlist=8 3 width=1 font="helvetica 10 normal roman" '
                        'select=1 highlite=1 dash=0 fixed=0 edit=1 move=1 delete=1 include=1 source=1\n'
                        'fk5\n'
                        )
                        reg_lines += f'circle({ra},{dec},8")\n'
                        with open(outfile,"w") as file:
                            file.write(reg_lines)
                    if verbose: print(f"Background found: ({round(ra,3)*u.deg},{round(dec,3)*u.deg})")
                    return (ra,dec)
                if verbose:
                    index_to_percentage(j+i*len(angl_arr),len(dist_arr)*len(angl_arr),phrase="% Scanned")
    # Exhausted the full search grid without finding a non-intersecting
    # candidate location.
    if verbose:
        print("No valid sources found")
    return None

def read_excess_region(regfile):
    """
    Read a DS9 fk5 region file produced by find_sources and
    return a structured NumPy array in sky coordinates.

    Parameters
    ----------
    regfile : str
        Path to DS9 region file.

    Returns
    -------
    excess : numpy structured array
        If circle:
            ('ra','dec','r')  [degrees]

        If ellipse:
            ('ra','dec','a','b','theta')  [degrees]
    """

    entries = []

    with open(regfile, "r") as f:
        lines = f.readlines()

    for line in lines:
        line = line.strip()

        if line.startswith("circle"):
            # Parse "circle(RA,DEC,R")" DS9 syntax into its numeric fields.
            inside = line.replace("circle(", "").replace(")", "")
            ra, dec, r = inside.split(",")
            ra = float(ra)
            dec = float(dec)
            r = float(r.replace('"', '')) / 3600.0  # arcsec → deg
            entries.append((ra, dec, r))

        elif line.startswith("ellipse"):
            # Parse "ellipse(RA,DEC,A",B",THETA)" DS9 syntax.
            inside = line.replace("ellipse(", "").replace(")", "")
            ra, dec, a, b, theta = inside.split(",")
            ra = float(ra)
            dec = float(dec)
            a = float(a.replace('"', '')) / 3600.0
            b = float(b.replace('"', '')) / 3600.0
            theta = float(theta)
            entries.append((ra, dec, a, b, theta))

    if not entries:
        return None

    # Determine shape from tuple length
    if len(entries[0]) == 3:
        dtype = np.dtype([
            ("RA", "f8"),
            ("DEC", "f8"),
            ("R", "f8")
        ])
    else:
        dtype = np.dtype([
            ("RA", "f8"),
            ("DEC", "f8"),
            ("SMaj", "f8"),
            ("SMin", "f8"),
            ("Angl", "f8")
        ])

    return np.array(entries, dtype=dtype)

def image_plotter(image_files,src_coord,bck_coords,exc_ls,save_image=False,save_name="UV_image.pdf",showplot=False,shape="circle",logscale=True):
    """
    Image plotting code for the background regions. Source regions are cyan, background 
    regions are green, and excess regions are red.

    Parameters
    ----------
    regfile : str
        Path to DS9 region file.
    image_files : str or list-like
        Single or list of image file names
    src_coord : list-like or SkyCoord
        (ra,dec) of source coordinate
    bck_coords : list-like 
        Single or list of (ra,dec) (or SkyCoords) of background region(s). 
        Must equal the name number of image files
    exc_ls : list-like
        List or lists of excess source structured arrays. Columns are labelled 
        (RA,DEC,R) for circles or (RA,DEC,SMaj,SMin,Angl) for ellipses
    save_image : bool
        Option to save image. Default is False
    save_name : str
        Name for output plot. Default is "UV_image.pdf".
    showplot : bool
        Option to show plot via plt.show()
    shape : str
        Shape of excess regions ("circle" or "ellipse"). Default is "circle"
    logscale : bool
        Option to have output plot to have image log 
        scaled or not. Default is True


    Returns
    -------
    None

    """

    # Normalize inputs into arrays so single-image and multi-image calls
    # can share the same code path.
    image_files = np.atleast_1d(image_files)
    n_images = len(image_files)
    bck_coords = np.atleast_1d(bck_coords)
    if len(exc_ls) != 1 and n_images == 1:
        exc_ls = [exc_ls]
    # exc_ls = np.atleast_1d(exc_ls)

    if n_images != len(bck_coords) or n_images != len(exc_ls):
        raise ValueError("There should be an equal number of image files to background region files")
    
    # Lay out a grid of subplots: single axis for one image, otherwise a
    # 2-column grid with enough rows to fit all images.
    if n_images == 1:
        ncols = 1
        mrows = 1
    else:
        ncols = 2
        mrows = int(round(0.49+n_images/2,0))

    fig, axes = plt.subplots(ncols=ncols,nrows=mrows,figsize=(6*ncols,6*mrows))


    for i,image in enumerate(image_files):
        
        # Pick out the correct subplot axis for this image index.
        if n_images == 1:
            ax = axes
        elif n_images < 3:
            ax = axes[i]
        else:
            ax = axes[i%2,int(i/2)]
        ax.set_title(Path(image).name)

        data = fitsio.read(image)
        with fits.open(image) as hdul:
            w = WCS(hdul[1].header)

        if logscale:
            lindata = data
            with np.errstate(divide='ignore'):
                data = np.log10(np.array(data))
        else:
            lindata = data

        # Convert the source (target) sky coordinate to this image's pixel
        # coordinates.
        if isinstance(src_coord,SkyCoord):
            x,y = w.world_to_pixel(src_coord)
        else:
            c0 = SkyCoord(src_coord[0],src_coord[1],unit=u.deg,frame="fk5")
            x,y = w.world_to_pixel(c0)

        # Convert this image's background coordinate to pixel coordinates.
        if isinstance(bck_coords[i],SkyCoord):
            xb,yb = w.world_to_pixel(bck_coords[i])
        else:
            b0 = SkyCoord(bck_coords[i,0],bck_coords[i,1],unit=u.deg,frame="fk5")
            xb,yb = w.world_to_pixel(b0)

        # Zoom the plot in around the target/background pair based on how
        # far apart they are in pixels, so both circles stay visible.
        dist = np.sqrt((x-xb)**2+(y-yb)**2)

        if dist < 130:
            ax.set_xlim([x-150,x+150])
            ax.set_ylim([y-150,y+150])
        elif dist < 290:
            ax.set_xlim([x-300,x+300])
            ax.set_ylim([y-300,y+300])


        # Display the image with a gray colormap, clipped to +/- 2 sigma
        # around the mean (ignoring -inf pixels from the log transform).
        nonDATA = [d for d in np.array(data).flatten() if not np.isneginf(d)]
        m, s = np.mean(nonDATA), np.std(nonDATA)
        im = ax.imshow(data, interpolation='nearest', cmap='gray',vmin=m-2*s, vmax=m+2*s, origin='lower')

        ax.axis('off')

        # Draw the target source (cyan) and chosen background region
        # (green) as circles on top of the image.
        circ0 = Circle(xy=(x,y),radius=5)
        circb = Circle(xy=(xb,yb),radius=8)

        circ0.set_facecolor('none')
        circ0.set_edgecolor('cyan')
        circ0.set_lw(2)
        ax.add_artist(circ0)


        circb.set_facecolor('none')
        circb.set_edgecolor('green')
        circb.set_lw(2)
        ax.add_artist(circb)

        excess = exc_ls[i]

        # Overlay every detected excess source (red) as either ellipses or
        # circles, depending on `shape`.
        if shape=="ellipse":
            # Handle both plain-float (assumed degrees) and Quantity-typed
            # excess arrays, converting sizes to arcsec and angle to
            # radians for matplotlib's Ellipse patch.
            if not isinstance(excess[0][0],u.Quantity):
                e0 = SkyCoord(excess["RA"],excess["DEC"],unit=u.deg,frame="fk5")
                x,y = w.world_to_pixel(e0)
                a = excess["SMaj"]*3600
                b = excess["SMin"]*3600
                theta = excess["Angl"]*np.pi/180
            else:
                e0 = SkyCoord(excess["RA"],excess["DEC"],frame="fk5")
                x,y = w.world_to_pixel(e0)
                a = excess["SMaj"].to(u.arcsec).value
                b = excess["SMin"].to(u.arcsec).value
                theta = excess["Angl"].to(u.rad).value
            dtype = np.dtype([
                ("x", "f8"),
                ("y", "f8"),
                ("a", "f8"),
                ("b", "f8"),
                ("theta", "f8")
            ])

            objects = np.zeros(len(x),dtype=dtype)

            # Pack pixel positions and ellipse geometry into the structured
            # array for convenient field access below.
            for i in range(len(objects)):
                row = (x[i],y[i],a[i],b[i],theta[i])
                objects[i] = row

            # Draw one red ellipse patch per detected excess source.
            for i in range(len(objects)):
                
                e = Ellipse(xy=(objects['x'][i], objects['y'][i]),
                            width=2*objects['a'][i],
                            height=2*objects['b'][i],
                            angle=objects['theta'][i] * 180. / np.pi)
                e.set_facecolor('none')
                e.set_edgecolor('red')
                e.set_lw(2)
                ax.add_artist(e)
        else:
            # Circle-shaped excess sources: 'R' column if present, else
            # fall back to 'SMaj' (semi-major axis used as a radius).
            if "R" in excess.names():
                if not isinstance(excess[0][0],u.Quantity):
                    e0 = SkyCoord(excess["RA"],excess["DEC"],unit=u.deg,frame="fk5")
                    x,y = w.world_to_pixel(e0)
                    r = excess["R"]
                else:
                    e0 = SkyCoord(excess["RA"],excess["DEC"],frame="fk5")
                    x,y = w.world_to_pixel(e0)
                    r = excess["R"].to(u.arcsec).value
            else:
                if not isinstance(excess[0][0],u.Quantity):
                    e0 = SkyCoord(excess["RA"],excess["DEC"],unit=u.deg,frame="fk5")
                    x,y = w.world_to_pixel(e0)
                    r = excess["SMaj"]*3600
                else:
                    e0 = SkyCoord(excess["RA"],excess["DEC"],frame="fk5")
                    x,y = w.world_to_pixel(e0)
                    r = excess["SMaj"].to(u.arcsec).value
            #     xb,yb = w.world_to_pixel(b0)
            dtype = np.dtype([
                ("x", "f8"),
                ("y", "f8"),
                ("r", "f8")
            ])

            objects = np.zeros(len(x),dtype=dtype)
            for i in range(len(objects)):
                row = (x[i],y[i],r[i])
                objects[i] = row


            # Draw one red circle patch per detected excess source.
            for i in range(len(objects)):
                e = Circle(xy=(objects['x'][i], objects['y'][i]),
                            r=objects['r'][i])
                e.set_facecolor('none')
                e.set_edgecolor('red')
                e.set_lw(2)
                ax.add_artist(e)

    # Save the composite figure (all image panels) to disk, and optionally
    # display it interactively as well.
    if save_image:
        fig.savefig(save_name,bbox_inches="tight")
    if showplot:
        plt.show()

    plt.close()

    return None

# ============================================================================
# Command-line entry point
# ============================================================================

# Example DS9 command for visually inspecting the resulting region files:
# ds9 V1405Casum2_sk.fits -region background_um2.reg -region bck_v1405.reg -region src_v1405.reg -region select all -region width 3 -region select none -scale zscale -cmap b

# UVOT filter name tags used to match filenames / build per-filter outputs.
filters = ["wh","uuu","ubb","uvv","uw1","uw2","um2"]

parser = argparse.ArgumentParser(
    prog = "Background region generator",
    description="Program to update observation files. This will download any missing "\
        "new init files, but not create new totaled files."
        )

parser.add_argument(
    "--filename",
    type=str,
    default=None,
    help=f"Image file name (default: %(default)s)"
)

parser.add_argument(
    "--file_pattern",
    type=str,
    default="_sk.img",
    help=f"File name pattern (default: %(default)s)"
)

parser.add_argument(
    "--excess_shape",
    type=str,
    default="ellipse",
    help=f"Input/output excess region file shape (ellipse/circle) (default: %(default)s)"
)

parser.add_argument(
    "--multi",
    type=str,
    default="True",
    help=f"Single (single) or multiple (multi) targets? (default: %(default)s)"
)

# parser.add_argument(
#     "--targets",
#     type=str,
#     default="targets.csv",
#     help=f"Targets file name? (default: %(default)s)"
# )

parser.add_argument(
    "--threshold",
    type=float,
    default=1.0,
    help=f"Excess threshold? (default: %(default)s)"
)

parser.add_argument(
    "--clobber",
    type=str,
    default="False",
    help="Overwrite the background regions (default: %(default)s)"
)

parser.add_argument(
    "--plotsrc",
    type=str,
    default="True",
    help="Make plots? (default: %(default)s)"
)

parser.add_argument(
    "--showplot",
    type=str,
    default="True",
    help="Show plots? (default: %(default)s)"
)

parser.add_argument(
    "--outplot",
    type=str,
    default=None,
    help="Output plot name. (default: %(default)s)"
)

parser.add_argument(
    "--verbose",
    type=str,
    default="True",
    help="Verbose? (default: %(default)s)"
)

parser.add_argument(
    "--src_loc",
    type=str,
    default=None,
    help="Location for dataSRC"
)

parser.add_argument(
    "--src_reg",
    type=str,
    default=None,
    help="Name of source region"
)

parser.add_argument(
    "--all_filters",
    type=str,
    default=False,
    help="Check all filters? (default: %(default)s)"
)

args = parser.parse_args()

filename = args.filename
file_pattern = args.file_pattern
# argparse args are read in as strings, so booleans passed on the command
# line ("True"/"y"/"yes") are converted to real Python bools here.
multi = (True if str(args.multi).lower() in ["true","y","yes"] else False)
# targets = args.targets
threshold = args.threshold
clobber = (True if str(args.clobber).lower() in ["true","y","yes"] else False)
shape = args.excess_shape
srcpath = args.src_loc
src_name = args.src_reg
plotsrc = (True if str(args.plotsrc).lower() in ["true","y","yes"] else False)
showplot = (True if str(args.showplot).lower() in ["true","y","yes"] else False)
outplot = args.outplot
verbose = (True if str(args.verbose).lower() in ["true","y","yes"] else False)
all_filters = (True if str(args.all_filters).lower() in ["true","y","yes"] else False)

if multi:
    # --- Multi-target mode: process a whole directory tree of targets. ---
    if srcpath is None:
        # Fall back to interactive input — list subdirectories of the
        # current directory instead of shelling out to `ls -d */`.
        for p in sorted(Path.cwd().iterdir()):
            if p.is_dir():
                print(f"{p.name}/")
        srcpath = input("What is the source data directory? ")
    # Resolve srcpath to an absolute path. (The original code did this by
    # os.chdir-ing into srcpath and back — with pathlib there's no need to
    # actually change directories just to resolve a path.)
    totpath = (Path.cwd() / srcpath).resolve()
else:
    # --- Single-target mode: process one image file. ---
    if filename is None:
        # List files in the current directory instead of shelling out to `ls`.
        for p in sorted(Path.cwd().iterdir()):
            if p.is_file():
                print(p.name)
        filename = input("What is the filename? ")
        if not Path(filename).is_file():
            print("File not found.")
            filename = input("What is the image filename? ")
            if not Path(filename).is_file(): raise ValueError("File not found.")
    # Resolve to an absolute path and split into directory + bare filename.
    # Everything downstream builds absolute paths from `loc` rather than
    # changing the process's working directory.
    filename_path = Path(filename).resolve()
    loc = filename_path.parent

    filename = filename_path.name
    filepath = loc
    filenames = [f.name for f in loc.iterdir() if f.is_file()]

if multi:
    # Walk every subdirectory under the source data directory looking for
    # targets to process. Path.walk() (Python 3.12+) mirrors os.walk() but
    # yields dirpath as a Path object. Since totpath is already absolute,
    # dirpath comes out absolute too, so every path built from it below
    # (dirpath / name) is a full path — no chdir needed.
    for dirpath, dirnames, filenames in totpath.walk():

        # Skip any "old"/archived directories.
        if "old" in str(dirpath): continue

        # Find candidate images (matching file_pattern) and source region
        # files (named "src*.reg") in this directory.
        images = [f for f in filenames if file_pattern in f]
        src_regs = [f for f in filenames if f.endswith("reg") and f.startswith("src")]
        # excess_regs = [f for f in filenames if f.startswith("excess")]

        # Nothing to do here if either images or source regions are missing.
        if not images or not src_regs:
            continue

        # Derive a target ID string from each "srcID.reg" filename.
        ID = [f.replace(".reg","").replace("src","") for f in src_regs]

        # Read the RA/Dec of each target out of its region file.
        RA = []
        DEC = []
        for src_reg in src_regs:
            with open(dirpath / src_reg,"r") as file:
                for line in file.readlines():
                    if "circle" in line or "ellipse" in line:
                        data = line.split(",")
                        RA.append(float(data[0].split("(")[1]))
                        DEC.append(float(data[1]))

        if verbose:
            print(f"{(len(dirpath.name)+10)*'%'}\nAnalyzing {dirpath.name}\n{(len(dirpath.name)+10)*'%'}\n")

        targets = SkyCoord(RA,DEC,unit=u.deg,frame="fk5")

        bck_ls = []
        exc_ls = []

        # Process each UVOT filter that has a matching image in this
        # directory.
        for filt in filters:
            image = [f for f in images if filt in f]

            if not image: continue

            if verbose:
                print(f"Analyzing filter: {filt}")

            excess_reg = f"excess_{filt}.reg"

            # Reuse a previously computed excess region file unless
            # clobbering is requested.
            if not clobber and excess_reg in filenames:
                excess = read_excess_region(dirpath / excess_reg)
            else:
                excess,excess_pxl = find_sources(dirpath / image[0],outreg=dirpath / excess_reg,threshold=threshold,shape=shape,plotsrc=False,verbose=verbose)
            exc_ls.append(excess)
            
            # For each target in this directory, try to find a valid
            # background region, progressively relaxing the search radius
            # / distance limit / iteration cap if earlier attempts fail.
            for i,c0 in enumerate(targets):
                coord = find_valid_background(excess,c0,bck_radius=15*u.arcsec,dist_limit=100*u.arcsec,max_iter=1e2,suffix=ID[i],filter=filt,verbose=verbose,clobber=clobber,directory=dirpath)

                if coord is None:
                    coord = find_valid_background(excess,c0,bck_radius=15*u.arcsec,dist_limit=100*u.arcsec,max_iter=3e2,suffix=ID[i],filter=filt,verbose=verbose,clobber=clobber,directory=dirpath)
                    if coord is None:
                        coord = find_valid_background(excess,c0,bck_radius=12*u.arcsec,dist_limit=100*u.arcsec,max_iter=3e2,suffix=ID[i],filter=filt,verbose=verbose,clobber=clobber,directory=dirpath)
                        if coord is None:
                            coord = find_valid_background(excess,c0,bck_radius=12*u.arcsec,dist_limit=200*u.arcsec,max_iter=3e2,suffix=ID[i],filter=filt,verbose=verbose,clobber=clobber,directory=dirpath)
                            if coord is None:
                                coord = find_valid_background(excess,c0,bck_radius=12*u.arcsec,dist_limit=150*u.arcsec,max_iter=1e3,suffix=ID[i],filter=filt,verbose=verbose,clobber=clobber,directory=dirpath)
                                print(f"Failed to find valid background region for {src_regs[i]}")
                bck_ls.append(coord)

        # Once all filters/targets in this directory are processed,
        # optionally save a diagnostic plot of the first target across all
        # its images.
        if plotsrc:
            image_paths = [dirpath / img for img in images]
            if outplot is None:
                image_plotter(image_paths,(RA[0],DEC[0]),bck_ls,exc_ls,shape=shape,save_image=True,showplot=showplot,save_name=dirpath / "UV_image.pdf")
            else:
                image_plotter(image_paths,(RA[0],DEC[0]),bck_ls,exc_ls,shape=shape,save_image=True,showplot=showplot,save_name=dirpath / outplot)
        if verbose:
            print(f"Completed Analysis of {dirpath.name}\n")

else:
    # Find every "srcID.reg" region file in this directory — one per
    # target source to process against the single given image.
    src_names = [f for f in filenames if (f.startswith("src") and f.endswith(".reg") and not "old" in f)]
    # src_indices = [int(src.replace("src","").replace(".reg","")) for src in src_names]
    hdu = fits.open(loc / filename)[1]
    w = WCS(hdu.header)
    # indeces = [f.replace("src","").replace(".reg","") for f in src_names]
    if len(src_names) > 0:
        
        for src in src_names:
            # Read the target's RA/Dec from its region file.
            with open(loc / src,"r") as file:
                openlines = file.readlines()
            for line in openlines:
                if "circle" in line or "annulus" in line or "ellipse" in line:
                    data = line.split(",")
                    ra = float(data[0].split("(")[1])
                    dec = float(data[1])
                    break
            c0 = SkyCoord(ra,dec,unit=u.deg,frame="fk5")
            # Target ID string, derived from the region filename
            # ("srcID.reg" -> "ID").
            s0 = src.replace("src","").replace(".reg","")

            filt = ""
            circles_ls = []
            bck_ls = []

            # Determine which UVOT filter this image corresponds to, based
            # on which filter tag appears in the filename.
            for f in filters: 
                # print(filename,filt)
                if f in filename: 
                    filt = f"_{f}" 
                    break

            excess_reg = f"excess{filt}.reg"

            # Reuse a previously computed excess region unless clobbering.
            if not clobber and excess_reg in filenames:
                excess = read_excess_region(loc / excess_reg)
            else:
                excess,excess_pxl = find_sources(loc / filename,outreg=loc / excess_reg,threshold=threshold,shape=shape,plotsrc=plotsrc,verbose=verbose)

            # for i,c0 in enumerate(targets):
            bck_reg = f"bck{s0}.reg"
            if not clobber and bck_reg in filenames:
                # Reuse a previously computed background region for this
                # target rather than recomputing it.
                with open(loc / bck_reg,"r") as file:
                    openlines = file.readlines()
                for line in openlines:
                    if "circle" in line or "annulus" in line or "ellipse" in line:
                        data = line.split(",")
                        ra = float(data[0].split("(")[1])
                        dec = float(data[1])
                        coord = (ra,dec)
                        break
            else:
                # Search for a valid background region, progressively
                # relaxing the search parameters if earlier attempts fail
                # to find a non-intersecting location.
                coord = find_valid_background(excess,c0,bck_radius=15*u.arcsec,dist_limit=100*u.arcsec,max_iter=1e2,suffix=s0,filter=filt,verbose=verbose,clobber=clobber,directory=loc)

                if coord is None:
                    coord = find_valid_background(excess,c0,bck_radius=15*u.arcsec,dist_limit=100*u.arcsec,max_iter=3e2,suffix=s0,filter=filt,verbose=verbose,clobber=clobber,directory=loc)
                    if coord is None:
                        coord = find_valid_background(excess,c0,bck_radius=12*u.arcsec,dist_limit=100*u.arcsec,max_iter=3e2,suffix=s0,filter=filt,verbose=verbose,clobber=clobber,directory=loc)
                        if coord is None:
                            coord = find_valid_background(excess,c0,bck_radius=12*u.arcsec,dist_limit=200*u.arcsec,max_iter=3e2,suffix=s0,filter=filt,verbose=verbose,clobber=clobber,directory=loc)
                            if coord is None:
                                coord = find_valid_background(excess,c0,bck_radius=12*u.arcsec,dist_limit=150*u.arcsec,max_iter=1e3,suffix=s0,filter=filt,verbose=verbose,clobber=clobber,directory=loc)
                                print(f"Failed to find valid background region for {src}")

            # Only plot/record if a valid background location was actually
            # found (coord is None means the search failed entirely).
            if not coord is None:
                bck_ls.append(coord)
                if plotsrc:
                    if outplot is None:
                        image_plotter([loc / filename],(ra,dec),bck_ls,excess,shape=shape,save_image=True,showplot=showplot,save_name=loc / "UV_image.pdf")
                    else:
                        image_plotter([loc / filename],(ra,dec),bck_ls,excess,shape=shape,save_image=True,showplot=showplot,save_name=loc / outplot)