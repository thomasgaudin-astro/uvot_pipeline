import fitsio

import numpy as np

from astropy.io import fits
from astropy.wcs import WCS

from pathlib import Path



class BackgroundGenerator():
    def __init__(self, tile_name, obsid, verbose=False, threshold=1, output=True, shape="circle", logscale=True, plotsrc=False):
        self.tile_name = tile_name
        self.obsid = obsid
        self.verbose = verbose
        self.threshold = threshold
        self.output = output
        self.shape = shape
        self.logscale = logscale
        self.plotsrc = plotsrc

        self.filepath = f'./S-CUBED/{self.tile_name}/UVOT/{self.obsid}/uvot/image/sw{self.obsid}uw1_sk.img.gz'

        self.excess, self.excess_pxl = self.find_sources(self.filepath, 
                                                         threshold=self.threshold, 
                                                         verbose=self.verbose, 
                                                         output=self.output, 
                                                         shape=self.shape, 
                                                         logscale=self.logscale, 
                                                         plotsrc=self.plotsrc
                                                         )

        


    def find_sources(self, filename, outreg="excess.reg", threshold=1,
                    verbose=False, output=True,
                    shape="circle", logscale=True, plotsrc=False):
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
                            directory=Path("."),
                            outpath=Path(".")):
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

        outfile = Path(outpath) / f"bck{suffix}{filter}.reg"

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