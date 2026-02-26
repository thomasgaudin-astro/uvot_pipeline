import time
import gc


def automated_aspect_correction(obs_table, base_path, save_path, side_buffer=7, num_stars=50):
    """
    Automated aspect correction using the observations table.

    KEY DIFFERENCES FROM clean_uvot_tiles.py, So you, Thomas(proably) Will know.

    1. DATA SOURCE:
       clean_uvot_tiles scanned directories with os.listdir() and built file
       paths manually (I.E. f'{filepath}/{obs_frame}/uvot/image/detect.fits').
       This instead pulls all observation metadata from obs_table,
       which was pre-built by populate_observations_table() (As we talked abut). 

    2. GROUP-BASED PROCESSING:
       clean_uvot_tiles processed every observation in a flat loop with a
       single reference frame for the entire tile. This function organizes
       work by Group_ID, which means each group can have its OWN best reference frame. ORPHAN
       groups (observations that couldn't be grouped) are skipped automatically. (As we talked about)

    3. REFERENCE FRAME SELECTION:
       clean_uvot_tiles picked the first DIRECT frame it found as the reference
       for ALL corrections in the tile. This function selects references
       PER GROUP and PER BAND, choosing the best DIRECT frame available for
       each specific correction. It also tries to match the extension number
       first, falling back to extension 1 if no match is found. This is mostly not important 
       It originally was a desperate gamble to get it to do Multi-Extensions 
       The only reason it has not been removed is because it did clean a test 
       frame that wassent fixed by the normal proccess and it barely used processing time.

    4. BAND-AWARE PROCESSING:
       clean_uvot_tiles was hardcoded to 'uw1' only (all paths used 'uw1').
       This function processes all the bands present in the data (uw1, uw2, ubb,
       uvv, um2, uuu) by iterating over unique filters within each group.

    5. RETRY LOGIC:
       clean_uvot_tiles had a manual retry loop where the user could change
       parameters and re-run the entire pipeline from scratch. This function
       has smarter retry logic: it tracks exactly which frames failed and
       only re-attempts those specific frames on retry, rather than
       re-processing everything. (I think thats what yours did if I read it right)

    6. PLATFORM SUPPORT:
       clean_uvot_tiles used direct shell commands (os.system, sh.gunzip)
       and assumed a native macOS/Linux HEASOFT installation. This might/should
       supports both WSL and native backends, however it does need your input
       I need you to but your UNICORR logic in a section bellow.


       You Thomas(proably) will also notice bellow I have tons of comments, 
       Both for bookkeeping reasons (mostly debuging, trying to keep track of the mess I was making) and also
       So you can track exactly in detail what has been changed, and why.
    """

    ###############################################################################
    # RETRY STATE VARIABLES
    failed_frames_to_retry = None
    attempt_num = 0

    ##############################################################################
    # MAIN RETRY LOOP
    #
    # CHANGE FROM clean_uvot_tiles.py:
    #   clean_uvot_tiles had a similar while loop (run_pipeline == True)
    #   but it wrapped the whole cleaning pipeline (download, detect,
    #   smear removal, unzip, and aspect correction). Here we only loop
    #   around the aspect correction step itself, since all the upstream
    #   work (detection, smear removal, etc.) has already been done and
    #   stored in obs_table.
    while True:
        print("\n" + "=" * 70)
        if attempt_num == 0:
            print("AUTOMATED ASPECT CORRECTION - INITIAL ATTEMPT")
        else:
            print(f"AUTOMATED ASPECT CORRECTION - RETRY ATTEMPT {attempt_num}")
        print("=" * 70)
        print(f"Parameters: side_buffer={side_buffer}, num_stars={num_stars}")
        print(f"HEASOFT Backend: {HEASOFT_BACKEND}")

        ##############################################################################
        # FILTER THE OBSERVATIONS TABLE
        # Remove smeared frames 
        #
        # CHANGE FROM clean_uvot_tiles.py:
        #   clean_uvot_tiles called up.detect_smeared_frames() and then up.remove_smeared()
        #   I instead just filter them out of our working table since
        #   obs_table already has a 'Smeared Flag' column pre-computed.
        working_table = obs_table[obs_table['Smeared Flag'] == False].copy()

        ##############################################################################
        # BUILD RETRY FILTER (attempts > 0 only)
        # 
        # On retry, we really only want to re-attempt the frames that failed, a lesson I have learned.
        # But we keep the FULL working_table available because we still
        # need access to DIRECT reference frames (which succeeded and are
        # used as references for the corrections). Originally the error here was I did not keep the table open
        # So when retry was attempted It simply couldn't find reference frames. Hingsight 20/20.
        frames_to_correct = None
        if attempt_num > 0 and failed_frames_to_retry:
            print(f"RETRY MODE: Only correcting {sum(len(v) for v in failed_frames_to_retry.values())} failed frames from previous attempt")

            # Parse the failed frame identifiers ("00033038050_ext1" format)
            # into (ObsID, extension_number) tuples for fast lookup
            frames_to_correct = set()
            for group_band, obsid_list in failed_frames_to_retry.items():
                for obsid_ext in obsid_list:
                    parts = obsid_ext.split('_ext')
                    if len(parts) == 2:
                        obsid = parts[0]
                        ext = int(parts[1])
                        frames_to_correct.add((obsid, ext))

            print(f"Will attempt correction on {len(frames_to_correct)} frames")
            print(f"(Keeping full dataset for reference selection)")

        
        # These track which frames failed during the attempt, and are used
        # both for the final return value and for building the retry bit.
        aspectnone_dict = {}        # count of failures
        aspectnone_tiles_dict = {}  # list of failed ObsID_ext strings

        ###############################################################################
        # GET UNIQUE GROUPS TO PROCESS
        
        # CHANGE FROM clean_uvot_tiles.py:
        #   clean_uvot_tiles iterated over directory names (each ObsID was
        #   a folder,). Ima iterate over Group_IDs, which cluster the related
        #   observations together.
        unique_groups = working_table['Group_ID'].unique()

        if len(working_table) == 0:
            print("No frames to process in this attempt")
            break

        print(f"\nFound {len(unique_groups)} unique groups to process")

        # DEBUG: Print the group status on first attempt
        if attempt_num == 0:
            print("\nGroup Status Breakdown:")
            for status in ['COMPLETED', 'READY', 'ORPHAN']:
                count = len(working_table[working_table['Group_Status'] == status]['Group_ID'].unique())
                print(f"  {status}: {count} groups")
            print()

        ###############################################################################
        # The Main loop

        for group_id in unique_groups:
            group_data = working_table[working_table['Group_ID'] == group_id]
            group_status = group_data['Group_Status'].iloc[0]

            # Skip groups that don't need processing

            # ORPHAN groups have no related observations to use as references, Thomas will take care of this. I think.
            if group_status == 'ORPHAN':
                if attempt_num == 0:
                    print(f"\n[Group {group_id}] Status: ORPHAN - Skipping")
                continue

            # COMPLETED groups.... Are completed.
            if group_status == 'COMPLETED':
                if attempt_num == 0:
                    print(f"\n[Group {group_id}] Status: COMPLETED - Already done")
                continue


            # Print out to know what the code is on.
            print(f"\n{'=' * 70}")
            print(f"Processing Group {group_id} (Status: {group_status})")
            print(f"{'=' * 70}")

            ###############################################################################
            # Go over bands within the group
            
            # CHANGE FROM clean_uvot_tiles.py:
            #   clean_uvot_tiles was hardcoded to only process 'uw1' (all
            #   file paths contained 'uw1'). This function processes every
            #   band present in the group, Turns out not that important givin the fact that
            #   It is mostly uw1 anyways, but hey.
            unique_bands = group_data['Filter'].unique()

            for band in unique_bands:
                band_data = group_data[group_data['Filter'] == band]

                print(f"\n--- Band: {band} ---")
                print(f"Total extensions: {len(band_data)}")
                print(f"DIRECT: {len(band_data[band_data['Extension_Status'] == 'DIRECT'])}")
                print(f"NONE: {len(band_data[band_data['Extension_Status'] == 'NONE'])}")
                print(f"UNICORR: {len(band_data[band_data['Extension_Status'] == 'UNICORR'])}")

                # Next we Find the direct reference frames, These are the references.
                #
                # IMPORTANT!!!!!!!!! We search for references in the FULL band_data,
                # NOT filtered by frames_to_correct. Even on retry, gotta get
                # access to all DIRECT frames as references.
                #
                # CHANGE FROM clean_uvot_tiles.py:
                #   clean_uvot_tiles picked ONE reference for the entire tile:
                #       ref_frame = direct_frames[0]
                #   We pick the best reference per band within each group.
                ref_candidates = band_data[band_data['Extension_Status'] == 'DIRECT']

                if ref_candidates.empty:
                    print(f"Ruh-Roh: ⚠️  No DIRECT reference found for {band} - skipping")
                    continue

                # Find frames that need correction, So NONE
                corrections_needed = band_data[band_data['Extension_Status'] == 'NONE']

                # On retry, make it the frames that failed last time.
                if frames_to_correct is not None:
                    corrections_needed = corrections_needed[
                        corrections_needed.apply(
                            lambda row: (row['ObsID'], row['Snapshot']) in frames_to_correct,
                            axis=1
                        )
                    ]

                print(f"Extensions needing correction: {len(corrections_needed)}")

                if corrections_needed.empty:
                    continue

                # Counters for this band's correction summary, Important for debuging and knowing what failed.
                corrections_attempted = 0
                corrections_successful = 0
                corrections_failed = 0

                #########################################################################
                # Now we correct the frames that are NONE
                # For each frame with ASPCORR='NONE', we are going to:
                #   1. Find a suitable DIRECT reference in the same group/band
                #   2. Detect bright stars in both the reference and observation
                #   3. Match stars between the two frames
                #   4. Run uvotunicorr to compute and apply the pointing correction
                #
                # CHANGE FROM clean_uvot_tiles.py:
                #   clean_uvot_tiles found stars in the reference frame ONCE
                #   and reused them for all corrections:
                #       ref_bright_stars = up.find_brightest_central_stars(ref_detect_path, ...)
                #       for obs_frame in aspect_uncorrected_frames:
                #           ...
                #   I of course Cant do this, Also I think This might have made a bug? 
                #   remove_separate_stars() changed ref_bright_stars, so by the second go of it the
                #   reference star list was getting filtered down.
                #   Anywhoo I need to find new stars for each correction pair anyways.
                for correction_num, (idx, obs_row) in enumerate(corrections_needed.iterrows(), start=1):

                    # Small pause between corrections to avoid overwhelming HEASOFT or filesystem, WSL loves to overwhelm.
                    # Turns out running subsystems in subsystems has some issues.
                    if correction_num > 1:
                        time.sleep(3)

                    obs_obsid = obs_row['ObsID']
                    obs_snapshot = obs_row['Snapshot']
                    obs_full_path = obs_row['Full_Path']

                    print(f"\n  [{correction_num}/{len(corrections_needed)}] "
                          f"Correcting ObsID {obs_obsid}, Extension {obs_snapshot}...")

                    obs_dir = os.path.dirname(obs_full_path)

                    # Bachelor for DIrect files.
                    # We want to find a DIRECT reference that matches our extension
                    # number first. If none exists, fall back to extension 1. 
                    # Again this porably can be removed, but it did fix 1 frame 1 time in a test
                    # Also I dont really want to.
                    #
                    # CHANGE FROM clean_uvot_tiles.py:
                    #   clean_uvot_tiles used a single global reference, I said it before.
                    suitable_ref = None

                    # First pass: try to match the same extension number
                    for _, ref_candidate in ref_candidates.iterrows():
                        candidate_path = ref_candidate['Full_Path']
                        candidate_obsid = ref_candidate['ObsID']
                        candidate_snapshot = ref_candidate['Snapshot']

                        # Skip if extension doesn't match
                        if candidate_snapshot != obs_snapshot:
                            continue

                        # Verify the reference directory and file exist on disk, Just in case Dont want it crashing for no good reason.
                        ref_dir_check = os.path.dirname(candidate_path)
                        if not os.path.exists(ref_dir_check):
                            continue

                        actual_files = os.listdir(ref_dir_check)
                        ref_base = f"sw{candidate_obsid}{band}_sk"

                        # Search for the sky image file
                        ref_file_found = None
                        for f in actual_files:
                            if f.startswith(ref_base):
                                ref_file_found = f
                                break

                        if ref_file_found:
                            suitable_ref = {
                                'obsid': candidate_obsid,
                                'snapshot': candidate_snapshot,
                                'full_path': os.path.join(ref_dir_check, ref_file_found),
                                'dir': ref_dir_check
                            }
                            print(f"Using reference: ObsID {candidate_obsid}, Extension {candidate_snapshot}")
                            break

                    # Second pass: fall back to extension 1 if no match found
                    if suitable_ref is None:
                        print(f" Issue 101: No extension {obs_snapshot} reference found, trying extension 1...")

                        for _, ref_candidate in ref_candidates.iterrows():
                            candidate_path = ref_candidate['Full_Path']
                            candidate_obsid = ref_candidate['ObsID']
                            candidate_snapshot = ref_candidate['Snapshot']

                            if candidate_snapshot != 1:
                                continue

                            ref_dir_check = os.path.dirname(candidate_path)
                            if not os.path.exists(ref_dir_check):
                                continue

                            actual_files = os.listdir(ref_dir_check)
                            ref_base = f"sw{candidate_obsid}{band}_sk"

                            ref_file_found = None
                            for f in actual_files:
                                if f.startswith(ref_base):
                                    ref_file_found = f
                                    break

                            if ref_file_found:
                                suitable_ref = {
                                    'obsid': candidate_obsid,
                                    'snapshot': 1,
                                    'full_path': os.path.join(ref_dir_check, ref_file_found),
                                    'dir': ref_dir_check
                                }
                                print(f"    Using fallback reference: ObsID {candidate_obsid}, "
                                      f"Extension 1 (for obs ext {obs_snapshot})")
                                break

                    # If no reference found at all, we can't correct that.
                    if suitable_ref is None:
                        print(f" ❌ No DIRECT reference found "
                              f"(tried extension {obs_snapshot} and extension 1) - skipping")
                        corrections_failed += 1
                        continue

                    ref_obsid = suitable_ref['obsid']
                    ref_snapshot = suitable_ref['snapshot']
                    ref_full_path = suitable_ref['full_path']
                    ref_dir = suitable_ref['dir']

                    # Loacate the detect files
                    # We need detect files sine they contain the source catalog 
                    # (star positions and brightnesses). We need these
                    # for both the reference and observation frames to find
                    # matching stars for the correction.
                    #
                    # CHANGE FROM clean_uvot_tiles.py:
                    #   clean_uvot_tiles assumed a single detect.fits per ObsID:
                    #       obs_detect_path = f'{filepath}/{obs_frame}/uvot/image/detect.fits'
                    #   We have to check for extension specific detect files first
                    #   (band_detect_ext1.fits), falling back to the generic
                    #   detect file if not found.
                    obs_detect_file = os.path.join(obs_dir, f"{band}_detect_ext{obs_snapshot}.fits")
                    if not os.path.exists(obs_detect_file):
                        obs_detect_file = os.path.join(obs_dir, f"{band}_detect.fits")

                    ref_detect_file = os.path.join(ref_dir, f"{band}_detect_ext{ref_snapshot}.fits")
                    if not os.path.exists(ref_detect_file):
                        ref_detect_file = os.path.join(ref_dir, f"{band}_detect.fits")

                    if not os.path.exists(obs_detect_file):
                        print(f" ❌ No detect file found "
                              f"(tried {band}_detect_ext{obs_snapshot}.fits and {band}_detect.fits) - skipping")
                        corrections_failed += 1
                        continue

                    if not os.path.exists(ref_detect_file):
                        print(f" ❌ No detect file found for reference - skipping")
                        corrections_failed += 1
                        continue


                    
                    # Matching the Bachelors and Bachelorettes  
                    try:
                        # Find bright stars near the center of both frames.
                        # side_buffer controls how far from center to look Set to 7'
                        # This is bcause of Thomas's preference But now it has to be that way. 
                        # Since thats also the way the groups are identified. On hindsght that I am having just now
                        # If you tried to change that number without making it go back up to orphan hunting that would proably 
                        # Crash the code.... Might need to fix that on a later update.
                        #
                        # CHANGE FROM clean_uvot_tiles.py:
                        #   clean_uvot_tiles found reference stars once outside
                        #   the loop and reused them. But remove_separate_stars()
                        #   modifies the list, so the reference stars
                        #   got progressively filtered with each iteration.
                        #   We now find stars fresh for each correction pair, avoiding that.
                        ref_bright_stars = find_brightest_central_stars(
                            ref_detect_file,
                            num_stars=num_stars,
                            side_buffer=side_buffer
                        )

                        obs_bright_stars = find_brightest_central_stars(
                            obs_detect_file,
                            num_stars=num_stars,
                            side_buffer=side_buffer
                        )

                        # Cross-match stars between the reference and observation.
                        # Stars that appear in only one frame are removed they're
                        # either transients, artifacts, or fell off the detector edge.
                        ref_stars_filtered, obs_stars_filtered = remove_separate_stars(
                            ref_bright_stars.copy(),
                            obs_bright_stars
                        )

                        # Need at least 3 matched stars for a decent geometric
                        # transformation (translation + rotation needs ≥3 points aparantly)
                        if len(ref_stars_filtered) < 3:
                            print(f" ❌ Not enough matching stars ({len(ref_stars_filtered)}) - skipping")
                            corrections_failed += 1
                            continue

                        print(f" Found {len(ref_stars_filtered)} matching stars")

                        # Write region files (.reg) marking the matched star
                        # positions. These will be used by uvotunicorr to get
                        # the pointing offset.
                        create_ref_obs_reg_files(
                            ref_stars_filtered,
                            obs_stars_filtered,
                            outpath=obs_dir
                        )

                        # Prepare the Bachelorettes  (References)
                        # uvotunicorr needs both the reference and observation sky images in the same directory.
                        # (Not actually true I could proably have it path the the location instead but this is easier.)
                        ref_img_name = os.path.basename(ref_full_path)

                        # Unzip if the reference is gzipped(thats a fun name)
                        if ref_full_path.endswith('.gz'):
                            ref_img_path = ref_full_path[:-3]
                            if not os.path.exists(ref_img_path):
                                print(f" Unzipping reference image...")
                                if HEASOFT_BACKEND == "wsl":
                                    wsl_path = prepare_path(ref_full_path)
                                    gunzip_cmd = f"gunzip -k '{wsl_path}'"
                                    run_heasoft_command(gunzip_cmd) # I saw Heasoft had A way to do this And I trust it more then windows.
                                else:
                                    os.system(f'gunzip -k "{ref_full_path}"') # Input you best MACOS version here, or this might work.
                                ref_img_name = os.path.basename(ref_img_path)
                        else:
                            ref_img_path = ref_full_path

                        if not os.path.exists(ref_img_path):
                            print(f" ❌ Failed to access reference image")
                            corrections_failed += 1
                            continue

                        # Copy reference image into the observation's directory
                        # so uvotunicorr can find both files together.
                        #
                        # CHANGE FROM clean_uvot_tiles.py:
                        #   clean_uvot_tiles did this identically:
                        #       shutil.copy(ref_file_path, obs_directory)
                        #   We add a check to avoid copying a file onto itself (Safty first).
                        ref_img_dest = os.path.join(obs_dir, ref_img_name)
                        try:
                            if os.path.abspath(ref_img_path) != os.path.abspath(ref_img_dest):
                                shutil.copy(ref_img_path, ref_img_dest)
                                print(f"    Copied reference image: {ref_img_name}")
                        except Exception as e:
                            print(f" ❌ Failed to copy reference: {e}")
                            corrections_failed += 1
                            continue

                        
                        # Ready the Bachelors (observation img) 
                        # Find and unzip the observation sky image
                        obs_base = f"sw{obs_obsid}{band}_sk"
                        obs_dir_files = os.listdir(obs_dir)
                        obs_file_found = None

                        for f in obs_dir_files:
                            if f.startswith(obs_base):
                                obs_file_found = f
                                break

                        if not obs_file_found:
                            print(f" ❌ Observation file not found")
                            corrections_failed += 1
                            continue

                        obs_img_path = os.path.join(obs_dir, obs_file_found)

                        # Unzip observation image if needed
                        #
                        # CHANGE FROM clean_uvot_tiles.py:
                        #   clean_uvot_tiles had a separate unzipping pass that
                        #   ran before aspect correction, unzipping ALL files
                        #   upfront. I am going to unzip on demand per frame instead,
                        #   This avoids wasting disk space on frames we won't
                        #   process (smeared, orphaned, already corrected, etc).
                        if obs_img_path.endswith('.gz'):
                            obs_img_unzipped = obs_img_path[:-3]
                            if not os.path.exists(obs_img_unzipped):
                                print(f"    Unzipping observation image...")
                                if HEASOFT_BACKEND == "wsl":
                                    wsl_path = prepare_path(obs_img_path)
                                    gunzip_cmd = f"gunzip -k '{wsl_path}'"
                                    run_heasoft_command(gunzip_cmd)
                                else:
                                    os.system(f'gunzip -k "{obs_img_path}"') # Input you best MACOS version here, or this might work.
                            obs_img_path = obs_img_unzipped
                            obs_file_found = os.path.basename(obs_img_unzipped)

                        if not os.path.exists(obs_img_path):
                            print(f" ❌ Failed to unzip observation image")
                            corrections_failed += 1
                            continue

                        # RUN UVOTUNICORR
                        #
                        # CHANGE FROM clean_uvot_tiles.py:
                        #   clean_uvot_tiles called:
                        #       unicorr_command = up.create_uvotunicorr_bash_command(
                        #           ref_frame, obs_frame, obspath=obs_directory)
                        #   Which assumed native HEASOFT. I use a WSL So that is a no-no
                        #   when running through WSL.
                        if HEASOFT_BACKEND == "wsl":
                            unicorr_command = create_uvotunicorr_full_command_wsl(
                                ref_frame=ref_obsid,
                                obs_frame=obs_obsid,
                                band=band,
                                ref_snapshot=ref_snapshot,
                                obs_snapshot=obs_snapshot,
                                obspath=obs_dir
                            )

                            print(f" Running uvotunicorr (WSL)...")

                            # Force Python to release any open file handles
                            # before calling WSL, which runs in a separate filesystem namespace
                            gc.collect()

                            corrections_attempted += 1
                            result = run_uvotunicorr_wsl(unicorr_command)

                            # Wait for WSL process to finish writing to disk
                            time.sleep(5)

                        else:
                            # Native macOS/Linux HEASOFT
                            #  not yet A thing in this, you gotta do that Thomas, This is all you.
                            print(f" Error Thomas: ⚠️  macOS support not yet implemented - skipping")
                            corrections_failed += 1
                            continue

                            
                        # Check to see if it worked
                        # After uvotunicorr runs, we will open the new(hopefully corrected) FITS file
                        # and check the ASPCORR header keyword:
                        #   'DIRECT'  = shouldn't Happen, That wouldnt be correct
                        #   'UNICORR' = We did it!
                        #   'NONE'    = Rip
                        #
                        # CHANGE FROM clean_uvot_tiles.py:
                        #   clean_uvot_tiles checked ASPCORR in a separate pass
                        #   After all corrections were done:
                        #       new_aspect_uncorrected_frames = up.check_aspect_correction(filepath)
                        #   We check immediately after each correction, which
                        #   gives me a per-frame success/failure reading, good for debuging.
                        time.sleep(2) # Gotta make sure it gets its naps in.

                        # Find the corrected output file
                        corrected_base = f"sw{obs_obsid}{band}_sk"
                        corrected_files = [
                            f for f in os.listdir(obs_dir)
                            if f.startswith(corrected_base) and not f.endswith('.gz')
                        ]

                        if not corrected_files:
                            print(f" ❌ No corrected file found after uvotunicorr")
                            corrections_failed += 1
                            continue

                        # Prefer the shortest filename (usually the main output, I think.)
                        corrected_files.sort(key=lambda x: (len(x), x))
                        corrected_file = corrected_files[0]
                        corrected_path = os.path.join(obs_dir, corrected_file)

                        # Read the ASPCORR keyword from the corrected FITS header
                        try:
                            with fits.open(corrected_path) as hdul:
                                if obs_snapshot < len(hdul):
                                    aspcorr_after = hdul[obs_snapshot].header.get('ASPCORR', 'NONE')
                                    print(f" ASPCORR value after correction: {aspcorr_after}")

                                    if aspcorr_after.strip().upper() in ['DIRECT', 'UNICORR']:
                                        print(f" ✓ Correction successful - ASPCORR = {aspcorr_after}")
                                        corrections_successful += 1
                                    else:
                                        print(f" Ruh-Roh: ❌ Correction failed - ASPCORR still {aspcorr_after}")
                                        corrections_failed += 1
                                else:
                                    print(f" Ruh-Roh:❌ Extension {obs_snapshot} not found in corrected file")
                                    corrections_failed += 1
                        except Exception as e:
                            print(f" Ruh-Roh: ❌ Error checking corrected file: {e}")
                            corrections_failed += 1

                    except Exception as e:
                        print(f" Ruh-Roh: ❌ Error during correction: {e}")
                        import traceback
                        traceback.print_exc()
                        corrections_failed += 1   # I frogot to add this for so long. This go so long I forgot it was in a "try"

                # Band summery, mostly for debuging attempts, could be removed.
                print(f"\n  Band {band} Summary:")
                print(f" Attempted: {corrections_attempted}")
                print(f" Successful: {corrections_successful}")
                print(f" Failed: {corrections_failed}")

                # Record any remaining failures for this group+band combination
                remaining = corrections_failed
                if remaining > 0:
                    key = f"{group_id}_{band}"
                    aspectnone_dict[key] = remaining
                    # Build list of failed frame identifiers for retry/manual inspection
                    failed_obsids = []
                    for idx, obs_row in corrections_needed.iterrows():
                        failed_obsids.append(f"{obs_row['ObsID']}_ext{obs_row['Snapshot']}")
                    aspectnone_tiles_dict[key] = failed_obsids[:remaining]

                    
        ######################################################################################
        # Attempt to retry 

        total_remaining = sum(aspectnone_dict.values())
        print("\n" + "=" * 70)
        if attempt_num == 0:
            print("INITIAL ATTEMPT COMPLETE")
        else:
            print(f"RETRY ATTEMPT {attempt_num} COMPLETE")
        print("=" * 70)
        print(f"Frames still needing correction: {total_remaining}")

        # If everything worked, we're done
        if total_remaining == 0:
            print("\n✓ All frames successfully corrected!")
            break

        
        # Because that wont happen, retry prompt.
        # CHANGE FROM clean_uvot_tiles.py:
        #   clean_uvot_tiles had a nearly identical prompt:
        #       go_again = input('Do you wish to change the global parameters...? [Y/N]')
        #   The key difference is that on retry, clean_uvot_tiles re-ran
        #   EVERYTHING (download, detect, smear, unzip, correct). We only
        #   re-run the correction step on the specific failed frames.
        
        print(f"\n⚠️ {total_remaining} frames failed.")
        print("\nFailed frames by group:")
        for key, count in aspectnone_dict.items():
            print(f" {key}: {count} frames")

        retry = input("\nDo you want to retry failed frames with different parameters? (yes/no): ").strip().lower()

        if retry not in ['yes', 'y']:
            print("Stopping correction process.")
            break

        # Get new parameters from the user
        try:
            new_side_buffer = input(f"Enter new side_buffer value (current: {side_buffer}, press Enter to keep): ").strip()
            if new_side_buffer:
                side_buffer = int(new_side_buffer) # Again I really need to go all the way back if this changes. I should proably get rid of this.

            new_num_stars = input(f"Enter new num_stars value (current: {num_stars}, press Enter to keep): ").strip()
            if new_num_stars:
                num_stars = int(new_num_stars)

            print(f"\nRetrying with side_buffer={side_buffer}, num_stars={num_stars}")

        except ValueError:
            print("Invalid input - keeping current parameters")

        # Store failed frames for the next attempt and increment
        failed_frames_to_retry = aspectnone_tiles_dict.copy()
        attempt_num += 1

    #######################################################################################
    # The End
    print("\n" + "=" * 70)
    print("ASPECT CORRECTION FINAL SUMMARY")
    print("=" * 70)
    final_remaining = sum(aspectnone_dict.values()) if aspectnone_dict else 0
    print(f"Total frames still needing correction: {final_remaining}")

    if final_remaining > 0:
        print("\nFailed frames by group:")
        for key, count in aspectnone_dict.items():
            print(f"  {key}: {count} frames")

    return aspectnone_dict, aspectnone_tiles_dict
