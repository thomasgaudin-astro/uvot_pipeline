def run_uvot_pipeline(manual_aspect_correction=False):
    """
    Complete Swift UVOT processing pipeline. You can set manual mode above.
    """

    print("\n" + "=" * 70)
    print("SWIFT UVOT COMPLETE PIPELINE")
    print("=" * 70)
 
    #####################################################################
    # STEP 1: SETUP DATA DIRECTORIES
    setup = setup_data_directories()
    if setup is None:
        print("\n ❌ Setup cancelled.")
        return

    # Check for batch mode short-circuit
    if setup.get('_batch_mode'):
        batch_mode_type = setup.get('_batch_mode_type', 'full')
        print(f"\nHanding off to batch runner (mode: {batch_mode_type})...")
        run_batch_pipeline(
            batch_file=None,        # Will prompt
            parent_dir=None,        # Will prompt
            manual_aspect_correction=manual_aspect_correction,
            mode=batch_mode_type,
        )
        return

    data_dir = setup['data_directory']
    save_dir = setup['save_directory']
    target_ra = setup['target_ra']
    target_dec = setup['target_dec']
 
    #####################################################################
    # STEP 2: DATA CLEANUP

    print("\n[PIPELINE STEP 2/4] Running data cleanup...")
 
    results = clean_up_data(
        automation_mode=True,
        base_path=data_dir,
        save_path=save_dir,
    )
 
    if results is None or results["observations_table"] is None:
        print("\n❌ Ruh-Roh-Cleanup failed.")
        return
 
    obs_table = results["observations_table"]
 
    #####################################################################
    # STEP 3: AUTOMATED ASPECT CORRECTION

    print("\n[PIPELINE STEP 3/4] Running automated aspect correction...")
    
    aspectnone_dict, aspectnone_tiles_dict = automated_aspect_correction(
        obs_table=obs_table,
        base_path=data_dir,
        save_path=save_dir,
        manual_mode=manual_aspect_correction,
    )


    ####################################################################
    # Step 3.4: Orphan rescue (synthetic reference correction)
    # Build deep synthetic reference per (group, band), retry uvotunicorr
    # on orphan frames against the synthetic. Must run BEFORE quarantine
    # so any successfully-rescued orphans avoid being moved out.

    print("\n[PIPELINE STEP 3.4] Running orphan rescue...")
    orphan_solutions = results.get('orphan_solutions')
    obs_table = rescue_orphan_frames(
        obs_table=obs_table,
        base_path=data_dir,
        save_path=save_dir,
        orphan_solutions=orphan_solutions,
        manual_mode=manual_aspect_correction,
    )
 
    ####################################################################
    # STEP 3.5: QUARANTINE UNUSABLE OBSERVATIONS
    # Before uvotsource runs, we move observations that are completely
    # unusable out of the data directory.  Two categories:
    #
    #   1. ORPHAN frames -> moved to "Orphans/" subfolder
    #      These have no nearby DIRECT reference to correct against. and that isnt setup yet
    #
    #   2. Fully uncorrected (ALL extensions NONE on ALL bands) → "NotASPCORR/"
    #      Aspect correction was attempted but failed on every extension.
    #
    # Partially-corrected observations (mix of DIRECT and NONE extensions)
    # are KEPT in place.  The summation step (4c) will use uvotimsum's
    # exclude parameter to skip the NONE extensions and only sum the
    # corrected ones, so the resulting summed image is clean. At least it should.

    print("\n" + "=" * 70)
    print("STEP 3.5: QUARANTINING UNUSABLE OBSERVATIONS")
    print("=" * 70)
 
    # Set up quarantine directories
    not_aspcorr_dir = os.path.join(data_dir, "NotASPCORR")
    orphans_dir = os.path.join(data_dir, "Orphans")
    os.makedirs(not_aspcorr_dir, exist_ok=True)
    os.makedirs(orphans_dir, exist_ok=True)
 
    # Folders we never touch (already quarantined or special)
    QUARANTINE_FOLDERS = {"Smeared", "NotASPCORR", "Orphans"}
 
    obsid_pattern = re.compile(r"(\d{11})")
 
    ################################################################
    # 3.5a: MOVE ORPHAN OBSERVATIONS
    # Collect orphan OBSIDs from the obs_table.
    # This includes:
    #   - Group_Status == 'ORPHAN' (couldn't be grouped spatially)
    #   - Group_ID == -1 (never matched into all_frames_df at all)
    #   - Group_Status == 'UNKNOWN' (populate_observations_table
    
    #     couldn't find group info — same root cause as Group -1)
    orphan_obsids = set()
    if obs_table is not None:
        orphan_mask = pd.Series(False, index=obs_table.index)
 
        if 'Group_Status' in obs_table.columns:
            orphan_mask |= (obs_table['Group_Status'] == 'ORPHAN')
            orphan_mask |= (obs_table['Group_Status'] == 'UNKNOWN')
 
        if 'Group_ID' in obs_table.columns:
            orphan_mask |= (obs_table['Group_ID'] == -1)
 
        orphan_obsids = set(obs_table.loc[orphan_mask, 'ObsID'].astype(str).unique())
 
    if orphan_obsids:
        print(f"\nOrphan OBSIDs identified: {len(orphan_obsids)}")
    else:
        print("\nNo orphan observations to quarantine.")
 
    orphan_moved = 0
    top_folders = [f for f in os.listdir(data_dir)
                   if os.path.isdir(os.path.join(data_dir, f))
                   and f not in QUARANTINE_FOLDERS]
 
    for folder in top_folders:
        match = obsid_pattern.search(folder)
        if not match:
            continue
        obsid = match.group(1)
 
        if obsid not in orphan_obsids:
            continue
 
        folder_path = os.path.join(data_dir, folder)
        dest = os.path.join(orphans_dir, folder)
 
        if os.path.exists(dest):
            print(f"  {obsid} — already in Orphans/, skipping")
            continue
 
        try:
            shutil.move(folder_path, dest)
            print(f"  Moved {folder} → Orphans/")
            orphan_moved += 1
        except Exception as e:
            print(f"  Error moving {folder}: {e}")
 
    print(f"Orphan observations quarantined: {orphan_moved}")
 
    ##############################################################################
    # 3.5b: MOVE FULLY-UNCORRECTED OBSERVATIONS
    # Only quarantine observations where EVERY extension on EVERY
    # band is NONE.  Partially-corrected observations are kept 
    # uvotimsum will exclude the bad extensions during summation... I think
    print(f"\nScanning remaining observations for fully-uncorrected files...")
 
    aspcorr_moved = 0
 
    # Re-read top folders (some may have been moved to Orphans above)
    top_folders = [f for f in os.listdir(data_dir)
                   if os.path.isdir(os.path.join(data_dir, f))
                   and f not in QUARANTINE_FOLDERS]
 
    for folder in top_folders:
        match = obsid_pattern.search(folder)
        if not match:
            continue
        obsid = match.group(1)
        folder_path = os.path.join(data_dir, folder)
 
        # Scan all SK files and check if ANY extension is corrected
        has_any_correction = False
        found_any_sk = False
 
        for root_d, _, fnames in os.walk(folder_path):
            for fname in fnames:
                if "_sk.img" not in fname:
                    continue
                band_found = False
                for b in ["uvv", "uuu", "ubb", "um2", "uw1", "uw2"]:
                    if b in fname:
                        band_found = True
                        break
                if not band_found:
                    continue
 
                found_any_sk = True
                fpath = os.path.join(root_d, fname)
 
                try:
                    with fits.open(fpath) as hdul:
                        for hdu in hdul:
                            naxis = hdu.header.get('NAXIS', 0)
                            if naxis < 2:
                                continue
                            val = str(hdu.header.get("ASPCORR", "NONE")).strip().upper()
                            if val in ("DIRECT", "UNICORR"):
                                has_any_correction = True
                                break
                    if has_any_correction:
                        break
                except Exception:
                    continue
            if has_any_correction:
                break
 
        if not found_any_sk:
            continue
 
        # If at least one extension is corrected, keep it, uvotimsum
        # will handle excluding the bad extensions during summation... I think?
        if has_any_correction:
            continue
 
        # Fully uncorrected, move to NotASPCORR
        dest = os.path.join(not_aspcorr_dir, folder)
        if os.path.exists(dest):
            print(f"  {obsid} — already in NotASPCORR/, skipping")
            continue
 
        try:
            shutil.move(folder_path, dest)
            print(f"  Moved {folder} -> NotASPCORR/  (all extensions NONE)")
            aspcorr_moved += 1
        except Exception as e:
            print(f"  Error moving {folder}: {e}")
 
    print(f"Fully-uncorrected observations quarantined: {aspcorr_moved}")
 
    # Summary
    total_quarantined = orphan_moved + aspcorr_moved
    print(f"\n{'─' * 70}")
    print(f"QUARANTINE SUMMARY")
    print(f"  Orphans -> Orphans/          : {orphan_moved}")
    print(f"  Fully NONE -> NotASPCORR/    : {aspcorr_moved}")
    print(f"  Total quarantined            : {total_quarantined}")
    remaining_folders = [f for f in os.listdir(data_dir)
                         if os.path.isdir(os.path.join(data_dir, f))
                         and f not in QUARANTINE_FOLDERS]
    print(f"  Remaining observations       : {len(remaining_folders)}")
    print(f"  (Partial corrections handled by uvotimsum exclude)")
    print(f"{'─' * 70}")

    #################################################################################
    # Step 3.6: Pre-summation SSS check
    # Runs uvotsource on individual extensions of multi-ext observations
    # to flag any extension where the source lands on a bad pixel
    # (AB_MAG=99). These flagged extensions get added to uvotimsum's
    # exclude list during Step 4 summation.
    
    print("\n[PIPELINE STEP 3.6] Running pre-summation SSS check...")
    obs_table = check_sss_before_summation(
        obs_table=obs_table,
        base_path=data_dir,
        save_path=save_dir,
        target_ra=target_ra,
        target_dec=target_dec,
    )
    
    #################################################################################
    # STEP 4: PHOTOMETRY EXTRACTION  (summation -> uvotsource -> master data)

    print("\n[PIPELINE STEP 4/4] Running photometry extraction...")

    # Reload obs_table so it reflects corrections
    table_path = os.path.join(save_dir, "observations_table.csv")
    if os.path.exists(table_path):
        obs_table = pd.read_csv(table_path)

    master_photometry = run_uvotsource_pipeline(
        obs_table=obs_table,
        base_path=data_dir,
        save_path=save_dir,
        source_reg=None,
        bkg_reg=None,
        target_ra=target_ra,       # NEW: passed through
        target_dec=target_dec,     # NEW: passed through
        automation_mode=False,
    )
 
    #################################################################################
    # FINAL SUMMARY

    print("\n" + "=" * 70)
    print("PIPELINE COMPLETE")
    print("=" * 70)
    print(f"Data directory: {data_dir}")
    print(f"Save directory: {save_dir}")
    print("\nGenerated files:")
    print("  - observations_table.csv")
    print("  - workload_summary.csv")
    print("  - Orphans/ (quarantined orphan observations)")
    print("  - NotASPCORR/ (quarantined uncorrected/partial observations)")
    if aspectnone_dict and sum(aspectnone_dict.values()) > 0:
        print("  - bad_frames.csv")
    print("  - master_photometry.csv")
    print("  - UVOT_Data_Analysis.xlsx")
    print("=" * 70)






def _obsid_has_uvot_data(obs_dir):
    """
    Check if an obsid folder has at least one UVOT sky image.
    Used by the batch downloader to detect partial/interrupted downloads
    and missing data, so they can be re-downloaded.

    Returns True if the folder has a valid-looking UVOT image dir with
    at least one _sk.img or _sk.img.gz file, False otherwise.
    """
    if not os.path.isdir(obs_dir):
        return False
    for root, dirs, files in os.walk(obs_dir):
        # Look for {obsid}/uvot/image/ subfolder
        norm = root.replace("\\", "/")
        if not norm.endswith("/uvot/image"):
            continue
        # Must have at least one sky image file
        for f in files:
            if f.endswith("_sk.img.gz") or f.endswith("_sk.img"):
                return True
    return False


def run_batch_pipeline(batch_file=None, parent_dir=None,
                       manual_aspect_correction=False,
                       mode='full'):
    """
    Batch mode: read a list of targets from a CSV/TXT file, then either
    download Swift data, run the pipeline, or both.

    mode controls behavior:
      'full'         - download AND run pipeline on each target (default)
      'download'     - download only; user can process later
      'process'      - skip download, assume data already exists; just process

    For 'process' mode: parent_dir must contain subfolders named exactly
    as each target's sanitized name from the CSV. e.g. for target
    '4FGL J0004.4-4001' the folder must be 'parent_dir/4FGL_J0004.4-4001/'.

    Per-target folder layout (matches what 'download' mode creates):
      parent_dir/
        Target_Name_1/
          (data + Steps 2-4 outputs)
        Target_Name_2/
          ...

    Returns a summary DataFrame of per-target outcomes.
    """
    if mode not in ('full', 'download', 'process'):
        print(f"ERROR: Invalid mode '{mode}'. Use 'full', 'download', or 'process'.")
        return None

    if batch_file is None:
        print("Select your batch input file (CSV or TXT)...")
        root = tk.Tk(); root.withdraw()
        batch_file = filedialog.askopenfilename(
            title="Select batch target list",
            filetypes=[("CSV/TXT", "*.csv *.txt"), ("All", "*.*")]
        )
        if not batch_file:
            print("No batch file selected. Aborting.")
            return None

    print(f"\nLoading batch file: {batch_file}")
    targets_df = load_batch_targets(batch_file)
    if targets_df is None or targets_df.empty:
        return None

    print(f"\nLoaded {len(targets_df)} target(s):")
    print(targets_df.to_string(index=False))

    # Different prompt depending on mode
    if mode == 'process':
        prompt = ("Select the PARENT directory where each target's folder lives "
                  "(subfolders must be named exactly like the Target column)...")
    elif mode == 'download':
        prompt = "Select the PARENT directory for batch download..."
    else:
        prompt = "Select a PARENT directory where all target folders will go..."

    if parent_dir is None:
        print(f"\n{prompt}")
        root = tk.Tk(); root.withdraw()
        parent_dir = filedialog.askdirectory(title=prompt)
        if not parent_dir:
            print("No parent directory selected. Aborting.")
            return None

    os.makedirs(parent_dir, exist_ok=True)
    print(f"\nParent directory: {parent_dir}")
    print(f"Per-target folders expected at: {parent_dir}/<Target>/")
    print(f"Mode: {mode}")

    # If process-only, sanity-check that the expected subfolders exist
    if mode == 'process':
        missing = []
        for _, row in targets_df.iterrows():
            tgt = row['Target']
            tdir = os.path.join(parent_dir, tgt)
            if not os.path.isdir(tdir):
                missing.append(tgt)
        if missing:
            print(f"\nERROR: Missing subfolders for {len(missing)} target(s):")
            for m in missing:
                print(f"  - {os.path.join(parent_dir, m)}")
            print(f"\nProcess-only mode requires existing subfolders. Aborting.")
            return None
        print(f"All {len(targets_df)} target folders found.")

    # Track per-target outcomes
    outcomes = []
    batch_start = time.time()

    # Use tqdm for a clean overall progress bar
    target_iter = tqdm(
        list(targets_df.iterrows()),
        desc="Batch progress",
        unit="target",
        bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
    )

    for i, row in target_iter:
        target_name = row['Target']
        target_ra = float(row['RA'])
        target_dec = float(row['Dec'])
        radius = float(row['Radius'])
        target_threshold = float(row.get('Threshold', DEFAULT_DETECT_THRESHOLD))

        target_dir = os.path.join(parent_dir, target_name)
        os.makedirs(target_dir, exist_ok=True)
        log_path = os.path.join(target_dir, "pipeline.log")

        target_iter.set_description(f"Target {i+1}/{len(targets_df)}: {target_name}")
        target_iter.write(f"\n[{target_name}] RA={target_ra}, Dec={target_dec}, "
                          f"R={radius}° — log: {log_path}")
        target_iter.write(f"  [{target_name}] Mode: {mode}")
        if mode != 'download':
            target_iter.write(f" [{target_name}] Detect threshold: {target_threshold}")

        target_start = time.time()
        outcome = {
            'Target': target_name,
            'RA': target_ra,
            'Dec': target_dec,
            'Radius': radius,
            'Threshold': target_threshold,
            'Folder': target_dir,
            'Mode': mode,
            'Downloaded': 0,
            'Pipeline_Status': 'NOT_STARTED',
            'Error': '',
            'Runtime_min': 0,
        }

        # --- DOWNLOAD (skipped in process-only mode) ---
        if mode in ('full', 'download'):
            stage_start = time.time()
            target_iter.write(f" [{target_name}] Stage 1/6: Downloading from Swift archive...")
            try:
                with _silenced_to_logfile(log_path):
                    query = ObsQuery(ra=str(target_ra), dec=str(target_dec), radius=radius)
                    total_obs = len(query)
                    skipped_existing = 0
                    redownloaded_partial = 0
                    if total_obs > 0:
                        for j, q in enumerate(query, start=1):
                            start_time_str = str(q.begin).replace(":", "-").replace(" ", "_")
                            obs_dir = os.path.join(target_dir, f"{q.obsid}_{start_time_str}")
                            os.makedirs(obs_dir, exist_ok=True)

                            # Skip only if obs has actual UVOT sky image data.
                            # Folder existing isn't enough, partial downloads
                            # or missing files trigger re-download.
                            if _obsid_has_uvot_data(obs_dir):
                                skipped_existing += 1
                                continue

                            # Re-download if folder existed but had no good data
                            if os.listdir(obs_dir):
                                redownloaded_partial += 1

                            try:
                                Data(obsid=q.obsid, uvot=True, clobber=True, outdir=obs_dir)
                                outcome['Downloaded'] += 1
                            except Exception:
                                pass

                target_iter.write(f"  [{target_name}] Downloaded {outcome['Downloaded']} new obs "
                                  f"({(time.time()-stage_start)/60:.1f} min)")
                if skipped_existing:
                    target_iter.write(f"  [{target_name}] Skipped {skipped_existing} obs (already complete)")
                if redownloaded_partial:
                    target_iter.write(f"  [{target_name}] Re-downloaded {redownloaded_partial} obs (missing UVOT data)")
                if total_obs == 0:
                    outcome['Pipeline_Status'] = 'NO_DATA'
                    outcome['Runtime_min'] = (time.time() - target_start) / 60.0
                    outcomes.append(outcome)
                    target_iter.write(f"  [{target_name}] No data found — skipping")
                    continue

            except Exception as e:
                outcome['Pipeline_Status'] = 'DOWNLOAD_FAILED'
                outcome['Error'] = str(e)[:300]
                outcome['Runtime_min'] = (time.time() - target_start) / 60.0
                outcomes.append(outcome)
                target_iter.write(f" [{target_name}] DOWNLOAD FAILED — see log")
                continue

        # If download-only mode, we're done with this target
        if mode == 'download':
            outcome['Pipeline_Status'] = 'DOWNLOAD_COMPLETE'
            outcome['Runtime_min'] = (time.time() - target_start) / 60.0
            outcomes.append(outcome)
            target_iter.write(f" [{target_name}] DOWNLOAD COMPLETE in "
                              f"{outcome['Runtime_min']:.1f} min")
            continue

        # --- PIPELINE STAGES (for 'full' and 'process' modes) ---
        try:
            # Step 2: cleanup
            stage_start = time.time()
            target_iter.write(f" [{target_name}] Stage 2/6: Data cleanup (uvotdetect, smear)...")
            with _silenced_append(log_path):
                results = clean_up_data(
                    automation_mode=True,
                    base_path=target_dir,
                    save_path=target_dir,
                    detect_threshold=target_threshold,
                )
            if results is None or results["observations_table"] is None:
                outcome['Pipeline_Status'] = 'CLEANUP_FAILED'
                outcome['Runtime_min'] = (time.time() - target_start) / 60.0
                outcomes.append(outcome)
                target_iter.write(f" [{target_name}] CLEANUP FAILED — see log")
                continue
            obs_table = results["observations_table"]
            target_iter.write(f" [{target_name}]    cleanup done "
                              f"({(time.time()-stage_start)/60:.1f} min)")

            # Step 3: aspect correction
            stage_start = time.time()
            target_iter.write(f" [{target_name}] Stage 3/6: Aspect correction...")
            with _silenced_append(log_path):
                automated_aspect_correction(
                    obs_table=obs_table,
                    base_path=target_dir,
                    save_path=target_dir,
                    manual_mode=manual_aspect_correction,
                )
            target_iter.write(f"[{target_name}] aspect correction done "
                              f"({(time.time()-stage_start)/60:.1f} min)")

            # Step 3.4: orphan rescue
            stage_start = time.time()
            target_iter.write(f"[{target_name}] Stage 4/6: Orphan rescue...")
            orphan_solutions = results.get('orphan_solutions')
            with _silenced_append(log_path):
                obs_table = rescue_orphan_frames(
                    obs_table=obs_table,
                    base_path=target_dir,
                    save_path=target_dir,
                    orphan_solutions=orphan_solutions,
                    manual_mode=manual_aspect_correction,
                )
            target_iter.write(f"[{target_name}] orphan rescue done "
                              f"({(time.time()-stage_start)/60:.1f} min)")

            # Step 3.5 quarantine + 3.6 SSS check
            stage_start = time.time()
            target_iter.write(f"[{target_name}] Stage 5/6: Quarantine + SSS check...")
            with _silenced_append(log_path):
                _run_quarantine(target_dir, obs_table)
                obs_table = check_sss_before_summation(
                    obs_table=obs_table,
                    base_path=target_dir,
                    save_path=target_dir,
                    target_ra=target_ra,
                    target_dec=target_dec,
                )
            target_iter.write(f"[{target_name}]  quarantine + SSS done "
                              f"({(time.time()-stage_start)/60:.1f} min)")

            # Step 4: photometry
            stage_start = time.time()
            target_iter.write(f"[{target_name}] Stage 6/6: Photometry extraction...")
            with _silenced_append(log_path):
                run_uvotsource_pipeline(
                    obs_table=obs_table,
                    base_path=target_dir,
                    save_path=target_dir,
                    source_reg=None,
                    bkg_reg=None,
                    target_ra=target_ra,
                    target_dec=target_dec,
                    automation_mode=False,
                )
            target_iter.write(f"[{target_name}]    photometry done "
                              f"({(time.time()-stage_start)/60:.1f} min)")

            outcome['Pipeline_Status'] = 'SUCCESS'

        except Exception as e:
            outcome['Pipeline_Status'] = 'PIPELINE_FAILED'
            outcome['Error'] = str(e)[:300]
            target_iter.write(f"[{target_name}] PIPELINE FAILED: {e} — see log")

        outcome['Runtime_min'] = (time.time() - target_start) / 60.0
        outcomes.append(outcome)

        target_iter.write(
            f"[{target_name}] FINISHED in {outcome['Runtime_min']:.1f} min "
            f"[{outcome['Pipeline_Status']}]"
        )

    target_iter.close()

    # Save batch summary
    summary_df = pd.DataFrame(outcomes)
    summary_path = os.path.join(parent_dir, f"batch_summary_{mode}.csv")
    summary_df.to_csv(summary_path, index=False)

    batch_runtime = (time.time() - batch_start) / 60.0

    print("\n" + "=" * 70)
    print(f"BATCH RUN COMPLETE ({mode.upper()} MODE)")
    print("=" * 70)
    print(f"Total runtime: {batch_runtime:.1f} min")
    print(f"Targets attempted: {len(targets_df)}")
    success_count = (summary_df['Pipeline_Status'].isin(
        ['SUCCESS', 'DOWNLOAD_COMPLETE'])).sum()
    print(f"Successful: {int(success_count)}")
    print(f"Failed: {len(targets_df) - int(success_count)}")
    print(f"Summary saved: {summary_path}")
    print("=" * 70)

    return summary_df
