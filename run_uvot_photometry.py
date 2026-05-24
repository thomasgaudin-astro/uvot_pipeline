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
        print("\nHanding off to batch runner...")
        run_batch_pipeline(
            batch_file=None,        # Will prompt
            parent_dir=None,        # Will prompt
            manual_aspect_correction=manual_aspect_correction,
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







def run_batch_pipeline(batch_file=None, parent_dir=None,
                       manual_aspect_correction=False):
    """
    Batch mode: read a list of targets from a CSV/TXT file, download
    SWIFT data for each, then run the full pipeline on each target
    sequentially.

    Per-target folder layout:
      parent_dir/
        Target_Name_1/
          (data + Steps 2-4 outputs)
        Target_Name_2/
          ...

    If a target's pipeline step fails, that target is logged and the
    batch continues with the next target.

    Returns a summary DataFrame of per-target outcomes.
    """
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

    if parent_dir is None:
        print("\nSelect a PARENT directory where all target folders will go...")
        root = tk.Tk(); root.withdraw()
        parent_dir = filedialog.askdirectory(
            title="Select parent directory for batch download"
        )
        if not parent_dir:
            print("No parent directory selected. Aborting.")
            return None

    os.makedirs(parent_dir, exist_ok=True)
    print(f"\nParent directory: {parent_dir}")
    print(f"Per-target folders will be created as: {parent_dir}/<Target>/")

    # Per-target outcomes
    outcomes = []
    batch_start = time.time()

# Use tqdm to show a clean overall progress bar
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

        target_dir = os.path.join(parent_dir, target_name)
        os.makedirs(target_dir, exist_ok=True)
        log_path = os.path.join(target_dir, "pipeline.log")

        # Compact status line for this target — shown above the progress bar
        target_iter.set_description(f"Target {i+1}/{len(targets_df)}: {target_name}")
        target_iter.write(f"\n[{target_name}] RA={target_ra}, Dec={target_dec}, "
                          f"R={radius}° — log: {log_path}")

        target_start = time.time()
        outcome = {
            'Target': target_name,
            'RA': target_ra,
            'Dec': target_dec,
            'Radius': radius,
            'Folder': target_dir,
            'Downloaded': 0,
            'Pipeline_Status': 'NOT_STARTED',
            'Error': '',
            'Runtime_min': 0,
        }

        # --- DOWNLOAD (silenced) ---
        stage_start = time.time()
        target_iter.write(f"  [{target_name}] Stage 1/6: Downloading from Swift archive...")
        try:
            with _silenced_to_logfile(log_path):
                query = ObsQuery(ra=str(target_ra), dec=str(target_dec), radius=radius)
                total_obs = len(query)
                if total_obs > 0:
                    for j, q in enumerate(query, start=1):
                        start_time_str = str(q.begin).replace(":", "-").replace(" ", "_")
                        obs_dir = os.path.join(target_dir, f"{q.obsid}_{start_time_str}")
                        os.makedirs(obs_dir, exist_ok=True)
                        if os.listdir(obs_dir):
                            continue
                        try:
                            Data(obsid=q.obsid, uvot=True, clobber=True, outdir=obs_dir)
                            outcome['Downloaded'] += 1
                        except Exception:
                            pass

            target_iter.write(f"  [{target_name}] Downloaded {outcome['Downloaded']} obs "
                              f"({(time.time()-stage_start)/60:.1f} min)")
            if total_obs == 0:
                outcome['Pipeline_Status'] = 'NO_DATA'
                outcome['Runtime_min'] = (time.time() - target_start) / 60.0
                outcomes.append(outcome)
                target_iter.write(f"  [{target_name}] No data found — skipping pipeline")
                continue
        except Exception as e:
            outcome['Pipeline_Status'] = 'DOWNLOAD_FAILED'
            outcome['Error'] = str(e)[:300]
            outcome['Runtime_min'] = (time.time() - target_start) / 60.0
            outcomes.append(outcome)
            target_iter.write(f"  [{target_name}] DOWNLOAD FAILED — see log")
            continue

        # --- PIPELINE STAGES (each silenced, status reported between) ---
        try:
            # Step 2: cleanup
            stage_start = time.time()
            target_iter.write(f"  [{target_name}] Stage 2/6: Data cleanup (uvotdetect, smear)...")
            with _silenced_to_logfile(log_path):
                # Append to log instead of overwriting from here on
                pass
            # Re-open log in append mode for subsequent stages
            with open(log_path, 'a', encoding='utf-8') as _:
                pass
            # Use append-mode silencer for all remaining stages
            with _silenced_append(log_path):
                results = clean_up_data(
                    automation_mode=True,
                    base_path=target_dir,
                    save_path=target_dir,
                )
            if results is None or results["observations_table"] is None:
                outcome['Pipeline_Status'] = 'CLEANUP_FAILED'
                outcome['Runtime_min'] = (time.time() - target_start) / 60.0
                outcomes.append(outcome)
                target_iter.write(f"  [{target_name}] CLEANUP FAILED — see log")
                continue
            obs_table = results["observations_table"]
            target_iter.write(f"  [{target_name}]    cleanup done "
                              f"({(time.time()-stage_start)/60:.1f} min)")

            # Step 3: aspect correction
            stage_start = time.time()
            target_iter.write(f"  [{target_name}] Stage 3/6: Aspect correction...")
            with _silenced_append(log_path):
                automated_aspect_correction(
                    obs_table=obs_table,
                    base_path=target_dir,
                    save_path=target_dir,
                    manual_mode=manual_aspect_correction,
                )
            target_iter.write(f"  [{target_name}]    aspect correction done "
                              f"({(time.time()-stage_start)/60:.1f} min)")

            # Step 3.4: orphan rescue
            stage_start = time.time()
            target_iter.write(f"  [{target_name}] Stage 4/6: Orphan rescue...")
            orphan_solutions = results.get('orphan_solutions')
            with _silenced_append(log_path):
                obs_table = rescue_orphan_frames(
                    obs_table=obs_table,
                    base_path=target_dir,
                    save_path=target_dir,
                    orphan_solutions=orphan_solutions,
                    manual_mode=manual_aspect_correction,
                )
            target_iter.write(f"  [{target_name}]    orphan rescue done "
                              f"({(time.time()-stage_start)/60:.1f} min)")

            # Step 3.5 quarantine + 3.6 SSS check
            stage_start = time.time()
            target_iter.write(f"  [{target_name}] Stage 5/6: Quarantine + SSS check...")
            with _silenced_append(log_path):
                _run_quarantine(target_dir, obs_table)
                obs_table = check_sss_before_summation(
                    obs_table=obs_table,
                    base_path=target_dir,
                    save_path=target_dir,
                    target_ra=target_ra,
                    target_dec=target_dec,
                )
            target_iter.write(f"  [{target_name}]    quarantine + SSS done "
                              f"({(time.time()-stage_start)/60:.1f} min)")

            # Step 4: photometry
            stage_start = time.time()
            target_iter.write(f"  [{target_name}] Stage 6/6: Photometry extraction...")
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
            target_iter.write(f"  [{target_name}]    photometry done "
                              f"({(time.time()-stage_start)/60:.1f} min)")

            outcome['Pipeline_Status'] = 'SUCCESS'

        except Exception as e:
            outcome['Pipeline_Status'] = 'PIPELINE_FAILED'
            outcome['Error'] = str(e)[:300]
            target_iter.write(f"  [{target_name}] PIPELINE FAILED: {e} — see log")

        outcome['Runtime_min'] = (time.time() - target_start) / 60.0
        outcomes.append(outcome)

        target_iter.write(
            f"  [{target_name}] FINISHED in {outcome['Runtime_min']:.1f} min "
            f"[{outcome['Pipeline_Status']}]"
        )

    target_iter.close()

    print(f"\n  Target {target_name} finished in "
              f"{outcome['Runtime_min']:.1f} min "
              f"[{outcome['Pipeline_Status']}]")

    # Save batch summary
    summary_df = pd.DataFrame(outcomes)
    summary_path = os.path.join(parent_dir, "batch_summary.csv")
    summary_df.to_csv(summary_path, index=False)

    batch_runtime = (time.time() - batch_start) / 60.0

    print("\n" + "=" * 70)
    print("BATCH RUN COMPLETE")
    print("=" * 70)
    print(f"Total runtime: {batch_runtime:.1f} min")
    print(f"Targets attempted: {len(targets_df)}")
    print(f"Successful: {(summary_df['Pipeline_Status'] == 'SUCCESS').sum()}")
    print(f"Failed:     {(summary_df['Pipeline_Status'] != 'SUCCESS').sum()}")
    print(f"Summary saved: {summary_path}")
    print("=" * 70)

    return summary_df
