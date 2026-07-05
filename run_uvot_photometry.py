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

    # dataInit pool mode short-circuit
    if setup.get('_datainit_mode'):
        print("\nHanding off to dataInit batch driver...")
        run_dataInit_batch(
            batch_file=None,          # will prompt
            datainit_path=None,       # will prompt
            dataSRC_path=None,        # will prompt
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
    # FINAL SUMMARY and cleanup

    _cleanup_after_processing(data_dir, save_root=save_dir, label="run")

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



# -----------------------------------------------------------------------
 
 
def _process_one_field_group(group, parent_dir, manual_aspect_correction,
                             mode, outer_iter=None):
    """
    Run the pipeline on ONE field group.
 
    Shared phase (ONCE for the whole field): cleanup, aspect correction,
    orphan rescue, quarantine, SSS check, summation.
 
    Per-target phase (ONCE per target in the field): regions, uvotsource,
    upper limits, compilation + light curves, all tagged, with each
    target's deliverables written to its own results subfolder inside the
    field folder.
 
    For a single-target / no-field group this goes into a shared phase +
    one untagged per-target pass written to the (target) folder itself.
    """
    def _say(msg):
        if outer_iter is not None:
            outer_iter.write(msg)
        else:
            print(msg)
 
    gname = group['folder_name']
    is_field = group['is_field_group']
    field_dir = os.path.join(parent_dir, gname)
    os.makedirs(field_dir, exist_ok=True)
    log_path = os.path.join(field_dir, "pipeline.log")
    targets = group['targets']
 
    # Threshold: use the first target's (shared cleanup needs one value).
    first = targets[0]
    detect_threshold = float(first.get('Threshold', DEFAULT_DETECT_THRESHOLD))
 
    group_start = time.time()
    outcomes = []
 
    _say(f"\n[{gname}] {'FIELD' if is_field else 'target'} group — "
         f"{len(targets)} target(s): "
         + ", ".join(str(t['Target']) for t in targets))
 
    # SHARED PHASE (once per field) 
    try:
        stage_start = time.time()
        _say(f"[{gname}] Shared 1/5: Data cleanup (uvotdetect, smear, table)...")
        with _silenced_append(log_path):
            results = clean_up_data(
                automation_mode=True,
                base_path=field_dir,
                save_path=field_dir,
                detect_threshold=detect_threshold,
            )
        if results is None or results.get("observations_table") is None:
            _say(f"[{gname}] CLEANUP FAILED — see log")
            for t in targets:
                outcomes.append(_failed_outcome(t, gname, field_dir, mode,
                                                'CLEANUP_FAILED', group_start))
            return outcomes
        obs_table = results["observations_table"]
        _say(f"[{gname}]   cleanup done ({(time.time()-stage_start)/60:.1f} min)")
 
        stage_start = time.time()
        _say(f"[{gname}] Shared 2/5: Aspect correction...")
        with _silenced_append(log_path):
            automated_aspect_correction(
                obs_table=obs_table, base_path=field_dir, save_path=field_dir,
                manual_mode=manual_aspect_correction,
            )
        _say(f"[{gname}]   aspect correction done "
             f"({(time.time()-stage_start)/60:.1f} min)")
 
        stage_start = time.time()
        _say(f"[{gname}] Shared 3/5: Orphan rescue...")
        with _silenced_append(log_path):
            obs_table = rescue_orphan_frames(
                obs_table=obs_table, base_path=field_dir, save_path=field_dir,
                orphan_solutions=results.get('orphan_solutions'),
                manual_mode=manual_aspect_correction,
            )
        _say(f"[{gname}]   orphan rescue done "
             f"({(time.time()-stage_start)/60:.1f} min)")
 
        # SSS 
        probe_ra = float(first['RA'])
        probe_dec = float(first['Dec'])
        stage_start = time.time()
        _say(f"[{gname}] Shared 4/5: Quarantine + SSS check...")
        with _silenced_append(log_path):
            _run_quarantine(field_dir, obs_table)
            obs_table = check_sss_before_summation(
                obs_table=obs_table, base_path=field_dir, save_path=field_dir,
                target_ra=probe_ra, target_dec=probe_dec,
            )
        _say(f"[{gname}]   quarantine + SSS done "
             f"({(time.time()-stage_start)/60:.1f} min)")
 
        stage_start = time.time()
        _say(f"[{gname}] Shared 5/5: Summation (once for field)...")
        with _silenced_append(log_path):
            image_dirs = run_summation_shared(obs_table, field_dir)
        _say(f"[{gname}]   summation done "
             f"({(time.time()-stage_start)/60:.1f} min)")

        # Total shared-phase time (everything from group_start through
        # summation). Added to each target's Runtime_min below so the
        # reported per-target time is the real time, not just the photometry
        shared_min = (time.time() - group_start) / 60.0
        _say(f"[{gname}]   shared phase total: {shared_min:.1f} min")

    except Exception as e:
        _say(f"[{gname}] SHARED PHASE FAILED: {str(e)[:200]} — see log")
        for t in targets:
            outcomes.append(_failed_outcome(t, gname, field_dir, mode,
                                            'SHARED_FAILED', group_start,
                                            err=str(e)[:300]))
        return outcomes
 
    # PER-TARGET PHASE 
    for t in targets:
        tname = t['Target']
        tra = float(t['RA'])
        tdec = float(t['Dec'])
        t_start = time.time()
 
        # Single/no-field group: write deliverables to the (target) folder itself
        if is_field:
            target_tag = tname
            save_dir = os.path.join(field_dir, f"results_{tname}")
            os.makedirs(save_dir, exist_ok=True)
        else:
            target_tag = None
            save_dir = field_dir
 
        _say(f"[{gname}] Photometry for target {tname} "
             f"(tag={target_tag or 'none'}) → {os.path.basename(save_dir)}/")
        try:
            with _silenced_append(log_path):
                run_photometry_for_target(
                    obs_table=obs_table,
                    base_path=field_dir,
                    save_path=save_dir,
                    image_dirs=image_dirs,
                    target_ra=tra, target_dec=tdec,
                    target=target_tag,
                    source_reg=None, bkg_reg=None,
                    automation_mode=False,
                    run_allframes=bool(t.get('AllFrames', RUN_ALLFRAMES)),
                    run_timeavg=bool(t.get('TimeAvg', RUN_TIMEAVG)),
                    finder_fov=(float(t['FinderFOV']) if pd.notna(t.get('FinderFOV', float('nan'))) else None),
                )
            status = 'SUCCESS'
        except Exception as e:
            _say(f"[{gname}] target {tname} PHOTOMETRY FAILED: {str(e)[:200]}")
            status = 'PHOTOMETRY_FAILED'
 
        photometry_min = (time.time() - t_start) / 60.0
        outcomes.append({
            'Target': tname, 'Field': group['field'],
            'Folder': save_dir, 'Mode': mode,
            'Pipeline_Status': status, 'Error': '',
            # Shared phase (cleanup/aspect/orphan/SSS/summation) runs once
            # per field; photometry is per-target. Runtime_min = shared +
            # this target's photometry, so a single-target group reports its
            # true total. For multi-target fields the shared time appears in
            # each target's row (don't sum the column across a field).
            'Shared_Min': round(shared_min, 4),
            'Photometry_Min': round(photometry_min, 4),
            'Runtime_min': round(shared_min + photometry_min, 4),
        })
        _say(f"[{gname}] target {tname} done "
             f"(photometry {photometry_min:.1f} min, "
             f"total incl. shared {shared_min + photometry_min:.1f} min) [{status}]")

    _cleanup_after_processing(field_dir, save_root=field_dir, label=gname)
    return outcomes
 
 
def _failed_outcome(t, gname, folder, mode, status, group_start, err=''):
    return {
        'Target': t['Target'], 'Field': gname, 'Folder': folder,
        'Mode': mode, 'Pipeline_Status': status, 'Error': err,
        'Runtime_min': (time.time() - group_start) / 60.0,
    }
 
 
def run_batch_pipeline(batch_file=None, parent_dir=None,
                       manual_aspect_correction=False,
                       mode='full'):
    """
    Batch mode: read a list of targets from a CSV/TXT file, then either
    download Swift data, run the pipeline, or both.

    mode controls behavior:
      'full'         - download AND run pipeline on each target (default)
      'download'     - download only, user can process later
      'process'      - skip download, assume data already exists, just process

    For 'process' mode: parent_dir must contain subfolders named exactly
    as each target's sanitized name from the CSV. e.g. for target
    '4FGL J0004.4-4001' the folder must be 'parent_dir/4FGL_J0004.4-4001/'.

    Per-target folder layout (matches what 'download' mode creates):
      parent_dir/
        Target_Name_1/
          (data + Steps 2-4 outputs)
        Target_Name_2/
          ...
    """
    if mode not in ('full', 'download', 'process'):
        print(f"ERROR: Invalid mode '{mode}'.")
        return None
 
    if batch_file is None:
        print("Select your batch input file (CSV or TXT)...")
        root = tk.Tk(); root.withdraw()
        batch_file = filedialog.askopenfilename(
            title="Select batch target list",
            filetypes=[("CSV/TXT", "*.csv *.txt"), ("All", "*.*")])
        if not batch_file:
            print("No batch file selected. Aborting.")
            return None
 
    print(f"\nLoading batch file: {batch_file}")
    targets_df = load_batch_targets(batch_file)
    if targets_df is None or targets_df.empty:
        return None
 
    print(f"\nLoaded {len(targets_df)} target(s):")
    print(targets_df.to_string(index=False))
 
    if mode == 'process':
        prompt = "Select the PARENT directory where each field/target folder lives..."
    elif mode == 'download':
        prompt = "Select the PARENT directory for batch download..."
    else:
        prompt = "Select a PARENT directory where all field/target folders will go..."
 
    if parent_dir is None:
        print(f"\n{prompt}")
        root = tk.Tk(); root.withdraw()
        parent_dir = filedialog.askdirectory(title=prompt)
        if not parent_dir:
            print("No parent directory selected. Aborting.")
            return None
 
    os.makedirs(parent_dir, exist_ok=True)
    print(f"\nParent directory: {parent_dir}")
    print(f"Mode: {mode}")
 
    # Group targets by field once, up front (used by all modes).
    groups = group_targets_by_field(targets_df)
    n_field = sum(1 for g in groups if g['is_field_group'])
    print(f"Grouping: {len(groups)} group(s) "
          f"({n_field} shared-field, {len(groups)-n_field} single-target)")
 
    outcomes = []
    batch_start = time.time()
 
    # DOWNLOAD MODE: field-grouped download, early return 
    if mode == 'download':
        grp_iter = tqdm(groups, desc="Batch download", unit="group",
                        bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} "
                                   "[{elapsed}<{remaining}]")
        for g in grp_iter:
            gname = g['folder_name']
            gdir = os.path.join(parent_dir, gname)
            os.makedirs(gdir, exist_ok=True)
            log_path = os.path.join(gdir, "pipeline.log")
            grp_iter.set_description(f"Downloading: {gname}")
            grp_iter.write(f"\n[{gname}] "
                           + ("FIELD" if g['is_field_group'] else "target")
                           + " — " + ", ".join(str(t['Target']) for t in g['targets']))
            gstart = time.time()
            try:
                with _silenced_to_logfile(log_path):
                    n_dl, n_skip, n_redl, total_unique = _download_field_group(
                        g, gdir, target_iter=grp_iter)
            except Exception as e:
                grp_iter.write(f"  [{gname}] DOWNLOAD FAILED: {str(e)[:200]}")
                for t in g['targets']:
                    outcomes.append(_failed_outcome(t, gname, gdir, mode,
                                                    'DOWNLOAD_FAILED', gstart,
                                                    err=str(e)[:300]))
                continue
            status = 'NO_DATA' if total_unique == 0 else 'DOWNLOAD_COMPLETE'
            grp_iter.write(f"  [{gname}] {n_dl} dl, {n_skip} skip, {n_redl} re-dl "
                           f"of {total_unique} unique obs "
                           f"({(time.time()-gstart)/60:.1f} min) [{status}]")
            for t in g['targets']:
                outcomes.append({
                    'Target': t['Target'], 'Field': g['field'], 'Folder': gdir,
                    'Mode': mode, 'Downloaded': n_dl, 'Unique_Obs': total_unique,
                    'Pipeline_Status': status, 'Error': '',
                    'Runtime_min': (time.time()-gstart)/60.0,
                })
        grp_iter.close()
        summary_df = pd.DataFrame(outcomes)
        summary_df.to_csv(os.path.join(parent_dir, f"batch_summary_{mode}.csv"),
                          index=False)
        print(f"\nBATCH DOWNLOAD COMPLETE — {(time.time()-batch_start)/60:.1f} min")
        return summary_df
 
    # PROCESS-ONLY:
    if mode == 'process':
        missing = []
        for g in groups:
            if not os.path.isdir(os.path.join(parent_dir, g['folder_name'])):
                missing.append(g['folder_name'])
        if missing:
            print(f"\nERROR: Missing folders for {len(missing)} group(s):")
            for m in missing:
                print(f"  - {os.path.join(parent_dir, m)}")
            print("Process-only requires existing folders. Aborting.")
            return None
 
    # FULL / PROCESS: shared-once + per-target loop, per group 
    grp_iter = tqdm(groups, desc="Batch progress", unit="group",
                    bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} "
                               "[{elapsed}<{remaining}]")
    for g in grp_iter:
        grp_iter.set_description(f"Processing: {g['folder_name']}")
        group_outcomes = _process_one_field_group(
            g, parent_dir, manual_aspect_correction, mode, outer_iter=grp_iter)
        outcomes.extend(group_outcomes)
    grp_iter.close()
 
    summary_df = pd.DataFrame(outcomes)
    summary_path = os.path.join(parent_dir, f"batch_summary_{mode}.csv")
    summary_df.to_csv(summary_path, index=False)
    batch_runtime = (time.time() - batch_start) / 60.0
 
    print("\n" + "=" * 70)
    print(f"BATCH RUN COMPLETE ({mode.upper()} MODE)")
    print("=" * 70)
    print(f"Total runtime: {batch_runtime:.1f} min")
    print(f"Groups: {len(groups)}  |  Targets: {len(targets_df)}")
    success = (summary_df['Pipeline_Status'].isin(
        ['SUCCESS', 'DOWNLOAD_COMPLETE'])).sum()
    print(f"Successful: {int(success)}  |  Summary: {summary_path}")
    print("=" * 70)
    return summary_df



############################
def run_dataInit_batch(batch_file=None, datainit_path=None, dataSRC_path=None,
                       manual_aspect_correction=False,
                       force_regenerate_bkg=False,
                       mode='full', auto_download=None):
    """
    dataInit batch driver: process every target in a CSV against ONE shared
    dataInit pool, writing per-target products to dataSRC/<target>/UVOT/.
 
    Each target:
      - resolves its obsids and (per DATAINIT_AUTO_DOWNLOAD) downloads any
        missing into the pool
      - cleans only the obsids that aren't already processed (the rest are
        reused from the persistent pool table)
      - runs photometry reading shared products from dataInit, writing its
        own finalsource/regions/deliverables into dataSRC
 
    Because obsids are shared, the SECOND target that needs an already-
    processed obsid skips cleaning entirely, the whole point of the pool... and all of this.
    """
    # the target list
    if batch_file is None:
        print("Select your target list (CSV or TXT)...")
        root = tk.Tk(); root.withdraw()
        batch_file = filedialog.askopenfilename(
            title="Select target list for dataInit run",
            filetypes=[("CSV/TXT", "*.csv *.txt"), ("All", "*.*")])
        if not batch_file:
            print("No target file selected. Aborting.")
            return None
 
    print(f"\nLoading target list: {batch_file}")
    targets_df = load_batch_targets(batch_file)
    if targets_df is None or targets_df.empty:
        return None
    print(f"\nLoaded {len(targets_df)} target(s):")
    print(targets_df.to_string(index=False))
 
    # pool (dataInit, the data, big D(haha, lamo even) if you will) directory
    if datainit_path is None:
        print("\nSelect the dataInit POOL directory "
              "(shared obsids live/are downloaded here)...")
        root = tk.Tk(); root.withdraw()
        datainit_path = filedialog.askdirectory(title="Select dataInit pool directory")
        if not datainit_path:
            print("No pool directory selected. Aborting.")
            return None
    os.makedirs(datainit_path, exist_ok=True)
 
    # output (dataSRC, output, Big D2 if you will) directory
    if dataSRC_path is None:
        print("\nSelect the dataSRC OUTPUT directory "
              "(per-target results go here)...")
        root = tk.Tk(); root.withdraw()
        dataSRC_path = filedialog.askdirectory(title="Select dataSRC output directory")
        if not dataSRC_path:
            print("No output directory selected. Aborting.")
            return None
    os.makedirs(dataSRC_path, exist_ok=True)
 
    print(f"\n  Pool   (dataInit): {datainit_path}")
    print(f"  Output (dataSRC):  {dataSRC_path}")
    print(f"  Auto-download missing obsids: {DATAINIT_AUTO_DOWNLOAD}")
 
    outcomes = []
    batch_start = time.time()
 
    tgt_iter = tqdm(list(targets_df.iterrows()), desc="dataInit batch",
                    unit="target",
                    bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} "
                               "[{elapsed}<{remaining}]")
    for i, row in tgt_iter:
        tname = row['Target']
        tra = float(row['RA'])
        tdec = float(row['Dec'])
        radius = float(row['Radius'])
        thresh = float(row.get('Threshold', DEFAULT_DETECT_THRESHOLD))
 
        tgt_iter.set_description(f"Target {i+1}/{len(targets_df)}: {tname}")
        t0 = time.time()
        try:
            res = run_dataInit_target(
                target_ra=tra, target_dec=tdec, radius=radius, target=tname,
                datainit_path=datainit_path, dataSRC_path=dataSRC_path,
                detect_threshold=thresh,
                manual_aspect_correction=manual_aspect_correction,
                force_regenerate_bkg=force_regenerate_bkg,
                run_allframes=bool(row.get('AllFrames', RUN_ALLFRAMES)),
                mode=mode, auto_download=auto_download,
                run_timeavg=bool(row.get('TimeAvg', RUN_TIMEAVG)),
                finder_fov=(float(row['FinderFOV']) if pd.notna(row.get('FinderFOV', float('nan'))) else None),) 
            status = res.get('status', 'UNKNOWN')
            res['Runtime_min'] = round((time.time() - t0) / 60.0, 3)
            outcomes.append(res)
            tgt_iter.write(f"[{tname}] {status} "
                           f"(cleaned {res.get('n_cleaned', 0)}, "
                           f"reused {res.get('n_reused', 0)}, "
                           f"{res['Runtime_min']:.1f} min)")
        except Exception as e:
            outcomes.append({'target': tname, 'status': 'ERROR',
                             'error': str(e)[:300],
                             'Runtime_min': round((time.time() - t0) / 60.0, 3)})
            tgt_iter.write(f"[{tname}] ERROR: {str(e)[:200]}")
    tgt_iter.close()
 
    summary_df = pd.DataFrame(outcomes)
    # Keep every run's summary for the record rather than overwriting.
    # Subfolder + index + timestamp so we get datainit_batch_summary_<N>_<stamp>.csv
    summary_dir = os.path.join(dataSRC_path, "batch_summaries")
    os.makedirs(summary_dir, exist_ok=True)
    existing_n = len([f for f in os.listdir(summary_dir)
                      if f.startswith("datainit_batch_summary_")
                      and f.endswith(".csv")])
    stamp = time.strftime("%Y%m%d_%H%M%S")
    summary_path = os.path.join(
        summary_dir, f"datainit_batch_summary_{existing_n + 1}_{stamp}.csv")
    summary_df.to_csv(summary_path, index=False)
 
    total = (time.time() - batch_start) / 60.0
    print("\n" + "=" * 70)
    print("dataInit BATCH COMPLETE")
    print("=" * 70)
    print(f"Total runtime: {total:.1f} min")
    print(f"Targets: {len(targets_df)}  |  Summary: {summary_path}")
    if not summary_df.empty and 'status' in summary_df.columns:
        ok = (summary_df['status'] == 'SUCCESS').sum()
        print(f"Successful: {int(ok)} / {len(summary_df)}")
    print("=" * 70)
    return summary_df

def target_background_path(dataSRC_path, target):
    """persistent background-region path for a target."""
    tag = _sanitize_target_name(target)
    return os.path.join(dataSRC_path, tag, "UVOT", f"auto_bkg_{tag}.reg")
 
 
def run_dataInit_target(target_ra, target_dec, radius, target,
                        datainit_path, dataSRC_path,
                        detect_threshold=3.0,
                        manual_aspect_correction=False,
                        force_regenerate_bkg=False,
                        auto_download=None,
                        retry_orphans=True,
                        retry_uncorr=True,
                        force_retry_failed=False, run_allframes=None, run_timeavg=None, mode='full',
                        finder_fov=None):
    """
    Process ONE target against the shared dataInit pool, writing per-target
    products to dataSRC/<target>/UVOT/<obsid>/. All pipeline output is logged
    to pipeline.log in that UVOT folder, the batch driver prints the one-line
    status to the screen.,
    

    ALSO we needed some fixes for SRC processing to work, those are for the record:
    FIX 1: when cleaning a raw subset, already-processed pool obsids in the same
           spatial groups are supplied to aspect correction as read-only DIRECT
           references (so subset cleaning no longer starves correction).
    FIX 2: failed_uncorr obsids are retried once the pool has grown since they
           last failed (failed_smeared stays permanent, smearing is intrinsic).
    """
    if auto_download is None:
        auto_download = DATAINIT_AUTO_DOWNLOAD
    if mode not in ('full', 'download', 'process'):
        raise ValueError(f"mode must be full/download/process, got {mode!r}")
    # 'process' = pool-as-is: never fetch, whatever auto_download says.
    if mode == 'process':
        auto_download = False
    ttag = _sanitize_target_name(target)
    t_uvot = os.path.join(dataSRC_path, ttag, "UVOT")
    os.makedirs(t_uvot, exist_ok=True)
    log_path = os.path.join(t_uvot, "pipeline.log")
 
    with _silenced_to_logfile(log_path):
        print("=" * 70)
        print(f"dataInit RUN — target {target} (tag {ttag})")
        print(f" pool: {datainit_path}")
        print(f" output: {t_uvot}")
        print("=" * 70)
 
        # 1. resolve
        res = resolve_obsids_for_target(target_ra, target_dec, radius,
                                        datainit_path=datainit_path)
        needed = res.get('needed', [])
        missing = res.get('missing', [])
        if not needed:
            print(" No obsids returned for this target. Nothing to do.")
            return {'target': target, 'status': 'NO_DATA', 'n_obsids': 0,
                    'n_cleaned': 0, 'n_reused': 0}
        print(f"  needed={len(needed)} present={len(res.get('present', []))} "
              f"missing={len(missing)}")
 
        # 2. download missing (non-interactive)
        if missing and auto_download:
            jobs = [(obsid, os.path.join(datainit_path, str(obsid)))
                    for obsid in missing]
            _download_obsids_parallel(jobs, desc="Downloading missing",
                                  reporter=print)
        elif missing:
            print(f"  {len(missing)} obsid(s) missing from pool; NOT downloading "
                  f"(mode={mode}, auto_download={auto_download}). Using only "
                  f"obsids already present in dataInit.")
            missing = []

        # 'download' = fetch into the pool, then stop.
        if mode == 'download':
            print(f"  mode=download: pool populated, skipping processing for {target}.")
            return {'target': target, 'status': 'DOWNLOAD_ONLY',
                    'n_obsids': len(needed)}
 
        # 3. classify (cause-aware for failures)
        manifest_df = read_manifest(datainit_path)
        pool_df = read_pool_obstable(datainit_path)
        existing_now = _datainit_existing_obsids(datainit_path)
 
        # Current processed-pool generation, used by the FIX 2 retry gate and
        # stored on this run's outcomes so future runs can detect pool growth.
        n_processed_pool = (int((manifest_df['State'] == 'processed').sum())
                            if not manifest_df.empty else 0)
        # Marker stored for obsids (re)classified this run: pool size INCLUDING
        # this run's intended cleaning, so a failed_uncorr only retries when
        # ANOTHER run later grows the pool.
 
        raw_obsids, processed_obsids = [], []
        n_skip_permanent = 0
        n_retry_orphan = 0
        n_retry_uncorr = 0
        for obsid in needed:
            state = manifest_state_for_obsid(obsid, datainit_path,
                                             manifest_df=manifest_df)
            if state == 'processed' and not pool_has_complete_rows(
                    datainit_path, obsid, pool_df=pool_df):
                state = 'raw'
 
            # freshness check: did the obsid's raw data change since we processed
            # it? (e.g. a re-downlink added a snapshot). Compare the recorded raw-
            # extension fingerprint against what's on disk now.
            if state == 'processed':
                disk_exts = _obsid_raw_ext_count(datainit_path, obsid)
                _rec = manifest_df.loc[
                    manifest_df['ObsID'].astype(str) == str(obsid), 'N_Exts']
                rec_exts = (int(_rec.iloc[0])
                            if len(_rec) and pd.notna(_rec.iloc[0]) else None)
                if disk_exts > 0 and rec_exts is not None and disk_exts != rec_exts:
                    print(f"  obsid {obsid}: raw data changed "
                          f"(ext fingerprint {rec_exts} -> {disk_exts}); "
                          f"marking raw for reprocessing.")
                    state = 'raw'
            if state == 'processed':
                processed_obsids.append(obsid)
            elif state == 'failed_smeared':
                # Smearing is intrinsic to the frame, it never improves.
                if force_retry_failed and obsid in existing_now:
                    raw_obsids.append(obsid)
                else:
                    n_skip_permanent += 1
            elif state == 'failed_uncorr':
                # FIX 2: was uncorrectable for lack of an available reference.
                # With pool-mode reference augmentation (FIX 1) the reference
                # set grows as the pool fills, so retry once the pool has grown
                # since this obsid last failed.
                prev_gen = _manifest_pool_size(obsid, manifest_df)
                grew = (force_retry_failed or prev_gen is None
                        or n_processed_pool > prev_gen)
                if (obsid in existing_now and (retry_uncorr or force_retry_failed)
                        and grew):
                    raw_obsids.append(obsid)
                    n_retry_uncorr += 1
                else:
                    n_skip_permanent += 1
            elif state == 'failed_orphan':
                if (retry_orphans or force_retry_failed) and obsid in existing_now:
                    raw_obsids.append(obsid)
                    n_retry_orphan += 1
                else:
                    n_skip_permanent += 1
            elif obsid in existing_now:
                raw_obsids.append(obsid)
            else:
                print(f" {obsid}: not present in pool, skipping")
        print(f"to clean (raw): {len(raw_obsids)} | "
              f"reuse (processed): {len(processed_obsids)} | "
              f"skip permanent-fail: {n_skip_permanent} | "
              f"retry orphan: {n_retry_orphan} | "
              f"retry uncorr: {n_retry_uncorr}")
 
        # Pool generation marker to stamp on this run's (re)classified obsids.
        pool_gen_marker = n_processed_pool + len(raw_obsids)
 
        # 4. clean only the raw obsids
        fresh_table = pd.DataFrame()
        if raw_obsids:
            # Retried orphan/uncorr obsids live in Orphans/ or NotASPCORR/ from a
            # previous run, move them back into the pool before reprocessing.
            _unquarantine_obsids(datainit_path,
                     list(raw_obsids) + list(processed_obsids),
                     reporter=print)
            results = clean_up_data(
                automation_mode=True, base_path=datainit_path,
                save_path=datainit_path, detect_threshold=detect_threshold,
                only_obsids=raw_obsids)
            if results is None or results.get('observations_table') is None:
                return {'target': target, 'status': 'CLEANUP_FAILED',
                        'n_obsids': len(needed), 'n_cleaned': 0,
                        'n_reused': len(processed_obsids)}
            fresh_table = results['observations_table']
 
            # --- FIX 1: augment with already-processed pool references 
            ref_rows = _collect_pool_references(
                fresh_table, results.get('all_frames'), raw_obsids,
                datainit_path, detect_threshold=detect_threshold,
                reporter=print)
            raw_set = set(str(o) for o in raw_obsids)
            if ref_rows is not None and not ref_rows.empty:
                aspect_table = pd.concat([fresh_table, ref_rows],
                                         ignore_index=True)
                automated_aspect_correction(
                    obs_table=aspect_table, base_path=datainit_path,
                    save_path=datainit_path, manual_mode=manual_aspect_correction)
                # Aspect correction mutates the table in place, keep only the
                # raw obsids' (now-corrected) rows going forward. References
                # were read-only and stay as the pool already has them.
                fresh_table = aspect_table[
                    aspect_table['ObsID'].astype(str).isin(raw_set)
                ].reset_index(drop=True)
            else:
                automated_aspect_correction(
                    obs_table=fresh_table, base_path=datainit_path,
                    save_path=datainit_path, manual_mode=manual_aspect_correction)
            # ----------------------------------------------------------------
 
            fresh_table = rescue_orphan_frames(
                obs_table=fresh_table, base_path=datainit_path,
                save_path=datainit_path,
                orphan_solutions=results.get('orphan_solutions'),
                manual_mode=manual_aspect_correction)
            _run_quarantine(datainit_path, fresh_table)
            fresh_table = check_sss_before_summation(
                obs_table=fresh_table, base_path=datainit_path,
                save_path=datainit_path, target_ra=target_ra,
                target_dec=target_dec)
 
            # 5. persist: pool table + manifest
            pool_df = upsert_obsids_into_pool(datainit_path, fresh_table,
                                              raw_obsids)
            for obsid in raw_obsids:
                orows = fresh_table[fresh_table['ObsID'].astype(str) == str(obsid)]
                n_bands = orows['Filter'].nunique() if not orows.empty else 0
                n_corr = (orows['Extension_Status'].isin(['DIRECT', 'UNICORR']).sum()
                          if 'Extension_Status' in orows.columns else 0)
 
                if n_corr > 0:
                    state = 'processed'
                    was_orphan = False
                    notes = ''
                else:
                    all_smeared = (
                        'Smeared Flag' in orows.columns
                        and len(orows) > 0
                        and bool(orows['Smeared Flag'].all()))
                    grp_status = (str(orows['Group_Status'].iloc[0])
                                  if ('Group_Status' in orows.columns
                                      and len(orows) > 0) else 'UNKNOWN')
                    if all_smeared:
                        state = 'failed_smeared'
                        was_orphan = False
                        notes = 'all extensions smeared (permanent)'
                    elif grp_status in ('ORPHAN', 'UNKNOWN'):
                        state = 'failed_orphan'
                        was_orphan = True
                        notes = 'no references in group; retry if pool grows'
                    else:
                        state = 'failed_uncorr'
                        was_orphan = False
                        notes = 'had references but uncorrectable; retry if pool grows'
 
                update_manifest_after_processing(
                    datainit_path, obsid, state, n_bands=int(n_bands),
                    n_corrected=int(n_corr), was_orphan=was_orphan, notes=notes,
                    pool_size=pool_gen_marker)
 
        # assemble working table
        reuse_rows = pool_rows_for_obsids(datainit_path, processed_obsids,
                                          pool_df=pool_df)
        parts = [df for df in (fresh_table, reuse_rows)
                 if df is not None and not df.empty]
        if not parts:
            return {'target': target, 'status': 'NO_USABLE_DATA',
                    'n_obsids': len(needed), 'n_cleaned': len(raw_obsids),
                    'n_reused': len(processed_obsids)}
        work_table = pd.concat(parts, ignore_index=True)
        work_table = work_table[work_table['ObsID'].astype(str).isin(
            set(str(o) for o in needed))].reset_index(drop=True)
 
        # 6. shared summation
        image_dirs = run_summation_shared(work_table, datainit_path)
 
        # 7. per-target photometry (read dataInit, write dataSRC)
        bkg_path = target_background_path(dataSRC_path, target)
        reuse_bkg = os.path.exists(bkg_path) and not force_regenerate_bkg
        df_all = run_photometry_for_target(
            obs_table=work_table, base_path=datainit_path, save_path=t_uvot,
            image_dirs=image_dirs, target_ra=target_ra, target_dec=target_dec,
            target=ttag, source_reg=None,
            bkg_reg=(bkg_path if reuse_bkg else None),
            automation_mode=False,
            output_root=t_uvot,
            persistent_bkg_path=bkg_path,
            run_allframes=run_allframes,
            run_timeavg=run_timeavg,
            finder_fov=finder_fov)
 
    _cleanup_after_processing(datainit_path, save_root=datainit_path, label=target)
    return {'target': target, 'status': 'SUCCESS',
            'n_obsids': len(needed), 'n_cleaned': len(raw_obsids),
            'n_reused': len(processed_obsids),
            'n_retry_uncorr': n_retry_uncorr,
            'n_photometry_rows': (0 if df_all is None else len(df_all))}
