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
        print("\n❌ Setup cancelled.")
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
