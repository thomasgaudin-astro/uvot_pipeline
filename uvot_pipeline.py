# -*- coding: utf-8 -*-
import os
import subprocess
import sys

import pandas as pd
import numpy as np
import csv

import math
import time
import gc
import uuid  #For threading with multiple open windows, creats a 32bit string that is impossbile to forge so the code knows when its finished and doesnt overwrite eachother
import select #For freezing errors, sometimes the code may crash on something and the tim3eout wont work becauseit only runs after readline() so this is the fix, select.select([fd], [], [], timeout)
# NVM, this would only work on LINUX, as windows doesnt treat this the same way. select, is socket-only so it wont work here on the pipeline because of how it runs.
import queue # Trying to replace select.
import faulthandler

import re

import shutil
from astropy.wcs import WCS
from astropy.io import fits
from astropy.table import QTable, Table
import astropy.units as u
from astropy.coordinates import SkyCoord
from astropy.time import Time

import argparse
import warnings

from swifttools.swift_too import Clock,TOO, Resolve, ObsQuery, Data

from tqdm import tqdm

import requests
from requests.auth import HTTPBasicAuth

import tkinter as tk
from tkinter import filedialog

import contextlib
from concurrent.futures import ThreadPoolExecutor, as_completed

import threading


import matplotlib
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as ticker
from matplotlib.patches import Circle
from datetime import datetime
from astropy.visualization import (ImageNormalize, LogStretch, PercentileInterval)

###############################################################################
# PLATFORM / HEASOFT CONFIGURATION
#
# These settings control how HEASoft commands are invoked on your system.
# Edit these to match your installation.
###############################################################################

# Backend: "wsl" (Windows) or "native" (Linux/macOS)
HEASOFT_BACKEND = "wsl"   # set to "wsl" on Windows

###############################################################################
# WSL CONFIGURATION (only used if HEASOFT_BACKEND == "wsl")
###############################################################################
# The conda environment name that has HEASoft installed inside WSL.
# If you installed HEASoft directly in WSL without conda, set to None
# and use WSL_HEASOFT_INIT_SCRIPT below instead.
WSL_CONDA_ENV = "henv"

# explicit path INSIDE WSL to a HEASoft init script.
# Only used if WSL_CONDA_ENV is None. Example:
#   "/home/user/heasoft/x86_64-pc-linux-gnu-libc2.31/headas-init.sh"
WSL_HEASOFT_INIT_SCRIPT = None

USE_WARM_SHELL = True           # master switch; flip to True to enable
WARM_SHELL_TIMEOUT = 600         # seconds per command before declaring a hang

# Path (inside WSL) to the HEASoft wrapper script.
# See README for setup instructions. Replace <your_wsl_user> with your
# WSL username (find it with `whoami` in WSL).
WSL_WRAPPER_PATH = "/home/allotheduke/run_heasoft_wrapper.sh"

###############################################################################
# NATIVE CONFIGURATION (only used if HEASOFT_BACKEND == "native")
###############################################################################
# The native backend auto-detects HEASoft and CALDB from environment
# variables ($HEADAS, $CALDB).  Leave these as None to use whatever is
# in the environment.  Only set them if you want to override or if your
# environment doesn't have them set (e.g. Jupyter launched from a non-
# interactive shell that didn't load ~/.bashrc).
NATIVE_HEADAS_PATH = None    # e.g. "/home/blentorvrella/Desktop/heasoft-6.36/x86_64-pc-linux-gnu-libc2.43"
NATIVE_CALDB_PATH = None     # e.g. "/home/blentorvrella/caldb"
#######################################################################
# ASPECT CORRECTION RETRY LADDER
# Parameters are tuned for UVOT. For other instruments, adjust these.
# Each tuple is (side_buffer_arcmin, num_stars).
# Attempts are tried in order; failed frames move to the next.
#######################################################################
ASPECT_RETRY_LADDER = [
    (7, 50),   # Attempt 1: standard
    (5, 30),   # Attempt 2: smaller search, fewer stars
    (3, 15),   # Attempt 3: last resort
]

###############################################################################
# ORPHAN-RESCUE NEIGHBOR DISTANCE CAP
# For an orphan to be rescued, its synthetic reference is built from
# neighbor frames at similar sky pointings.  Neighbors more than
# ORPHAN_MAX_NEIGHBOR_ARCMIN away from the orphan's pointing center
# are rejected — they are observations of a different physical target
# that just happen to be in the same sky region, and including them
# would build a synthetic with wrong pointing.
#
# UVOT FOV is ~17 arcmin diameter; same-target observations typically
# scatter within ~30" of each other.  Default cap of 3' allows generous
# pointing scatter for the same target while rejecting cross-target
# spillover.  Set to a large value (e.g. 30) to effectively disable
# the cap and rely on uvotunicorr's intrinsic failure mode.
###############################################################################
ORPHAN_MAX_NEIGHBOR_ARCMIN = 30

###############################################################################
# ORPHAN-RESCUE SHIFT SCREENING
# After uvotunicorr applies a correction during orphan rescue, compute
# the angular shift between original and corrected CRVAL. If the shift
# exceeds this threshold, treat the rescue as suspicious and revert.
#
# Typical uvotunicorr corrections are 1-10 arcsec (from the 12 I have tested). Shifts of 15"+ usually
# indicate a wrong-target synthetic reference and produce wrong WCS.
# Default: 15".
###############################################################################
ORPHAN_MAX_SHIFT_ARCSEC = 5


#######################################################################
# OUTPUT FORMAT TOGGLE
#######################################################################
# The pipeline always writes master_photometry.txt as its 
# output (tab-separated, plain text, universal). Set WRITE_CSV_COPY
# to True to also write a comma-separated .csv version for visual inspection
WRITE_CSV_COPY = True


#######################################################################
# PARALLELISM
#######################################################################
# Number of observations processed concurrently during the HEASoft-heavy
# Step-4 phases (summation and uvotsource). Each observation runs in its
# own thread; bands within an observation stay sequential. the GIL
# is released while the thread waits on the subprocess. Each HEASoft call
# also gets its own PFILES dir (per-PID) so parallel calls don't clobber
# each other's parameter files. Atleast It should, Currently testing.
#
# Set to 1 to run the SAME code path sequentially, Bellow sets the Cpu count
# It will take the lowerst number, so you dont try to take more cores then you have
MAX_WORKERS = min(12, (os.cpu_count() or 12))

DOWNLOAD_WORKERS = 12   # simultaneous archive downloads


#######################################################################
# CONFIG — Storage saving mechanism
#######################################################################
CLEANUP_AFTER_RUN   = True    # the Big switch for automatic end-of-run cleanup
CLEANUP_DELETE_GZ   = True    # also delete _sk.img.gz (keeps corrected .img)
CLEANUP_DELETE_IMG  = False   # DESTRUCTIVE!!!: strip SK to summed-only. Leave False
                              # unless you KNOW you won't do per-extension work or want.
                              # or need the sk.img.gz or sk.img, if you have summed files.

BANDS = ["uvv", "uuu", "ubb", "um2", "uw1", "uw2"] # Bands you want processed 
QUARANTINE = {"Smeared", "NotASPCORR", "Orphans"} # Dont touch

##########################################################################################

class DownloadError(Exception):
    """Raise when requests status quo does not return 200."""
    pass

###############################################################################
# BATCH MODE — LOAD TARGET LIST AND RUN PIPELINE PER TARGET
###############################################################################

DEFAULT_SEARCH_RADIUS = 0.05   # degrees, used if no Radius column in input

DEFAULT_DETECT_THRESHOLD = 3.0 # In sigma error, 3 is the standred to screen out noise.
# But their might be moments for some objects where it will have to be lowered to produce outputs.

# Accepted column names (case-insensitive) for each required field
_BATCH_COL_ALIASES = {
    'target': ['target', 'name', 'source', 'source_name', 'object'],
    'ra':     ['ra', 'ra_deg', 'ra_obj', 'right_ascension'],
    'dec':    ['dec', 'de', 'dec_deg', 'de_obj', 'declination'],
    'radius': ['radius', 'search_radius', 'radius_deg', 'r'],
    'threshold': ['threshold', 'detect_threshold', 'sigma', 'sig'],
    'field':  ['field', 'fld', 'field_name', 'region'],
    'allframes': ['allframes', 'all_frames', 'perframe', 'per_frame'],
    'timeavg': ['timeavg', 'time_avg', 'timeaveraged', 'time_averaged', 'allsummed', 'all_summed'],
    'finderfov': ['finderfov', 'finder_fov', 'fov', 'fov_arcmin', 'finder_zoom'],
}


def _resolve_column(df, aliases):
    """Find a column in df matching any name in aliases (case-insensitive)."""
    lower_cols = {c.lower(): c for c in df.columns}
    for alias in aliases:
        if alias.lower() in lower_cols:
            return lower_cols[alias.lower()]
    return None


def _sanitize_target_name(name):
    """
    Convert a target name into a safe folder/file name.
    Replaces spaces with underscores, strips characters that break shells.
    """
    if not isinstance(name, str):
        name = str(name)
    name = name.strip().replace(' ', '_')
    # Keep alphanumerics, underscore, dot, dash, plus
    name = re.sub(r'[^A-Za-z0-9_.\-+]', '', name)
    return name


def load_batch_targets(filepath):
    """
    Load a batch target file (CSV or TXT) and return a normalised
    DataFrame with columns: Target, RA, Dec, Radius.

    Format is auto-detected from extension:
      .csv -> comma-separated with header row
      .txt -> tab-separated with header row

    Required columns (case-insensitive, multiple aliases accepted):
      Target / Name / Source / Source_Name / Object
      RA / RA_deg / RA_obj / Right_Ascension
      Dec / De / Dec_deg / De_obj / Declination

    Optional column:
      Radius / Search_Radius / Radius_deg / R   (degrees)

    If no Radius column is provided, DEFAULT_SEARCH_RADIUS is used.
    """
    if not os.path.exists(filepath):
        print(f"ERROR: Batch input file not found: {filepath}")
        return None

    ext = filepath.lower().rsplit('.', 1)[-1]
    try:
        if ext == 'csv':
            df = pd.read_csv(filepath)
        elif ext == 'txt':
            df = pd.read_csv(filepath, sep='\t')
        else:
            # Try CSV first, fall back to whitespace
            try:
                df = pd.read_csv(filepath)
            except Exception:
                df = pd.read_csv(filepath, delim_whitespace=True)
    except Exception as e:
        print(f"ERROR: Could not parse batch input file: {e}")
        return None

    # Resolve column names
    target_col = _resolve_column(df, _BATCH_COL_ALIASES['target'])
    ra_col = _resolve_column(df, _BATCH_COL_ALIASES['ra'])
    dec_col = _resolve_column(df, _BATCH_COL_ALIASES['dec'])
    radius_col = _resolve_column(df, _BATCH_COL_ALIASES['radius'])

    missing = []
    if target_col is None:
        missing.append("Target name")
    if ra_col is None:
        missing.append("RA")
    if dec_col is None:
        missing.append("Dec")

    if missing:
        print("ERROR: Required column(s) not found in batch input file:")
        for m in missing:
            print(f"  - {m}")
        print(f"\nFound columns: {list(df.columns)}")
        return None

    # Build normalised DataFrame
    out = pd.DataFrame()
    out['Target'] = df[target_col].apply(_sanitize_target_name)

    try:
        out['RA'] = pd.to_numeric(df[ra_col], errors='coerce')
        out['Dec'] = pd.to_numeric(df[dec_col], errors='coerce')
    except Exception as e:
        print(f"ERROR: Could not parse RA/Dec as numeric: {e}")
        return None

    if radius_col is not None:
        out['Radius'] = pd.to_numeric(df[radius_col], errors='coerce')
        out['Radius'] = out['Radius'].fillna(DEFAULT_SEARCH_RADIUS)
    else:
        out['Radius'] = DEFAULT_SEARCH_RADIUS

    threshold_col = _resolve_column(df, _BATCH_COL_ALIASES['threshold'])
    if threshold_col is not None:
        out['Threshold'] = pd.to_numeric(df[threshold_col], errors='coerce')
        out['Threshold'] = out['Threshold'].fillna(DEFAULT_DETECT_THRESHOLD)
    else:
        out['Threshold'] = DEFAULT_DETECT_THRESHOLD

    # Per-target All-frames toggle. Blank/missing -> global RUN_ALLFRAMES.
    allframes_col = _resolve_column(df, _BATCH_COL_ALIASES['allframes'])
    if allframes_col is not None:
        def _af_flag(v):
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return RUN_ALLFRAMES
            s = str(v).strip().lower()
            return RUN_ALLFRAMES if s == '' else s in (
                'true', '1', 'yes', 't', 'y', 'on')
        out['AllFrames'] = df[allframes_col].apply(_af_flag)
    else:
        out['AllFrames'] = RUN_ALLFRAMES

    # Per-target Time-Averaged toggle. Blank/missing -> global RUN_TIMEAVG.
    timeavg_col = _resolve_column(df, _BATCH_COL_ALIASES['timeavg'])
    if timeavg_col is not None:
        def _ta_flag(v):
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return RUN_TIMEAVG
            s = str(v).strip().lower()
            return RUN_TIMEAVG if s == '' else s in (
                'true', '1', 'yes', 't', 'y', 'on')
        out['TimeAvg'] = df[timeavg_col].apply(_ta_flag)
    else:
        out['TimeAvg'] = RUN_TIMEAVG

    # Per-target finder-chart field of view (arcmin). Blank -> module default.
    fov_col = _resolve_column(df, _BATCH_COL_ALIASES['finderfov'])
    if fov_col is not None:
        out['FinderFOV'] = pd.to_numeric(df[fov_col], errors='coerce')
    else:
        out['FinderFOV'] = float('nan')
    # Optional Field column. Blank/NaN/missing → empty string, which the
    # grouping treats as "no field" (each such target is its own group).
    # Sanitized the same way target names are, so a field like
    # "4FGL J1637.5+3005" becomes a valid folder name "4FGL_J1637.5+3005".
    field_col = _resolve_column(df, _BATCH_COL_ALIASES['field'])
    if field_col is not None:
        out['Field'] = df[field_col].apply(
            lambda v: _sanitize_target_name(v)
            if isinstance(v, str) and v.strip() else ""
        )
    else:
        out['Field'] = ""

    # Drop any rows with invalid RA/Dec
    valid_mask = out['RA'].notna() & out['Dec'].notna()
    invalid_count = (~valid_mask).sum()
    if invalid_count > 0:
        print(f"WARNING: {invalid_count} target(s) dropped due to "
              f"unparseable RA/Dec")
    out = out[valid_mask].reset_index(drop=True)

    if len(out) == 0:
        print("ERROR: No valid targets in batch file after parsing.")
        return None

    return out

  
def group_targets_by_field(targets_df):
    """
    Group batch targets into DOWNLOAD UNITS based on the optional Field
    column.

    The IDea is:
      - A Field value shared by 2+ targets  -> ONE group, folder named after
        the (sanitized) field. The group downloads the UNION of its targets'
        obsid queries once.
      - A Field with only 1 target, OR a blank/missing Field -> that target
        is its own group, folder named after the (sanitized) target
        (identical to the original per-target behavior).

    Returns a list of group dicts:
        {
          'folder_name': str,        # field name (multi) or target name (single)
          'is_field_group': bool,    # True only for multi-target shared fields
          'field': str,              # sanitized field label ("" if none)
          'targets': [row, ...],     # list of target rows (pandas Series)
        }
    """
    # Count how many targets share each non-empty field
    field_counts = {}
    for _, row in targets_df.iterrows():
        fld = row.get('Field', "") or ""
        if fld:
            field_counts[fld] = field_counts.get(fld, 0) + 1

    groups = []
    field_groups = {}  # field -> group dict (for accumulating multi-target fields)

    for _, row in targets_df.iterrows():
        fld = row.get('Field', "") or ""
        shared = bool(fld) and field_counts.get(fld, 0) >= 2

        if shared:
            if fld not in field_groups:
                g = {
                    'folder_name': fld,
                    'is_field_group': True,
                    'field': fld,
                    'targets': [],
                }
                field_groups[fld] = g
                groups.append(g)
            field_groups[fld]['targets'].append(row)
        else:
            # Single target or no field -> its own target-named group
            groups.append({
                'folder_name': row['Target'],
                'is_field_group': False,
                'field': fld,
                'targets': [row],
            })

    return groups

def _download_field_group(group, group_dir, target_iter=None):
    """
    Download the UNION of all targets' Swift observations for one group into
    group_dir, each obsid once. Returns (n_downloaded, n_skipped,
    n_redownloaded, total_unique_obs).

    For a single-target group this is identical to the original per-target
    download. For a multi-target field group it unions the obsids so shared
    observations download only once.
    """
    def _say(msg):
        if target_iter is not None:
            target_iter.write(msg)

    # Build the union of obsid
    union = {}  # obsid -> query entry
    for row in group['targets']:
        tra = float(row['RA'])
        tdec = float(row['Dec'])
        trad = float(row['Radius'])
        try:
            q = ObsQuery(ra=str(tra), dec=str(tdec), radius=trad)
        except Exception as e:
            _say(f"  [{group['folder_name']}] Query failed for "
                 f"RA={tra},Dec={tdec}: {e}")
            continue
        for entry in q:
            if entry.obsid not in union:
                union[entry.obsid] = entry

    total_unique = len(union)
    n_dl = n_skip = n_redl = 0

    if total_unique == 0:
        return 0, 0, 0, 0

    jobs = [(obsid, os.path.join(group_dir, f"{obsid}")) for obsid in union]
    results = _download_obsids_parallel(
        jobs, desc=f"Downloading {group['folder_name']}", reporter=_say)
    n_dl = sum(1 for v in results.values() if v == 'ok')
    n_skip = sum(1 for v in results.values() if v == 'skip')
    n_redl = 0  # (the parallel helper folds re-download into 'ok'; see note)
    return n_dl, n_skip, n_redl, total_unique


def _download_obsids_parallel(jobs, desc="Downloading", reporter=None):
    """
    Download a set of obsids concurrently
 
    jobs : iterable of (obsid, obs_dir) tuples. Each obsid downloads into its
           OWN obs_dir, so workers never share a write target. obs_dir is
           created if missing.
    desc : tqdm label.
    reporter : optional callable(msg) for per-obsid lines (e.g. tqdm.write or
               a field group's _say). If None, lines are suppressed.
 
    Returns a dict: {obsid: 'ok' | 'skip' | 'fail'}.
      'skip' = already had UVOT data on disk (not re-downloaded)
      'ok'   = downloaded this run
      'fail' = Data() raised (logged via reporter)
    """
    jobs = list(jobs)
    results = {}
    if not jobs:
        return results
 
    def _say(msg):
        if reporter is not None:
            reporter(msg)
 
    def _one(obsid, obs_dir):
        os.makedirs(obs_dir, exist_ok=True)
        # Skip if this obsid already has UVOT data
        if _obsid_has_uvot_data(obs_dir):
            return obsid, 'skip'
        try:
            Data(obsid=obsid, uvot=True, clobber=True, outdir=obs_dir)
            return obsid, 'ok'
        except Exception as e:
            return obsid, ('fail', str(e)[:200])
 
    workers = max(1, int(DOWNLOAD_WORKERS))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_one, oid, d): oid for oid, d in jobs}
        for fut in tqdm(as_completed(futures), total=len(futures), desc=desc, unit="obs"):
            oid = futures[fut]
            try:
                obsid, status = fut.result()
            except Exception as e:
                results[oid] = 'fail'
                _say(f"  [{oid}] download worker crashed: {str(e)[:200]}")
                continue
            if isinstance(status, tuple):  # ('fail', errmsg)
                results[obsid] = 'fail'
                _say(f"  [{obsid}] download failed: {status[1]}")
            else:
                results[obsid] = status
    return results



@contextlib.contextmanager 
def _silenced_to_logfile(log_path):
    """
    Context manager that redirects stdout AND stderr to a log file.
    The screen stays clean; everything still gets recorded for debugging.

    Usage:
        with _silenced_to_logfile('/path/to/pipeline.log'):
            noisy_function_that_prints_a_lot()
    """
    log_file = open(log_path, 'w', encoding='utf-8')
    old_stdout = sys.stdout
    old_stderr = sys.stderr
    try:
        sys.stdout = log_file
        sys.stderr = log_file
        yield
    finally:
        sys.stdout = old_stdout
        sys.stderr = old_stderr
        log_file.close()


@contextlib.contextmanager    #Same with this, more or less taken wholesale from what i have found online.
def _silenced_append(log_path):
    """
    Like _silenced_to_logfile but opens the log file in append mode.
    Use this for second-and-onwards silenced blocks within the same target
    so earlier output isn't lost.
    """
    log_file = open(log_path, 'a', encoding='utf-8')
    old_stdout = sys.stdout
    old_stderr = sys.stderr
    try:
        sys.stdout = log_file
        sys.stderr = log_file
        yield
    finally:
        sys.stdout = old_stdout
        sys.stderr = old_stderr
        log_file.close()



###############################################################################
# DATAINIT / DATASRC SHARED-POOL CONFIGURATION
###############################################################################
# When a target needs obsids not present in dataInit:
#   True  -> download the missing obsids into dataInit automatically
#   False -> list the missing obsids and ask the user before downloading
DATAINIT_AUTO_DOWNLOAD = True


def resolve_obsids_for_target(target_ra, target_dec, radius,
                              datainit_path=None):
    """
    Determine which obsids a target needs, and checks against dataInit.

    It does: (1) query the Swift archive for obsids covering
    the target's position, then (2) check which of those already exist in
    dataInit so the driver knows what (if anything) must be downloaded.

    Returns a dict:
      {
        'needed':   [obsid, ...],   # all obsids the target's query returns
        'present':  [obsid, ...],   # needed obsids already in dataInit
        'missing':  [obsid, ...],   # needed obsids NOT yet in dataInit
        'query_entries': {obsid: q} # ObsQuery entries, for downloading missing
      }
    If datainit_path is None, 'present'/'missing' are not computed (needed only).
    """
    # So first(1) Query the archive for everything covering this position.
    needed = []
    query_entries = {}
    try:
        query = ObsQuery(ra=str(target_ra), dec=str(target_dec), radius=radius)
        for q in query:
            if q.obsid not in query_entries:
                query_entries[q.obsid] = q
                needed.append(q.obsid)
    except Exception as e:
        print(f"  resolve_obsids_for_target: query failed: {e}")
        return {'needed': [], 'present': [], 'missing': [], 'query_entries': {}}

    # Big #2 check against what's already in dataInit.
    present, missing = [], []
    if datainit_path and os.path.isdir(datainit_path):
        existing = _datainit_existing_obsids(datainit_path)
        for obsid in needed:
            if obsid in existing:
                present.append(obsid)
            else:
                missing.append(obsid)
    else:
        # No pool given/exists yet. everything is "missing".
        missing = list(needed)

    return {
        'needed': needed,
        'present': present,
        'missing': missing,
        'query_entries': query_entries,
    }


def _datainit_existing_obsids(datainit_path):
    """
    Return the set of obsids that have an actual uvot/image directory with
    at least one SK file present in dataInit. Membership = "this obsid's raw
    data is in the pool" (not necessarily processed, state is a separate
    check). Folder names in dataInit are obsid-only (no date tag!!!!).
    """
    existing = set()
    sk_re = re.compile(r"^sw(\d{11})[a-z0-9]+_sk\.img(\.gz)?$")
    QUARANTINE = ("Smeared", "Orphans", "NotASPCORR")
    try:
        for entry in os.listdir(datainit_path):
            obs_folder = os.path.join(datainit_path, entry)
            if not os.path.isdir(obs_folder):
                continue

            # Quarantine folders hold obsids that were processed then moved
            # aside (smeared/orphan/uncorrectable). They still count as
            # "present in the pool" so scan inside them so those obsids aren't
            # treated as missing and redownloaded.
            if entry in QUARANTINE:
                for root, _, files in os.walk(obs_folder):
                    if root.endswith(os.path.join("uvot", "image")):
                        for f in files:
                            mm = sk_re.match(f)
                            if mm:
                                existing.add(mm.group(1))
                continue

            m = re.search(r"(\d{11})", entry)
            if not m:
                continue
            obsid = m.group(1)
            # Confirm it actually has SK data, not just an empty folder.
            img_dir = os.path.join(obs_folder, obsid, "uvot", "image")
            if not os.path.isdir(img_dir):
                # Fall back to searching for any uvot/image under this folder
                found = False
                for root, _, files in os.walk(obs_folder):
                    if root.endswith(os.path.join("uvot", "image")):
                        if any(sk_re.match(f) for f in files):
                            found = True
                            break
                if found:
                    existing.add(obsid)
                continue
            if any(sk_re.match(f) for f in os.listdir(img_dir)):
                existing.add(obsid)
    except Exception as e:
        print(f"  _datainit_existing_obsids: error scanning pool: {e}")
    return existing


# ============================================================================
# DATAINIT MANIFEST — record of obsid processing state
# ============================================================================
# A CSV at the dataInit area (dataInit_manifest.csv), one row per obsid:
#   ObsID, State, Date_Cleaned, N_Bands, N_Corrected, Was_Orphan, Notes
#
# 'raw' (downloaded, not processed) | 'processed' (shared phase done)
#        | 'failed' (shared phase ran but produced nothing usable)
#
# To know:
#   - the shared phase calls update_manifest_after_processing() when it
#     finishes an obsid 
#   - rebuild_manifest_from_disk() regenerates the whole manifest by scanning
#     ASPCORR headers, for recovery when the manifest drifts or after manual
#     changes to the pool
#   - Reads VERIFY against disk, a 'processed' obsid whose folder/data is gone is
#     moved to 'missing' so the driver reprocesses instead of skipping and then failing
#    

MANIFEST_NAME = "dataInit_manifest.csv"
# ----------------------------------------------------------------------


_MANIFEST_COLUMNS = [
    'ObsID', 'State', 'Date_Cleaned',
    'N_Bands', 'N_Corrected', 'N_Exts', 'Was_Orphan', 'Notes', 'Pool_Size',
]


def _manifest_path(datainit_path):
    return os.path.join(datainit_path, MANIFEST_NAME)


def read_manifest(datainit_path):
    """
    Load the manifest as a DataFrame (ObsID as str). Returns an empty
    manifest (correct columns) if none exists yet.
    """
    p = _manifest_path(datainit_path)
    if not os.path.exists(p):
        return pd.DataFrame(columns=_MANIFEST_COLUMNS)
    try:
        df = pd.read_csv(p, dtype={'ObsID': str})
        # Ensure all expected columns exist
        for c in _MANIFEST_COLUMNS:
            if c not in df.columns:
                df[c] = None
        return df[_MANIFEST_COLUMNS]
    except Exception as e:
        print(f" read_manifest: could not read {p} ({e}); treating as empty")
        return pd.DataFrame(columns=_MANIFEST_COLUMNS)


def write_manifest(datainit_path, manifest_df):
    """
    Write the manifest atomically (write to a temp file, then replace) so an
    interrupted write can't corrupt the existing manifest.
    """
    p = _manifest_path(datainit_path)
    tmp = p + ".tmp"
    try:
        manifest_df.to_csv(tmp, index=False)
        os.replace(tmp, p) 
    except Exception as e:
        print(f" write_manifest: failed to write {p} ({e})")
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass


def _find_obsid_image_dir(obsid, datainit_path):
    """
    Locate an obsid's uvot/image dir in the pool, or None.

    Looks in the normal location first, then inside the quarantine
    subfolders (Smeared/, Orphans/, NotASPCORR/) a quarantined obsid has
    still been PROCESSED, it was just moved aside. Finding it there lets the
    manifest's processed/failed verdict stand (the verify-on-read confirms
    the data exists somewhere) so we don't redownload/reprocess it.
    """
    # Normal (un-quarantined) location
    obs_folder = os.path.join(datainit_path, obsid)
    candidate = os.path.join(obs_folder, obsid, "uvot", "image")
    if os.path.isdir(candidate):
        return candidate
    if os.path.isdir(obs_folder):
        for root, _, files in os.walk(obs_folder):
            if root.endswith(os.path.join("uvot", "image")) and obsid in root:
                return root

    # Quarantine subfolders, the obsid was processed then moved aside.
    for qfolder in ("Smeared", "Orphans", "NotASPCORR"):
        qpath = os.path.join(datainit_path, qfolder)
        if not os.path.isdir(qpath):
            continue
        for root, _, files in os.walk(qpath):
            if root.endswith(os.path.join("uvot", "image")) and obsid in root:
                return root

    return None


def manifest_state_for_obsid(obsid, datainit_path, manifest_df=None):
    """
    Return the verified state of one obsid: 'processed', 'raw', or 'missing'.

    Reads the manifest, then VERIFIES against disk: if the manifest says
    'processed' (or 'raw') but the obsid's data is gone, returns 'missing'
    so the driver reprocesses/redownloads rather than trusting a stale claim.
    A 'failed' manifest entry is reported as 'failed' (driver decides whether
    to retry).

    manifest_df may be passed in to avoid re-reading the CSV per obsid.
    """
    if manifest_df is None:
        manifest_df = read_manifest(datainit_path)

    img_dir = _find_obsid_image_dir(obsid, datainit_path)
    data_present = img_dir is not None

    row = manifest_df[manifest_df['ObsID'].astype(str) == str(obsid)]
    claimed = row.iloc[0]['State'] if not row.empty else None

    # Verify, manifest claim only counts if the data is actually there
    if claimed == 'processed' or (claimed is not None and str(claimed).startswith('failed')):
        if not data_present:
            return 'missing'   # stale claim, data gone
        return claimed
    if claimed == 'raw':
        return 'raw' if data_present else 'missing'

    # No manifest entry? fall back to disk presence
    return 'raw' if data_present else 'missing'


def update_manifest_after_processing(datainit_path, obsid, state,
                                     n_bands=None, n_corrected=None,
                                     was_orphan=None, notes="",
                                     pool_size=None, n_exts=None):
    """
    Record/update one obsid's state after the shared phase processed it.
    pool_size records how big the processed pool was at this point, so a
    later run can tell whether the pool has GROWN since an obsid last failed
    (used to decide whether to retry a failed_uncorr obsid).
    """
    manifest_df = read_manifest(datainit_path)
    if n_exts is None:
        # Auto-fingerprint the obsid's raw data volume so a later re-downlink
        # that adds a snapshot is detectable (recorded count won't match).
        n_exts = _obsid_raw_ext_count(datainit_path, obsid)
    new_row = {
        'ObsID': str(obsid),
        'State': state,
        'Date_Cleaned': time.strftime("%Y-%m-%d %H:%M:%S"),
        'N_Bands': n_bands,
        'N_Corrected': n_corrected,
        'N_Exts': n_exts,
        'Was_Orphan': was_orphan,
        'Notes': notes,
        'Pool_Size': pool_size,
    }
    mask = manifest_df['ObsID'].astype(str) == str(obsid)
    if mask.any():
        for k, v in new_row.items():
            manifest_df.loc[mask, k] = v
    else:
        manifest_df = pd.concat(
            [manifest_df, pd.DataFrame([new_row])], ignore_index=True)
    write_manifest(datainit_path, manifest_df)
    return manifest_df

# small manifest helper =
def _manifest_pool_size(obsid, manifest_df):
    """Return the stored Pool_Size for an obsid (the pool generation at its last
    attempt), or None if unknown."""
    if (manifest_df is None or manifest_df.empty
            or 'Pool_Size' not in manifest_df.columns):
        return None
    row = manifest_df[manifest_df['ObsID'].astype(str) == str(obsid)]
    if row.empty:
        return None
    try:
        return int(row.iloc[0]['Pool_Size'])
    except (ValueError, TypeError):
        return None

# Fixing helper to collect pool references
def _collect_pool_references(fresh_table, all_frames, raw_obsids,
                             datainit_path, detect_threshold=3.0,
                             reporter=print):
    """
    Build read-only DIRECT reference rows from already-processed pool obsids so
    that aspect correction can correct a raw subset's NONE frames against
    references that live elsewhere in the pool.
 
    WHY THIS EXISTS!!!!!!!!!!!!!!!!!
    When only a subset of obsids is cleaned, aspect correction only sees that
    subset as candidate references, even though _run_core_engine grouped the
    WHOLE pool (so Group_Status is already correct) the reference ROWS aren't in
    the working table. A NONE frame whose DIRECT reference is an
    already-processed pool obsid therefore fails with "no DIRECT reference".
    This collects those references (using THIS run's consistent whole-pool
    grouping in all_frames), makes sure each has a detect catalog on disk
    (regenerating any a prior cleanup swept), and returns per-extension DIRECT
    rows to concat into the aspect-correction table.
 
    The references are READ-ONLY.
    """
    if (all_frames is None or getattr(all_frames, 'empty', True)
            or fresh_table is None or fresh_table.empty):
        return pd.DataFrame()
 
    raw_set = set(str(o) for o in raw_obsids)
 
    # Groups that actually need correction (a NONE frame among the raw obsids).
    raw_none_groups = set(
        fresh_table.loc[fresh_table['Extension_Status'] == 'NONE',
                        'Group_ID'].unique())
    raw_none_groups.discard(-1)
    if not raw_none_groups:
        return pd.DataFrame()
 
    af = all_frames
    refmask = (
        af['ASPCORR'].isin(['DIRECT', 'READYRESUM']) &
        af['Group_ID'].isin(raw_none_groups) &
        (~af['OBSID'].astype(str).isin(raw_set)))
    ref_obsids = sorted(set(af.loc[refmask, 'OBSID'].astype(str).unique()))
    if not ref_obsids:
        return pd.DataFrame()
 
    reporter(f"  Pool references: {len(ref_obsids)} processed obsid(s) supply "
             f"DIRECT references for {len(raw_none_groups)} group(s) needing "
             f"correction.")
 
    # Per-extension skeleton for the references, grouped with THIS run's
    # whole-pool grouping so Group_IDs line up with fresh_table.
    ref_skeleton = build_observations_skeleton(datainit_path,
                                               only_obsids=ref_obsids)
    if ref_skeleton is None or ref_skeleton.empty:
        return pd.DataFrame()
    ref_skeleton = update_skeleton_with_grouping(ref_skeleton, all_frames, None)
 
    # Ensure each reference has a detect catalog. A prior run's cleanup may have
    # swept the precorrection detects; batch_run_uvotdetect skips any that still
    # exist, so this only regenerates what's missing.
    try:
        batch_run_uvotdetect(datainit_path, threshold=detect_threshold,
                             obs_table=ref_skeleton, only_obsids=ref_obsids)
    except Exception as e:
        reporter(f"  (reference detect regeneration warning: {str(e)[:120]})")
 
    # Only DIRECT extensions act as references; drop any NONE rows so a
    # reference obsid can never be picked up as a correction target. Mark the
    # group READY (these groups have a reference by construction).
    ref_direct = ref_skeleton[
        ref_skeleton['Extension_Status'].isin(['DIRECT', 'UNICORR'])].copy()
    if not ref_direct.empty:
        ref_direct['Group_Status'] = 'READY'
    return ref_direct




def _obsid_aspcorr_summary(img_dir, obsid):
    """
    Scan an obsid's SK files and summarize aspect-correction state, used by
    rebuild-from-disk. Returns (n_bands, n_corrected_exts, total_exts).
    n_corrected = image extensions whose ASPCORR is DIRECT/UNICORR.
    """
    n_bands = 0
    n_corrected = 0
    total_exts = 0
    sk_re = re.compile(rf"^sw{obsid}([a-z0-9]+)_sk\.img(\.gz)?$")
    seen_bands = set()
    try:
        for f in os.listdir(img_dir):
            m = sk_re.match(f)
            if not m:
                continue
            band = m.group(1)
            if band not in BANDS or band in seen_bands:
                continue
            seen_bands.add(band)
            n_bands += 1
            try:
                with fits.open(os.path.join(img_dir, f)) as hdul:
                    for hdu in hdul:
                        if hdu.header.get('NAXIS', 0) < 2:
                            continue
                        total_exts += 1
                        val = str(hdu.header.get('ASPCORR', 'NONE')).strip().upper()
                        if val in ('DIRECT', 'UNICORR'):
                            n_corrected += 1
            except Exception:
                continue
    except Exception:
        pass
    return n_bands, n_corrected, total_exts


def _obsid_raw_ext_count(datainit_path, obsid):
    """Fingerprint of an obsid's RAW data volume: total science image
    extensions across its sw<obsid><band>_sk.img[.gz] files. Grows when a
    re-downlink adds a snapshot, so a change vs the manifest's recorded value
    means the data changed and the obsid should be reprocessed. Returns 0 when
    the raw SK isn't on disk (never downloaded, or cleaned away)."""
    img_dir = _find_obsid_image_dir(str(obsid), datainit_path)
    if not img_dir:
        return 0
    try:
        _n_bands, _n_corrected, total_exts = _obsid_aspcorr_summary(
            img_dir, str(obsid))
        return int(total_exts or 0)
    except Exception:
        return 0


def rebuild_manifest_from_disk(datainit_path):
    """
    Recovery / reconciliation. To regenerate the manifest by scanning every
    obsid in the pool and inferring state from exisiting evidence.

    That Evidence being:
      - no SK data            -> (obsid not listed; it's just absent)
      - SK present, >=1 ext corrected (DIRECT/UNICORR) -> 'processed'
      - SK present, 0 exts corrected                   -> 'raw'
        (could be genuinely raw OR processed but uncorrectable, we can't
         distinguish from headers alone, so we call it 'raw'. The shared
         phase is cheap on already handled obsids, so a needless
         reprocess is safe although wastes some time and the manifest written by the shared phase
         will record the true outcome, including 'failed' for uncorrectable.)
    """
    rows = []
    obsid_re = re.compile(r"(\d{11})")
    try:
        entries = sorted(os.listdir(datainit_path))
    except Exception as e:
        print(f"  rebuild_manifest_from_disk: cannot list {datainit_path} ({e})")
        return pd.DataFrame(columns=_MANIFEST_COLUMNS)

    seen = set()
    for entry in entries:
        full = os.path.join(datainit_path, entry)
        if not os.path.isdir(full):
            continue
        m = obsid_re.search(entry)
        if not m:
            continue
        obsid = m.group(1)
        if obsid in seen:
            continue
        seen.add(obsid)

        img_dir = _find_obsid_image_dir(obsid, datainit_path)
        if img_dir is None:
            continue  # no data → not in manifest

        n_bands, n_corrected, total_exts = _obsid_aspcorr_summary(img_dir, obsid)
        if total_exts == 0 and n_bands == 0:
            continue
        state = 'processed' if n_corrected > 0 else 'raw'
        rows.append({
            'ObsID': obsid,
            'State': state,
            'Date_Cleaned': "(rebuilt from disk)",
            'N_Bands': n_bands,
            'N_Corrected': n_corrected,
            'Was_Orphan': (n_corrected == 0),
            'Notes': "rebuilt",
        })

    manifest_df = pd.DataFrame(rows, columns=_MANIFEST_COLUMNS)
    write_manifest(datainit_path, manifest_df)
    print(f"  Rebuilt manifest: {len(manifest_df)} obsids "
          f"({(manifest_df['State']=='processed').sum()} processed, "
          f"{(manifest_df['State']=='raw').sum()} raw)")
    return manifest_df


# ============================================================================
# POOL OBS_TABLE PERSISTENCE (dataInit/observations_table.csv)
# ============================================================================
# The persistent, dataInit-wide observations table. Same schema as the
# per-run obs_table, but it gets across runs: each time a target's
# obsids are cleaned, their rows are putinto (obsid-level replace) into this
# pool table. It is the deep, per-extension store of what we got (SSS/Smeared/
# Saturated/ASPCORR/Group) so a future target sharing an obsid never re-cleans
# it. The lightweight manifest is the fast obsid-level index ON TOP of this.
#
# Old single-folder pipeline NEVER touches this file, it keeps building its
# own per-target observations_table.csv exactly as before. Only the dataInit
# driver reads/writes the pool table, so many new fucntions :<
POOL_OBSTABLE_NAME = "observations_table.csv"
# ----------------------------------------------------------------------


def _pool_obstable_path(datainit_path):
    return os.path.join(datainit_path, POOL_OBSTABLE_NAME)


def read_pool_obstable(datainit_path):
    """
    Load the persistent pool obs_table (ObsID as str). Returns None if it
    doesn't exist yet (caller treats that as 'empty pool').
    """
    p = _pool_obstable_path(datainit_path)
    if not os.path.exists(p):
        return None
    try:
        df = pd.read_csv(p, dtype={'ObsID': str})
        # Normalize the flag columns back to real bools if they came in as
        # strings (CSV round-trip), matching how the rest of the pipeline
        # expects them. Only touch columns that exist.
        for col in ('Smeared Flag', 'SSS Flag', 'Saturated Flag', 'AspCorr Flag'):
            if col in df.columns and df[col].dtype == object:
                df[col] = df[col].map(
                    lambda v: str(v).strip().lower() in ('true', '1', 'yes', 't'))
        return df
    except Exception as e:
        print(f"  read_pool_obstable: could not read {p} ({e})")
        return None


def write_pool_obstable(datainit_path, pool_df):
    """Write the pool obs_table atomically (temp-then-replace)."""
    p = _pool_obstable_path(datainit_path)
    tmp = p + ".tmp"
    try:
        pool_df.to_csv(tmp, index=False)
        os.replace(tmp, p)
    except Exception as e:
        print(f" write_pool_obstable: failed to write {p} ({e})")
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass


def upsert_obsids_into_pool(datainit_path, fresh_rows_df, obsids):
    """
    Merge our freshly cleaned obs_table rows into the persistent pool table,
    replacing ALL rows for each obsid in `obsids` 

    Important to know we got:
    fresh_rows_df : the obs_table produced by cleaning, containing rows for
                    (at least) the obsids in `obsids`. (ima be typing obsids alot)
    obsids        : a iterable of obsid strings whose rows should be replaced.

    and so it returns the updated pool DataFrame (also written to disk).
    """
    obsids = set(str(o) for o in obsids)

    pool_df = read_pool_obstable(datainit_path)
    if pool_df is None or pool_df.empty:
        pool_df = fresh_rows_df.copy()
    else:
        # Drop existing rows for the obsids being reprocessed, then append
        # the fresh rows for those obsids.
        keep_mask = ~pool_df['ObsID'].astype(str).isin(obsids)
        pool_kept = pool_df[keep_mask]
        fresh_for_obsids = fresh_rows_df[
            fresh_rows_df['ObsID'].astype(str).isin(obsids)]
        pool_df = pd.concat([pool_kept, fresh_for_obsids], ignore_index=True)

    write_pool_obstable(datainit_path, pool_df)
    return pool_df


def pool_rows_for_obsids(datainit_path, obsids, pool_df=None):
    """
    Return the pool obs_table rows for a set of obsids (I.E  to feed the already
    processed obsids into a new cleaning run as references, and to recover the
    persisted SSS/Smeared/etc. detail without re-running checks).

    Returns a DataFrame (possibly empty) with the matching rows.
    """
    if pool_df is None:
        pool_df = read_pool_obstable(datainit_path)
    if pool_df is None or pool_df.empty:
        return pd.DataFrame()
    obsids = set(str(o) for o in obsids)
    return pool_df[pool_df['ObsID'].astype(str).isin(obsids)].copy()


def pool_has_complete_rows(datainit_path, obsid, pool_df=None):
    """
    it asks does the pool table actually contain usable rows
    for this obsid? Used to validate a manifest 'processed' claim if the
    manifest says processed but the pool has no rows (corrupt/lost), the
    driver should re-clean rather than trust this LIE. A bit of a safty check

    Returns True if at least one row for the obsid exists with a non-empty
    Extension_Status.
    """
    if pool_df is None:
        pool_df = read_pool_obstable(datainit_path)
    if pool_df is None or pool_df.empty:
        return False
    rows = pool_df[pool_df['ObsID'].astype(str) == str(obsid)]
    if rows.empty:
        return False
    if 'Extension_Status' in rows.columns:
        return rows['Extension_Status'].notna().any()
    return True
    
###########################################################################################

class _HeasoftResult:
    """Mimics subprocess.CompletedProcess for the fields callers use."""
    def __init__(self, returncode, stdout="", stderr=""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
 
 
# Thread-local storage so each worker thread gets its own warm shell.
_warm_local = threading.local()

# Registry of every live warm shell, so they can all be closed at run end
# (or between phases). Thread-local shells from dead worker threads otherwise
# leave their wsl bash + _pump reader thread running forever.
_ALL_WARM_SHELLS = set()
_ALL_WARM_SHELLS_LOCK = threading.Lock()
 
def _warm_init_lines():
    """
    One-time init for a warm shell — mirrors run_heasoft_wrapper.sh exactly,
    EXCEPT the PFILES dir, which we make per-shell (the wrapper used $$ = the
    per-call bash PID; here the long-lived shell is the isolation boundary, so
    we derive a unique dir from PID + thread id and set it once).
 
    Your wrapper inherits $HEADAS from `conda activate henv` (HEASoft installed
    in the conda env), so we do NOT set HEADAS or source headas-init here —
    conda activation provides it, same as the wrapper.
    """
    uniq = f"{os.getpid()}_{threading.get_ident()}"
    pfiles_dir = f"/tmp/pfiles_warm_{uniq}"
    if HEASOFT_BACKEND == "wsl":
        return [
            'source ~/miniforge3/etc/profile.d/conda.sh',
            'conda activate henv',
            'export CALDB="$HEADAS/caldb"',
            'export CALDBCONFIG="$CALDB/software/tools/caldb.config"',
            'export CALDBALIAS="$CALDB/software/tools/alias_config.fits"',
            f'mkdir -p "{pfiles_dir}"',
            f'export PFILES="{pfiles_dir};$HEADAS/syspfiles"',
            'export HEADASNOQUERY=1',
            'export HEADASPROMPT=/dev/null',
        ]
    else:
        lines = []
        if NATIVE_HEADAS_PATH:
            lines.append(f'export HEADAS="{NATIVE_HEADAS_PATH}"')
            lines.append('. $HEADAS/headas-init.sh 2>/dev/null || true')
        if NATIVE_CALDB_PATH:
            lines.append(f'export CALDB="{NATIVE_CALDB_PATH}"')
            lines.append('. $CALDB/software/tools/caldbinit.sh 2>/dev/null || true')
        lines.append(f'mkdir -p "{pfiles_dir}"')
        lines.append(f'export PFILES="{pfiles_dir};$HEADAS/syspfiles"')
        lines.append('export HEADASNOQUERY=1')
        lines.append('export HEADASPROMPT=/dev/null')
        return lines
 
 
class _WarmShell:
    """A persistent, HEASoft-initialized bash bound to one thread."""
 
    def __init__(self):
        argv = ["wsl", "bash"] if HEASOFT_BACKEND == "wsl" else ["bash"]
        # CRLF hazard: our Python is on Windows. We must not let '\r' reach the
        # WSL bash on the other end of the pipe, or it mis-parses every line
        # (e.g. tries to EXECUTE 'conda.sh\r' instead of sourcing 'conda.sh').
        # We guarantee clean Unix newlines by routing ALL writes throughas_completed
        # _send(), which strips '\r' and appends a single '\n'. (Note: Popen
        # has no 'newline' kwarg — that's an open() parameter — so the
        # stripping in _send is what enforces this.)
        self.proc = subprocess.Popen(
            argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True, bufsize=1)
        self._q = queue.Queue()
        self._alive = True
        self._reader = threading.Thread(target=self._pump, daemon=True)
        self._reader.start()
        for line in _warm_init_lines():
            self._send(line)
        self._flush()
        with _ALL_WARM_SHELLS_LOCK:
            _ALL_WARM_SHELLS.add(self)
 
    def _flush(self):
        """Flush the underlying binary buffer (we write bytes in _send)."""
        try:
            self.proc.stdin.buffer.flush()
        except AttributeError:
            self.proc.stdin.flush()
 
    def _send(self, line):
        """Write one command line to the shell with a guaranteed-clean Unix
        newline. We strip any embedded CR, then write the bytes to the binary
        buffer underneath the text stream. Writing bytes bypasses Windows
        text-mode '\\n'->'\\r\\n' translation, which would otherwise re-insert a
        carriage return that WSL bash chokes on."""
        data = (line.replace("\r", "") + "\n").encode("utf-8", "replace")
        try:
            self.proc.stdin.buffer.write(data)
        except AttributeError:
            # stdin has no .buffer (shouldn't happen with text=True, but be
            # safe): fall back to a plain write.
            self.proc.stdin.write(line.replace("\r", "") + "\n")
 
    def _pump(self):
        """Background: block on readline(), push lines into the queue.
        Pushes None as a sentinel when the pipe closes (shell died).
        Strips trailing CR/LF artifacts so marker matching is reliable."""
        try:
            for line in self.proc.stdout:
                self._q.put(line)
        except Exception:
            pass
        finally:
            self._q.put(None)  # EOF / shell closed
 
    def run(self, command, timeout=None):
        """Run one command, block until done, return (status, output)."""
        if timeout is None:
            timeout = WARM_SHELL_TIMEOUT
        nonce = uuid.uuid4().hex
        marker = f"__DONE_{nonce}__"
        self._send(command)
        self._send(f"echo {marker}$?")
        self._flush()
 
        out_lines = []
        start = time.time()
        while True:
            # Wait for the next line with a real deadline. A silent hang
            # produces no lines, so queue.get times out and we raise — this is
            # the cross-platform replacement for select() on the pipe.
            remaining = timeout - (time.time() - start)
            if remaining <= 0:
                raise TimeoutError(f"command exceeded {timeout}s")
            try:
                line = self._q.get(timeout=min(remaining, 1.0))
            except queue.Empty:
                continue  # nothing yet; loop re-checks the deadline
            if line is None:
                raise BrokenPipeError("warm shell closed unexpectedly")
            # tolerate a stray CR on the marker line too
            if line.lstrip().startswith(marker):
                tail = line.lstrip()[len(marker):].strip()
                status = int(tail or "1")
                return status, "".join(out_lines)
            out_lines.append(line)
 
    def close(self):
        try:
            self.proc.stdin.close()
        except Exception:
            pass
        # Force-kill: a hung command's child (e.g. uvotunicorr) may outlive a
        # gentle terminate, especially through WSL. kill() then wait so we
        # don't leak a zombie or a still-running HEASoft process.
        try:
            self.proc.kill()
        except Exception:
            pass
        try:
            self.proc.wait(timeout=5)
        except Exception:
            pass
        with _ALL_WARM_SHELLS_LOCK:
            _ALL_WARM_SHELLS.discard(self)
 
 
def _get_warm_shell():
    """Return this thread's warm shell, creating it lazily on first use."""
    sh = getattr(_warm_local, "shell", None)
    if sh is None:
        sh = _WarmShell()
        _warm_local.shell = sh
    return sh
 
def shutdown_all_warm_shells():
    """Close every live warm shell (kills each wsl bash + its _pump thread).
    Safe to call between phases or at run end, the next HEASoft call in any
    thread lazily rebuilds a fresh shell. close() removes each from the
    registry, so we iterate over a snapshot."""
    with _ALL_WARM_SHELLS_LOCK:
        shells = list(_ALL_WARM_SHELLS)
    for sh in shells:
        try:
            sh.close()
        except Exception:
            pass
            
def _reset_warm_shell():
    """Tear down this thread's shell so the next call rebuilds it."""
    sh = getattr(_warm_local, "shell", None)
    if sh is not None:
        sh.close()
    _warm_local.shell = None
 
 
def _run_via_warm_shell(command, quiet=False):
    """
    Execute through the per-thread warm shell.
 
    Two distinct failure modes, handled differently:
      - BrokenPipeError: the shell died before/between commands, so THIS
        command never ran. Safe to rebuild the shell and retry once.
      - TimeoutError: the command ran but hung. Retrying would likely hang
        again and, for non-idempotent HEASoft tools, could corrupt state.
        Tear the shell down (so the thread's NEXT command gets a fresh one)
        but do NOT retry — return failure now.
    """
    for attempt in (1, 2):
        try:
            shell = _get_warm_shell()
            status, output = shell.run(command)
            if not quiet and output.strip():
                print(output, end="" if output.endswith("\n") else "\n")
            if status != 0:
                err = output if output.strip() else f"command exited {status}"
            else:
                err = ""
            return _HeasoftResult(returncode=status, stdout=output, stderr=err)
        except TimeoutError as e:
            # command ran and hung — kill the shell, do not retry
            if not quiet:
                print(f"[warm shell] timeout: {e} — shell reset, not retrying")
            _reset_warm_shell()
            return _HeasoftResult(returncode=1, stdout="",
                                  stderr=f"warm shell timeout: {e}")
        except BrokenPipeError as e:
            # shell died between commands — this command never ran; rebuild
            if not quiet:
                print(f"[warm shell] broken pipe: {e} — rebuilding "
                      f"(attempt {attempt})")
            _reset_warm_shell()
            if attempt == 2:
                return _HeasoftResult(returncode=1, stdout="",
                                      stderr=f"warm shell failed: {e}")
    return _HeasoftResult(returncode=1, stderr="warm shell unreachable")
 

def run_heasoft_command(command, quiet=False):
    """
    Execute a HEASoft command. With USE_WARM_SHELL the call routes through a
    per-thread persistent shell (no per-call conda/HEASoft startup); otherwise
    it uses the original fresh-shell-per-call path. Return value has
    .returncode and .stderr in both modes.
    """
    if USE_WARM_SHELL:
        return _run_via_warm_shell(command, quiet=quiet)
    return _run_heasoft_command_per_call(command, quiet=quiet)



def _run_heasoft_command_per_call(command, quiet=False):
    """
    FALLBACK = existing implementation. 
    """
    """
    Runs HEASOFT commands through the appropriate backend.

    WSL: makes a WSL bash shell, activates conda env (WSL_CONDA_ENV)
         or sources WSL_HEASOFT_INIT_SCRIPT, then runs the command.

    NATIVE (Linux/macOS): Sources $HEADAS/headas-init.sh then runs the
         command.  Auto-detects $HEADAS and $CALDB from environment
         unless NATIVE_HEADAS_PATH / NATIVE_CALDB_PATH are set in the
         config block.

    if quite=true, then surpresses the system/result calls
    if in Parallel, as the workers collect their own logs lines and print them
    in clean per-observation blocks. which can look like alot.
    """
    if not quiet:
        print(f"\n[SYSTEM]: Running HEASOFT command...")

    if HEASOFT_BACKEND == "wsl":
        if not WSL_WRAPPER_PATH:
            raise RuntimeError(
                "WSL backend requires WSL_WRAPPER_PATH to be set in the "
                "config block. See README for wrapper setup instructions."
            )
        result = subprocess.run(
            ["wsl", "bash", WSL_WRAPPER_PATH],
            input=command,
            text=True,
            capture_output=True,
        )
    else:
        # Native (Linux / macOS)
        headas = NATIVE_HEADAS_PATH or os.environ.get("HEADAS")
        if not headas:
            raise RuntimeError(
                "Native backend needs $HEADAS set in the environment, "
                "or NATIVE_HEADAS_PATH set in the config block at the "
                "top of this file. Run 'echo $HEADAS' in your terminal -- "
                "if empty, source headas-init.sh before launching Python, "
                "or add it to your ~/.bashrc."
            )

        init_script = os.path.join(headas, "headas-init.sh")
        if not os.path.exists(init_script):
            raise RuntimeError(
                f"HEASoft init script not found at {init_script}"
            )

        caldb = NATIVE_CALDB_PATH or os.environ.get("CALDB", "")
        if not caldb:
            raise RuntimeError(
                "Native backend needs $CALDB set in the environment, "
                "or NATIVE_CALDB_PATH set in the config block."
            )

        caldb_config = (os.environ.get("CALDBCONFIG")
                        or f"{caldb}/software/tools/caldb.config")
        caldb_alias = (os.environ.get("CALDBALIAS")
                       or f"{caldb}/software/tools/alias_config.fits")

        full_cmd = (
            f"export HEADASNOQUERY=1 && "
            f"export HEADASPROMPT=/dev/null && "
            f"export CALDB='{caldb}' && "
            f"export CALDBCONFIG='{caldb_config}' && "
            f"export CALDBALIAS='{caldb_alias}' && "
            f"source '{init_script}' && "
            f"export PFILES=\"/tmp/pfiles_$$;${{HEADAS}}/syspfiles\" && "
            f"mkdir -p /tmp/pfiles_$$ && "
            f"{command}"
        )

        result = subprocess.run(
            ["bash", "-c", full_cmd],
            text=True,
            capture_output=True,
        )

    if result.returncode != 0:
        if not quiet:
            print("  [RESULT]: FAILED")
            print("--- Error Details ---")
            print(result.stderr)
    elif result.stderr and "ERROR" in result.stderr.upper():
        if not quiet:
            print("  [RESULT]: FAILED (HEASoft error in stderr)")
            print("--- Error Details ---")
            print(result.stderr)
    else:
        if not quiet:
            print("  [RESULT]: SUCCESS")
    time.sleep(0.05)
    return result

# UTILS FOR CROSS-PLATFORM PATHS 

def prepare_path(path):
    """
    - On Windows/WSL: Translates C:\ to /mnt/c/
    - On Mac/Linux: Returns the path exactly as it is.
    """
    if HEASOFT_BACKEND == "native":
        return path  # Do nothing for Mac users, I dont know if you need anything here. I guess no buuuuut.
    
    # WSL logic only executes if backend is 'wsl'
    abs_path = os.path.abspath(path)
    drive, rest = os.path.splitdrive(abs_path)
    if drive:
        drive_letter = drive[0].lower()
        return f"/mnt/{drive_letter}{rest.replace('\\', '/')}"
    return abs_path.replace('\\', '/')


######################################################################################
# File sorting
# Module-level cache for the file index, keyed by base_path.
# Maps base_path -> { (obsid, band, file_type): full_path }
_OBS_FILE_INDEX = {}
_OBS_FILE_INDEX_LOCK = threading.Lock()

# Matches e.g. sw00033038054uw1_sk.img.gz / .img, _ex.img.gz, etc.
_OBS_FILE_RE = re.compile(r"^sw(\d{11})([a-z0-9]+)_([a-z]{2})\.img(\.gz)?$")


def _build_obs_file_index(base_path):
    """
    Walk base_path ONCE and index every UVOT image file by
    (obsid, band, file_type). Prefers uncompressed .img over .img.gz when
    both exist. Should be Thread-safe as the first worker to need it builds it under a lock, everyone else reuses.
    """
    index = {}
    folder_obsid_re = re.compile(r"(\d{11})")
    for root, dirs, files in os.walk(base_path):
        if not root.endswith(os.path.join("uvot", "image")):
            continue
        # The obsid that OWNS this directory (from the folder path).
        folder_m = folder_obsid_re.search(root)
        folder_obsid = folder_m.group(1) if folder_m else None
        for f in files:
            m = _OBS_FILE_RE.match(f)
            if not m:
                continue
            obsid, band, ftype, gz = m.group(1), m.group(2), m.group(3), m.group(4)
            # Reject strays: a file whose filename-obsid doesn't match the
            # folder it lives in is almost always a reference-frame copy
            # left by aspect correction, not a real observation of this obsid.
            if folder_obsid is not None and obsid != folder_obsid:
                continue
            key = (obsid, band, ftype)
            full = os.path.join(root, f)
            existing = index.get(key)
            if existing is None or (existing.endswith('.gz') and not f.endswith('.gz')):
                index[key] = full
    return index


def find_obs_file(base_path, obsid, band, file_type='sk'):
    """
    Return the full path to sw{obsid}{band}_{file_type}.img[.gz], or None.

    Backed by a one-time directory index per base_path instead of walking
    the whole tree on every call. Original version re-walked the entire
    data tree for each lookup.
    """
    with _OBS_FILE_INDEX_LOCK:
        index = _OBS_FILE_INDEX.get(base_path)
        if index is None:
            index = _build_obs_file_index(base_path)
            _OBS_FILE_INDEX[base_path] = index
    return index.get((obsid, str(band), file_type))


def obs_file_name(band, kind, obsid=None, target=None):
    """
    Build the FILENAME (not full path, no existence check) for a given kind.
    Used by code that WRITES files (which can't use obs_file_for, since that
    only resolves existing files). Keeps the per-target tag convention in one
    place so writers and the resolver never disagree.
    """
    tag = f"_{target}" if target else ""
    names = {
        'summed_sk':      f"{band}_ex_summed.fits",
        'summed_expmap':  f"{band}_expmap_summed.fits",
        'detect':         f"{band}_corrected_detect.fits",
        'finalsource':    f"{band}_finalsource{tag}.fits",
        'finalsource_ul': f"{band}_finalsource_ul{tag}.fits",
        'source_reg':     f"auto_source{tag}.reg",
        'bkg_reg':        f"auto_bkg{tag}.reg",
    }
    if kind not in names:
        raise ValueError(f"obs_file_name: unknown/unsupported kind '{kind}'")
    return names[kind]
    

def obs_file_for(img_dir, obsid, band, kind, target=None):
    """
    Resolve the path to a specific UVOT file inside one observation's
    uvot/image directory. Returns the full path if it exists, else None.

    Single source of truth for UVOT file naming, the .img/.img.gz
    preference, summed-product names, AND the per-target tagging used in
    multi-target field processing. It is the master for these choices

    SHARED kinds (one file per obsid/band, identical for every target in a
    field) ignore `target`:
      'sk', 'expmap', 'summed_sk', 'summed_expmap', 'detect'

    PER-TARGET kinds (one file per target) append '_{target}' when target
    is given. With target=None they fall back to the untagged name, which
    is the original behavior so existing single-target runs are the exact same.
      'finalsource'     -> {band}_finalsource[_{target}].fits
      'finalsource_ul'  -> {band}_finalsource_ul[_{target}].fits
      'source_reg'      -> auto_source[_{target}].reg
      'bkg_reg'         -> auto_bkg[_{target}].reg

    """
    tag = f"_{target}" if target else ""

    candidates = {
        # ---- shared (target ignored) ----
        'sk':              [f"sw{obsid}{band}_sk.img",
                            f"sw{obsid}{band}_sk.img.gz"],
        'expmap':          [f"sw{obsid}{band}_ex.img",
                            f"sw{obsid}{band}_ex.img.gz"],
        'summed_sk':       [f"{band}_ex_summed.fits"],
        'summed_expmap':   [f"{band}_expmap_summed.fits"],
        'detect':          [f"{band}_corrected_detect.fits",
                            f"{band}_detect.fits",
                            f"{band}_detect_ext1.fits"],
        # ---- per-target (tag applied when target given) ----
        'finalsource':     [f"{band}_finalsource{tag}.fits"],
        'finalsource_ul':  [f"{band}_finalsource_ul{tag}.fits"],
        'source_reg':      [f"auto_source{tag}.reg"],
        'bkg_reg':         [f"auto_bkg{tag}.reg"],
    }.get(kind)

    if candidates is None:
        raise ValueError(f"obs_file_for: unknown kind '{kind}'")

    for name in candidates:
        p = os.path.join(img_dir, name)
        if os.path.exists(p):
            return p
    return None

# This is a Major update to the runtime of the code, trying to direct into files instead of walk to find them.
def build_observations_skeleton(base_path, only_obsids=None):
    """
    Build the observations table SKELETON at the very start of a run, from
    the raw downloaded files alone, before anything runs

    Populated now will be:
        ObsID, Filter, Snapshot, Full_Path,
        Extension_Status (per-extension ASPCORR), File_Status (overall)

    Placeholders that can be filled later by update_skeleton_with_grouping()
    smear flagging / the SSS check:
        Smeared Flag, SSS Flag, Saturated Flag, AspCorr Flag,
        Group_ID, Group_Status, RA, Dec
    """
    possible_bands = ['uvv', 'ubb', 'uuu', 'uw1', 'um2', 'uw2']
    
    with _OBS_FILE_INDEX_LOCK:
        _OBS_FILE_INDEX.pop(base_path, None)
        
    columns = [
        'ObsID', 'Filter', 'Snapshot', 'Smeared Flag', 'SSS Flag',
        'Saturated Flag', 'AspCorr Flag', 'Group_ID', 'Group_Status',
        'Extension_Status', 'File_Status', 'RA', 'Dec', 'Full_Path'
    ]

    # Reuse the one-time file index.
    # We want the SK files specifically.
    with _OBS_FILE_INDEX_LOCK:
        index = _OBS_FILE_INDEX.get(base_path)
        if index is None:
            index = _build_obs_file_index(base_path)
            _OBS_FILE_INDEX[base_path] = index

    rows = []
    # index keys are (obsid, band, file_type); we want file_type == 'sk'
    # When only_obsids is given (dataInit subset processing), restrict the
    # skeleton to just those obsids; None = every obsid (normal behavior).
    _only = set(str(o) for o in only_obsids) if only_obsids else None
    sk_entries = [(obsid, band, path)
                  for (obsid, band, ftype), path in index.items()
                  if ftype == 'sk' and band in possible_bands
                  and (_only is None or str(obsid) in _only)]

    for obsid, band, full_path in sk_entries:
        # Per-extension ASPCORR + snapshot count, read once from the file.
        extension_statuses = _scan_header_for_aspcorr_per_extension(full_path)
        file_status = _scan_header_for_aspcorr(full_path)

        try:
            with fits.open(full_path) as hdul:
                num_snapshots = sum(
                    1 for hdu in hdul if hdu.header.get('NAXIS', 0) >= 2
                )
        except Exception as e:
            print(f"  Warning: could not open {full_path}: {e}")
            continue

        if num_snapshots < 1:
            continue

        for ext in range(1, num_snapshots + 1):
            ext_status = (extension_statuses[ext - 1]
                          if (ext - 1) < len(extension_statuses) else 'NONE')
            aspcorr_flag = (ext_status == 'DIRECT')

            rows.append({
                'ObsID': obsid,
                'Filter': band,
                'Snapshot': ext,
                'Smeared Flag': False,      # filled by smear detection
                'SSS Flag': False,          # filled by SSS check
                'Saturated Flag': False,    # filled by SSS check (recorded only)
                'AspCorr Flag': aspcorr_flag,
                'Group_ID': -1,             # filled by grouping
                'Group_Status': 'UNKNOWN',  # filled by grouping
                'Extension_Status': ext_status,
                'File_Status': file_status,
                'RA': None,                 # filled by grouping
                'Dec': None,                # filled by grouping
                'Full_Path': full_path,
            })

    obs_table = pd.DataFrame(rows, columns=columns)
    print(f"Skeleton observations table built: {len(obs_table)} rows "
          f"across {len(sk_entries)} files (groups/flags filled in later).")
    return obs_table


def update_skeleton_with_grouping(obs_table, all_frames_df, summary_df):
    """
    Fill the grouping-dependent columns into an existing skeleton table,
    in place, once the IAC engine has made us the all_frames_df / summary_df.

    Updates: Group_ID, Group_Status, RA, Dec.
    Leaves everything else (paths, ASPCORR, snapshot, flags) untouched.
    """
    if obs_table is None or obs_table.empty:
        print("  update_skeleton_with_grouping: empty table, nothing to do.")
        return obs_table
    if all_frames_df is None or all_frames_df.empty:
        print("  update_skeleton_with_grouping: no frame data, leaving "
              "group columns as placeholders.")
        return obs_table

    # Build quick lookups keyed by (obsid, band).
    frame_lookup = {}
    for _, fr in all_frames_df.iterrows():
        frame_lookup[(str(fr['OBSID']), fr['Band'])] = (
            fr['RA'], fr['Dec'], fr['Group_ID'])

    status_lookup = {}
    if summary_df is not None and not summary_df.empty:
        for _, sr in summary_df.iterrows():
            status_lookup[(sr['Group_ID'], sr['Band'])] = sr['Status']

    for idx, row in obs_table.iterrows():
        key = (str(row['ObsID']), row['Filter'])
        if key in frame_lookup:
            ra, dec, gid = frame_lookup[key]
            obs_table.at[idx, 'RA'] = ra
            obs_table.at[idx, 'Dec'] = dec
            obs_table.at[idx, 'Group_ID'] = gid
            gstatus = status_lookup.get((gid, row['Filter']), 'UNKNOWN')
            obs_table.at[idx, 'Group_Status'] = gstatus

    n_grouped = int((obs_table['Group_ID'] != -1).sum())
    print(f"  Skeleton updated with grouping: {n_grouped}/{len(obs_table)} "
          f"rows now have a group.")
    return obs_table



############################################################################
# Storage helpers — for saveing space on your device, scripts to delete unneeded files
############################################################################
OBSID_RE = re.compile(r"(\d{11})")
# Named diagnostic files removed from the save/field root (not in image dirs).
DIAGNOSTIC_FILES = ("sss_failures.csv",)


def _cleanup_after_processing(root, save_root=None, label=""):
    """
    Run the storage sweep on data this pipeline just finished.
 
    By default removes only temp/scratch (copied refs, raw images, scratch
    regions, pre-correction detect, sss_failures.csv) — never science products
    and never SK files. The SK flags are governed by the CLEANUP_DELETE_*
    switches and are OFF by default. but those files can be removed automatically
    if more storage is needed for the user and the reward outweighs any risk.
    """
    if not CLEANUP_AFTER_RUN:
        return
    try:
        res = sweep_field(
            root, apply=True,
            delete_gz=CLEANUP_DELETE_GZ,
            delete_img=CLEANUP_DELETE_IMG,
            save_root=save_root,
            verbose=False,
        )
        freed = res.get('freed_bytes', 0) / 1e6
        print(f"  [cleanup{(' ' + label) if label else ''}] "
              f"removed {res.get('deleted', 0)} temp file(s), freed {freed:.1f} MB"
              + (f"; {res['stripped_bands']} SK stripped to summed"
                 if res.get('stripped_bands') else ""))
    except Exception as e:
        print(f"  [cleanup{(' ' + label) if label else ''}] skipped: {str(e)[:150]}")

def _band_of(fname):
    m = re.match(r"sw\d{11}([a-z0-9]+)_(?:sk|ex|rw)\.img", fname.lower())
    return m.group(1) if m else ""


def _folder_obsid(image_dir):
    m = OBSID_RE.search(image_dir)
    return m.group(1) if m else None


def _is_keep_file(fname):
    """Files always kept (science products / reusable). SK .img/.gz are NOT
    listed here — they're governed by the delete_gz / delete_img flags."""
    f = fname.lower()
    if f.endswith("_finalsource.fits") or ("_finalsource_" in f and f.endswith(".fits")):
        return True
    if f.endswith("_ex_summed.fits") or f.endswith("_expmap_summed.fits"):
        return True
    if f.endswith("_corrected_detect.fits"):
        return True
    if f.startswith("auto_bkg") or f.startswith("auto_source"):
        return True
    # exposure maps (per-extension exposure) — keep both forms
    if re.match(r"sw\d{11}[a-z0-9]+_ex\.img(\.gz)?$", f):
        return True
    return False


def _plan_sk_deletions(has_img, has_gz, has_summed, del_gz, del_img):
    """
    Decide which SK forms to delete for one (obsid, band).
    Returns (to_delete:set subset of {'img','gz'}, stripped:bool) where
    stripped=True means the deletion leaves ONLY the summed product
    (per-extension data gone — re-download to recover).
    """
    to_delete = set()
    if del_img and has_summed:           # strip to summed: drop BOTH forms
        if has_img:
            to_delete.add('img')
        if has_gz:
            to_delete.add('gz')
    # del_gz: drop the redundant COMPRESSED original ONLY when the uncompressed
    # .img remains, so per-extension data still survives as the .img. With no
    # .img the .gz is the only per-extension copy, and deleting it would strip
    # to summed-only which is bad.
    if del_gz and has_gz and has_img:                   # drop compressed original
        to_delete.add('gz')
    remains_img = has_img and 'img' not in to_delete
    remains_gz = has_gz and 'gz' not in to_delete
    stripped = (not remains_img and not remains_gz) and has_summed and bool(to_delete)
    return to_delete, stripped


def _obsid_is_done(image_dir):
    """Safe to sweep only if processing produced finalsource OR corrected_detect."""
    try:
        for f in os.listdir(image_dir):
            fl = f.lower()
            if fl.endswith("_finalsource.fits") or "_finalsource_" in fl:
                return True
            if fl.endswith("_corrected_detect.fits"):
                return True
    except OSError:
        return False
    return False


def classify_dir(image_dir, delete_gz=False, delete_img=False):
    """
    Return deletion candidates for one uvot/image dir as a list of
    (filepath, reason, size, requires_redownload).
    """
    candidates = []
    folder_obsid = _folder_obsid(image_dir)
    try:
        files = os.listdir(image_dir)
    except OSError:
        return candidates
    present = set(files)

    # which bands have a summed SKY product present
    summed_bands = set()
    for f in files:
        m = re.match(r"([a-z0-9]+)_ex_summed\.fits$", f.lower())
        if m:
            summed_bands.add(m.group(1))

    for f in files:
        fpath = os.path.join(image_dir, f)
        if not os.path.isfile(fpath):
            continue
        fl = f.lower()
        try:
            size = os.path.getsize(fpath)
        except OSError:
            size = 0

        # ---- ALWAYS-DELETE temp files ----
        # copied reference image: sk.img[.gz] whose obsid != folder's obsid
        m_sk = re.match(r"sw(\d{11})([a-z0-9]+)_sk\.img(\.gz)?$", fl)
        if m_sk and folder_obsid and m_sk.group(1) != folder_obsid:
            candidates.append((fpath, "copied_reference", size, False))
            continue
        # raw images
        if re.match(r"sw\d{11}[a-z0-9]+_rw\.img(\.gz)?$", fl):
            candidates.append((fpath, "raw_image_unused", size, False))
            continue
        # aspect-correction scratch
        if fl in ("ref.reg", "obs.reg"):
            candidates.append((fpath, "aspcorr_scratch", size, False))
            continue
        # pre-correction detect catalogs (corrected_detect excluded by keep-list)
        if (re.match(r"[a-z0-9]+_detect\.fits$", fl)
                or re.match(r"[a-z0-9]+_detect_ext\d+\.fits$", fl)):
            candidates.append((fpath, "precorrection_detect", size, False))
            continue

        # ---- KEEP files ----
        if _is_keep_file(f):
            continue
        # this obsid's own SK .img/.gz are handled by the flag planner below.

    # ---- SK files for THIS obsid, governed by flags ----
    if folder_obsid and (delete_gz or delete_img):
        for band in BANDS:
            img = f"sw{folder_obsid}{band}_sk.img"
            gz = f"sw{folder_obsid}{band}_sk.img.gz"
            has_img = img in present
            has_gz = gz in present
            has_summed = band in summed_bands
            to_del, stripped = _plan_sk_deletions(
                has_img, has_gz, has_summed, delete_gz, delete_img)
            for form in to_del:
                name = img if form == 'img' else gz
                fpath = os.path.join(image_dir, name)
                try:
                    size = os.path.getsize(fpath)
                except OSError:
                    size = 0
                reason = "sk_strip_to_summed" if stripped else "compressed_sk_original"
                candidates.append((fpath, reason, size, True))

    return candidates


def find_image_dirs(field_root):
    for root, dirs, files in os.walk(field_root):
        norm = os.path.normpath(root)
        if any(q in norm.split(os.sep) for q in QUARANTINE):
            continue
        if norm.endswith(os.path.join("uvot", "image")):
            yield root


def _diagnostic_candidates(roots):
    """Named diagnostic files (sss_failures.csv) directly under given roots."""
    out = []
    seen = set()
    for r in roots:
        if not r or r in seen or not os.path.isdir(r):
            continue
        seen.add(r)
        for name in DIAGNOSTIC_FILES:
            p = os.path.join(r, name)
            if os.path.isfile(p):
                try:
                    size = os.path.getsize(p)
                except OSError:
                    size = 0
                out.append((p, "sss_diagnostic", size, False))
    return out

def _write_manifest(path, candidates, stamp):
    with open(path, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["timestamp", "obsid", "band", "filename", "reason",
                    "size_bytes", "requires_redownload"])
        for fpath, reason, size, redl, obsid, band in candidates:
            w.writerow([stamp, obsid, band, os.path.basename(fpath),
                        reason, size, "yes" if redl else "no"])


def _append_manifest(path, rows):
    new = not os.path.exists(path)
    with open(path, "a", newline="") as fh:
        w = csv.writer(fh)
        if new:
            w.writerow(["timestamp", "obsid", "band", "filename", "reason",
                        "size_bytes", "requires_redownload"])
        for r in rows:
            w.writerow(r)
            
def sweep_field(field_root, apply=False, delete_gz=False, delete_img=False,
                force_unfinished=False, save_root=None, verbose=True):
    """
    Reclaim disk under set rules like:

    apply        : False = dry run (default). True = actually delete.
    delete_gz    : delete _sk.img.gz when .img or summed present (opt-in).
    delete_img   : delete BOTH _sk.img and .gz keeping only summed (opt-in,
    save_root    : extra dir to clean named diagnostics (sss_failures.csv) from,
                   since these are located away from the normal directories in some cases.
    verbose      : print the summary.

    Returns a dict: candidates, by_reason, total_bytes, deleted, freed_bytes,
    errors, manifest, skipped_unfinished, stripped_bands.
    """
    def _p(*a):
        if verbose:
            print(*a)

    result = {'candidates': [], 'by_reason': {}, 'total_bytes': 0,
              'deleted': 0, 'freed_bytes': 0, 'errors': 0, 'manifest': None,
              'skipped_unfinished': 0, 'stripped_bands': 0}

    if not os.path.isdir(field_root):
        _p(f"ERROR: not a directory: {field_root}")
        return result

    mode = "APPLY (deleting)" if apply else "DRY-RUN (nothing deleted)"
    _p("=" * 70)
    _p(f"UVOT STORAGE SWEEP — {mode}")
    _p(f"  field root:  {field_root}")
    _p(f"  delete_gz:   {delete_gz}    delete_img: {delete_img}")
    _p("=" * 70)

    all_candidates = []
    skipped = 0
    for image_dir in find_image_dirs(field_root):
        if not force_unfinished and not _obsid_is_done(image_dir):
            skipped += 1
            continue
        for fpath, reason, size, redl in classify_dir(
                image_dir, delete_gz=delete_gz, delete_img=delete_img):
            obsid = _folder_obsid(image_dir) or "?"
            band = _band_of(os.path.basename(fpath))
            all_candidates.append((fpath, reason, size, redl, obsid, band))

    # named diagnostics at field_root and save_root
    for fpath, reason, size, redl in _diagnostic_candidates([field_root, save_root]):
        all_candidates.append((fpath, reason, size, redl, "-", "-"))

    result['candidates'] = all_candidates
    result['skipped_unfinished'] = skipped
    result['stripped_bands'] = sum(1 for c in all_candidates if c[1] == "sk_strip_to_summed")

    if skipped:
        _p(f"  Skipped {skipped} dir(s) with no finished products "
           f"(force_unfinished=True to override).\n")
    if not all_candidates:
        _p("Nothing to delete.")
        return result

    by_reason = {}
    total = 0
    for _, reason, size, *_ in all_candidates:
        by_reason.setdefault(reason, [0, 0])
        by_reason[reason][0] += 1
        by_reason[reason][1] += size
        total += size
    result['by_reason'] = {k: tuple(v) for k, v in by_reason.items()}
    result['total_bytes'] = total

    _p("Deletion candidates by category:")
    for reason, (n, b) in sorted(by_reason.items(), key=lambda kv: -kv[1][1]):
        _p(f"  {reason:24s} {n:5d} files   {b/1e6:9.1f} MB")
    _p(f"  {'TOTAL':24s} {len(all_candidates):5d} files   {total/1e6:9.1f} MB")
    if result['stripped_bands']:
        _p(f"\n  ⚠ WARNING: {result['stripped_bands']} SK file(s) will be stripped to "
           f"summed-only — per-extension photometry will NO LONGER be possible "
           f"for those without re-downloading.")
    _p("")

    manifest_path = os.path.join(field_root, "deleted_files_manifest.csv")
    stamp = time.strftime("%Y-%m-%d %H:%M:%S")

    if not apply:
        preview = manifest_path.replace(".csv", "_PREVIEW.csv")
        _write_manifest(preview, all_candidates, stamp)
        result['manifest'] = preview
        _p("DRY-RUN: call again with apply=True to delete the above.")
        _p(f"Preview manifest: {preview}")
        return result

    deleted, freed, errors, rows = 0, 0, 0, []
    for fpath, reason, size, redl, obsid, band in all_candidates:
        try:
            os.remove(fpath)
            deleted += 1
            freed += size
            rows.append((stamp, obsid, band, os.path.basename(fpath),
                         reason, size, "yes" if redl else "no"))
        except OSError as e:
            errors += 1
            _p(f"  could not delete {fpath}: {e}")

    _append_manifest(manifest_path, rows)
    result.update(deleted=deleted, freed_bytes=freed, errors=errors,
                  manifest=manifest_path)
    _p(f"Deleted {deleted} files, freed {freed/1e6:.1f} MB ({errors} errors).")
    n_redl = sum(1 for r in rows if r[6] == "yes")
    if n_redl:
        _p(f"Re-hydrate manifest: {manifest_path} "
           f"({n_redl} file(s) need re-download to recover).")
    return result








###
###################################################################################
def _resolve_good_extensions(img_path, obsid, band, obs_table):
    """
    Determine which image extensions of a raw SK file are scientifically
    usable, combining ASPCORR status (from the file) with quality flags
    (from obs_table).

    An image extension is GOOD when ALL of the following hold:
      - its ASPCORR is DIRECT or UNICORR (NONE is bad)
      - it is NOT flagged SSS in obs_table
      - it is NOT flagged Smeared in obs_table

    Returns a dict:
        {
          'good':  [ext_num, ...],   # usable image extensions
          'bad':   [ext_num, ...],   # NONE / SSS / Smeared
          'total': int,              # total image extensions on disk
        }
    On read failure returns all-empty with total=0.
    """
    good, bad = [], []

    # Pull SSS/Smeared-flagged extension numbers from obs_table for this
    flagged_exts = set()
    if obs_table is not None:
        if 'SSS Flag' in obs_table.columns:
            m = ((obs_table['ObsID'].astype(str) == str(obsid)) &
                 (obs_table['Filter'] == band) &
                 (obs_table['SSS Flag'] == True))
            flagged_exts.update(obs_table.loc[m, 'Snapshot'].astype(int).tolist())
        if 'Smeared Flag' in obs_table.columns:
            m = ((obs_table['ObsID'].astype(str) == str(obsid)) &
                 (obs_table['Filter'] == band) &
                 (obs_table['Smeared Flag'] == True))
            flagged_exts.update(obs_table.loc[m, 'Snapshot'].astype(int).tolist())

    try:
        with fits.open(img_path) as hdul:
            ext_num = 0
            for hdu in hdul:
                if hdu.header.get('NAXIS', 0) < 2:
                    continue
                ext_num += 1
                aspcorr = str(hdu.header.get('ASPCORR', 'NONE')).strip().upper()
                is_aspcorr_ok = aspcorr in ('DIRECT', 'UNICORR')
                is_flagged = ext_num in flagged_exts
                if is_aspcorr_ok and not is_flagged:
                    good.append(ext_num)
                else:
                    bad.append(ext_num)
    except Exception:
        return {'good': [], 'bad': [], 'total': 0}

    return {'good': good, 'bad': bad, 'total': ext_num}




# name -> {'total': seconds, 'count': times called, 'last': last duration}
_PHASE_REPORT = {}
_PHASE_LOCK = threading.Lock()
 

def phase(name, announce=False):
    """
    Time a named block and accumulate into the phase report.
 
    announce=True prints a line when the phase starts and finishes (useful
    for long stages so you see progress), default is silent until the report.
    """
    if announce:
        print(f"⏱  [{name}] starting...")
    t0 = time.time()
    try:
        yield
    finally:
        dt = time.time() - t0
        with _PHASE_LOCK:
            rec = _PHASE_REPORT.setdefault(
                name, {'total': 0.0, 'count': 0, 'last': 0.0})
            rec['total'] += dt
            rec['count'] += 1
            rec['last'] = dt
        if announce:
            print(f"⏱  [{name}] done in {dt/60:.2f} min ({dt:.1f}s)")
 
 
def reset_phase_report():
    """Clear all recorded phases (call before a fresh run if desired)."""
    with _PHASE_LOCK:
        _PHASE_REPORT.clear()
 
 
def get_phase_report():
    """Return a copy of the raw phase data"""
    with _PHASE_LOCK:
        return {k: dict(v) for k, v in _PHASE_REPORT.items()}
 
 
def print_phase_report(title="PHASE TIMING REPORT"):
    """Print a breakdown of all recorded phases, slowest first."""
    with _PHASE_LOCK:
        items = sorted(_PHASE_REPORT.items(),
                       key=lambda kv: kv[1]['total'], reverse=True)
        grand_total = sum(v['total'] for _, v in items)
 
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)
    if not items:
        print("  (no phases recorded)")
        print("=" * 70)
        return
 
    # column widths
    name_w = max(len(n) for n, _ in items)
    name_w = max(name_w, 5)
    print(f"  {'phase'.ljust(name_w)}   {'total':>10}   {'%':>6}   "
          f"{'calls':>6}   {'avg':>10}")
    print("  " + "-" * (name_w + 42))
    for name, rec in items:
        total_min = rec['total'] / 60.0
        pct = (rec['total'] / grand_total * 100.0) if grand_total else 0.0
        avg_min = (rec['total'] / rec['count'] / 60.0) if rec['count'] else 0.0
        print(f"  {name.ljust(name_w)}   {total_min:>8.2f}m   {pct:>5.1f}%   "
              f"{rec['count']:>6}   {avg_min:>8.2f}m")
    print("  " + "-" * (name_w + 42))
    print(f"  {'TOTAL'.ljust(name_w)}   {grand_total/60:>8.2f}m   "
          f"{'100.0':>5}%")
    print("=" * 70)


#############################################################
#Begining uvotfunctions
#############################################################
#WSL UVOTDETECT version, Thomas if you so desire and think my logical bellow is good and would like to use it, you can edit to the code to add
# If WSL elements, As currently this is later called with a If WSL rather then being built in.
def batch_run_uvotdetect(base_path, threshold=3.0, obs_table=None, only_obsids=None):

    def get_extension_count(filepath):
        try:
            with fits.open(filepath) as hdul:
                return len(hdul) - 1
        except Exception as e:
            return 0, f" Error reading FITS: {e}"

    print("\n" + "=" * 70)
    print("BATCH UVOTDETECT")
    print("=" * 70)

    # Big Step 1: collect every image directory to process.
    # Driven off the observations-table skeleton (built first) instead of
    # walking the tree. 
    _only = set(str(o) for o in only_obsids) if only_obsids else None
    image_dirs = []
    if obs_table is not None and not obs_table.empty and 'Full_Path' in obs_table.columns:
        _seen = set()
        for _, _row in obs_table.iterrows():
            if _only is not None and str(_row['ObsID']) not in _only:
                continue
            _fp = _row['Full_Path']
            if not isinstance(_fp, str) or not _fp:
                continue
            _d = os.path.dirname(_fp)
            if _d in _seen or not os.path.isdir(_d):
                continue
            _seen.add(_d)
            image_dirs.append(_d)
    else:
        _obsid_re = re.compile(r"(\d{11})")
        for root, dirs, files in os.walk(base_path):
            if os.path.basename(root) != "image":
                continue
            if _only is not None:
                _m = _obsid_re.search(root)
                if not _m or _m.group(1) not in _only:
                    continue
            image_dirs.append(root)

    print(f"Found {len(image_dirs)} image directories. "
          f"Running detect across up to {MAX_WORKERS} in parallel...\n")

    obsid_pattern = re.compile(r"sw(\d{11})([a-z0-9]+)_sk\.img\.gz")

    def _detect_one_dir(root):
        """
        Run uvotdetect on every UVOT SK file in ONE image directory.
        Multi-extension files get one detect per extension (with [ext]);
        single-extension files get one detect. Returns a list of log lines.
        Each directory is independent, so this is safe to run concurrently.
        At least it should be.
        """
        lines = []
        files = os.listdir(root)
        img_dir_heasoft = prepare_path(root)

        for file in files:
            match = obsid_pattern.match(file)
            if not match:
                continue
            OBSID, band = match.groups()
            if band not in BANDS:
                continue

            sk_file_path = find_obs_file(base_path, OBSID, band, file_type='sk')
            if not sk_file_path:
                lines.append(f"  [{OBSID}/{band}] Could not find SK file")
                continue

            ext_count = get_extension_count(sk_file_path)
            if isinstance(ext_count, tuple):  # error case
                lines.append(f"  [{OBSID}/{band}] {ext_count[1]}")
                continue

            sk_filename = os.path.basename(sk_file_path)

            if ext_count > 1:  # multi-extension -> one detect per extension
                for ext in range(1, ext_count + 1):
                    detect_ext = f"{band}_detect_ext{ext}.fits"
                    detect_ext_path = os.path.join(root, detect_ext)
                    if os.path.exists(detect_ext_path):
                        continue
                    cmd = (
                        f"cd '{img_dir_heasoft}' && "
                        f"uvotdetect "
                        f"infile='{sk_filename}[{ext}]' "
                        f"outfile='{detect_ext}' "
                        f"expfile=NONE threshold={threshold} clobber=YES mode=h < /dev/null"
                    )
                    run_heasoft_command(cmd, quiet=True)
                    if os.path.exists(detect_ext_path):
                        lines.append(f" [{OBSID}/{band}] ext{ext} ✅")
                    else:
                        lines.append(f" [{OBSID}/{band}] ext{ext} ❌ detect failed")
            else:  # single extension
                detect_base = f"{band}_detect.fits"
                detect_path = os.path.join(root, detect_base)
                if os.path.exists(detect_path):
                    continue
                cmd = (
                    f"cd '{img_dir_heasoft}' && "
                    f"uvotdetect "
                    f"infile='{sk_filename}' "
                    f"outfile='{detect_base}' "
                    f"expfile=NONE threshold={threshold} clobber=YES mode=h < /dev/null"
                )
                run_heasoft_command(cmd, quiet=True)
                if os.path.exists(detect_path):
                    lines.append(f" [{OBSID}/{band}] ✅")
                else:
                    lines.append(f" [{OBSID}/{band}] ❌ detect failed")

        return lines

    # Big step 2: thread across directories 
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_detect_one_dir, root): root
                   for root in image_dirs}
        for fut in tqdm(as_completed(futures), total=len(futures), desc="Running uvotdetect", unit="dir"):
            oid = futures[fut]
            WATCHDOG.beat(f"Uvotdetect: finished obsid {oid}")
            try:
                lines = fut.result()
            except Exception as e:
                tqdm.write(f"❌ detect worker crashed for {futures[fut]}: {e}")
                continue
            if lines:
                tqdm.write("\n".join(lines))

    print("\n UVOT Detect processing complete!")


####################################################################################


def create_uvotunicorr_command(ref_frame, obs_frame, band, ref_snapshot,
                               obs_snapshot, obspath=None):
    """
    Build the uvotunicorr command string. Works for both backends -
    prepare_path() is a no-op on native, WSL translation on Windows.
    """
    if obspath:
        ref_filepath = os.path.join(obspath, f'sw{ref_frame}{band}_sk.img')
        obs_filepath = os.path.join(obspath, f'sw{obs_frame}{band}_sk.img')
        ref_reg_filepath = os.path.join(obspath, 'ref.reg')
        obs_reg_filepath = os.path.join(obspath, 'obs.reg')
    else:
        ref_filepath = f'sw{ref_frame}{band}_sk.img'
        obs_filepath = f'sw{obs_frame}{band}_sk.img'
        ref_reg_filepath = 'ref.reg'
        obs_reg_filepath = 'obs.reg'

    ref_filepath = prepare_path(ref_filepath)
    obs_filepath = prepare_path(obs_filepath)
    ref_reg_filepath = prepare_path(ref_reg_filepath)
    obs_reg_filepath = prepare_path(obs_reg_filepath)

    ref_filepath += f'[{ref_snapshot}]'
    obs_filepath += f'[{obs_snapshot}]'

    command = (
        f"uvotunicorr "
        f"obsfile='{obs_filepath}' "
        f"reffile='{ref_filepath}' "
        f"obsreg='{obs_reg_filepath}' "
        f"refreg='{ref_reg_filepath}' mode=h < /dev/null"
    )
    return command



def detect_smeared_frames(base_path, obs_table=None, only_obsids=None):
    """
    Walk base_path, analyze detect files, identify smeared frames at
    per-extension level.

    Returns a tuple (smeared_obs_folders, smeared_extensions) where:
      - smeared_obs_folders : list of obs folders where ALL extensions
        are smeared (these get moved to Smeared)
      - smeared_extensions : list of dicts with keys 'obsid', 'band',
        'extension' for individual smeared extensions in observations
        that have at least one good extension (these stay in place but
        get flagged for exclusion from uvotimsum)
    """
    detect_pattern = re.compile(r'.*_detect(?:_ext\d+)?\.fits$') #This is so weird to look at but it is correct
    # as the file names are either "{band}_detect.fits" , or "{band}_detect_ext{N}.fits"
    print("\n Scanning for detect files...")

    _only = set(str(o) for o in only_obsids) if only_obsids else None
    detect_files = []
    if obs_table is not None and not obs_table.empty and 'Full_Path' in obs_table.columns:
        _seen = set()
        _img_dirs = []
        for _, _row in obs_table.iterrows():
            if _only is not None and str(_row['ObsID']) not in _only:
                continue
            _fp = _row['Full_Path']
            if not isinstance(_fp, str) or not _fp:
                continue
            _d = os.path.dirname(_fp)
            if _d in _seen or not os.path.isdir(_d):
                continue
            _seen.add(_d)
            _img_dirs.append(_d)
        for _d in _img_dirs:
            for file in os.listdir(_d):
                if "_corrected_detect" in file:
                    continue
                if detect_pattern.match(file):
                    detect_files.append(os.path.join(_d, file))
    else:
        _obsid_re = re.compile(r"(\d{11})")
        for root, dirs, files in os.walk(base_path):
            if os.path.basename(root) == "image":
                if _only is not None:
                    _m = _obsid_re.search(root)
                    if not _m or _m.group(1) not in _only:
                        continue
                for file in files:
                    if "_corrected_detect" in file:
                        continue
                    if detect_pattern.match(file):
                        detect_files.append(os.path.join(root, file))

    print(f"Found {len(detect_files)} detect files to analyze")

    # Per-(obs_folder, band, ext) smearing flag
    smeared_by_ext = {}   # {(obs_folder, band, ext): True}
    all_exts_seen = {}    # {(obs_folder, band): set(ext_numbers)} — track total exts per band

    band_pattern = re.compile(r'([a-z0-9]+)_detect(?:_ext(\d+))?\.fits$')

    for filename in tqdm(detect_files, desc="Analyzing frames"):
        try:
            base_fn = os.path.basename(filename)
            m = band_pattern.match(base_fn)
            if not m:
                continue
            band = m.group(1)
            ext_str = m.group(2)
            ext_num = int(ext_str) if ext_str else 1   # Single-ext files are ext 1

            obs_folder = get_observation_folder(filename, base_path)
            if not obs_folder:
                continue

            # Track that we've seen this extension exists
            key = (obs_folder, band)
            if key not in all_exts_seen:
                all_exts_seen[key] = set()
            all_exts_seen[key].add(ext_num)

            with fits.open(filename, memmap=False) as hdul:
                if len(hdul) < 2:
                    continue
                data = hdul[1].data
                if data is None or len(data) == 0:
                    continue

                prof_major = np.array(data['PROF_MAJOR'], dtype=np.float64)
                prof_minor = np.array(data['PROF_MINOR'], dtype=np.float64)
                flags = np.array(data['FLAGS'], dtype=np.int32)

                df = pd.DataFrame({
                    'PROF_MAJOR': prof_major,
                    'PROF_MINOR': prof_minor,
                    'FLAGS': flags
                })
                df = df[df['FLAGS'] == 0]
                if len(df) == 0:
                    continue

                # Use median instead of mean better for outlier
                # detections (cosmic rays, satellite trails, edge-of-FOV
                # artifacts) that could otherwise inflate a clean frame's
                # apparent elongation. Real smearing affects all sources
                # uniformly, so median captures it just as well as mean
                # without the outlier risk. At least So I have been told
                a = np.median(df['PROF_MAJOR'])
                b = np.median(df['PROF_MINOR'])
                if a > 0 and a >= b:
                    c = math.sqrt(a**2 - b**2)
                    e = c / a
                    if e >= 0.50:  # Might make this a variable later.
                        smeared_by_ext[(obs_folder, band, ext_num)] = True
                        print(f"  Smeared: {os.path.basename(obs_folder)} "
                              f"{band} ext{ext_num} (e={e:.3f})")
        except Exception as ex:
            print(f"  Error processing {filename}: {ex}")

    # Now classify: which obs folders have ALL extensions smeared (whole move),
    # vs. partial smearing (exclude-only)?
    smeared_obs_folders = set()
    smeared_extensions = []

    # Group flagged extensions by (obs_folder, band)
    flagged_by_obs_band = {}
    for (obs_folder, band, ext_num) in smeared_by_ext:
        key = (obs_folder, band)
        if key not in flagged_by_obs_band:
            flagged_by_obs_band[key] = set()
        flagged_by_obs_band[key].add(ext_num)

    # Determine which observations are FULLY smeared
    # Whole move if EVERY (obs_folder, band) is fully smeared
    obs_folder_band_count = {}  # how many bands does each obs have
    obs_folder_band_all_bad = {}  # how many bands are entirely smeared

    for (obs_folder, band), exts_seen in all_exts_seen.items():
        if obs_folder not in obs_folder_band_count:
            obs_folder_band_count[obs_folder] = 0
            obs_folder_band_all_bad[obs_folder] = 0
        obs_folder_band_count[obs_folder] += 1
        flagged_exts = flagged_by_obs_band.get((obs_folder, band), set())
        if flagged_exts == exts_seen:
            # All extensions of this band are smeared
            obs_folder_band_all_bad[obs_folder] += 1
        else:
            # Partial smearing — record individual extensions
            for ext_num in flagged_exts:
                obsid_match = re.search(r"(\d{11})",
                                        os.path.basename(obs_folder))
                obsid = obsid_match.group(1) if obsid_match else None
                if obsid:
                    smeared_extensions.append({
                        'obsid': obsid,
                        'band': band,
                        'extension': ext_num,
                    })

    # Whole move only if EVERY band is fully smeared
    for obs_folder, total_bands in obs_folder_band_count.items():
        if obs_folder_band_all_bad.get(obs_folder, 0) == total_bands:
            smeared_obs_folders.add(obs_folder)

    smeared_obs_folders = list(smeared_obs_folders)

    print(f"\n Smearing detection complete:")
    print(f"  Fully-smeared observations (will be moved): "
          f"{len(smeared_obs_folders)}")
    print(f"  Partially-smeared extensions (will be excluded from summation): "
          f"{len(smeared_extensions)}")

    return smeared_obs_folders, smeared_extensions


####################################################
def remove_smeared(base_path, smeared_obs_folders):
    """
    Move WHOLE moving-smeared observation folders into a Smeared/ subdirectory.
    """
    if not smeared_obs_folders:
        print("No smeared observations to move.")
        return 0

    smeared_dir = os.path.join(base_path, "Smeared")
    os.makedirs(smeared_dir, exist_ok=True)

    print(f"\n Moving {len(smeared_obs_folders)} smeared "
          f"observations to Smeared/...")
    moved = 0
    for obs_folder in smeared_obs_folders:
        try:
            obs_name = os.path.basename(obs_folder)
            destination = os.path.join(smeared_dir, obs_name)
            if os.path.exists(destination):
                print(f"{obs_name} already in Smeared/, skipping")
                continue
            shutil.move(obs_folder, destination)
            print(f" Moved: {obs_name}")
            moved += 1
        except Exception as ex:
            print(f" Error moving {obs_folder}: {ex}")

    print(f"\n smeared removal complete! Moved {moved} folders.")
    print(f" (Partial-smear extensions remain in place; excluded at uvotimsum.)")
    return moved

#############################


def get_observation_folder(detect_filepath, base_path):
    """
    Gets the top-level observation folder from a detect file path.
    """
    # Normalize paths to handle different separators
    detect_filepath = os.path.normpath(detect_filepath)
    base_path = os.path.normpath(base_path)
    
    # Walk up from the detect file until we find a folder directly under base_path
    current = os.path.dirname(detect_filepath)
    
    while current != base_path and os.path.dirname(current) != base_path:
        current = os.path.dirname(current)
    
    # current should now be the observation folder
    if os.path.dirname(current) == base_path:
        return current
    
    return None
        



        
def find_brightest_central_stars(detect_path, num_stars=15, side_buffer=5):
 
    # Read the source catalog up front. We need it for the star selection
    # anyway, and it doubles as the fallback for the pointing center if the
    # header has no RA_PNT (synthetic references / summed images lose it).
    stars = QTable.read(detect_path).to_pandas()
 
    # Pointing center: RA_PNT/DEC_PNT from the primary header (normal detect
    # files, exact original path), then the first-extension header, then the
    # median position of the detected sources. The fallback only ever runs
    # when the keyword is absent, so normal frames are unaffected.
    with fits.open(detect_path) as hdul:
        hdr0 = hdul[0].header
        hdr1 = hdul[1].header if len(hdul) > 1 else None
 
    ra_pnt = hdr0.get('RA_PNT', None)
    dec_pnt = hdr0.get('DEC_PNT', None)
    if (ra_pnt is None or dec_pnt is None) and hdr1 is not None:
        ra_pnt = hdr1.get('RA_PNT', ra_pnt)
        dec_pnt = hdr1.get('DEC_PNT', dec_pnt)
    if ra_pnt is None or dec_pnt is None:
        if (len(stars) > 0 and 'RA' in stars.columns
                and 'DEC' in stars.columns):
            ra_pnt = float(np.median(stars['RA']))
            dec_pnt = float(np.median(stars['DEC']))
            print(f"    (find_brightest_central_stars: no RA_PNT in header — "
                  f"using catalog median center "
                  f"{ra_pnt:.5f}, {dec_pnt:.5f})")
        else:
            raise ValueError(
                f"find_brightest_central_stars: {detect_path} has no "
                f"RA_PNT/DEC_PNT and no catalog sources to recover a center.")
 
    # read header to find central pointing position
    center_ra = float(ra_pnt) * u.deg
    center_dec = float(dec_pnt) * u.deg
 
    # set up buffers
    center_coords = SkyCoord(ra=center_ra, dec=center_dec, frame='fk5')
    position_angle1 = 0 * u.deg
    position_angle2 = 90 * u.deg
    position_angle3 = 180 * u.deg
    position_angle4 = 270 * u.deg
    sep = side_buffer * u.arcmin
 
    # create upper and lower ra/dec bounds
    dec_max = center_coords.directional_offset_by(position_angle1, sep).dec.degree
    dec_min = center_coords.directional_offset_by(position_angle3, sep).dec.degree
 
    ra_max = center_coords.directional_offset_by(position_angle2, sep).ra.degree
    ra_min = center_coords.directional_offset_by(position_angle4, sep).ra.degree
 
    # filter the already-loaded catalog to the central box
    stars = stars[(stars['RA'] >= ra_min) & (stars['RA'] <= ra_max)]
    stars = stars[(stars['DEC'] >= dec_min) & (stars['DEC'] <= dec_max)]
 
    # keep only the brightest sources
    bright_stars = stars.sort_values('MAG', ascending=True)
    bright_stars = bright_stars.iloc[:num_stars+1, :]
 
    nearby_stars = []
 
    # loop over all bright central stars
    # use positions to calculate separation between each star
    # remove stars closer together than 1 arcminute
    for i in range(num_stars+1):
        for j in range(num_stars+1):
 
            if i != j:
                star1_ra = bright_stars.iloc[i, 0]
                star1_dec = bright_stars.iloc[i, 1]
                star1_coords = SkyCoord(star1_ra, star1_dec, unit='deg', frame='fk5')
 
                star2_ra = bright_stars.iloc[j, 0]
                star2_dec = bright_stars.iloc[j, 1]
                star2_coords = SkyCoord(star2_ra, star2_dec, unit='deg', frame='fk5')
 
                sep = star1_coords.separation(star2_coords).to(u.arcsecond) / u.arcsecond
 
                if sep <= 31:
                    nearby_stars.append(j)
 
            else:
                continue
 
    star_indices = [star for star in range(num_stars) if star not in nearby_stars]
    bright_stars = bright_stars.iloc[star_indices, :]
 
    return bright_stars

def remove_separate_stars(ref_bright_stars, obs_bright_stars):

    sep_frame = pd.DataFrame(columns=obs_bright_stars.index, index=ref_bright_stars.index)

    ref_coords = []
    obs_coords = []
    
    for ind in ref_bright_stars.index:
        ref_star_ra = ref_bright_stars.loc[ind, 'RA']
        ref_star_dec = ref_bright_stars.loc[ind, 'DEC']
    
        ref_star_coords = SkyCoord(ref_star_ra, ref_star_dec, unit='deg', frame='fk5')
        ref_coords.append(ref_star_coords)
    
    for ind in obs_bright_stars.index:
        obs_star_ra = obs_bright_stars.loc[ind, 'RA']
        obs_star_dec = obs_bright_stars.loc[ind, 'DEC']
    
        obs_star_coords = SkyCoord(obs_star_ra, obs_star_dec, unit='deg', frame='fk5')
        obs_coords.append(obs_star_coords)
    
    for obs_ind, obs_star in zip(obs_bright_stars.index, obs_coords):
        for ref_ind, ref_star in zip(ref_bright_stars.index, ref_coords):
    
            sep_frame.loc[ref_ind, obs_ind] = obs_star.separation(ref_star).to(u.arcsecond) / u.arcsecond
    
    sep_frame = sep_frame.where(sep_frame<(30.0)).dropna(axis=1, how='all').dropna(axis=0, how='all')

    if len(sep_frame.index) == len(sep_frame.columns):
        
        ref_bright_stars = ref_bright_stars.loc[list(sep_frame.index), :]
        obs_bright_stars = obs_bright_stars.loc[list(sep_frame.columns), :]

    else:
        print("Bright Stars Did Not Match. Please Adjust Filter Parameters and Try Again.")

    return ref_bright_stars, obs_bright_stars

def create_ref_obs_reg_files(ref_bright_stars, obs_bright_stars, outpath=None):

    ref_circles = []
    ref_coords = []
    
    for ind in ref_bright_stars.index:
        ref_ra = ref_bright_stars.loc[ind, 'RA']
        ref_dec = ref_bright_stars.loc[ind, 'DEC']
    
        ref_star_coords = SkyCoord(ref_ra, ref_dec, unit='deg', frame='fk5')
        # region = CircleSkyRegion(star_coords, radius=5*u.arcsecond)
        # region.write('ref.reg', format='ds9')
        ref_circle = f'circle({ref_ra},{ref_dec},5.000")\n'
        ref_circles.append(ref_circle)
        ref_coords.append(ref_star_coords)
        
    if outpath:
        ref_filename = outpath+'/ref.reg'
    else:
        ref_filename = 'ref.reg'
        
    reg_header = '# Region file format: DS9 version 4.1\nfk5\n'

    ref_circles_sum = "".join(ref_circles)
    ref_reg_text = reg_header + ref_circles_sum
    
    with open(ref_filename, mode='w', encoding='utf-8') as reffile:
        reffile.write(ref_reg_text)

    obs_circles = []
    obs_coords = []
    
    for ind in obs_bright_stars.index:
        obs_ra = obs_bright_stars.loc[ind, 'RA']
        obs_dec = obs_bright_stars.loc[ind, 'DEC']
    
        obs_star_coords = SkyCoord(obs_ra, obs_dec, unit='deg', frame='fk5')
        # region = CircleSkyRegion(star_coords, radius=5*u.arcsecond)
        # region.write('ref.reg', format='ds9')
        obs_circle = f'circle({obs_ra},{obs_dec},5.000")\n'
        obs_circles.append(obs_circle)
        obs_coords.append(obs_star_coords)
        
    if outpath:
        obs_filename = outpath+'/obs.reg'
    else:
        obs_filename = 'obs.reg'
    
    obs_circles_sum = "".join(obs_circles)
    obs_reg_text = reg_header + obs_circles_sum
    
    with open(obs_filename, mode='w', encoding='utf-8') as obsfile:
        obsfile.write(obs_reg_text)

def run_upper_limit_uvotsource(obs_table, base_path, save_path,
                                target_ra, target_dec,
                                source_radius=5.0,
                                bkg_reg_name="auto_bkg.reg",
                                target=None, output_root=None):
    """
    Compute 3-sigma upper limits for non-detections.

    A "non-detection" here is any obs+band that did NOT get a normal
    {band}_finalsource.fits during the main uvotsource pass. That covers:
      - Frames where uvotdetect failed (file moved to DetectFailed/)
      - Frames where uvotdetect found field sources but none within the
        max_offset of the target (no source region was written)

    For each such frame we run a NORMAL uvotsource with:
      - srcreg : a 5" source region placed at the catalog target position
      - bkgreg : the SAME auto_bkg.reg the background generator already
                 wrote into this directory 

    We then read AB_MAG_LIM from the output — the 3-sigma limiting
    magnitude and report it as the upper limit. Because these are
    non-detections by construction, every row from this pass is flagged
    UpperLimit=True; we do not attempt to salvage a measurement from
    AB_MAG.

    Output files are named {band}_finalsource_ul.fits and are tagged
    UpperLimit=True during master compilation.

    Returns
    -------
    upper_limit_paths : list of str
    """

    print("\n" + "=" * 70)
    print("UPPER-LIMIT PHOTOMETRY FOR NON-DETECTIONS (Option 1)")
    print("=" * 70)
    print(f"Target: RA={target_ra:.6f}, Dec={target_dec:.6f}")
    print(f"Method: normal uvotsource, src region at target, "
          f"reusing {bkg_reg_name}; report AB_MAG_LIM")

    # Source region at the catalog target position. Required because
    # srcreg=NONE is broken in uvotsource v4.5 (it gets passed to
    # uvotinteg as a literal filename). A real region at the target
    # gives uvotsource what it needs to compute AB_MAG_LIM.
    ul_src_text = (
        f'# Region file format: DS9 version 4.1\n'
        f'# Upper-limit source region (catalog target position)\n'
        f'fk5\n'
        f'circle({target_ra},{target_dec},{source_radius}")\n'
    )

    # Per-target tagging
    if target:
        bkg_reg_name = obs_file_name(None, 'bkg_reg', target=target)

    upper_limit_paths = []
    n_processed = 0
    n_skipped = 0
    n_failed = 0
    # Build the list of (obsid, image_dir) to process from the observations
    # table instead of walking the tree. Full_Path gives each row's directory
    # Verify the dir exists on disk (a row's Full_Path can be stale if its folder was quarantined/moved).
    ul_dirs = []
    if obs_table is not None and not obs_table.empty and 'Full_Path' in obs_table.columns:
        seen = set()
        for _, row in obs_table.iterrows():
            obsid = str(row['ObsID'])
            full_path = row['Full_Path']
            if not isinstance(full_path, str) or not full_path:
                continue
            img_dir = os.path.dirname(full_path)
            parts = os.path.normpath(img_dir).split(os.sep)
            if any(q in parts for q in QUARANTINE):
                continue
            key = (obsid, img_dir)
            if key in seen:
                continue
            if not os.path.isdir(img_dir):
                continue
            seen.add(key)
            ul_dirs.append((obsid, img_dir))
    else:
        # Fallback: walk if no table available
        obsid_re = re.compile(r"(\d{11})")
        for root, dirs, files in os.walk(base_path):
            normalised = os.path.normpath(root)
            if not normalised.endswith(os.path.join("uvot", "image")):
                continue
            if any(q in normalised.split(os.sep) for q in QUARANTINE):
                continue
            m = obsid_re.search(root)
            ul_dirs.append((m.group(1) if m else "?", root))

    def _ul_one_observation(obsid, root):
        """
        Compute upper limits for ONE observation (all bands). Bands run
        sequentially within the obsid, obsids run in parallel. Workers never
        mutate obs_table or shared counters. they return results the main
        thread folds in. Each worker owns its own root/write_dir, so the
        DetectFailed restore and region writes can't collide across threads.
        """
        w_paths = []
        counts = dict(processed=0, skipped=0, failed=0)
        lines = []
        def log(msg): lines.append(msg)

        write_dir = _obsid_write_dir(output_root, root, obsid)

        # Reuse the background region the generator already produced.
        bkg_reg_path = os.path.join(write_dir, bkg_reg_name)
        if not os.path.exists(bkg_reg_path):
            return {'paths': w_paths, 'counts': counts, 'log': lines}

        current_files = os.listdir(root)
        out_files = os.listdir(write_dir) if os.path.isdir(write_dir) else []
        detect_failed_dir = os.path.join(root, "DetectFailed")
        detect_failed_files = (os.listdir(detect_failed_dir)
                               if os.path.isdir(detect_failed_dir) else [])

        for band in BANDS:
            # If a normal detection exists for this band, this is NOT a
            # non-detection skip it. The UL pass only handles leftovers.
            # In split mode the finalsource lives in write_dir (dataSRC).
            finalsource_file = obs_file_name(band, 'finalsource', target=target)
            if finalsource_file in out_files:
                # Skip ONLY if it's a real detection. A finalsource holding
                # AB_MAG==99 is a non-detection the main pass wrote because a
                # source region existed, still compute its 3-sigma limit so it
                # appears in Mixed as a UL (so we can match All-frames) instead of
                # vanishing into the 99-drop / finalsource-exists hole as it was.
                if _finalsource_is_detection(
                        os.path.join(write_dir, finalsource_file)):
                    continue

            # Skip frames that are 100% bad data, a genuine non-detection
            # deserves an upper limit, but bad data does NOT. A frame is
            # bad-beyond-use when it has NO good extension, where "good"
            # means: ASPCORR corrected (DIRECT/UNICORR) AND not SSS-flagged
            # AND not smeared. Frames reach this pass via DetectFailed/, and
            # all-bad frames land there too, without this check they'd
            # become meaningless "upper limits" 
            if obs_table is not None:
                band_rows = obs_table[
                    (obs_table['ObsID'].astype(str) == str(obsid)) &
                    (obs_table['Filter'] == band)
                ]
                if len(band_rows) > 0:
                    # An extension is GOOD only if corrected AND unflagged.
                    is_corrected = band_rows['Extension_Status'].isin(
                        ['DIRECT', 'UNICORR'])
                    is_sss = (band_rows['SSS Flag'] == True
                              if 'SSS Flag' in band_rows.columns
                              else pd.Series(False, index=band_rows.index))
                    is_smeared = (band_rows['Smeared Flag'] == True
                                  if 'Smeared Flag' in band_rows.columns
                                  else pd.Series(False, index=band_rows.index))
                    is_good = is_corrected & ~is_sss & ~is_smeared
                    if not bool(is_good.any()):
                        log(f" [{obsid} / {band}] No good extensions "
                            f"(all smeared / SSS / uncorrected) — skipping "
                            f"(no upper limit from bad data)")
                        continue

            # Already computed the upper limit on a previous run? Reuse it.
            ul_output = obs_file_name(band, 'finalsource_ul', target=target)
            ul_output_path = os.path.join(write_dir, ul_output)
            
            if os.path.exists(ul_output_path):
                w_paths.append(ul_output_path)
                counts['skipped'] += 1
                continue

            # Locate an image file to run on. Priority: summed SK, raw SK,
            # then anything matching this band sitting in DetectFailed/.
            summed_file = f"{band}_ex_summed.fits"
            sk_img = f"sw{obsid}{band}_sk.img"
            sk_gz = f"sw{obsid}{band}_sk.img.gz"

            input_file = None
            if summed_file in current_files:
                input_file = summed_file
            elif sk_img in current_files:
                input_file = sk_img
            elif sk_gz in current_files:
                input_file = sk_gz
            else:
                # Try restoring a file from DetectFailed/
                for ff in detect_failed_files:
                    if band in ff.lower():
                        src_p = os.path.join(detect_failed_dir, ff)
                        dst_p = os.path.join(root, ff)
                        if not os.path.exists(dst_p):
                            try:
                                shutil.copy2(src_p, dst_p)
                            except Exception:
                                continue
                        input_file = ff
                        break

            if input_file is None:
                # No image for this band in this obs — nothing to do.
                continue

            # Locate the exposure map (same priority logic as normal pass)
            exp_summed = f"{band}_expmap_summed.fits"
            exp_img = f"sw{obsid}{band}_ex.img"
            exp_gz = f"sw{obsid}{band}_ex.img.gz"
            exp_file = "NONE"
            if input_file == summed_file:
                if os.path.exists(os.path.join(root, exp_summed)):
                    exp_file = exp_summed
            else:
                if os.path.exists(os.path.join(root, exp_img)):
                    exp_file = exp_img
                elif os.path.exists(os.path.join(root, exp_gz)):
                    exp_file = exp_gz

            # Write the source region at the target position (per-target -> write_dir).
            ul_src_name = (f"auto_source_ul_{target}.reg" if target
                           else "auto_source_ul.reg")
            with open(os.path.join(write_dir, ul_src_name), 'w') as f:
                f.write(ul_src_text)

            log(f" [{obsid} / {band}] Non-detection — computing "
                f"upper limit (input: {input_file})")

            # Normal uvotsource. The output's AB_MAG_LIM is the 3-sigma
            # limiting magnitude we report as the upper limit.
            if output_root is None:
                # NORMAL MODE
                if HEASOFT_BACKEND == "wsl":
                    wsl_dir = prepare_path(root)
                    cmd = (
                        f"cd '{wsl_dir}' && "
                        f"uvotsource image='{input_file}' "
                        f"srcreg='{ul_src_name}' "
                        f"bkgreg='{bkg_reg_name}' "
                        f"sigma=3 "
                        f"expfile='{exp_file}' "
                        f"zerofile=CALDB coinfile=CALDB psffile=CALDB lssfile=CALDB "
                        f"syserr=NO frametime=DEFAULT apercorr=NONE output=ALL "
                        f"outfile='{ul_output}' "
                        f"cleanup=YES clobber=YES chatter=1 mode=h < /dev/null"
                    )
                else:
                    cmd = (
                        f"cd '{root}' && "
                        f"uvotsource image='{input_file}' "
                        f"srcreg='{ul_src_name}' "
                        f"bkgreg='{bkg_reg_name}' "
                        f"sigma=3 "
                        f"expfile='{exp_file}' "
                        f"zerofile=CALDB coinfile=CALDB psffile=CALDB lssfile=CALDB "
                        f"syserr=NO frametime=DEFAULT apercorr=NONE output=ALL "
                        f"outfile='{ul_output}' "
                        f"cleanup=YES clobber=YES chatter=1 mode=h < /dev/null"
                    )
            else:
                # SPLIT MODE?absolute paths: read input from `root`
                # (dataInit), write region/output to `write_dir` (dataSRC).
                img_abs = prepare_path(os.path.join(root, input_file))
                src_abs = prepare_path(os.path.join(write_dir, ul_src_name))
                bkg_abs = prepare_path(bkg_reg_path)
                out_abs = prepare_path(ul_output_path)
                exp_abs = (prepare_path(os.path.join(root, exp_file))
                           if exp_file != "NONE" else "NONE")
                cmd = (
                    f"uvotsource image='{img_abs}' "
                    f"srcreg='{src_abs}' "
                    f"bkgreg='{bkg_abs}' "
                    f"sigma=3 "
                    f"expfile='{exp_abs}' "
                    f"zerofile=CALDB coinfile=CALDB psffile=CALDB lssfile=CALDB "
                    f"syserr=NO frametime=DEFAULT apercorr=NONE output=ALL "
                    f"outfile='{out_abs}' "
                    f"cleanup=YES clobber=YES chatter=1 mode=h < /dev/null"
                )

            run_heasoft_command(cmd, quiet=True)
            time.sleep(1)

            if os.path.exists(ul_output_path):
                w_paths.append(ul_output_path)
                counts['processed'] += 1
                log(f" ✅ {ul_output} [{obsid}/{band}]")
            else:
                counts['failed'] += 1
                log(f" ❌ uvotsource failed for {obsid}/{band}")

        return {'paths': w_paths, 'counts': counts, 'log': lines}

    print(f"Computing upper limits across up to {MAX_WORKERS} "
          f"observation(s) in parallel...\n")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_ul_one_observation, obsid, root): obsid
                   for obsid, root in ul_dirs}
        for fut in tqdm(as_completed(futures), total=len(futures), desc="Upper limits", unit="obs"):
            oid = futures[fut]
            WATCHDOG.beat(f"Upper Limits: finished obsid {oid}")
            try:
                res = fut.result()
            except Exception as e:
                tqdm.write(f" ❌ UL worker crashed for {futures[fut]}: {e}")
                continue
            c = res['counts']
            n_processed += c['processed']
            n_skipped += c['skipped']
            n_failed += c['failed']
            upper_limit_paths.extend(res['paths'])
            if res['log']:
                tqdm.write("\n".join(res['log']))

    print(f"\nUpper-limit photometry summary:")
    print(f" Processed:    {n_processed}")
    print(f" Skipped:      {n_skipped} (already computed)")
    print(f" Failed:       {n_failed}")
    print(f" Total upper-limit files: {len(upper_limit_paths)}")
    print("=" * 70)

    return upper_limit_paths


#######################################################################################



#################################################################################### Bellow functions are Currently Not apart of the pipeline
# They can be removed at anytime currently, keeping them for now.

def download_ogle_data(ogle_name, source_name):

    ogle = requests.get(f'https://www.astrouw.edu.pl/ogle/ogle4/xrom/{ogle_name}/phot.dat')

    if ogle.status_code != 200:
        raise DownloadError("An Error occurred when downloading the file. Please check the name of the OGLE Source and try again.")
    else:
        ogle_local_filename = f"./OGLE_Outputs/{source_name}.dat"
        with open(ogle_local_filename, 'wb') as f:
            for chunk in ogle.iter_content(chunk_size=8192):
                f.write(chunk)

def download_xrt_data(xrt_num, source_name):

    xrt = requests.get(f'https://www.swift.ac.uk/SMC/data/source{xrt_num}/curve/PC_incbad.qdp', auth=HTTPBasicAuth('smc', 'T1le_th3_$MC'))

    if xrt.status_code != 200:
        raise DownloadError("An Error occurred when downloading the file. Please check the number of the XRT Source and try again.")
    else:
        xrt_local_filename = f"./XRT_Outputs/{source_name}.qdp"
        with open(xrt_local_filename, 'wb') as f:
            for chunk in xrt.iter_content(chunk_size=8192):
                f.write(chunk)

def read_ogle_data(source_name):

    ogle_data = pd.read_csv(f'./OGLE_Outputs/{source_name}.dat', sep=r'\s+', header=None, names=['Time', 'I', 'I_Err', 'Seeing', 'Sky'])
    
    ogle_data['MJD'] = ogle_data['Time'] - 2400000

    return ogle_data

def read_uvot_data(source_name):

    uvot_data = pd.read_csv(f'./UVOT_Outputs/{source_name}_uvot_data.txt', header=None, sep=r'\s+', names=['MJD', 'Mag', 'Mag_Err', 'F_lam', 'F_lam_err'])

    return uvot_data

def read_xrt_data(source_name):

    xrt_data = Table.read(f"./XRT_Outputs/{source_name}.qdp", format='ascii.qdp', table_id=0, names=['MJD', 'CR'])
    xrt_ul_data = Table.read(f"./XRT_Outputs/{source_name}.qdp", format='ascii.qdp', table_id=1, names=['MJD', 'CR'])

    xrt_data['MJD_nerr'] = -1*xrt_data['MJD_nerr']
    xrt_data['CR_nerr'] = -1*xrt_data['CR_nerr']

    return xrt_data, xrt_ul_data

#################################################################################### 

# -------------------- The hunt for Red ASPCORR -----------------------------
# This has given me some pause for some time as what I did in the past was a very basic bit of code that used existing fkeyprint code and read the extension
# That on hindsight didnt work to well for two reasons, 1: I was only reading the first extension and not the whole list(whops) 2: Meant that code only worked for WSL, this needs to be universal.

#So what we now do instead is a scan the FITS file itself for the extension and read the proper sheet. I.E. the fits files themselves have extension as the photos do so they may have-- Sheet 0 (Primary) Sheet 1(Image) Sheet 2(Image), etc.
# So we will have to Loop through all sheets for our hunt
def _scan_header_for_aspcorr_per_extension(file_path):
    """
    Returns a list of ASPCORR statuses, one per image extension.
    Used for building the detailed observations table.
    
    It Also recognizes UNICORR as a corrected status
    """
    if not file_path:
        return []
    
    try:
        with fits.open(file_path) as hdul:
            statuses = []
            for hdu in hdul:
                # Only process image extensions (skip primary header with NAXIS=0)
                naxis = hdu.header.get('NAXIS', 0)
                if naxis >= 2:
                    val = hdu.header.get('ASPCORR', 'NONE')
                    status = str(val).strip().upper()
                    
                    # Treat UNICORR as DIRECT (both are corrected... Might have to change this?) 
                    if status == 'UNICORR':
                        status = 'DIRECT'
                    
                    statuses.append(status)
            return statuses
    except:
        return []


def _scan_header_for_aspcorr(file_path):
    """
    Returns overall status for the file.
    If both DIRECT and NONE exist across extensions, returns 'READYRESUM'.
    
    I also now recognizes UNICORR as a corrected status, This may need to change As UNICORR shouldnt be used for aspect corrections? Possible issue.
    """
    if not file_path:
        return "NONE"
    
    try:
        with fits.open(file_path) as hdul:
            statuses = set()
            for hdu in hdul:
                val = hdu.header.get('ASPCORR', 'NONE')
                status = str(val).strip().upper()
                
                # Treat UNICORR as DIRECT for grouping purposes
                if status == 'UNICORR':
                    status = 'DIRECT'
                
                statuses.add(status)
            
            # READYRESUM logic
            if 'DIRECT' in statuses and 'NONE' in statuses:
                return 'READYRESUM'
            if 'NONE' in statuses:
                return 'NONE'
            if 'DIRECT' in statuses:
                return 'DIRECT'
            return "NONE"
    except:
        return "NONE"

# Essentially the same as above,we are grabing the RA/DEC from the image header. The only funny little thing about this is NAXIS
# A bit of a problem you may run into if you make a few misteps is not all FITS extensions are images, we need to find which ones are. The best way I would find is to look for NAXIS I.E. does the file have Height and Width.
def _get_coords(file_path):
    if not file_path: 
        return None, None
    try:
        with fits.open(file_path) as hdul:  
            for i, hdu in enumerate(hdul):
                naxis = hdu.header.get('NAXIS', 0)
                
                if naxis >= 2:
                    w = WCS(hdu.header)
                    
                    naxis1 = hdu.header.get('NAXIS1', 0)
                    naxis2 = hdu.header.get('NAXIS2', 0)
                    
                    cx, cy = naxis1/2.0, naxis2/2.0
                    ra, dec = w.all_pix2world(cx, cy, 0)
                    return float(ra), float(dec)
            
            print(f" No 2D image found in any HDU")
            return None, None
    except Exception as e:
        print(f" DEBUG _get_coords ERROR: {e}")
        return None, None


# The Engine of the operation
def _run_core_engine(base_folder=None, save_dir=None):
        # This was funky to figure out, the os.path.normpath and strip inputs where only added to have the code be universal and convienent. Atleast it should work
    # Since I only have windows Not 100% sure on it working or not, but it should be striping quotes from drag anddrop folders like Mac allows and Normalzing slashes.
    if not base_folder:
        base_folder = os.path.normpath(input("1. Path to UVOT raw data: ").strip().strip('"').strip("'"))
    if not save_dir:
        save_dir = os.path.normpath(input("2. Save directory: ").strip().strip('"').strip("'"))
    
    if not os.path.exists(save_dir): 
        os.makedirs(save_dir)

    bands_list = ["uvv", "uuu", "ubb", "um2", "uw1", "uw2"]
    raw_results = []
    
    # Now normally you would try to use a walk here I know I did. Thats a bad idea turns out as if you do that it dives into every sub-sub-folder Immediately.
    # Leading to double counting, so instead we try to control it a bit.
    try:
        # EXCLUDE the Smeared folder
        top_folders = [f for f in os.listdir(base_folder) 
                      if os.path.isdir(os.path.join(base_folder, f)) 
                      and f != "Smeared"]
        print(f"DEBUG: Found {len(top_folders)} top-level folders (excluding Smeared)")
        
        # Sample first 3 folders to see what we're dealing with
        print(f"DEBUG: Sample folder names:")
        for i, folder in enumerate(top_folders[:3]):
            print(f"  {i+1}. {folder}")
            
    except Exception as e: 
        print(f"DEBUG: Error listing directories: {e}")
        return None, None, None

    folder_pattern = re.compile(r"(\d{11})")

    # For debuging bellow since it wasnt working.
    matched_folders = 0
    files_found_count = 0
    coords_failed = 0
    
    # Check first matching folder in detail
    first_detailed_check = False

    for folder in top_folders:
        match = folder_pattern.search(folder)
        if not match: 
            print(f"DEBUG: Folder '{folder}' doesn't match pattern - skipping")
            continue
        
        matched_folders += 1
        obsid = match.group(1).zfill(11) # The subfolders we have are just the OBSIDS so this is a quick and easy way to get those now for later.
        # This bit had a lot of work but into it because of an intresting error I wasent expecting to have. We havesummed files which are the Images we want to use but those miss keywords
        # We also have Sky files which we dont want to use unless we have to but those always have keywords like the ASPCORR we need, so we had to scan and use both types if they exist to accomplish differnt goals.
        
        # Detailed check on first folder only
        if not first_detailed_check:
            first_detailed_check = True
            folder_path = os.path.join(base_folder, folder)
            
            # Walk and show structure
            print(f"DEBUG: Folder structure:")
            for root, dirs, files in os.walk(folder_path): 
                rel_path = os.path.relpath(root, folder_path)
                if rel_path == ".":
                    rel_path = "[root]"
                print(f"  {rel_path}/ - {len(files)} files, {len(dirs)} subdirs")
                
                # Show SK files if any
                sk_pattern = re.compile(r'.*_sk.*\.(img|fits|gz)$')
                sk_files = [f for f in files if sk_pattern.match(f)] # Finding the exact files in the folder we are looking for
                if sk_files:
                    print(f" SK files found: {sk_files[:3]}") 
                
                # Show summed files if any
                summed_pattern = re.compile(r'.*summed.*\.(img|fits|gz)$')
                summed_files = [f for f in files if summed_pattern.match(f)] # [f for f in files if ...]: Iterates through each item (f) in the files list.
                if summed_files:
                    print(f" Summed files found: {summed_files[:3]}") # This all just exists for debuging and can be removed!!!!
        
        band_files = {b: {'sum': None, 'sky': None} for b in bands_list}

        for root, _, filenames in os.walk(os.path.join(base_folder, folder)):  # It okay to use walk now since we took care of the double counting to find what we need.
            for f in filenames:
                f_low = f.lower()
                if not any(ext in f_low for ext in ['.img', '.fits', '.gz']): 
                    continue
                for band in bands_list:
                    if band in f_low:
                        if "summed" in f_low: 
                            band_files[band]['sum'] = os.path.join(root, f) # Directly making the path.
                            files_found_count += 1
                        elif "_sk" in f_low: 
                            band_files[band]['sky'] = os.path.join(root, f)
                            files_found_count += 1

        # Use Summed for the image/coords, but Sky for the ASPCORR status, this is very old logic That I am not chaning, this was because I had summed files and belived they would be more useful for the coords. Not really true but since its working I no touchy.
        for band, files in band_files.items():
            sum_f, sky_f = files['sum'], files['sky']
            if not sum_f and not sky_f: 
                continue
            
            target_file = sum_f if sum_f else sky_f  # Setting the target for what files should be processed that being the Summed files.
            status_source_file = sky_f if sky_f else sum_f # Setting the target for what files should be used for ASPCORR that being the sky files.

            ra, dec = _get_coords(target_file) # Get RA,DEC from the img, since we only got the center location before and still should proably need this.
            if ra is None: 
                coords_failed += 1
                continue 
            
            raw_results.append({
                "OBSID": obsid, "Band": band, "RA": ra, "Dec": dec,
                "Full_Path": target_file, 
                "Filename": os.path.basename(target_file),
                "ASPCORR": _scan_header_for_aspcorr(status_source_file)
            })

    print(f"\nDEBUG SUMMARY:")
    print(f"  Folders matching pattern: {matched_folders}")
    print(f"  SK/summed files found: {files_found_count}")
    print(f"  Files with failed coordinate extraction: {coords_failed}")
    print(f"  collected: {len(raw_results)} raw results") #All debuging things to check where it was failing. Fun fact I found it, I never inported astropy.wcs, i added dozens of debuging lines of code just to tell me something I already knew.... Im stupid, and Now im not removing them since I might need them again.

# Just a bit more cleanup
    df = pd.DataFrame(raw_results)
    if df.empty: 
        print("DEBUG: DataFrame is empty - no data found")
        return None, None, None
        
    df = df.drop_duplicates(subset=['OBSID', 'Band'], keep='first') # This is just to 100% make sure that the same file couldnt have been scanned twice by  chekcing for duplicants, the code kept thinking there was more extensions then there was somewhere and now im paranoid.

    # SPATIAL GROUPING 
     # We group by RA/Dec so that OBSIDs looking at the same spot are linked. This is what we where talking about before when it comes to finding nearby frames and sorting them.
    # This allows uncorrected files to use a 'DIRECT' neighbor as a reference, as long as it is within the parameters. (within 240 arcsec, I know this is 6arc min not 7 which is what we are going to check for stars and thats on purpose. I wanted extra wiggle room.).
    merged = df.copy()
    merged['Group_ID'] = -1
    group_cnt = 0
    for i in range(len(merged)):
        if merged.iloc[i]['Group_ID'] != -1: continue
        mask = (np.abs(merged['RA']-merged.iloc[i]['RA']) <= 240/3600) & \
               (np.abs(merged['Dec']-merged.iloc[i]['Dec']) <= 240/3600) & \
               (merged['Group_ID'] == -1)
        merged.loc[mask, 'Group_ID'] = group_cnt
        group_cnt += 1 #This code is makeing a box and find all ids that fit within that box and assiging them them a group id and then making a new one an repating until finished. 
        #Also something I learned from testing this, is some bands in certain groups are taken at differnt angels, which I didnt think would happen.

    
    def check_status(g):
        # A Reference can be a fully DIRECT file OR a READYRESUM file (since it has DIRECT parts).
        # A group needs work if there is a NONE or a READYRESUM present.
        has_ref = (g['ASPCORR'].isin(['DIRECT', 'READYRESUM'])).any()
        needs_work = (g['ASPCORR'].isin(['NONE', 'READYRESUM'])).any()
        # Defines the rest of the grouping types, completed is if the group only has_ref, ready is a mix of both and Orphan is only has_unc
        status = 'COMPLETED' if not needs_work else ('READY' if has_ref else 'ORPHAN')
        return pd.Series({'Status': status, 'Total_Frames': len(g)})
    # Use the auto rename function from above here.
    summary = merged.groupby(['Group_ID', 'Band']).apply(check_status, include_groups=False).reset_index()
    return merged, summary, save_dir


# --- MODES ---
def swift_automation_mode(base_path=None, save_path=None):
    all_frames, summary, _ = _run_core_engine(base_path, save_path)
    return all_frames, summary


    
#Orhan group sorting
# -------------------- ORPHAN HUNTING EXPANSION (Name patent pending) -----------------------------
def solve_orphan_frames_by_group(base_path=None, save_dir=None, return_data=False, input_df=None, input_summary=None):
    """
    It will
    1. Loads data using the existing swift_automation_mode.
    2. Identifies 'ORPHAN' frames (groups with no valid aspect correction).
    3. Identifies 'REFERENCE' frames (DIRECT or READYRESUM).
    4. For each Orphan, finds the nearest Reference in 4 directions (N, S, E, W).
    5. Saves a CSV for each Orphan with those 4 neighbors.
    """
    
    # Data Input Using existing Funtion from IAC to get the DataFrames directly
    # If you want to skip the input prompts(for automation), Input a base_path and save_dir.
    if input_df is not None and input_summary is not None:
        all_frames, summary_df = input_df, input_summary
        active_save_dir = save_dir if save_dir else os.getcwd()
    else:
        # Calls the core engine from IAC code
        all_frames, summary_df, active_save_dir = _run_core_engine(base_path, save_dir)

    if all_frames is None or summary_df is None:
        print(" No data returned from core engine. Cannot solve orphan frames.")
        return None

    # DEBUG, Print what we got, just to check
    print(f"\n Data loaded:")
    print(f"Total frames: {len(all_frames)}")
    print(f"Summary groups: {len(summary_df)}")
    print(f"Status breakdown:\n{summary_df['Status'].value_counts()}")

    # Identify Orphan Groups 
    # We find which (Group_ID, Band) combos are orphans from the summary report we get above.
    orphan_group_keys = summary_df[summary_df['Status'] == 'ORPHAN'][['Group_ID', 'Band']]
    
    print(f"\n Found {len(orphan_group_keys)} orphan group-band combinations")
    
    if orphan_group_keys.empty:
        print(" No orphan frames found - nothing to solve!")
        return {} if return_data else None
    
    # Pool of the valid reference frames (must have some usable data)
    reference_pool = all_frames[all_frames['ASPCORR'].isin(['DIRECT', 'READYRESUM'])].copy()
    
    print(f" Reference pool: {len(reference_pool)} frames available")
    
    automation_results = {} #Set up container for automation results
    
    # Create folder if saving CSVs, And set up folder if we are in manual/save mode
    if not return_data:
        orphan_save_path = os.path.join(active_save_dir, "Orphan_Solutions")
        if not os.path.exists(orphan_save_path): 
            os.makedirs(orphan_save_path)
        print(f" Saving CSVs to: {orphan_save_path}")

    # Process Every Frame in Every Orphan Group 
    count = 0
    for _, orphan_row in orphan_group_keys.iterrows():
        g_id = orphan_row['Group_ID']
        band = orphan_row['Band']

        # Get ALL individual frames belonging to this specific Orphan Group and Band
        target_frames = all_frames[(all_frames['Group_ID'] == g_id) & (all_frames['Band'] == band)]

        # Filter the reference pool for the matching band (um2 can only use um2, etc.)
        band_refs = reference_pool[reference_pool['Band'] == band].copy()
        
        if band_refs.empty: # If no valid references exist for this specific band, we skip
            print(f" No references for Group {g_id}, Band {band} - skipping")
            continue

        for _, frame in target_frames.iterrows(): #Grab some info we will need life the location both in space and on the computer.
            f_ra, f_dec, f_obsid = frame['RA'], frame['Dec'], frame['OBSID']
            f_path = frame['Full_Path']
            # Simple subtraction for distance math
            # dRA: Positive = East, Negative = West
            # dDec: Positive = North, Negative = South, Kinda weird part here if you check SAO it has east being positive even though logic would make it negative.
            band_refs['dRA'] = band_refs['RA'] - f_ra
            band_refs['dDec'] = band_refs['Dec'] - f_dec
            
            # Filter candidate neighbors by angular distance from the
            # orphan's pointing center. Neighbors more than
            # ORPHAN_MAX_NEIGHBOR_ARCMIN away are off-target observations
            # that just happen to share sky, including them in the synthetic
            # would give uvotunicorr a wrong-pointing reference and produce
            # a bad correction.
            ang_dist_deg = np.sqrt(band_refs['dRA']**2 + band_refs['dDec']**2)
            ang_dist_arcmin = ang_dist_deg * 60.0
            distance_mask = ang_dist_arcmin <= ORPHAN_MAX_NEIGHBOR_ARCMIN
            filtered_refs = band_refs[distance_mask]

            if len(filtered_refs) < 2:
                # Not enough same-pointing neighbors to build a useful
                # synthetic. Skip this orphan; it'll go to quarantine.
                continue

            neighbors = []
            
            # Find closest in 4 directions by finding the smallest absolute difference
            # East (+RA) Find frames where dRA is Positive, sort by smallest distance, The rest follow same logic
            east = band_refs[band_refs['dRA'] > 0].sort_values('dRA').head(1)
            # West (-RA) Find frames where dRA is Negative, sort by smallest absolute distance (largest value -> closer to 0)
            west = band_refs[band_refs['dRA'] < 0].sort_values('dRA', ascending=False).head(1)
            # North (+Dec)
            north = band_refs[band_refs['dDec'] > 0].sort_values('dDec').head(1)
            # South (-Dec)
            south = band_refs[band_refs['dDec'] < 0].sort_values('dDec', ascending=False).head(1)

            for n in [east, west, north, south]:
                if not n.empty:  # Safty measure if its not empty run it, Had to add this since one of the orphan frames doesnt have a neighbor in a direction.
                    neighbors.append(n)

            if neighbors:
                # Compile the 4 neighbors
                result_df = pd.concat(neighbors)[['OBSID', 'RA', 'Dec', 'Full_Path', 'Band', 'ASPCORR']]
                result_df.attrs['orphan_path'] = f_path # We store the orphan's own path as metadata in the result for Automation Mode
                
                unique_key = f"{f_obsid}_{band}"
                
                # Write CSV or return result
                if return_data:
                    automation_results[unique_key] = result_df
                else:
                    save_file = os.path.join(orphan_save_path, f"{unique_key}.csv")
                    result_df.to_csv(save_file, index=False)
                    print(f"✅ Created: {os.path.basename(save_file)}")
                
                count += 1

    print(f"\n Directives generated for {count} individual orphan frames.")
    
    if not return_data and count > 0:
        print(f" All CSVs saved to: {orphan_save_path}")
    
    return automation_results if return_data else None



###############################################################################
# ORPHAN RESCUE — BUILD SYNTHETIC REFERENCE PER GROUP, RUN UVOTUNICORR
###############################################################################

def build_synthetic_reference(group_orphans, base_path, save_path,
                              band, group_id, work_dir):
    """
    Build a synthetic reference image for one orphan group + band.

    Combines all 4-direction reference frames identified by
    solve_orphan_frames_by_group() (across all orphans in this group) into
    one fappended multi-extension file, then summed via uvotimsum.

    Returns the path to the synthetic ref image, or None on failure.
    """
    # Collect unique reference paths from the orphan solutions
    ref_paths = set()
    for solution_df in group_orphans:
        if solution_df is None or solution_df.empty:
            continue
        band_solutions = solution_df[solution_df['Band'] == band]
        for ref_path in band_solutions['Full_Path']:
            if not isinstance(ref_path, str):
                continue
            ref_paths.add(ref_path)

    if len(ref_paths) < 2:
        print(f" [Group {group_id} / {band}] Only {len(ref_paths)} reference "
              f"frame(s) — insufficient to build synthetic")
        return None

    ref_paths = sorted(ref_paths)  # deterministic ordering
    print(f"  [Group {group_id} / {band}] Building synthetic from "
          f"{len(ref_paths)} reference frames")

    # Copy references into a working directory
    synth_dir = os.path.join(work_dir, f"group_{group_id}_{band}")
    os.makedirs(synth_dir, exist_ok=True)

    copied_refs = []
    for i, ref_path in enumerate(ref_paths):
        if not os.path.exists(ref_path):
            continue
        # Copy and unzip if necessary
        local_name = f"ref_{i:03d}_{os.path.basename(ref_path)}"
        local_path = os.path.join(synth_dir, local_name)
        if ref_path.endswith(".gz"):
            # Copy then gunzip
            shutil.copy(ref_path, local_path)
            run_heasoft_command(f"gunzip -f '{prepare_path(local_path)}'")
            local_path = local_path[:-3]
        else:
            shutil.copy(ref_path, local_path)
        if os.path.exists(local_path):
            copied_refs.append(local_path)

    if len(copied_refs) < 2:
        print(f" [Group {group_id} / {band}] Could not copy enough references")
        return None

    # Use first reference as the master; fappend others into it
    master_path = os.path.join(synth_dir, f"master_{band}.img")
    shutil.copy(copied_refs[0], master_path)

    for ref in copied_refs[1:]:
        # fappend: appends all extensions of ref into master
        # bash command runs through run_heasoft_command which handles
        # cross-platform routing
        master_heasoft = prepare_path(master_path)
        ref_heasoft = prepare_path(ref)
        cmd = f"fappend '{ref_heasoft}' '{master_heasoft}' mode=h < /dev/null"
        run_heasoft_command(cmd)
        time.sleep(0.5)

    # Sum all extensions of master into synthetic
    synthetic_path = os.path.join(synth_dir, f"synthetic_{band}.fits")
    if os.path.exists(synthetic_path):
        try:
            os.remove(synthetic_path)
        except Exception:
            pass

    synth_dir_heasoft = prepare_path(synth_dir)
    sum_cmd = (
        f"cd '{synth_dir_heasoft}' && "
        f"uvotimsum infile='{os.path.basename(master_path)}' "
        f"outfile='{os.path.basename(synthetic_path)}' "
        f"exclude=NONE clobber=YES mode=h < /dev/null"
    )
    run_heasoft_command(sum_cmd)
    time.sleep(1)

    if not os.path.exists(synthetic_path):
        print(f" [Group {group_id} / {band}] uvotimsum failed to produce "
              f"synthetic")
        return None

    # Generate detect file for the synthetic (needed for star matching)
    synth_detect = os.path.join(synth_dir, f"synthetic_{band}_detect.fits")
    detect_cmd = (
        f"cd '{synth_dir_heasoft}' && "
        f"uvotdetect "
        f"infile='{os.path.basename(synthetic_path)}' "
        f"outfile='{os.path.basename(synth_detect)}' "
        f"expfile=NONE threshold=3 clobber=YES mode=h < /dev/null"
    )
    run_heasoft_command(detect_cmd)
    time.sleep(1)

    if not os.path.exists(synth_detect):
        print(f" [Group {group_id} / {band}] uvotdetect failed on synthetic")
        return None

    return synthetic_path


def rescue_orphan_frames(obs_table, base_path, save_path,
                         orphan_solutions=None, manual_mode=False):
    """
    Attempt to recover orphan frames by aspect-correcting them against
    synthetic reference images built from their N/E/S/W neighbors.

    Returns updated obs_table with Group_Status updated for any frames
    that were successfully corrected (ORPHAN -> READY, READYRESUM, or
    COMPLETED depending on per-extension status).


    This has been difficult, I have run many tests and have read up on
    how UVOTUNICORR actually works to discover my issue.
    UVOTUNICORR just chanes the WCS it changes what pixels=what position on the sky
    as this might not be right, what the code is trying to do is shift it to the
    true values that are confirmed.

    However the orphan code it its earlyier state was have a massive issue with this,
    Two bad test orphans, were screened and showed that they were in fact pointing 
    towards a different object, case 1 being SwiftJ005606, so uvotunicorr's did its job 
    to match the orphan's stars against the synthetic's stars 
    it succeeded at this mathematical task but the synthetic represents a different sky region, 
    so the "match" pulls the orphan's WCS toward SXP 5.05's sky position, 
    not toward SwiftJ005606's actual sky position. causing the source to "fall" off the region.
    As the SK file's WCS now says "this image is cnetered on SXP 5.05 regon" which is wrong
    so _corrected_detect.fits, genrated from this file reads correct pixel positions but
    assigns them wrong sky coordinates. That is a bit of an issue, one that isnt easy to solve
    even after you spend many a hour looking into what could be causing it, what I am now
    resorting to is just checks. two in particular, since I can exactly fix the WCS (in any way I know)
    we instead do the following

    Check 1: check the uvotunicorr correction shifct, typically seems to within 1-10", so at the top
    we have a variable set to 15" and if anything goes above that, revert the file from .gz and mark failed

    check 2: is a backup, if somehow 1 fails. After rescue, run uvotdetect on the corrected SK files. The
    Source Should now appear in the catalog at sky position close to our target RA-DEC. If no source is within
    10" the WCS is proably wrong, revert the file from .gz and mark failed.
    
    """
    print("\n" + "=" * 70)
    print("ORPHAN RESCUE — SYNTHETIC REFERENCE CORRECTION")
    print("=" * 70)

    if orphan_solutions is None or len(orphan_solutions) == 0:
        print("No orphan solutions available — nothing to rescue.")
        return obs_table

    # Group orphan solutions by (Group_ID, Band)
    # orphan_solutions is dict: {f"{obsid}_{band}": DataFrame of 4 neighbors}
    # We need to back out which group each orphan belongs to via obs_table

    work_dir = os.path.join(save_path, "_orphan_rescue_work")
    os.makedirs(work_dir, exist_ok=True)

    # Build a mapping: (group_id, band) -> list of orphan solution DataFrames
    group_band_orphans = {}      # {(group_id, band): [solution_df, ...]}
    group_band_orphan_meta = {}  # {(group_id, band): [(obsid, snapshot, full_path), ...]}

    for key, solution_df in orphan_solutions.items():
        if solution_df is None or solution_df.empty:
            continue
        # Parse key: "{obsid}_{band}"
        parts = key.rsplit('_', 1)
        if len(parts) != 2:
            continue
        obsid, band = parts

        # Find this orphan's group_id from obs_table
        mask = ((obs_table['ObsID'].astype(str) == obsid) &
                (obs_table['Filter'] == band))
        if not mask.any():
            continue
        group_id = obs_table.loc[mask, 'Group_ID'].iloc[0]

        bk = (group_id, band)
        if bk not in group_band_orphans:
            group_band_orphans[bk] = []
            group_band_orphan_meta[bk] = []
        group_band_orphans[bk].append(solution_df)

        # Find orphan-frame metadata: each orphan can have multiple extensions
        for _, row in obs_table.loc[mask].iterrows():
            if row['Extension_Status'] == 'NONE':
                group_band_orphan_meta[bk].append({
                    'obsid': str(row['ObsID']),
                    'snapshot': int(row['Snapshot']),
                    'full_path': row['Full_Path'],
                })

    if not group_band_orphans:
        print("No orphan groups identified for rescue.")
        return obs_table

    print(f"Found {len(group_band_orphans)} orphan group/band combinations "
          f"to attempt rescue on.")

    # For each (group, band), build a synthetic and try to correct
    total_rescued = 0
    total_attempted = 0

    for (group_id, band), solution_list in group_band_orphans.items():
        print(f"\n--- Group {group_id} / Band {band} ---")
        meta_list = group_band_orphan_meta[(group_id, band)]
        if not meta_list:
            print("  No orphan extensions to correct.")
            continue

        print(f" {len(meta_list)} orphan extension(s) to attempt")

        # Build synthetic reference
        synthetic_path = build_synthetic_reference(
            group_orphans=solution_list,
            base_path=base_path,
            save_path=save_path,
            band=band,
            group_id=group_id,
            work_dir=work_dir,
        )

        if synthetic_path is None:
            print(f" Could not build synthetic for Group {group_id} / {band}")
            continue

        synth_dir = os.path.dirname(synthetic_path)
        synth_detect = os.path.join(synth_dir, f"synthetic_{band}_detect.fits")

        # Now iterate through each orphan extension, run uvotunicorr against synthetic
        for orphan in meta_list:
            obs_obsid = orphan['obsid']
            obs_snapshot = orphan['snapshot']
            obs_full_path = orphan['full_path']
            obs_dir = os.path.dirname(obs_full_path)

            if not os.path.exists(obs_dir):
                continue

            obs_detect = os.path.join(obs_dir, f"{band}_detect_ext{obs_snapshot}.fits")
            if not os.path.exists(obs_detect):
                obs_detect = os.path.join(obs_dir, f"{band}_detect.fits")
            if not os.path.exists(obs_detect):
                print(f" [{obs_obsid} ext{obs_snapshot}] No detect file — skipping")
                continue

            print(f" [{obs_obsid} ext{obs_snapshot}] Attempting correction "
                  f"against synthetic...")
            total_attempted += 1

            # Try the retry ladder same as automated_aspect_correction
            success = False
            for attempt_num, (sb, ns) in enumerate(ASPECT_RETRY_LADDER):
                try:
                    ref_bright = find_brightest_central_stars(
                        synth_detect, num_stars=ns, side_buffer=sb
                    )
                    obs_bright = find_brightest_central_stars(
                        obs_detect, num_stars=ns, side_buffer=sb
                    )
                    ref_filt, obs_filt = remove_separate_stars(
                        ref_bright.copy(), obs_bright
                    )
                    if len(ref_filt) < 3:
                        continue

                    create_ref_obs_reg_files(ref_filt, obs_filt, outpath=obs_dir)

                    # Copy synthetic into obs dir
                    synth_local = os.path.join(obs_dir, os.path.basename(synthetic_path))
                    if not os.path.exists(synth_local):
                        shutil.copy(synthetic_path, synth_local)

                    # Find or unzip obs SK file
                    obs_base = f"sw{obs_obsid}{band}_sk"
                    obs_files_list = [f for f in os.listdir(obs_dir)
                                      if f.startswith(obs_base) and not f.endswith('.gz')]
                    if not obs_files_list:
                        # Need to unzip
                        gz_file = os.path.join(obs_dir, f"sw{obs_obsid}{band}_sk.img.gz")
                        if os.path.exists(gz_file):
                            run_heasoft_command(f"gunzip -k '{prepare_path(gz_file)}'")
                            obs_files_list = [f for f in os.listdir(obs_dir)
                                              if f.startswith(obs_base) and not f.endswith('.gz')]
                    if not obs_files_list:
                        continue

                    obs_img = obs_files_list[0]

                    # Run uvotunicorr — synthetic uses extension 1 only
                    obs_heasoft = prepare_path(obs_dir)
                    cmd = (
                        f"cd '{obs_heasoft}' && "
                        f"uvotunicorr "
                        f"obsfile='{obs_img}[{obs_snapshot}]' "
                        f"reffile='{os.path.basename(synthetic_path)}[1]' "
                        f"obsreg='obs.reg' "
                        f"refreg='ref.reg' mode=h < /dev/null"
                    )
                    run_heasoft_command(cmd)
                    time.sleep(3)

                    # Verify
                    corrected_files = sorted(
                        [f for f in os.listdir(obs_dir)
                         if f.startswith(obs_base) and not f.endswith('.gz')],
                        key=lambda x: (len(x), x)
                    )
                    if corrected_files:
                        with fits.open(os.path.join(obs_dir, corrected_files[0])) as hdul:
                            if obs_snapshot < len(hdul):
                                aspcorr = str(hdul[obs_snapshot].header.get(
                                    'ASPCORR', 'NONE')).strip().upper()
                                if aspcorr in ('DIRECT', 'UNICORR'):
                                    # Read corrected CRVAL and compare to original
                                    new_crval1 = float(hdul[obs_snapshot].header.get(
                                        'CRVAL1', 0.0))
                                    new_crval2 = float(hdul[obs_snapshot].header.get(
                                        'CRVAL2', 0.0))

                                    # Get original CRVAL from .gz (immutable)
                                    gz_path = os.path.join(obs_dir,
                                        f"sw{obs_obsid}{band}_sk.img.gz")
                                    original_crval1 = new_crval1
                                    original_crval2 = new_crval2
                                    if os.path.exists(gz_path):
                                        try:
                                            with fits.open(gz_path) as orig_hdul:
                                                if obs_snapshot < len(orig_hdul):
                                                    original_crval1 = float(
                                                        orig_hdul[obs_snapshot].header.get(
                                                            'CRVAL1', new_crval1))
                                                    original_crval2 = float(
                                                        orig_hdul[obs_snapshot].header.get(
                                                            'CRVAL2', new_crval2))
                                        except Exception:
                                            pass

                                    shift_arcsec = float(
                                        np.sqrt(
                                            (new_crval1 - original_crval1) ** 2 +
                                            (new_crval2 - original_crval2) ** 2
                                        ) * 3600
                                    )

                                    if shift_arcsec > ORPHAN_MAX_SHIFT_ARCSEC:
                                        # Suspicious correction — likely wrong-target
                                        # synthetic reference. Revert the file to
                                        # its original state and mark rescue as failed.
                                        print(f"      ⚠ REJECTED (attempt "
                                              f"{attempt_num+1}, params=({sb},{ns})) — "
                                              f"shift {shift_arcsec:.1f}\" exceeds "
                                              f"threshold {ORPHAN_MAX_SHIFT_ARCSEC}\"")
                                        # Restore the SK file from .gz
                                        if os.path.exists(gz_path):
                                            try:
                                                run_heasoft_command(
                                                    f"gunzip -kf '{prepare_path(gz_path)}'"
                                                )
                                            except Exception as e:
                                                print(f"      ⚠ Could not restore "
                                                      f".img from .gz: {e}")
                                        # Continue to next attempt in retry ladder
                                        # (or fall through if this was the last attempt)
                                        continue

                                    print(f"✅ Rescued (attempt "
                                          f"{attempt_num+1}, params=({sb},{ns})) — "
                                          f"shift {shift_arcsec:.1f}\"")
                                    success = True
                                    total_rescued += 1

                                    # Update obs_table for this row
                                    upd_mask = (
                                        (obs_table['ObsID'].astype(str) == obs_obsid) &
                                        (obs_table['Filter'] == band) &
                                        (obs_table['Snapshot'].astype(int) == obs_snapshot)
                                    )
                                    obs_table.loc[upd_mask, 'Extension_Status'] = 'DIRECT'
                                    obs_table.loc[upd_mask, 'AspCorr Flag'] = True
                                    break
                except Exception as e:
                    print(f" ❌ Error on attempt {attempt_num+1}: {e}")
                    continue

            if not success:
                print(f" ❌ Could not rescue [{obs_obsid} ext{obs_snapshot}] "
                      f"after {len(ASPECT_RETRY_LADDER)} attempts")

    # After all rescues, update Group_Status for groups that are no longer orphans
    print("\nUpdating Group_Status after rescue...")
    for group_id in obs_table['Group_ID'].unique():
        for band in obs_table[obs_table['Group_ID'] == group_id]['Filter'].unique():
            sub = obs_table[(obs_table['Group_ID'] == group_id) &
                            (obs_table['Filter'] == band)]
            has_direct = (sub['Extension_Status'].isin(['DIRECT', 'UNICORR'])).any()
            has_none = (sub['Extension_Status'] == 'NONE').any()
            if has_direct and has_none:
                new_status = 'READYRESUM'
            elif has_direct:
                new_status = 'COMPLETED'
            elif has_none:
                new_status = 'ORPHAN'
            else:
                continue
            obs_table.loc[((obs_table['Group_ID'] == group_id) &
                          (obs_table['Filter'] == band)), 'Group_Status'] = new_status

    # Save updated obs_table
    table_path = os.path.join(save_path, "observations_table.csv")
    obs_table.to_csv(table_path, index=False)

    # Clean up the work directory, (This can be removed if you wanna see the work for bug testing)
    try:
        shutil.rmtree(work_dir)
    except Exception:
        pass

    print(f"\nORPHAN RESCUE SUMMARY")
    print(f"  Attempted:   {total_attempted}")
    print(f"  Rescued:     {total_rescued}")
    print(f"  Failed:      {total_attempted - total_rescued}")
    print("=" * 70)

    return obs_table


######################################################################################
#Bellow is testing for populating table bads.
def populate_observations_table(base_path, all_frames_df, summary_df):
    """
    Populates observations table with one row per OBSID + Band + Extension.

    uses os.walk to find uvot/image directories and extracts
    OBSIDs from the FITS filenames themselves, so it works regardless of
    folder naming conventions.
    Works with ANY folder structure.... I think.
    """

    possible_bands = ['uvv', 'ubb', 'uuu', 'uw1', 'um2', 'uw2']

    # Regex to pull OBSID and band from UVOT sky image filenames
    # Matches: sw00033038054uw1_sk.img.gz  or  sw03111173093um2_sk.img
    sk_pattern = re.compile(r'^sw(\d{11})([a-z0-9]+)_sk\.img(\.gz)?$') # This is a handy bit of code

    # Initialize empty DataFrame
    obs_table = pd.DataFrame(columns=[
        'ObsID', 'Filter', 'Snapshot', 'Smeared Flag', 'SSS Flag',
        'Saturated Flag', 'AspCorr Flag',
        'Group_ID', 'Group_Status', 'Extension_Status', 'File_Status',
        'RA', 'Dec', 'Full_Path'
    ])

    counter = 0

    

    # Walk the ENTIRE tree looking for directories that end in uvot/image.
    # This is the key fix, don't assume any specific parent structure.
    for root, dirs, files in os.walk(base_path):
        normalised = os.path.normpath(root)

        # Only process directories that end with uvot/image
        if not normalised.endswith(os.path.join("uvot", "image")):
            continue

        # Skip anything inside a Smeared or NotASPCORR quarantine folder
        path_parts = normalised.split(os.sep)
        if "Smeared" in path_parts or "NotASPCORR" in path_parts:
            continue

        
        # Scan the files in this image directory for SK files.
        # We extract OBSID and band FROM THE FILENAME. this is always
        # reliable regardless of how the parent folders are named.
        # Collect unique (obsid, band) pairs and their file paths in this dir
        found_files = {} 

        for f in files:
            match = sk_pattern.match(f)
            if not match:
                continue
            obsid = match.group(1)
            band = match.group(2)

            if band not in possible_bands:
                continue

            # If both .img and .img.gz exist, prefer .img (uncompressed)
            key = (obsid, band)
            existing = found_files.get(key)
            if existing is None:
                found_files[key] = os.path.join(root, f)
            elif f.endswith('.img') and existing.endswith('.gz'):
                # Prefer uncompressed over compressed
                found_files[key] = os.path.join(root, f)


        
        # Process each (obsid, band) we found in this directory
        for (obsid, band), full_path in found_files.items():

            try:
                hdul = fits.open(full_path)
            except Exception as e:
                print(f"  Warning: Could not open {full_path}: {e}")
                continue

            num_snapshots = len(hdul) - 1  # Subtract 1 for primary HDU

            if num_snapshots < 1:
                hdul.close()
                continue

            # Get per-extension ASPCORR statuses
            extension_statuses = _scan_header_for_aspcorr_per_extension(full_path)

            # Get overall file status
            file_status = _scan_header_for_aspcorr(full_path)

            # Get RA/Dec and Group_ID from all_frames_df
            frame_info = all_frames_df[
                (all_frames_df['OBSID'] == obsid) &
                (all_frames_df['Band'] == band)
            ]

            if frame_info.empty:
                ra, dec, group_id = None, None, -1
            else:
                ra = frame_info.iloc[0]['RA']
                dec = frame_info.iloc[0]['Dec']
                group_id = frame_info.iloc[0]['Group_ID']

            # Get group status from summary
            group_status = "UNKNOWN"
            if group_id != -1:
                group_info = summary_df[
                    (summary_df['Group_ID'] == group_id) &
                    (summary_df['Band'] == band)
                ]
                if not group_info.empty:
                    group_status = group_info.iloc[0]['Status']

            # Process each extension (snapshot)
            for ext in range(1, num_snapshots + 1):
                # Get extension-specific status
                ext_status = extension_statuses[ext - 1] if (ext - 1) < len(extension_statuses) else 'NONE'

                # Determine AspCorr Flag (True if extension has DIRECT correction)
                aspcorr_flag = (ext_status == 'DIRECT')

                # Add row to table
                obs_table.loc[counter] = {
                    'ObsID': obsid,
                    'Filter': band,
                    'Snapshot': ext,
                    'Smeared Flag': False,  # Will be updated later
                    'SSS Flag': False,      # Placeholder
                    'Saturated Flag': False,  # Placeholder, recorded only, no behavior yet
                    'AspCorr Flag': aspcorr_flag,
                    'Group_ID': group_id,
                    'Group_Status': group_status,
                    'Extension_Status': ext_status,
                    'File_Status': file_status,
                    'RA': ra,
                    'Dec': dec,
                    'Full_Path': full_path,
                }

                counter += 1

            hdul.close()

    print(f'Found {counter} snapshots across all uvot/image directories.')
    return obs_table


def update_smeared_flags(obs_table, smeared_obs_folders,
                        smeared_extensions=None):
    """
    Update the obs_table 'Smeared Flag' column to reflect detected smearing.
      - smeared_obs_folders : list of folder paths where ALL extensions are
        smeared. All rows for those OBSIDs get flagged. These rows will
        be filtered out of summation and uvotsource by the smeared-obs
        skip logic AND by remove_smeared() which moves the folder out
        entirely.
      - smeared_extensions : list of dicts with 'obsid', 'band', 'extension'
        for individual bad extensions. Only that specific row gets flagged.
        The observation stays in place, and the summation logic uses the
        flag to exclude that extension from uvotimsum.
    """
    if not smeared_obs_folders and not smeared_extensions:
        return obs_table

    obsid_pattern = re.compile(r'(\d{11})')

    # 1. Whole moveing-smeared observations
    if smeared_obs_folders:
        print(f"\nFlagging {len(smeared_obs_folders)} wholesale-smeared "
              f"observations...")
        for smeared_folder in smeared_obs_folders:
            folder_name = (os.path.basename(smeared_folder)
                           if os.sep in str(smeared_folder)
                           else str(smeared_folder))
            match = obsid_pattern.search(folder_name)
            if match:
                obsid = match.group(1)
                mask = obs_table['ObsID'].astype(str) == obsid
                if mask.any():
                    obs_table.loc[mask, 'Smeared Flag'] = True

    # 2. Per-extension flags (only flag specific rows)
    if smeared_extensions:
        print(f"Flagging {len(smeared_extensions)} individual smeared "
              f"extensions...")
        for smeared in smeared_extensions:
            mask = ((obs_table['ObsID'].astype(str) == str(smeared['obsid'])) &
                    (obs_table['Filter'] == smeared['band']) &
                    (obs_table['Snapshot'].astype(int) == int(smeared['extension'])))
            if mask.any():
                obs_table.loc[mask, 'Smeared Flag'] = True

    return obs_table
    

###########################################################
def write_source_reg_files(base_path, target_ra, target_dec,
                           save_path=None,
                           source_radius=5.0, max_offset=10.0,
                           output_name="auto_source.reg",
                          obs_table=None, target=None, output_root=None):
    """
    Auto-generate a source region file per observation directory.

    Hello thomas, some changes had to be made, the normal ones being
    not hard coding it ad having it loop through the data.
    you had it pulling from the same detect.fits file for everything,
    I cant do that because I have summed files that need new detect.fits
    since the position on the star(especially after aspect correction)
    may be very different, so I use/ make a detect.fits for each.
    
    """
    source_coords = SkyCoord(target_ra, target_dec, unit='deg', frame='icrs')

    # Per-target tagging, when a target is given, the source region is
    # named auto_source_{target}.reg so multiple targets in one field don't
    # overwrite each other. target=None keeps the original untagged name.
    if target:
        output_name = obs_file_name(None, 'source_reg', target=target)

    created = 0
    used_corrected_detect = 0
    used_old_detect = 0
    skipped_no_source = 0
    skipped_no_detect = 0
    corrected_detects_run = 0

    print(f"Generating source regions for target RA={target_ra:.6f}, Dec={target_dec:.6f}")

    #
    # Run uvotdetect on summed images that don't have detect
    # files yet. we need this for the catalogs that match
    # the summed image coordinate system. I also just figured out
    # we also need to do this for anything that was aspect corrected.
    #
    
    # Track files that had to be quarantined due to detect failures
    detect_failures = []

    # Build the (obsid, image_dir) work list from the observations table
    # instead of walking the tree. Dedupe to unique dirs, verify on disk.
    def _table_image_dirs(obs_table_local, base_path_local, quarantine):
        dirs_out = []
        if (obs_table_local is not None and not obs_table_local.empty
                and 'Full_Path' in obs_table_local.columns):
            seen_local = set()
            for _, row in obs_table_local.iterrows():
                oid = str(row['ObsID'])
                fp = row['Full_Path']
                if not isinstance(fp, str) or not fp:
                    continue
                d = os.path.dirname(fp)
                if any(q in os.path.normpath(d).split(os.sep) for q in quarantine):
                    continue
                k = (oid, d)
                if k in seen_local:
                    continue
                if not os.path.isdir(d):
                    continue
                seen_local.add(k)
                dirs_out.append((oid, d))
        else:
            oid_re = re.compile(r"(\d{11})")
            for r, _, _ in os.walk(base_path_local):
                n = os.path.normpath(r)
                if not n.endswith(os.path.join("uvot", "image")):
                    continue
                if any(q in n.split(os.sep) for q in quarantine):
                    continue
                m = oid_re.search(r)
                dirs_out.append((m.group(1) if m else "?", r))
        return dirs_out

    work_dirs = _table_image_dirs(obs_table, base_path, QUARANTINE)

    def _detect_one_observation(obsid, root):
        """
        Run the corrected-detect pass for ONE observation (all bands).
        Bands sequential within, obsids run in parallel. Each obsid owns its
        own dir, so the DetectFailed/ move and detect-file writes can't
        collide across threads. At least in theory.
        """
        local_runs = 0
        local_failures = []
        lines = []
        def log(msg): lines.append(msg)

        current_files = os.listdir(root)

        for band in BANDS:
            # Find the file uvotsource will actually use
            summed_file = f"{band}_ex_summed.fits"
            sk_img = f"sw{obsid}{band}_sk.img"
            sk_gz = f"sw{obsid}{band}_sk.img.gz"

            input_file = None
            if summed_file in current_files:
                input_file = summed_file
            elif sk_img in current_files:
                input_file = sk_img
            elif sk_gz in current_files:
                input_file = sk_gz
            else:
                continue

            # The detect file for this input
            detect_file = f"{band}_corrected_detect.fits"
            detect_path = os.path.join(root, detect_file)

            # Skip if already created
            if os.path.exists(detect_path):
                continue

            # THE EXTENSION GUARD
            # A summed file is always single-extension and unambiguous.
            # A RAW SK file may have multiple image extensions on disk,
            # running uvotdetect on it raw(haha) hits "multiple image extensions"
            # (status 3). Worse, if EVERY extension is bad (NONE / SSS /
            # smeared) there is nothing worth detecting, that observation
            # is a real-genuine non-detection and should fall straight through to
            # the upper-limit pass (Or I guess lower Limit? Still not clear on it), 
            # not burn two detect retries and get shuffled to DetectFailed.
            #
            # So for a RAW multi-ext file we resolve good extensions and:
            #   - 0 good   → skip detection entirely (becomes an upper limit)
            #   - 1 good   → detect on that specific extension [ext]
            #   - ≥2 good  → shouldn't normally happen (summation would(should) have
            #                made a summed file), detect on first good ext
            # A single-extension raw file (1 image ext) is unambiguous -> as-is.
            detect_target = input_file  # what we pass to uvotdetect's infile=
            if input_file != summed_file:
                img_full = os.path.join(root, input_file)
                good_exts, bad_exts, total_exts = [], [], 0
                # SSS/Smeared flags for this obsid+band from obs_table
                flagged = set()
                if obs_table is not None:
                    for flagcol in ('SSS Flag', 'Smeared Flag'):
                        if flagcol in obs_table.columns:
                            mm = ((obs_table['ObsID'].astype(str) == str(obsid)) &
                                  (obs_table['Filter'] == band) &
                                  (obs_table[flagcol] == True))
                            flagged.update(
                                obs_table.loc[mm, 'Snapshot'].astype(int).tolist())
                try:
                    with fits.open(img_full) as hdul:
                        en = 0
                        for hdu in hdul:
                            if hdu.header.get('NAXIS', 0) < 2:
                                continue
                            en += 1
                            val = str(hdu.header.get('ASPCORR', 'NONE')).strip().upper()
                            if val in ('DIRECT', 'UNICORR') and en not in flagged:
                                good_exts.append(en)
                            else:
                                bad_exts.append(en)
                        total_exts = en
                        
                except Exception as e:
                    log(f"  [{obsid}/{band}] Could not read extensions ({e}) — "
                        f"skipping detection")
                    continue

                if total_exts > 1:
                    if len(good_exts) == 0:
                        # Genuine non-detection, nothing to detect on. Let
                        # the upper-limit pass handle it, do NOT move to
                        # DetectFailed, do NOT run uvotdetect. Do NOT pass GO.
                        # Do NOT collect 200$
                        log(f"  [{obsid}/{band}] No good extensions "
                            f"(all NONE/SSS/smeared) — skipping detection, "
                            f"will be handled as upper limit")
                        continue
                    # 1+ good extensions on a raw multi-ext file: target the
                    # first good one explicitly so uvotdetect isn't random.
                    detect_target = f"{input_file}[{good_exts[0]}]"
                    if len(good_exts) > 1:
                        log(f"  [{obsid}/{band}] raw multi-ext with "
                            f"{len(good_exts)} good extensions and no summed "
                            f"file — detecting on first good ext "
                            f"{good_exts[0]}")

            # END EXTENSION GUARD

            log(f"Running uvotdetect on {obsid}/{band} ({detect_target})...")

            if HEASOFT_BACKEND == "wsl":
                wsl_dir = prepare_path(root)
                cmd = (f"cd '{wsl_dir}' && "
                       f"uvotdetect infile='{detect_target}' "
                       f"outfile='{detect_file}' "
                       f"expfile=NONE threshold=3 clobber=YES mode=h < /dev/null")
            else:
                cmd = (f"cd '{root}' && "
                       f"uvotdetect infile='{detect_target}' "
                       f"outfile='{detect_file}' "
                       f"expfile=NONE threshold=3 clobber=YES mode=h < /dev/null")

            run_heasoft_command(cmd, quiet=True)
            time.sleep(2)

            # Retry once if it failed
            if not os.path.exists(detect_path):
                log(f"Retrying {obsid}/{band}...")
                time.sleep(3)
                run_heasoft_command(cmd, quiet=True)
                time.sleep(2)

            if os.path.exists(detect_path):
                local_runs += 1
            else:
                # Failed twice, move the input file to a subfolder
                # so it can't be used with a mismatched source region
                log(f"❌ uvotdetect failed twice for {obsid}/{band}")
                log(f"Moving {input_file} to DetectFailed/")

                failed_dir = os.path.join(root, "DetectFailed")
                os.makedirs(failed_dir, exist_ok=True)

                src_path = os.path.join(root, input_file)
                dst_path = os.path.join(failed_dir, input_file)

                try:
                    shutil.move(src_path, dst_path)
                    log(f"Moved: {input_file}")
                except Exception as e:
                    log(f"Error moving: {e}")

                local_failures.append({
                    'ObsID': obsid,
                    'Band': band,
                    'File': input_file,
                    'Directory': root,
                })

        return {'runs': local_runs, 'failures': local_failures, 'log': lines}

    # Thread the detect pass across observations (bands sequential within).
    print(f"  Running corrected-detect across up to {MAX_WORKERS} "
          f"observation(s) in parallel...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_detect_one_observation, obsid, root): obsid
                   for obsid, root in work_dirs}
        for fut in tqdm(as_completed(futures), total=len(futures), desc="Corrected detect", unit="obs"):
            oid = futures[fut]
            WATCHDOG.beat(f"Corrected detect: finished obsid {oid}")
            try:
                res = fut.result()
            except Exception as e:
                tqdm.write(f" ❌ detect worker crashed for {futures[fut]}: {e}")
                continue
            corrected_detects_run += res['runs']
            detect_failures.extend(res['failures'])
            if res['log']:
                tqdm.write("\n".join(res['log']))

    if corrected_detects_run > 0:
        print(f"  Created {corrected_detects_run} new detect files from corrected images")

    # Save detect failure report
    if detect_failures:
        print(f"\n  WARNING: {len(detect_failures)} files moved to DetectFailed/")
        fail_df = pd.DataFrame(detect_failures)
        fail_path = os.path.join(save_path, "detect_failures.csv")
        fail_df.to_csv(fail_path, index=False)
        print(f"Failure report saved: {fail_path}")

    # 
    # For each observation directory, find the best detect file
    # and centroid the source from it. First use {band}_corrected_detect.fits
    # if that isnt in the folder use the normal {band}_detect.fits / {band}_detect_ext1.fits
    #
    print("  Centroiding source positions...")

    for obsid, root in work_dirs:
        # Refresh file list (detect files may have just been created)
        current_files = os.listdir(root)

        best_detect = None
        is_corrected = False

        # First: look for corrected detect file (ALWAYS preferred)
        for f in current_files:
            if f.endswith("_corrected_detect.fits"):
                detect_path = os.path.join(root, f)
                try:
                    with fits.open(detect_path) as hdul:
                        if len(hdul) >= 2 and hdul[1].data is not None:
                            if len(hdul[1].data) > 0:
                                best_detect = detect_path
                                is_corrected = True
                                break
                except Exception:
                    continue

        # Second: only if no corrected detect, fall back to old detect
        if best_detect is None:
            best_count = 0
            for f in current_files:
                if f.endswith("_detect.fits") or f.endswith("_detect_ext1.fits"):
                    if "_corrected_detect" in f:
                        continue
                    detect_path = os.path.join(root, f)
                    try:
                        with fits.open(detect_path) as hdul:
                            if len(hdul) >= 2 and hdul[1].data is not None:
                                n = len(hdul[1].data)
                                if n > best_count:
                                    best_detect = detect_path
                                    best_count = n
                                    is_corrected = False
                    except Exception:
                        continue

        # No detect file at all — skip this directory
        if best_detect is None:
            print(f"[{obsid}] No detect file found — skipping")
            skipped_no_detect += 1
            continue

        # Open detect file and find closest source to target
        try:
            with fits.open(best_detect) as hdul:
                data = hdul[1].data

                detected_frame = pd.DataFrame(columns=['RA', 'DEC', 'SEP'])

                for ind, val in enumerate(data):
                    detected_frame.loc[ind, 'RA'] = val['RA']
                    detected_frame.loc[ind, 'DEC'] = val['DEC']

                if len(detected_frame.index) < 1:
                    print(f"    [{obsid}] Detect file empty — skipping")
                    skipped_no_detect += 1
                    continue

                # Calculate separation from target to each detected source
                # Thomas might recognize this part.
                for ind in detected_frame.index:
                    # Generate a SkyCoord object for each star
                    ra = detected_frame.loc[ind, 'RA']
                    dec = detected_frame.loc[ind, 'DEC']

                    star_coords = SkyCoord(ra, dec, unit='deg', frame='fk5')
                    # Calculate separation to source and append to dataframe
                    sep = star_coords.separation(source_coords).to(u.arcsecond)
                    detected_frame.loc[ind, 'SEP'] = sep

                # Look for star with min separation and grab coordinates
                min_sep = detected_frame['SEP'].idxmin()

                min_ra = detected_frame.loc[min_sep, 'RA']
                min_dec = detected_frame.loc[min_sep, 'DEC']
                min_dist = detected_frame.loc[min_sep, 'SEP']

                # Check to see how far away the nearest star is before
                # writing a region file. If too far, no region is created
                # and uvotsource will skip this observation.
                if min_dist <= (max_offset * u.arcsecond):
                    _wdir = _obsid_write_dir(output_root, root, obsid)
                    reg_path = os.path.join(_wdir, output_name)
                    reg_text = (
                        f'# Region file format: DS9 version 4.1\n'
                        f'fk5\n'
                        f'circle({min_ra},{min_dec},{source_radius}")\n'
                    )
                    with open(reg_path, 'w') as f:
                       f.write(reg_text)

                    created += 1
                    if is_corrected:
                        used_corrected_detect += 1
                    else:
                        used_old_detect += 1
                else:
                    print(f"[{obsid}] Nearest source {min_dist:.1f} away "
                          f"(>{max_offset}\") — no region written")
                    skipped_no_source += 1

        except Exception as e:
            print(f"[{obsid}] Error reading detect file: {e}")
            skipped_no_detect += 1
            continue

    print(f"\n  Source region summary:")
    print(f" Created: {created}")
    print(f" Used corrected detect centroid: {used_corrected_detect}")
    print(f" Used old detect centroid: {used_old_detect}")
    print(f" Skipped (no source within {max_offset}\"): {skipped_no_source}")
    print(f" Skipped (no detect file): {skipped_no_detect}")


###############################################################################
# SSS (SMALL-SCALE SENSITIVITY) PRE-SUMMATION CHECK
###############################################################################

def check_sss_before_summation(obs_table, base_path, save_path,
                               target_ra, target_dec,
                               source_radius=5.0, bkg_radius=8.0,
                               bkg_offset=30.0):
    """
    Pre-summation small-scale-sensitivity (SSS) check.

    For each MULTI-EXTENSION observation, runs uvotsource on each individual
    extension to determine whether the target source lands on a known bad
    pixel.  uvotsource reports AB_MAG=99 when this happens so these extensions
    must be excluded from uvotimsum or they will corrupt the summed image.

    Updates the 'SSS Flag' column in obs_table for any extension that
    returns AB_MAG=99.  Single-extension observations are not checked here;
    they fall through to the regular uvotsource pass where AB_MAG=99 will
    be caught at the final filter stage.

    Uses temporary source/background regions (target coords + offset) just
    to get a uvotsource run.  Centroid accuracy doesn't matter for the SSS
    detection — if the source lands on a bad pixel, uvotsource returns 99
    regardless of how perfectly the region is centered or if the bkg is bad. (I THINK)

    Parameters are as follows,
    obs_table : pd.DataFrame
        The observations table with one row per (OBSID, Band, Snapshot).
    base_path : str
        Root data directory.
    save_path : str
        Where to write sss_failures.csv diagnostic.
    target_ra, target_dec : float
        Source coordinates in decimal degrees.
    source_radius : float
        Temp source aperture, arcsec.  Default 5".
    bkg_radius : float
        Temp background aperture, arcsec.  Default 8".
    bkg_offset : float
        Offset from source where temp background is placed, arcsec.
        Default 30".

    Returns
    -------
    obs_table : pd.DataFrame
        Updated obs_table with 'SSS Flag' set to True for extensions
        whose uvotsource produced AB_MAG=99.
    """
    print("\n" + "=" * 70)
    print("PRE-SUMMATION SSS CHECK")
    print("=" * 70)
    print(f"Target: RA={target_ra:.6f}, Dec={target_dec:.6f}")

    # Find which OBSID+Band combos are multi-extension (>1 row in obs_table)
    multi_ext_groups = (
        obs_table.groupby(['ObsID', 'Filter']).filter(lambda g: len(g) > 1))
    if multi_ext_groups.empty:
        print("No multi-extension observations to check. Skipping SSS check.")
        return obs_table

    n_to_check = len(multi_ext_groups)
    print(f"Checking {n_to_check} multi-extension snapshots across "
          f"{multi_ext_groups[['ObsID', 'Filter']].drop_duplicates().shape[0]} "
          f"observations...")

    # Build temp source region content
    src_reg_text = (
        f'# Region file format: DS9 version 4.1\n'
        f'fk5\n'
        f'circle({target_ra},{target_dec},{source_radius}")\n'
    )
    # Temp background offset to the East by bkg_offset arcsec
    bkg_dec = target_dec
    bkg_ra = target_ra + (bkg_offset / 3600.0) / max(0.001, abs(np.cos(np.radians(target_dec))))
    bkg_reg_text = (
        f'# Region file format: DS9 version 4.1\n'
        f'fk5\n'
        f'circle({bkg_ra},{bkg_dec},{bkg_radius}")\n'
    )

    sss_failures = []

    def _sss_check_one_group(obsid, band, group):
        """
        Run the per-extension SSS uvotsource check for ONE (obsid, band)
        group. Extensions run sequentially within the group (they share
        temp region files in the same directory). Returns a dict of
        results the main thread folds back into obs_table, cores never
        write obs_table themselves.

        Also computes the saturation / coincidence-loss flag per extension
        from AB_FLUX_AA, AB_FLUX_AA_ERR, AB_FLUX_AA_COI_LIM (recorded only).
        """
        local = {
            'sss_hits': [],       # list of ext numbers flagged SSS (AB_MAG=99)
            'sat_hits': [],       # list of ext numbers flagged saturated (COI)
            'checked': 0,
            'errored': 0,
            'failures': [],       # rows for the diagnostic CSV
            'log': [],
        }

        img_dir = os.path.dirname(group['Full_Path'].iloc[0])
        if not os.path.exists(img_dir):
            return local

        sk_img = f"sw{obsid}{band}_sk.img"
        sk_gz = f"sw{obsid}{band}_sk.img.gz"
        if os.path.exists(os.path.join(img_dir, sk_img)):
            sk_filename = sk_img
        elif os.path.exists(os.path.join(img_dir, sk_gz)):
            sk_filename = sk_gz
        else:
            return local

        # Unique temp region names per group so concurrent groups in
        # DIFFERENT dirs never clash (same dir is fine, sequential here).
        temp_src = os.path.join(img_dir, f"_sss_src_{obsid}_{band}.reg")
        temp_bkg = os.path.join(img_dir, f"_sss_bkg_{obsid}_{band}.reg")
        with open(temp_src, 'w') as f:
            f.write(src_reg_text)
        with open(temp_bkg, 'w') as f:
            f.write(bkg_reg_text)

        for _, row in group.iterrows():
            ext = int(row['Snapshot'])
            if row['Extension_Status'] not in ('DIRECT', 'UNICORR'):
                continue

            temp_out = os.path.join(img_dir, f"_sss_check_{obsid}_{band}_ext{ext}.fits")
            if os.path.exists(temp_out):
                try:
                    os.remove(temp_out)
                except Exception:
                    pass

            if HEASOFT_BACKEND == "wsl":
                wsl_d = prepare_path(img_dir)
                cmd = (f"cd '{wsl_d}' && "
                       f"uvotsource image='{sk_filename}[{ext}]' "
                       f"srcreg='{os.path.basename(temp_src)}' "
                       f"bkgreg='{os.path.basename(temp_bkg)}' "
                       f"sigma=3 expfile=NONE "
                       f"zerofile=CALDB coinfile=CALDB psffile=CALDB lssfile=CALDB "
                       f"syserr=NO frametime=DEFAULT apercorr=NONE output=ALL "
                       f"outfile='{os.path.basename(temp_out)}' "
                       f"cleanup=YES clobber=YES chatter=0 mode=h < /dev/null")
            else:
                cmd = (f"cd '{img_dir}' && "
                       f"uvotsource image='{sk_filename}[{ext}]' "
                       f"srcreg='{os.path.basename(temp_src)}' "
                       f"bkgreg='{os.path.basename(temp_bkg)}' "
                       f"sigma=3 expfile=NONE "
                       f"zerofile=CALDB coinfile=CALDB psffile=CALDB lssfile=CALDB "
                       f"syserr=NO frametime=DEFAULT apercorr=NONE output=ALL "
                       f"outfile='{os.path.basename(temp_out)}' "
                       f"cleanup=YES clobber=YES chatter=0 mode=h < /dev/null")

            run_heasoft_command(cmd, quiet=True)
            local['checked'] += 1

            if not os.path.exists(temp_out):
                local['errored'] += 1
                continue

            try:
                with fits.open(temp_out) as hdul:
                    if len(hdul) >= 2 and hdul[1].data is not None and len(hdul[1].data) > 0:
                        data = hdul[1].data
                        ab_mag = float(data['AB_MAG'][0])

                        # SSS check
                        if ab_mag == 99.0 or not np.isfinite(ab_mag):
                            local['sss_hits'].append(ext)
                            local['failures'].append({
                                'ObsID': obsid, 'Band': band, 'Snapshot': ext,
                                'AB_MAG': ab_mag, 'Directory': img_dir,
                            })
                            local['log'].append(
                                f"  [{obsid} / {band} ext{ext}] SSS-flagged (AB_MAG=99)")

                        # Saturation / coincidence-loss check
                        # Bright sources suffer coincidence loss above
                        # AB_FLUX_AA_COI_LIM; flux+1sigma over that limit means
                        # the photometry is unreliable. We mark this
                        # I dont know what to do with it yet
                        try:
                            cols = data.columns.names
                            if all(c in cols for c in
                                   ('AB_FLUX_AA', 'AB_FLUX_AA_ERR', 'AB_FLUX_AA_COI_LIM')):
                                flux = float(data['AB_FLUX_AA'][0])
                                flux_err = float(data['AB_FLUX_AA_ERR'][0])
                                coi_lim = float(data['AB_FLUX_AA_COI_LIM'][0])
                                if (np.isfinite(flux) and np.isfinite(coi_lim)
                                        and (flux + flux_err) >= coi_lim):
                                    local['sat_hits'].append(ext)
                                    local['log'].append(
                                        f" [{obsid} / {band} ext{ext}] "
                                        f"Saturated/COI-flagged "
                                        f"(flux+err {flux+flux_err:.4g} ≥ "
                                        f"COI limit {coi_lim:.4g})")
                        except Exception:
                            pass  

            except Exception as e:
                local['log'].append(
                    f"  [{obsid} / {band} ext{ext}] Error reading result: {e}")
                local['errored'] += 1

            try:
                os.remove(temp_out)
            except Exception:
                pass

        for tmp in (temp_src, temp_bkg):
            try:
                os.remove(tmp)
            except Exception:
                pass

        return local

    # Parallel driver across (ObsID, Filter) groups
    sss_count = 0
    sat_count = 0
    checked = 0
    errored = 0

    group_items = list(multi_ext_groups.groupby(['ObsID', 'Filter']))
    print(f"Checking across up to {MAX_WORKERS} observation(s) in parallel...\n")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(_sss_check_one_group, obsid, band, group): (obsid, band)
            for (obsid, band), group in group_items
        }
        for fut in tqdm(as_completed(futures), total=len(futures), desc="SSS check", unit="obs"):
            obsid, band = futures[fut]
            oid = futures[fut]
            WATCHDOG.beat(f"SSS check: finished obsid {oid}")
            try:
                res = fut.result()
            except Exception as e:
                tqdm.write(f" ❌ SSS worker crashed for {obsid}/{band}: {e}")
                continue

            checked += res['checked']
            errored += res['errored']
            sss_failures.extend(res['failures'])

            # Fold flags back into obs_table HERE, in the main thread only.
            for ext in res['sss_hits']:
                mask = ((obs_table['ObsID'] == obsid) &
                        (obs_table['Filter'] == band) &
                        (obs_table['Snapshot'] == ext))
                obs_table.loc[mask, 'SSS Flag'] = True
                sss_count += 1
            for ext in res['sat_hits']:
                mask = ((obs_table['ObsID'] == obsid) &
                        (obs_table['Filter'] == band) &
                        (obs_table['Snapshot'] == ext))
                obs_table.loc[mask, 'Saturated Flag'] = True
                sat_count += 1

            if res['log']:
                tqdm.write("\n".join(res['log']))
                
    # Save diagnostic
    if sss_failures:
        sss_df = pd.DataFrame(sss_failures)
        sss_path = os.path.join(save_path, "sss_failures.csv")
        sss_df.to_csv(sss_path, index=False)
        print(f"\n  SSS failure report saved: {sss_path}")

    # Save updated obs_table
    table_path = os.path.join(save_path, "observations_table.csv")
    obs_table.to_csv(table_path, index=False)

    print(f"\nSSS check summary:")
    print(f"  Extensions checked: {checked}")
    print(f"  Flagged as SSS (AB_MAG=99): {sss_count}")
    print(f"  Flagged as Saturated/COI (recorded only): {sat_count}")
    print(f"  Errored during check: {errored}")
    print(f"  observations_table.csv updated with SSS + Saturated flags")
    print("=" * 70)

    return obs_table


    
# ----------------------------------------------------------------------------
 
 
def run_summation_shared(obs_table, base_path):
    """
    compute smeared obsids, build the image-dir
    work list, and sum multi-extension files. Runs ONCE per field
    regardless of how many targets share it, operates only on observation
    pixels and the shared table, never on a target position.
    """
 
    # Fully-smeared obsids are skipped whole, partial-smear handled by
    # per-extension exclude during summation.
    smeared_obsids = set()
    if obs_table is not None and 'Smeared Flag' in obs_table.columns:
        for obsid_val, grp in obs_table.groupby('ObsID'):
            if bool(grp['Smeared Flag'].all()):
                smeared_obsids.add(str(obsid_val))
        if smeared_obsids:
            print(f"Will skip {len(smeared_obsids)} fully-smeared OBSIDs "
                  f"(partial-smear obsids kept; smeared exts excluded)")
 
    # Build (obsid, image_dir) from the table.
    image_dirs = []
    if obs_table is not None and not obs_table.empty and 'Full_Path' in obs_table.columns:
        seen = set()
        for _, row in obs_table.iterrows():
            obsid = str(row['ObsID'])
            if obsid in smeared_obsids:
                continue
            full_path = row['Full_Path']
            if not isinstance(full_path, str) or not full_path:
                continue
            img_dir = os.path.dirname(full_path)
            parts = os.path.normpath(img_dir).split(os.sep)
            if any(qf in parts for qf in ("Smeared", "NotASPCORR", "Orphans")):
                continue
            key = (obsid, img_dir)
            if key in seen or not os.path.isdir(img_dir):
                continue
            seen.add(key)
            image_dirs.append((obsid, img_dir))
    else:
        obsid_pattern = re.compile(r"(\d{11})")
        for root_dir, dirs, files in os.walk(base_path):
            normalised = os.path.normpath(root_dir)
            if not normalised.endswith(os.path.join("uvot", "image")):
                continue
            match = obsid_pattern.search(root_dir)
            if not match:
                continue
            obsid = match.group(1)
            if obsid in smeared_obsids:
                continue
            if any(qf in root_dir.split(os.sep)
                   for qf in ("Smeared", "NotASPCORR", "Orphans")):
                continue
            image_dirs.append((obsid, root_dir))
 
    print(f"Found {len(image_dirs)} uvot/image directories to process")
    print(f"Unique OBSIDs: {len(set(o for o, _ in image_dirs))}\n")
 
    if not image_dirs:
        print("No observation directories found — check base_path.")
        return image_dirs
 
    print("=" * 70)
    print("SUMMING MULTI-EXTENSION FILES (uvotimsum) — ASPCORR-AWARE")
    print("=" * 70)
 
    summed_count = sum_skipped = sum_failed = 0
    sum_not_needed = sum_with_excludes = exp_summed_count = 0
 
    def _sum_exposure_map(obsid, band, img_dir, exclude_str=None, quiet=False):
        exp_summed_outfile = obs_file_name(band, 'summed_expmap')
        exp_summed_outpath = os.path.join(img_dir, exp_summed_outfile)
        if os.path.exists(exp_summed_outpath):
            return True
        ex_img = f"sw{obsid}{band}_ex.img"
        ex_gz = f"sw{obsid}{band}_ex.img.gz"
        ex_file = None
        if os.path.exists(os.path.join(img_dir, ex_img)):
            ex_file = ex_img
        elif os.path.exists(os.path.join(img_dir, ex_gz)):
            ex_file = ex_gz
        else:
            return False
        if HEASOFT_BACKEND == "wsl":
            wsl_d = prepare_path(img_dir)
            ecmd = (f"cd '{wsl_d}' && uvotimsum infile='{ex_file}' "
                    f"outfile='{exp_summed_outfile}' method=EXPMAP")
        else:
            ecmd = (f"cd '{img_dir}' && uvotimsum infile='{ex_file}' "
                    f"outfile='{exp_summed_outfile}' method=EXPMAP")
        if exclude_str:
            ecmd += f" exclude={exclude_str}"
        run_heasoft_command(ecmd, quiet=quiet)
        time.sleep(1)
        return os.path.exists(exp_summed_outpath)
 
    def _sum_one_observation(obsid, img_dir):
        counts = dict(summed=0, skipped=0, failed=0, not_needed=0,
                      with_excludes=0, exp_summed=0)
        lines = []
        def log(msg): lines.append(msg)
 
        for band in BANDS:
            summed_outfile = obs_file_name(band, 'summed_sk')
            summed_outpath = os.path.join(img_dir, summed_outfile)
            if os.path.exists(summed_outpath):
                counts['skipped'] += 1
                if _sum_exposure_map(obsid, band, img_dir, quiet=True):
                    counts['exp_summed'] += 1
                continue
 
            sk_img = f"sw{obsid}{band}_sk.img"
            sk_gz = f"sw{obsid}{band}_sk.img.gz"
            img_file = img_full_path = None
            if os.path.exists(os.path.join(img_dir, sk_img)):
                img_file = sk_img; img_full_path = os.path.join(img_dir, sk_img)
            elif os.path.exists(os.path.join(img_dir, sk_gz)):
                img_file = sk_gz; img_full_path = os.path.join(img_dir, sk_gz)
            else:
                continue
 
            try:
                # Single open: collect ASPCORR, FRAMTIME and EXPOSURE for every
                # image extension in one pass (the FRAMETIME check below reuses
                # this instead of re-opening the same file, like i was doing).
                ext_meta = {}
                with fits.open(img_full_path) as hdul:
                    ext_num = 0
                    for hdu in hdul:
                        if hdu.header.get('NAXIS', 0) < 2:
                            continue
                        ext_num += 1
                        ext_meta[ext_num] = {
                            'aspcorr': str(hdu.header.get('ASPCORR', 'NONE')).strip().upper(),
                            'ft': hdu.header.get('FRAMTIME', None),
                            'exp': float(hdu.header.get('EXPOSURE', 0.0)),
                        }
                good_exts, bad_exts = [], []
                for en, meta in ext_meta.items():
                    (good_exts if meta['aspcorr'] == 'DIRECT' else bad_exts).append(en)
                total_exts = len(good_exts) + len(bad_exts)
                if obs_table is not None:
                    flagged_exts = set()
                    for col in ('SSS Flag', 'Smeared Flag'):
                        if col in obs_table.columns:
                            m = ((obs_table['ObsID'].astype(str) == str(obsid)) &
                                 (obs_table['Filter'] == band) &
                                 (obs_table[col] == True))
                            flagged_exts.update(
                                obs_table.loc[m, 'Snapshot'].astype(int).tolist())
                    if flagged_exts:
                        moved = []
                        for ext in flagged_exts:
                            if ext in good_exts:
                                good_exts.remove(ext); bad_exts.append(ext)
                                moved.append(ext)
                        if moved:
                            log(f" [{obsid} / {band}] Quality-flagged moved to "
                                f"exclude: {sorted(moved)}")
            except Exception as e:
                log(f" [{obsid} / {band}] Error reading FITS: {e}")
                continue
 
            if len(good_exts) > 1:
                FRAMETIME_TOL = 0.0004
                try:
                    # Reuse ext_meta from the single open above — no re-read.
                    ft_by_ext, exp_by_ext = {}, {}
                    for en in good_exts:
                        ft = ext_meta[en]['ft']
                        if ft is not None:
                            ft_by_ext[en] = float(ft)
                            exp_by_ext[en] = float(ext_meta[en]['exp'])
                    if len(ft_by_ext) > 1:
                        ft_groups = []
                        for ext in sorted(ft_by_ext, key=lambda e: ft_by_ext[e]):
                            ft = ft_by_ext[ext]; placed = False
                            for g in ft_groups:
                                if abs(ft - g['ft']) <= FRAMETIME_TOL:
                                    g['exts'].append(ext)
                                    g['exp'] += exp_by_ext.get(ext, 0.0)
                                    placed = True; break
                            if not placed:
                                ft_groups.append({'ft': ft, 'exts': [ext],
                                                  'exp': exp_by_ext.get(ext, 0.0)})
                        if len(ft_groups) > 1:
                            ft_groups.sort(key=lambda g: (len(g['exts']), g['exp']),
                                           reverse=True)
                            keep = set(ft_groups[0]['exts'])
                            dropped = []
                            for ext in list(good_exts):
                                if ext not in keep:
                                    good_exts.remove(ext); bad_exts.append(ext)
                                    dropped.append(ext)
                            if dropped:
                                log(f"  [{obsid} / {band}] Mixed FRAMTIME: keeping "
                                    f"{ft_groups[0]['ft']:.5f}s "
                                    f"({len(keep)} ext), excluding {sorted(dropped)}")
                except Exception as e:
                    log(f" [{obsid} / {band}] FRAMTIME check failed ({e})")
 
            if total_exts == 0:
                continue
            if len(good_exts) == 0:
                log(f" [{obsid} / {band}] All {total_exts} extensions NONE — skipping")
                continue
            if total_exts == 1:
                counts['not_needed'] += 1
                if _sum_exposure_map(obsid, band, img_dir, quiet=True):
                    counts['exp_summed'] += 1
                continue
            if len(good_exts) == 1 and total_exts > 1:
                log(f"  [{obsid} / {band}] {total_exts} exts, 1 good "
                    f"(ext {good_exts[0]}) → summing, excluding {sorted(bad_exts)}")
 
            if bad_exts:
                exclude_str = ",".join(str(e) for e in bad_exts)
                log(f" [{obsid} / {band}] {total_exts} exts: {len(good_exts)} good, "
                    f"{len(bad_exts)} bad → exclude={exclude_str}")
            else:
                exclude_str = None
                log(f" [{obsid} / {band}] {total_exts} exts, all corrected → summing")
 
            if HEASOFT_BACKEND == "wsl":
                wsl_img_dir = prepare_path(img_dir)
                if exclude_str:
                    sum_cmd = (f"cd '{wsl_img_dir}' && uvotimsum infile='{img_file}' "
                               f"outfile='{summed_outfile}' exclude={exclude_str}")
                else:
                    sum_cmd = (f"cd '{wsl_img_dir}' && "
                               f"uvotimsum '{img_file}' '{summed_outfile}'")
            else:
                if exclude_str:
                    sum_cmd = (f"cd '{img_dir}' && uvotimsum infile='{img_file}' "
                               f"outfile='{summed_outfile}' exclude={exclude_str}")
                else:
                    sum_cmd = (f"cd '{img_dir}' && "
                               f"uvotimsum '{img_file}' '{summed_outfile}'")
 
            result = run_heasoft_command(sum_cmd, quiet=True)
            time.sleep(1)
 
            if os.path.exists(summed_outpath):
                if bad_exts:
                    log(f"✅ {summed_outfile} (excluded {exclude_str})")
                    counts['with_excludes'] += 1
                else:
                    log(f"✅ {summed_outfile}")
                counts['summed'] += 1
                if _sum_exposure_map(obsid, band, img_dir, exclude_str, quiet=True):
                    counts['exp_summed'] += 1
            else:
                log(f"❌ uvotimsum failed for {obsid}/{band}")
                counts['failed'] += 1
 
        return {'obsid': obsid, 'counts': counts, 'log': lines}
 
    print(f"Summing across up to {MAX_WORKERS} observation(s) in parallel...\n")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_sum_one_observation, obsid, img_dir): obsid
                   for obsid, img_dir in image_dirs}
        for fut in tqdm(as_completed(futures), total=len(futures), desc="Summing extensions", unit="obs"):
            oid = futures[fut]
            WATCHDOG.beat(f"summing extensions: finished obsid {oid}")
            try:
                res = fut.result()
            except Exception as e:
                tqdm.write(f" ❌ Summation worker crashed: {e}")
                continue
            c = res['counts']
            summed_count += c['summed']; sum_skipped += c['skipped']
            sum_failed += c['failed']; sum_not_needed += c['not_needed']
            sum_with_excludes += c['with_excludes']; exp_summed_count += c['exp_summed']
            if res['log']:
                tqdm.write("\n".join(res['log']))
 
    print(f"\nSummation results:")
    print(f" SK images created : {summed_count}")
    print(f" (with excludes)   : {sum_with_excludes}")
    print(f" Exp maps summed   : {exp_summed_count}")
    print(f" Already existed   : {sum_skipped}")
    print(f" Not needed        : {sum_not_needed}")
    print(f" Failed            : {sum_failed}\n")
 
    return image_dirs
 



# ============================================================================
# READ/WRITE SPLIT FOR dataInit MODE
# ============================================================================
# Adds an optional output_root to the per-target functions. When None,
# behavior is identical to the old (write_dir == read_dir == img_dir).
# When given (dataInit mode), shared products are READ from the dataInit
# img_dir, per-target products are WRITTEN to output_root/<obsid>/.
#
# The uvotsource/uvotdetect commands switch to ABSOLUTE paths only in split
# mode so read-dir and write-dir can differ; normal mode keeps the cd-relative
# form unchanged.
# ----------------------------------------------------------------------


def _obsid_write_dir(output_root, img_dir, obsid):
    """
    Resolve where per-target products for one obsid should be WRITTEN.

    output_root None  -> normal mode: write into the obsid's own img_dir.
                         
    output_root set   -> dataInit mode: write into output_root/<obsid>/,
                         created on demand! Hot and ready. Shared inputs are still READ
                         from img_dir (dataInit), only per-target products
                         land here.
    """
    if output_root is None:
        return img_dir
    d = os.path.join(output_root, str(obsid))
    os.makedirs(d, exist_ok=True)
    return d


def run_photometry_for_target(obs_table, base_path, save_path, image_dirs,
                              target_ra, target_dec, target=None,
                              source_reg=None, bkg_reg=None,
                              automation_mode=True, output_root=None,
                              persistent_bkg_path=None, run_allframes=None, run_timeavg=None, finder_fov=None):
    """
    PER-TARGET work: source region, background region, uvotsource photometry,
    upper limits, compilation + light curves.

    output_root : if None (normal mode), per-target products are written into
        each obsid's own image dir. If set (dataInit mode),
        shared products are READ from dataInit's image dirs while per-target
        products are WRITTEN to output_root/<obsid>/ and deliverables to
        save_path.
    persistent_bkg_path : if set, a per-target background region is reused from
        (or saved to) this path so the same background is applied across all
        of a target's observations and future runs. 
    """
    tlabel = f" [{target}]" if target else ""
    split_mode = output_root is not None

    # SOURCE REGION
    if source_reg is None:
        print("=" * 70)
        print(f"GENERATING SOURCE REGIONS{tlabel}")
        print("=" * 70)
        if target_ra is None or target_dec is None:
            raise ValueError("target_ra/target_dec required when source_reg is None.")
        src_reg_name = obs_file_name(None, 'source_reg', target=target)
        write_source_reg_files(base_path, target_ra, target_dec,
                               save_path=save_path, output_name=src_reg_name,
                               obs_table=obs_table, target=target,
                               output_root=output_root)
    else:
        src_reg_name = os.path.basename(source_reg)

    # BACKGROUND REGION
    # In dataInit mode the persistent per-target background is reused if it
    # already exists (consistency across runs). bkg_reg being passed in (a
    # path) means "reuse this one", bkg_reg None means "generate".
    if bkg_reg is None:
        bkg_result = generate_best_background(
            base_path, save_path, target_ra, target_dec,
            obs_table=obs_table, target=target, output_root=output_root,
            persistent_bkg_path=persistent_bkg_path)
        if bkg_result is None:
            print("Background generation failed — aborting target.")
            return None
        bkg_reg_name = obs_file_name(None, 'bkg_reg', target=target)
    else:
        bkg_reg_name = os.path.basename(bkg_reg)

    # UVOTSOURCE
    print("=" * 70)
    print(f"RUNNING UVOTSOURCE{tlabel}")
    print("=" * 70)
    processed = skipped = failed = 0

    def _uvotsource_one_observation(obsid, img_dir):
        counts = dict(processed=0, skipped=0, failed=0)
        lines = []
        def log(msg): lines.append(msg)
        write_dir = _obsid_write_dir(output_root, img_dir, obsid)
        for band in BANDS:
            finalsource_file = obs_file_name(band, 'finalsource', target=target)
            finalsource_path = os.path.join(write_dir, finalsource_file)
            if os.path.exists(finalsource_path):
                counts['skipped'] += 1
                continue
            summed_file = obs_file_name(band, 'summed_sk')
            sk_img = f"sw{obsid}{band}_sk.img"
            sk_gz = f"sw{obsid}{band}_sk.img.gz"
            # shared inputs READ from img_dir (dataInit)
            input_file = None
            if os.path.exists(os.path.join(img_dir, summed_file)):
                input_file = summed_file
            elif os.path.exists(os.path.join(img_dir, sk_img)):
                input_file = sk_img
            elif os.path.exists(os.path.join(img_dir, sk_gz)):
                input_file = sk_gz
            else:
                continue
            if input_file != summed_file:
                input_full_path = os.path.join(img_dir, input_file)
                try:
                    all_good = True; has_image_ext = False
                    with fits.open(input_full_path) as hdul:
                        for hdu in hdul:
                            if hdu.header.get('NAXIS', 0) < 2:
                                continue
                            has_image_ext = True
                            val = str(hdu.header.get('ASPCORR', 'NONE')).strip().upper()
                            if val not in ('DIRECT', 'UNICORR'):
                                all_good = False
                    if not has_image_ext or not all_good:
                        counts['skipped'] += 1
                        continue
                except Exception:
                    counts['skipped'] += 1
                    continue
            # regions: read from write_dir (dataSRC in split mode, else img_dir)
            src_reg_path = os.path.join(write_dir, src_reg_name)
            bkg_reg_path = os.path.join(write_dir, bkg_reg_name)
            if not os.path.exists(src_reg_path):
                counts['skipped'] += 1
                continue
            if not os.path.exists(bkg_reg_path):
                counts['skipped'] += 1
                continue

            exp_summed = obs_file_name(band, 'summed_expmap')
            exp_img = f"sw{obsid}{band}_ex.img"
            exp_gz = f"sw{obsid}{band}_ex.img.gz"
            exp_file = "NONE"
            if input_file == summed_file:
                if os.path.exists(os.path.join(img_dir, exp_summed)):
                    exp_file = exp_summed
            else:
                if os.path.exists(os.path.join(img_dir, exp_img)):
                    exp_file = exp_img
                elif os.path.exists(os.path.join(img_dir, exp_gz)):
                    exp_file = exp_gz

            if not split_mode:
                # NORMAL MODE 
                cd = prepare_path(img_dir) if HEASOFT_BACKEND == "wsl" else img_dir
                cmd = (f"cd '{cd}' && uvotsource image='{input_file}' "
                       f"srcreg='{src_reg_name}' bkgreg='{bkg_reg_name}' sigma=5 "
                       f"expfile='{exp_file}' zerofile=CALDB coinfile=CALDB "
                       f"psffile=CALDB lssfile=CALDB syserr=NO frametime=DEFAULT "
                       f"apercorr=NONE output=ALL outfile='{finalsource_file}' "
                       f"cleanup=YES clobber=YES chatter=1 mode=h < /dev/null")
            else:
                # SPLIT MODE 
                img_abs = prepare_path(os.path.join(img_dir, input_file))
                src_abs = prepare_path(src_reg_path)
                bkg_abs = prepare_path(bkg_reg_path)
                out_abs = prepare_path(finalsource_path)
                exp_abs = (prepare_path(os.path.join(img_dir, exp_file))
                           if exp_file != "NONE" else "NONE")
                cmd = (f"uvotsource image='{img_abs}' "
                       f"srcreg='{src_abs}' bkgreg='{bkg_abs}' sigma=5 "
                       f"expfile='{exp_abs}' zerofile=CALDB coinfile=CALDB "
                       f"psffile=CALDB lssfile=CALDB syserr=NO frametime=DEFAULT "
                       f"apercorr=NONE output=ALL outfile='{out_abs}' "
                       f"cleanup=YES clobber=YES chatter=1 mode=h < /dev/null")
            run_heasoft_command(cmd, quiet=True)
            time.sleep(1)
            if os.path.exists(finalsource_path):
                log(f"✅ {finalsource_file}")
                counts['processed'] += 1
            else:
                log(f"❌ uvotsource no output for {obsid}/{band}")
                counts['failed'] += 1
        return {'obsid': obsid, 'counts': counts, 'log': lines}

    print(f"Running uvotsource across up to {MAX_WORKERS} obs in parallel...\n")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_uvotsource_one_observation, obsid, img_dir): obsid
                   for obsid, img_dir in image_dirs}
        for fut in tqdm(as_completed(futures), total=len(futures), desc="Running uvotsource", unit="obs"):
            oid = futures[fut]
            WATCHDOG.beat(f"uvotsource: finished obsid {oid}")
            try:
                res = fut.result()
            except Exception as e:
                tqdm.write(f"❌ uvotsource worker crashed: {e}")
                continue
            c = res['counts']
            processed += c['processed']; skipped += c['skipped']; failed += c['failed']
            if res['log']:
                tqdm.write("\n".join(res['log']))

    print(f"\nUVOTSOURCE SUMMARY{tlabel}: processed {processed}, "
          f"skipped {skipped}, failed {failed}\n")

    # UPPER LIMITS
    if target_ra is not None and target_dec is not None:
        run_upper_limit_uvotsource(
            obs_table=obs_table, base_path=base_path, save_path=save_path,
            target_ra=target_ra, target_dec=target_dec, target=target,
            output_root=output_root)

    # COMPILATION + LIGHT CURVES (Mixed always; All-frames if enabled)
    if run_allframes is None:
        run_allframes = RUN_ALLFRAMES
    df_mixed = _compile_and_plot_mode(
        obs_table, image_dirs, save_path, target, output_root,
        mode='mixed', derive_flags=False)
    if run_allframes:
        try:
            run_allframes_for_target(
                obs_table=obs_table, base_path=base_path, save_path=save_path,
                image_dirs=image_dirs, src_reg_name=src_reg_name,
                bkg_reg_name=bkg_reg_name, target=target, output_root=output_root,
                target_ra=target_ra, target_dec=target_dec)
        except Exception as e:
            print(f"  All-frames pass failed{tlabel}: {str(e)[:200]}")
    if run_timeavg is None:
        run_timeavg = RUN_TIMEAVG
    if run_timeavg:
        try:
            run_timeavg_for_target(
                obs_table=obs_table, base_path=base_path, save_path=save_path,
                image_dirs=image_dirs, src_reg_name=src_reg_name,
                bkg_reg_name=bkg_reg_name, target=target, output_root=output_root,
                target_ra=target_ra, target_dec=target_dec)
        except Exception as e:
            print(f"  Time-averaged pass failed{tlabel}: {str(e)[:200]}")

    if MAKE_FINDER_IMAGES and target_ra is not None and target_dec is not None:
        try:
            make_finder_images_for_target(
                image_dirs=image_dirs, save_path=save_path,
                target_ra=target_ra, target_dec=target_dec, target=target,
                output_root=output_root,
                src_reg_name=src_reg_name, bkg_reg_name=bkg_reg_name, fov_arcmin=finder_fov)
        except Exception as e:
            print(f"  finder image failed{tlabel}: {str(e)[:160]}")
    return df_mixed
 


def run_uvotsource_pipeline(obs_table, base_path, save_path, source_reg=None,
                            bkg_reg=None, target_ra=None, target_dec=None,
                            automation_mode=True):
    """
    wrapper for shared summation + ONE untagged per-target Pass
    """
    image_dirs = run_summation_shared(obs_table, base_path)
    if not image_dirs:
        return None if automation_mode else None
    df_all = run_photometry_for_target(
        obs_table=obs_table, base_path=base_path, save_path=save_path,
        image_dirs=image_dirs, target_ra=target_ra, target_dec=target_dec,
        target=None, source_reg=source_reg, bkg_reg=bkg_reg,
        automation_mode=automation_mode, fov_arcmin=finder_fov)
    return df_all if automation_mode else None



## Currently not called when running, This is to run as a diagonstic check if you wanted too.
def diagnose_obs_table(obs_table):
    print("=" * 70)
    print("OBS_TABLE DIAGNOSTIC")
    print("=" * 70)

    print(f"\nTotal rows: {len(obs_table)}")

    # Extension_Status
    print(f"\n--- Extension_Status ---")
    print(f" dtype: {obs_table['Extension_Status'].dtype}")
    print(f" Unique values (repr): {[repr(v) for v in obs_table['Extension_Status'].unique()]}")
    print(f" Value counts:\n{obs_table['Extension_Status'].value_counts()}")

    nan_count = obs_table['Extension_Status'].isna().sum()
    if nan_count > 0:
        print(f" WARNING: {nan_count} NaN values!")

    # Smeared Flag
    print(f"\n--- Smeared Flag ---")
    print(f" dtype: {obs_table['Smeared Flag'].dtype}")
    print(f" Unique values: {obs_table['Smeared Flag'].unique()}")

    if obs_table['Smeared Flag'].dtype == object:
        print("  WARNING: Smeared Flag is string, not bool!")

    # ObsID type
    print(f"\n--- ObsID ---")
    print(f" dtype: {obs_table['ObsID'].dtype}")
    print(f" Sample: {obs_table['ObsID'].head(3).tolist()}")

    # Simulate filtering for first non-trivial group
    working = obs_table[obs_table['Smeared Flag'] == False].copy()
    print(f"\nAfter smeared filter: {len(working)} rows")

    for group_id in sorted(working['Group_ID'].unique())[:3]:
        gd = working[working['Group_ID'] == group_id]
        gs = gd['Group_Status'].iloc[0]
        if gs in ('ORPHAN', 'COMPLETED'):
            continue
        for band in gd['Filter'].unique():
            bd = gd[gd['Filter'] == band]
            none_count = len(bd[bd['Extension_Status'] == 'NONE'])
            none_filter = bd[bd['Extension_Status'] == 'NONE']
            print(f"\n  Group {group_id}/{band}: total={len(bd)}, "
                  f"NONE_counted={none_count}, NONE_filtered={len(none_filter)}")
            if none_count != len(none_filter):
                print("  *** MISMATCH ***")
            for v in bd['Extension_Status'].unique():
                print(f"    {repr(v)}: {len(bd[bd['Extension_Status'] == v])}")



######################################################
"""
auto_background_generator.py — Automatic Background Region Generation

Based on Kyles's find_sources() and find_valid_background() from
his background generation code. Changes from Kyles's version:
  - fitsio.read() replaced with fits.getdata() (fitsio won't install on Windows)
  - find_valid_background collects up to N candidates instead of returning 
    at the first valid position, however to relabibly do this, it checks all permutations....
    So it takes alittle while.
  - Added FOV checking against all observations' exposure maps
  - Added best-candidate selection and diagnostic CSV output
"""

import os
import re
import numpy as np
import pandas as pd

import sep

from astropy.io import fits
from astropy.wcs import WCS
from astropy.coordinates import SkyCoord
import astropy.units as u

from regions import CircleSkyRegion


###############################################################################
# SOURCE DETECTION — find_sources with fitsio replaced
###############################################################################

def find_sources(filename, threshold=1, logscale=True, shape="circle"):
    """
    Finds all excess UV sources above the background.
    
    This is Kyles's find_sources() with fitsio.read() replaced by
    fits.getdata(). All other logic is identical.

    the Parameters are:
    filename : Name of fits UV image file 
    threshold : float, Threshold sigma for source detection (default is 1.0)
    logscale : bool, Search for sources using logarithmic (default is True)
    shape : str, Return shape of sources with 'circle' or 'ellipse'

    It will try to Return
    excess : structured array, Sources in fk5 degrees. Circle: (RA, DEC, R). Ellipse: (RA, DEC, SMaj, SMin, Angl)
    excess_pxl : structured array, Sources in pixels: (X, Y, a, b, theta)
    """

    if not os.path.isfile(filename):
        raise ValueError(f'{filename} could not be found.')
    if threshold <= 0:
        raise ValueError('Threshold must be > 0.')

    # CHANGED: fitsio.read(filename) to fits.getdata()
    # Read the first image extension
    data = None
    hdu_idx = None
    with fits.open(filename) as hdul:
        for i, hdu in enumerate(hdul):
            if hdu.header.get('NAXIS', 0) >= 2:
                data = hdu.data.astype(np.float64)
                w = WCS(hdu.header)
                hdu_idx = i
                break

    if data is None:
        raise ValueError(f'No 2D image extension found in {filename}')

    # Log or linear
    if logscale:
        lindata = data
        with np.errstate(invalid='ignore', divide='ignore'):
            data = np.log10(np.array(data))
    else:
        lindata = data

    # Ensure C-contiguous (SEP requirement)
    data = np.ascontiguousarray(data)

    # Background estimation
    bkg = sep.Background(data)

    # subtract the background
    data_sub = data - bkg
    nonSUB = [d for d in np.array(data_sub).flatten() if not np.isneginf(d)]

    # total objects detected
    objects = sep.extract(data_sub, threshold, err=bkg.globalrms)

    # Allocate structured arrays
    n = len(objects)

    if shape == "circle":
        sky_dtype = np.dtype([
            ("RA", "f8"),
            ("DEC", "f8"),
            ("R", "f8")
        ])
    else:
        sky_dtype = np.dtype([
            ("RA", "f8"),
            ("DEC", "f8"),
            ("SMaj", "f8"),
            ("SMin", "f8"),
            ("Angl", "f8")
        ])

    pixel_dtype = np.dtype([
        ("X", "f8"),
        ("Y", "f8"),
        ("a", "f8"),
        ("b", "f8"),
        ("theta", "f8")
    ])

    excess = np.zeros(n, dtype=sky_dtype)
    excess_pxl = np.zeros(n, dtype=pixel_dtype)

    # Fill arrays
    for i, obj in enumerate(objects):

        x = float(obj["x"])
        y = float(obj["y"])

        c0 = w.pixel_to_world(x, y)
        ra = c0.fk5.ra.deg
        dec = c0.fk5.dec.deg

        a3 = float(3 * obj["a"])   # 3-sigma semi-major (pixels)
        b3 = float(3 * obj["b"])
        theta_deg = obj["theta"] * 180 / np.pi

        # Pixel array (store 1-sigma) 
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

    return excess, excess_pxl


###############################################################################
# The helper function (unchanged)
###############################################################################

def circle_intersects_circle(c1, r1, c2, r2):
    """Kyles's circle intersection check."""
    return c1.separation(c2) < (r1 + r2)

###############################################################################
# MODIFIED find_valid_background — collects N candidates
###############################################################################

def find_valid_background_candidates(excess, target_center,
                                     target_radius=10*u.arcsec,
                                     bck_radius=8*u.arcsec,
                                     step_size=1*u.arcsec,
                                     dist_limit=200*u.arcsec,
                                     max_iter=None,
                                     n_candidates=10,
                                     verbose=True):
    """
    Kyles's find_valid_background adapted to collect multiple candidates
    instead of returning at the first valid position.

    All search logic is identical to Kyles's version, radial spiral
    search using position angles from detected excess sources. The only
    change is collecting up to n_candidates instead of stopping at 1.

    The Parameters are:
    excess : structured array, Detected sources with (RA, DEC, R) columns in degrees.
    target_center : SkyCoord or array-like, Target sky coordinates.
    target_radius : Quantity, Radius of the target source region.
    bck_radius : Quantity, Radius of the background circle to place.
    step_size : Quantity, Radial increment for searching outward.
    dist_limit : Quantity, Maximum search distance from target.
    max_iter : int or None, Maximum number of theta steps.
    n_candidates : int, Number of candidate positions to collect.
    verbose : bool, Print progress information.

    Should Return:
    list of dict, Each dict has 'ra', 'dec', 'distance_arcsec', 'angle_deg'.
    """

    # Kyles's coordinate setup (unchanged)
    if isinstance(target_center, SkyCoord):
        c0 = target_center
        target_center_qty = np.array((c0.ra.deg, c0.dec.deg)) * u.deg
    else:
        if not isinstance(target_center[0], u.Quantity):
            target_center_qty = np.array(target_center) * u.deg
        else:
            target_center_qty = target_center
        c0 = SkyCoord(target_center_qty[0], target_center_qty[1], frame="fk5")

    if not isinstance(target_radius, u.Quantity):
        target_radius = np.array(target_radius) * u.arcsec
    if not isinstance(dist_limit, u.Quantity):
        dist_limit = np.array(dist_limit) * u.arcsec
    if not isinstance(bck_radius, u.Quantity):
        bck_radius = np.array(bck_radius) * u.arcsec
    if not isinstance(step_size, u.Quantity):
        step_size = np.array(step_size) * u.arcsec

    # Kyles's distance/angle arrays (unchanged) 
    n_steps = int(((dist_limit - (target_radius + bck_radius)) / step_size).decompose())
    dist_arr = np.linspace(
        (target_radius + bck_radius).to(u.arcsec),
        dist_limit,
        n_steps
    )

    np.random.shuffle(excess)

    ex_center = SkyCoord(excess["RA"], excess["DEC"], unit=u.deg, frame="fk5")
    sep_arr = c0.separation(ex_center).to(u.arcsec)
    angl_arr = c0.position_angle(ex_center).degree
    n_excess = len(sep_arr)
    if max_iter is None or max_iter > n_excess:
        max_iter = n_excess

    if n_excess < 5:
        angl_arr = np.linspace(0, 360, 20) * u.deg
    if n_excess < 3:
        sep_arr = np.linspace(
            (target_radius + bck_radius).to(u.arcsec),
            dist_limit / 2,
            len(sep_arr)
        ) * u.arcsec

    angl_arr = angl_arr[:int(max_iter)]

    # Kyles's circle intersection search (modified to collect N)
    cols = excess.dtype.names
    if "R" not in cols:
        if verbose:
            print("  WARNING: excess array doesn't have 'R' column. "
                  "Only circle shapes are supported in this version.")
        return []

    candidates = []

    if verbose:
        print(f"  Checking {len(dist_arr)*len(angl_arr)} permutations "
              f"against {n_excess} excess sources.")

    for i, dist in enumerate(dist_arr):
        if len(candidates) >= n_candidates:
            break

        for j, angl in enumerate(angl_arr):
            if len(candidates) >= n_candidates:
                break

            candidate = c0.directional_offset_by(angl, dist)
            intersects = False

            # Kyles's proximity check
            if n_excess > 4:
                c_sep = sep_arr[j]
                if c_sep > candidate.separation(c0):
                    intersects = True

            # Kyles's circle intersection check
            if not intersects:
                if circle_intersects_circle(
                    candidate, bck_radius,
                    ex_center, excess["R"] * u.degree
                ).any():
                    intersects = True

            if not intersects:
                ra = float(candidate.ra.deg)
                dec = float(candidate.dec.deg)

                candidates.append({
                    'ra': ra,
                    'dec': dec,
                    'distance_arcsec': float(dist.to(u.arcsec).value),
                    'angle_deg': float(angl if isinstance(angl, float)
                                       else angl.to(u.deg).value
                                       if isinstance(angl, u.Quantity)
                                       else float(angl)),
                })

                if verbose:
                    print(f"  Candidate #{len(candidates)}: "
                          f"RA={ra:.6f}, Dec={dec:.6f}, "
                          f"dist={dist.to(u.arcsec).value:.1f}\", "
                          f"angle={candidates[-1]['angle_deg']:.0f}°")

    return candidates


###############################################################################
# FOV CHECK USING EXPOSURE MAP
###############################################################################

def check_region_in_fov(image_path, exp_path, region_ra, region_dec, region_radius_arcsec):
    """
    Check whether a circular region falls entirely within the exposed
    area of a UVOT image by checking the exposure map.

    Pixels with zero exposure are outside the detector FOV (the black
    areas caused by the detector being rotated within the square image).

    Returns True if ALL pixels under the circle have non-zero exposure.
    """
    try:
        # Get WCS from the image
        with fits.open(image_path) as hdul:
            for hdu in hdul:
                if hdu.header.get('NAXIS', 0) >= 2:
                    w = WCS(hdu.header)
                    cdelt1 = hdu.header.get('CDELT1', None)
                    plate_scale = abs(cdelt1) * 3600 if cdelt1 else 0.502
                    break
            else:
                return False

        # Get exposure map data
        with fits.open(exp_path) as hdul:
            for hdu in hdul:
                if hdu.header.get('NAXIS', 0) >= 2:
                    exp_data = hdu.data
                    ny, nx = exp_data.shape
                    break
            else:
                return False

        # Convert region center to pixel coords
        cx, cy = w.all_world2pix(region_ra, region_dec, 0)
        cx = float(cx)
        cy = float(cy)

        # Convert radius to pixels
        r_pix = region_radius_arcsec / plate_scale

        # Bounding box
        x_min = max(0, int(cx - r_pix) - 1)
        x_max = min(nx - 1, int(cx + r_pix) + 1)
        y_min = max(0, int(cy - r_pix) - 1)
        y_max = min(ny - 1, int(cy + r_pix) + 1)

        if x_min >= nx or x_max < 0 or y_min >= ny or y_max < 0:
            return False

        # Check each pixel in the circle
        for y in range(y_min, y_max + 1):
            for x in range(x_min, x_max + 1):
                dist = ((x - cx) ** 2 + (y - cy) ** 2) ** 0.5
                if dist <= r_pix:
                    if exp_data[y, x] <= 0:
                        return False

        return True

    except Exception:
        return False


###############################################################################
# GENERATE BEST BACKGROUND ACROSS ALL OBSERVATIONS
###############################################################################

def generate_best_background(base_path, save_path, target_ra, target_dec, bkg_radius=8.0,
                             n_candidates=10, threshold=1.0, output_name="auto_bkg.reg", obs_table=None, 
                             target=None, output_root=None, persistent_bkg_path=None):
    """
    Generate the best background region that works across the most
    observations.

    Uses Kyles's find_sources for source detection and his
    find_valid_background search logic for candidate generation,
    then checks each candidate against all observation exposure maps.

    The Parameters are:
    base_path : str, Root data directory.
    save_path : str, Directory for diagnostic CSV output.
    target_ra, target_dec : float, Target coordinates in degrees.
    bkg_radius : float, Background circle radius in arcseconds (default 8").
    n_candidates : int, Number of candidate positions to evaluate.
    threshold : float, Source detection threshold in sigma.
    output_name : str, Region filename written into each observation directory.

    Should Return:
    dict or None, Best candidate with 'ra', 'dec', 'distance_arcsec',
    'angle_deg', 'n_valid', or None if no valid candidate found.
    """

    # Per-target tagging for the background region filename.
    if target:
        output_name = obs_file_name(None, 'bkg_reg', target=target)
    # dataInit: if a persistent per-target background already exists, reuse
    # it for consistency across runs, just copy it into each write_dir and
    # return its parameters.
    if persistent_bkg_path and os.path.exists(persistent_bkg_path):
        print(f"  Reusing persistent background: {persistent_bkg_path}")
        _reuse_persistent_background(
            persistent_bkg_path, output_name, output_root, obs_table,
            base_path)
        return {'ra': None, 'dec': None, 'reused': True}

    print("\n" + "=" * 70)
    print("GENERATING BACKGROUND REGIONS")
    print("=" * 70)
    print(f"Target: RA={target_ra:.6f}, Dec={target_dec:.6f}")
    print(f"Background radius: {bkg_radius}\"")
    print(f"Candidates to evaluate: {n_candidates}")

    ###################################################################
    # First things first: Find a representative image for source detection.
    # Prefer summed images (deeper, more sources detected).
    ###################################################################
    print("\n  Finding best-centered image for source detection...")

    best_image = None
    best_center_dist = float('inf')  # distance from target to frame center in pixels

    for root, dirs, files in os.walk(base_path):
        normalised = os.path.normpath(root)
        if not normalised.endswith(os.path.join("uvot", "image")):
            continue
        path_parts = normalised.split(os.sep)
        if any(q in path_parts for q in QUARANTINE):
            continue

        for f in files:
            # Check summed images and raw SK images
            if not (f.endswith("_ex_summed.fits") or "_sk.img" in f):
                continue

            fpath = os.path.join(root, f)

            try:
                with fits.open(fpath) as hdul:
                    for hdu in hdul:
                        if hdu.header.get('NAXIS', 0) >= 2:
                            w = WCS(hdu.header)
                            nx = hdu.header['NAXIS1']
                            ny = hdu.header['NAXIS2']

                            # Where does the target fall in this image?
                            tx, ty = w.all_world2pix(target_ra, target_dec, 0)

                            # Distance from target to center of frame
                            cx = nx / 2.0
                            cy = ny / 2.0
                            dist = ((float(tx) - cx) ** 2 + (float(ty) - cy) ** 2) ** 0.5

                            # Also check the target is actually ON the frame
                            if 0 < float(tx) < nx and 0 < float(ty) < ny:
                                if dist < best_center_dist:
                                    best_center_dist = dist
                                    best_image = fpath
                            break
            except Exception:
                continue

    if best_image is None:
        print("ERROR: No suitable image found for source detection.")
        return None

    print(f"Using: {os.path.basename(best_image)}")
    print(f"From: {os.path.dirname(best_image)}")
    print(f"Target is {best_center_dist:.0f} pixels from frame center")

    ###################################################################
    # STEP 2: Detect sources using find_sources
    ###################################################################
    print(f"\n  Detecting sources (threshold={threshold} sigma)...")

    try:
        excess, excess_pxl = find_sources(
            best_image, threshold=threshold,
            logscale=True, shape="circle"
        )
        print(f"Found {len(excess)} sources")
    except Exception as e:
        print(f"Source detection failed: {e}")
        print(f"Creating empty source list")
        excess = np.zeros(0, dtype=np.dtype([("RA", "f8"), ("DEC", "f8"), ("R", "f8")]))

    ###################################################################
    # STEP 3: Find candidate background positions using spiral search logic
    ###################################################################
    print(f"\n  Searching for {n_candidates} candidate background positions...")

    target_center = SkyCoord(target_ra, target_dec, unit='deg', frame='fk5')

    if len(excess) > 0:
        candidates = find_valid_background_candidates(
            excess, target_center,
            target_radius=10 * u.arcsec,
            bck_radius=bkg_radius * u.arcsec,
            step_size=1 * u.arcsec,
            dist_limit=200 * u.arcsec,
            n_candidates=n_candidates,
            verbose=True,
        )
    else:
        # No sources detected, generate geometric candidates
        print("No sources detected, generating geometric candidates...")
        candidates = []
        angles = np.linspace(0, 360, n_candidates, endpoint=False)
        for angle in angles:
            cand = target_center.directional_offset_by(
                angle * u.deg, 30 * u.arcsec
            )
            candidates.append({
                'ra': float(cand.ra.deg),
                'dec': float(cand.dec.deg),
                'distance_arcsec': 30.0,
                'angle_deg': float(angle),
            })

    if not candidates:
        print("ERROR: No valid background candidates found.")
        return None

    print(f"\n  Found {len(candidates)} candidates")

    ###################################################################
    # STEP 4: Collect all observation image/exposure map pairs
    ###################################################################
    print(f"\n  Collecting observation files for FOV checking...")

    obs_files = []

    # Directory list from the table instead of walking.
    _bg_dirs = []
    if obs_table is not None and not obs_table.empty and 'Full_Path' in obs_table.columns:
        _seen_bg = set()
        for _, _row in obs_table.iterrows():
            _oid = str(_row['ObsID'])
            _fp = _row['Full_Path']
            if not isinstance(_fp, str) or not _fp:
                continue
            _d = os.path.dirname(_fp)
            if any(q in os.path.normpath(_d).split(os.sep) for q in QUARANTINE):
                continue
            if (_oid, _d) in _seen_bg or not os.path.isdir(_d):
                continue
            _seen_bg.add((_oid, _d))
            _bg_dirs.append((_oid, _d))
    else:
        _oid_re = re.compile(r"(\d{11})")
        for root, dirs, files in os.walk(base_path):
            normalised = os.path.normpath(root)
            if not normalised.endswith(os.path.join("uvot", "image")):
                continue
            if any(q in normalised.split(os.sep) for q in QUARANTINE):
                continue
            _m = _oid_re.search(root)
            _bg_dirs.append((_m.group(1) if _m else "?", root))

    for obsid, root in _bg_dirs:
        current_files = os.listdir(root)

        for band in BANDS:
            # Find the image uvotsource will use
            summed_file = f"{band}_ex_summed.fits"
            sk_img = f"sw{obsid}{band}_sk.img"
            sk_gz = f"sw{obsid}{band}_sk.img.gz"

            image_file = None
            if summed_file in current_files:
                image_file = summed_file
            elif sk_img in current_files:
                image_file = sk_img
            elif sk_gz in current_files:
                image_file = sk_gz
            else:
                continue

            image_path = os.path.join(root, image_file)

            # Find the matching exposure map
            exp_summed = f"{band}_expmap_summed.fits"
            exp_img = f"sw{obsid}{band}_ex.img"
            exp_gz = f"sw{obsid}{band}_ex.img.gz"

            exp_file = None
            if image_file == summed_file:
                if exp_summed in current_files:
                    exp_file = exp_summed
            else:
                if exp_img in current_files:
                    exp_file = exp_img
                elif exp_gz in current_files:
                    exp_file = exp_gz

            if exp_file is None:
                continue

            exp_path = os.path.join(root, exp_file)
            obs_files.append((obsid, band, image_path, exp_path))

    print(f"  Found {len(obs_files)} observation/band combinations to check")

    for i, (obsid, band, image_path, exp_path) in enumerate(obs_files[:3]):
        print(f" [{i}] obsid={obsid} band={band}")
        print(f" image_path: {image_path}")
        print(f" exp_path: {exp_path}")
        print(f" image exists: {os.path.exists(image_path)}")
        print(f" exp exists: {os.path.exists(exp_path)}")

    if not obs_files:
        print("WARNING: No observation files with exposure maps found.")
        print("Using closest candidate without FOV validation.")
        best = candidates[0]
        best['n_valid'] = 0
        _write_background_regions_split(
            base_path, best, bkg_radius, output_name, QUARANTINE,
            obs_table=obs_table, output_root=output_root,
            persistent_bkg_path=persistent_bkg_path)
        return best

    ###################################################################
    # STEP 5: Check each candidate against all observations
    ###################################################################
    print(f"\n  Checking {len(candidates)} candidates against "
          f"{len(obs_files)} observations...")

    results = []

    for i, cand in enumerate(candidates):
        valid_count = 0
        per_obs_results = {}

        for obsid, band, image_path, exp_path in obs_files:
            key = f"{obsid}_{band}"
            in_fov = check_region_in_fov(
                image_path, exp_path,
                cand['ra'], cand['dec'],
                bkg_radius
            )
            per_obs_results[key] = in_fov
            if in_fov:
                valid_count += 1

        pct = 100 * valid_count / len(obs_files)
        print(f"    Candidate #{i+1}: {valid_count}/{len(obs_files)} "
              f"observations valid ({pct:.0f}%)")

        results.append({
            'candidate': i + 1,
            'ra': cand['ra'],
            'dec': cand['dec'],
            'distance': cand['distance_arcsec'],
            'angle': cand['angle_deg'],
            'n_valid': valid_count,
            'n_checked': len(obs_files),
            'per_obs': per_obs_results,
        })

    ###################################################################
    # STEP 6: Save diagnostic CSV
    ###################################################################
    print(f"\n  Saving diagnostic CSV...")

    obs_keys = sorted(set(f"{o}_{b}" for o, b, _, _ in obs_files))

    csv_rows = []
    for r in results:
        row = {
            'Candidate': r['candidate'],
            'RA': r['ra'],
            'Dec': r['dec'],
            'Distance_arcsec': r['distance'],
            'Angle_deg': r['angle'],
            'N_Valid': r['n_valid'],
            'N_Checked': r['n_checked'],
            'Valid_Pct': 100 * r['n_valid'] / r['n_checked']
                         if r['n_checked'] > 0 else 0,
        }
        for key in obs_keys:
            row[key] = r['per_obs'].get(key, False)
        csv_rows.append(row)

    csv_df = pd.DataFrame(csv_rows)
    csv_path = os.path.join(save_path, "background_candidate_check.csv")
    csv_df.to_csv(csv_path, index=False)
    print(f"Saved: {csv_path}")

    ###################################################################
    # STEP 7: Pick the best candidate Most valid observations first, then closest to source
    ###################################################################
    results.sort(key=lambda r: (-r['n_valid'], r['distance']))

    best = results[0]

    print(f"\n  BEST CANDIDATE: #{best['candidate']}")
    print(f"RA={best['ra']:.6f}, Dec={best['dec']:.6f}")
    print(f"Distance from source: {best['distance']:.1f}\"")
    print(f"Angle: {best['angle']:.0f}°")
    print(f"Valid for {best['n_valid']}/{best['n_checked']} observations "
          f"({100*best['n_valid']/best['n_checked']:.0f}%)")

    if best['n_valid'] < best['n_checked']:
        print(f"\n  WARNING: Background does not work for all observations.")
        print(f"{best['n_checked'] - best['n_valid']} observations will have "
              f"the background outside the FOV.")

    ###################################################################
    # STEP 8: Write background region to all observation directories
    ###################################################################
    best_cand = {
        'ra': best['ra'],
        'dec': best['dec'],
        'distance_arcsec': best['distance'],
        'angle_deg': best['angle'],
        'n_valid': best['n_valid'],
    }

    n_written = _write_background_regions_split(
        base_path, best_cand, bkg_radius, output_name, QUARANTINE,
        obs_table=obs_table, output_root=output_root,
        persistent_bkg_path=persistent_bkg_path)
    
    print(f"\n  Wrote {n_written} background region files ({output_name})")

    return best_cand


def _write_background_regions_split(base_path, candidate, bkg_radius,
                                    output_name, quarantine_folders,
                                    obs_table=None, output_root=None,
                                    persistent_bkg_path=None):
    """
    Write the chosen background region. In normal mode (output_root None) this
    behaves like the original _write_background_regions (one .reg per
    uvot/image dir). In dataInit mode it writes one .reg per per-target
    write_dir (output_root/<obsid>/) and also saves the persistent copy.
    """
    reg_text = (
        f'# Region file format: DS9 version 4.1\n'
        f'# Auto-generated background region\n'
        f'fk5\n'
        f'circle({candidate["ra"]},{candidate["dec"]},{bkg_radius}")\n'
    )
    count = 0
 
    if output_root is None:
        # NORMAL MODE, original behavior: walk and write into each img dir
        for root, dirs, files in os.walk(base_path):
            normalised = os.path.normpath(root)
            if not normalised.endswith(os.path.join("uvot", "image")):
                continue
            if any(q in normalised.split(os.sep) for q in quarantine_folders):
                continue
            with open(os.path.join(root, output_name), 'w') as f:
                f.write(reg_text)
            count += 1
        return count
 
    # SPLIT MODE, one .reg per per-target obsid write_dir
    if obs_table is not None and not obs_table.empty and 'Full_Path' in obs_table.columns:
        seen = set()
        for _, row in obs_table.iterrows():
            obsid = str(row['ObsID'])
            fp = row['Full_Path']
            if not isinstance(fp, str) or not fp:
                continue
            img_dir = os.path.dirname(fp)
            if obsid in seen or not os.path.isdir(img_dir):
                continue
            seen.add(obsid)
            wdir = _obsid_write_dir(output_root, img_dir, obsid)
            with open(os.path.join(wdir, output_name), 'w') as f:
                f.write(reg_text)
            count += 1
 
    # Save the persistent copy so future runs reuse this exact background
    if persistent_bkg_path:
        try:
            os.makedirs(os.path.dirname(persistent_bkg_path), exist_ok=True)
            with open(persistent_bkg_path, 'w') as f:
                f.write(reg_text)
        except Exception as e:
            print(f"  Could not save persistent background: {e}")
 
    return count
 
 
def _reuse_persistent_background(persistent_bkg_path, output_name,
                                 output_root, obs_table, base_path):
    """
    Copy an existing persistent per-target background into each obsid's
    write_dir so uvotsource finds it, without re-running the search.
    """
    try:
        with open(persistent_bkg_path) as f:
            reg_text = f.read()
    except Exception as e:
        print(f"  Could not read persistent background: {e}")
        return 0
    count = 0
    if obs_table is not None and not obs_table.empty and 'Full_Path' in obs_table.columns:
        seen = set()
        for _, row in obs_table.iterrows():
            obsid = str(row['ObsID'])
            fp = row['Full_Path']
            if not isinstance(fp, str) or not fp:
                continue
            img_dir = os.path.dirname(fp)
            if obsid in seen or not os.path.isdir(img_dir):
                continue
            seen.add(obsid)
            wdir = _obsid_write_dir(output_root, img_dir, obsid)
            with open(os.path.join(wdir, output_name), 'w') as f:
                f.write(reg_text)
            count += 1
    return count



def automated_aspect_correction(obs_table, base_path, save_path, side_buffer=None, num_stars=None, manual_mode=False):
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
       So you can track exactly in detail what has been changed, and why, I think the why is important on occasion.

       ADDED: MANUAL MODE
       Uses the provided side_buffer and num_stars for attempt 1, then
       prompts the user between attempts to adjust parameters
    """

    
    # retry sequence
    if manual_mode:
        if side_buffer is None:
            side_buffer = ASPECT_RETRY_LADDER[0][0]
        if num_stars is None:
            num_stars = ASPECT_RETRY_LADDER[0][1]
        retry_sequence = [(side_buffer, num_stars)]   # extended via prompts
    else:
        retry_sequence = list(ASPECT_RETRY_LADDER)
 
    failed_frames_to_retry = None
    attempt_num = 0
 
    # Carried out of the loop so the final summary can see the last attempt.
    aspectnone_dict = {}
    aspectnone_tiles_dict = {}
 
    # small local helpers
    def _find_detect(d, band, snap):
        """Prefer the per-extension detect catalog, fall back to the generic."""
        p = os.path.join(d, f"{band}_detect_ext{snap}.fits")
        if os.path.exists(p):
            return p
        p = os.path.join(d, f"{band}_detect.fits")
        if os.path.exists(p):
            return p
        return None
 
    ########################################################################
    # RETRY LOOP (sequential)
    ########################################################################
    for attempt_params in retry_sequence:
        current_side_buffer, current_num_stars = attempt_params
 
        print("\n" + "=" * 70)
        if attempt_num == 0:
            print("AUTOMATED ASPECT CORRECTION - INITIAL ATTEMPT")
        else:
            print(f"AUTOMATED ASPECT CORRECTION - RETRY ATTEMPT {attempt_num}")
        print("=" * 70)
        print(f"Parameters: side_buffer={current_side_buffer}, "
              f"num_stars={current_num_stars}")
        print(f"HEASOFT Backend: {HEASOFT_BACKEND}  |  workers: {MAX_WORKERS}")
 
        working_table = obs_table[obs_table['Smeared Flag'] == False].copy()
        if len(working_table) == 0:
            print("No frames to process in this attempt")
            break
 
        # Retry filter: on attempts > 0 only re-touch frames that failed last
        # time, but keep the FULL table available for reference selection.
        frames_to_correct = None
        if attempt_num > 0 and failed_frames_to_retry:
            n_failed = sum(len(v) for v in failed_frames_to_retry.values())
            print(f"RETRY MODE: re-attempting {n_failed} failed frame(s)")
            frames_to_correct = set()
            for _gb, obsid_list in failed_frames_to_retry.items():
                for obsid_ext in obsid_list:
                    parts = obsid_ext.split('_ext')
                    if len(parts) == 2:
                        frames_to_correct.add((str(parts[0]), int(parts[1])))
 
        # reset per-attempt failure trackers
        aspectnone_dict = {}
        aspectnone_tiles_dict = {}
 
        unique_groups = working_table['Group_ID'].unique()
        print(f"\nFound {len(unique_groups)} unique group(s) to scan")
 
        if attempt_num == 0:
            print("\nGroup Status Breakdown:")
            for status in ['COMPLETED', 'READY', 'ORPHAN', 'UNICORR']:
                count = len(working_table[
                    working_table['Group_Status'] == status]['Group_ID'].unique())
                print(f"  {status}: {count} groups")
            print()
 
        #####################################################################
        # BUILD PER-OBSID JOBS for this attempt
        #   obsid -> {
        #     'group_id': gid,
        #     'corrections': [ {band, snapshot, obs_dir}, ... ],
        #     'refs_by_band': { band: [ {obsid, snapshot, dir}, ... ] },
        #   }
        # A worker handles one obsid (all its NONE exts). The reference list per
        # band is the group+band DIRECT frames (read-only).
        #####################################################################
        obsid_jobs = {}
        skipped_no_ref = 0
 
        for group_id in unique_groups:
            group_data = working_table[working_table['Group_ID'] == group_id]
            group_status = group_data['Group_Status'].iloc[0]
 
            # Nothing to do for these, skip at build time. (This is also where
            # the old broken Orphans_Exist block used to be, gone now.)
            if group_status in ('ORPHAN', 'COMPLETED', 'UNICORR'):
                continue
 
            for band in group_data['Filter'].unique():
                band_data = group_data[group_data['Filter'] == band]
 
                ref_rows = band_data[band_data['Extension_Status'] == 'DIRECT']
                if ref_rows.empty:
                    # No reference for this band? these NONE frames can't be
                    # corrected, they'll be quarantined by Step 3.5. Match the
                    # sequential behavior. skip, don't record as failures.
                    n_none = int((band_data['Extension_Status'] == 'NONE').sum())
                    if n_none:
                        skipped_no_ref += n_none
                    continue
 
                ref_list = []
                for _, rc in ref_rows.iterrows():
                    rdir = os.path.dirname(rc['Full_Path'])
                    ref_list.append({
                        'obsid': str(rc['ObsID']),
                        'snapshot': int(rc['Snapshot']),
                        'dir': rdir,
                    })
 
                corrections_needed = band_data[
                    band_data['Extension_Status'] == 'NONE']
                if frames_to_correct is not None:
                    corrections_needed = corrections_needed[
                        corrections_needed.apply(
                            lambda r: (str(r['ObsID']), int(r['Snapshot']))
                            in frames_to_correct, axis=1)]
                if corrections_needed.empty:
                    continue
 
                for _, row in corrections_needed.iterrows():
                    oid = str(row['ObsID'])
                    entry = obsid_jobs.setdefault(oid, {
                        'group_id': group_id,
                        'corrections': [],
                        'refs_by_band': {},
                    })
                    entry['corrections'].append({
                        'band': band,
                        'snapshot': int(row['Snapshot']),
                        'obs_dir': os.path.dirname(row['Full_Path']),
                    })
                    entry['refs_by_band'].setdefault(band, ref_list)
 
        if skipped_no_ref:
            print(f" {skipped_no_ref} NONE frame(s) have no DIRECT reference "
                  f"in their group/band — will be quarantined by Step 3.5.")
 
        total_jobs = sum(len(j['corrections']) for j in obsid_jobs.values())
        print(f" {len(obsid_jobs)} obsid(s), {total_jobs} extension(s) "
              f"to correct this attempt.\n")
 
        # If there's genuinely nothing to correct, we're done.
        if not obsid_jobs:
            print("\n✅ No frames needing correction.")
            break
 
        #################################################################
        # WORKER — one obsid, all its NONE extensions (sequential within)
        # closes over current_side_buffer / current_num_stars.
        #################################################################
        def _correct_one_obsid(obs_obsid, job):
            gid = job['group_id']
            corrections = job['corrections']
            refs_by_band = job['refs_by_band']
 
            result = {
                'obsid': obs_obsid, 'group_id': gid,
                'successes': [],          # (band, snapshot) now corrected
                'failed_frames': [],      # (band, snapshot) still NONE
                'attempted': 0, 'successful': 0, 'failed': 0,
                'log': [],
            }
            log = result['log'].append
 
            # Per-worker caches so a reference shared by several of this obsid's
            # extensions is copied/gunzipped only once, and deleted once.
            local_ref_img = {}     # (ref_obsid, band) -> local .img path (in obs_dir)
            to_delete = []         # foreign-ref copies we made (delete at end)
            obs_gunzipped = set()  # bands whose obs SK we've already gunzipped
 
            def _ensure_obs_img(obs_dir, band):
                """Gunzip THIS obsid's own SK file in its own dir (keep .gz)."""
                if band in obs_gunzipped:
                    return os.path.join(obs_dir, f"sw{obs_obsid}{band}_sk.img")
                img = os.path.join(obs_dir, f"sw{obs_obsid}{band}_sk.img")
                gz = img + ".gz"
                if not os.path.exists(img) and os.path.exists(gz):
                    run_heasoft_command(f"gunzip -kf '{prepare_path(gz)}'", quiet=True)
                obs_gunzipped.add(band)
                return img if os.path.exists(img) else None
 
            def _ensure_ref_img(obs_dir, band, refs):
                """
                Make a usable reference .img available IN obs_dir for this band.
                Copy the reference's .gz here and gunzip locally. if
                only an .img exists upstream, copy that. Returns (ref_obsid,
                ref_snapshot, ref_local_img, ref_dir) or None.
                Same-obsid references (mixed frames) use the obs's own file and
                are never copied or deleted.
                """
                for ref in refs:
                    r_obsid = ref['obsid']
                    r_snap = ref['snapshot']
                    r_dir = ref['dir']
                    if not os.path.isdir(r_dir):
                        continue
 
                    # Already prepared this (ref_obsid, band) for this worker?
                    cached = local_ref_img.get((r_obsid, band))
                    if cached and os.path.exists(cached):
                        return r_obsid, r_snap, cached, r_dir
 
                    # Mixed/self reference: use the obs's own gunzipped file.
                    if r_obsid == obs_obsid:
                        own = _ensure_obs_img(obs_dir, band)
                        if own and os.path.exists(own):
                            local_ref_img[(r_obsid, band)] = own
                            return r_obsid, r_snap, own, r_dir
                        continue
 
                    gz_home = os.path.join(r_dir, f"sw{r_obsid}{band}_sk.img.gz")
                    img_home = os.path.join(r_dir, f"sw{r_obsid}{band}_sk.img")
                    local_img = os.path.join(obs_dir, f"sw{r_obsid}{band}_sk.img")
                    local_gz = local_img + ".gz"
 
                    try:
                        if os.path.exists(gz_home):
                            # Copy the .gz, gunzip locally (keep gz).
                            if not os.path.exists(local_gz):
                                shutil.copy(gz_home, local_gz)
                                to_delete.append(local_gz)
                            if not os.path.exists(local_img):
                                run_heasoft_command(
                                    f"gunzip -kf '{prepare_path(local_gz)}'",
                                    quiet=True)
                                to_delete.append(local_img)
                        elif os.path.exists(img_home):
                            # No .gz upstream, copy the .img, and then do it.
                            if not os.path.exists(local_img):
                                shutil.copy(img_home, local_img)
                                to_delete.append(local_img)
                        else:
                            continue
                    except Exception as e:
                        log(f"    [{obs_obsid}/{band}] ref copy failed "
                            f"({r_obsid}): {e}")
                        continue
 
                    if os.path.exists(local_img):
                        local_ref_img[(r_obsid, band)] = local_img
                        return r_obsid, r_snap, local_img, r_dir
 
                return None
 
            # process each NONE extension for this obsid
            for c in corrections:
                band = c['band']
                obs_snapshot = c['snapshot']
                obs_dir = c['obs_dir']
                if not os.path.isdir(obs_dir):
                    result['failed'] += 1
                    result['failed_frames'].append((band, obs_snapshot))
                    continue
 
                try:
                    refs = refs_by_band.get(band, [])
                    ref_info = _ensure_ref_img(obs_dir, band, refs)
                    if ref_info is None:
                        log(f"    [{obs_obsid}/{band} ext{obs_snapshot}] "
                            f"no usable reference — skipping")
                        result['failed'] += 1
                        result['failed_frames'].append((band, obs_snapshot))
                        continue
                    ref_obsid, ref_snapshot, ref_local_img, ref_dir = ref_info
 
                    # detect catalogs (read-only, never modified by aspcorr)
                    obs_detect = _find_detect(obs_dir, band, obs_snapshot)
                    ref_detect = _find_detect(ref_dir, band, ref_snapshot)
                    if obs_detect is None or ref_detect is None:
                        log(f" [{obs_obsid}/{band} ext{obs_snapshot}] "
                            f"missing detect file — skipping")
                        result['failed'] += 1
                        result['failed_frames'].append((band, obs_snapshot))
                        continue
 
                    # match stars
                    ref_bright = find_brightest_central_stars(
                        ref_detect, num_stars=current_num_stars,
                        side_buffer=current_side_buffer)
                    obs_bright = find_brightest_central_stars(
                        obs_detect, num_stars=current_num_stars,
                        side_buffer=current_side_buffer)
                    ref_filt, obs_filt = remove_separate_stars(
                        ref_bright.copy(), obs_bright)
                    if len(ref_filt) < 3:
                        log(f" [{obs_obsid}/{band} ext{obs_snapshot}] "
                            f"only {len(ref_filt)} matched star(s) — skipping")
                        result['failed'] += 1
                        result['failed_frames'].append((band, obs_snapshot))
                        continue
 
                    create_ref_obs_reg_files(ref_filt, obs_filt, outpath=obs_dir)
 
                    # ensure the obs's own SK is gunzipped in its dir
                    if _ensure_obs_img(obs_dir, band) is None:
                        log(f" [{obs_obsid}/{band} ext{obs_snapshot}] "
                            f"could not access obs image — skipping")
                        result['failed'] += 1
                        result['failed_frames'].append((band, obs_snapshot))
                        continue
 
                    # run uvotunicorr (both files now live in obs_dir)
                    cmd = create_uvotunicorr_command(
                        ref_frame=ref_obsid, obs_frame=obs_obsid, band=band,
                        ref_snapshot=ref_snapshot, obs_snapshot=obs_snapshot,
                        obspath=obs_dir)
                    gc.collect()
                    result['attempted'] += 1
                    run_heasoft_command(cmd, quiet=True)
                    time.sleep(2)   # let the in-place WCS write settle
 
                    # verify ASPCORR on the corrected extension
                    obs_img = os.path.join(obs_dir, f"sw{obs_obsid}{band}_sk.img")
                    ok = False
                    if os.path.exists(obs_img):
                        try:
                            with fits.open(obs_img) as hdul:
                                if obs_snapshot < len(hdul):
                                    asp = str(hdul[obs_snapshot].header.get(
                                        'ASPCORR', 'NONE')).strip().upper()
                                    if asp in ('DIRECT', 'UNICORR'):
                                        ok = True
                        except Exception as e:
                            log(f" [{obs_obsid}/{band} ext{obs_snapshot}] "
                                f"could not read corrected header: {e}")
 
                    if ok:
                        log(f" ✅ [{obs_obsid}/{band} ext{obs_snapshot}] "
                            f"corrected (ref {ref_obsid})")
                        result['successful'] += 1
                        result['successes'].append((band, obs_snapshot))
                    else:
                        log(f" ❌ [{obs_obsid}/{band} ext{obs_snapshot}] "
                            f"still NONE after uvotunicorr")
                        result['failed'] += 1
                        result['failed_frames'].append((band, obs_snapshot))
 
                except Exception as e:
                    log(f" ❌ [{obs_obsid}/{band} ext{obs_snapshot}] "
                        f"error: {str(e)[:160]}")
                    result['failed'] += 1
                    result['failed_frames'].append((band, obs_snapshot))
 
            # delete the foreign-reference copies this worker made 
            # Scoped strictly to copies in THIS obs_dir with a foreign obsid,
            # the originals in their home dirs and the obs's own files are never
            # touched (same-obsid references were never added to to_delete).
            for p in to_delete:
                try:
                    if os.path.exists(p):
                        os.remove(p)
                except OSError:
                    pass
 
            return result
 
        ################################################################
        # RUN THIS ATTEMPT IN PARALLEL, fold results in the main thread
        ################################################################
        attempt_successes = []   # (obsid, band, snapshot)
        att_attempted = att_successful = att_failed = 0
 
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(_correct_one_obsid, oid, job): oid
                       for oid, job in obsid_jobs.items()}
            for fut in tqdm(as_completed(futures), total=len(futures), desc="Aspect correction", unit="obs"):
                oid = futures[fut]
                WATCHDOG.beat(f"aspect correction: finished obsid {oid}")
                try:
                    res = fut.result()
                except Exception as e:
                    tqdm.write(f" ❌ aspect worker crashed for {oid}: {e}")
                    continue
 
                att_attempted += res['attempted']
                att_successful += res['successful']
                att_failed += res['failed']
 
                for (band, snap) in res['successes']:
                    attempt_successes.append((res['obsid'], band, snap))
 
                # bucket failures by group+band (worker is one obsid/one group)
                gid = res['group_id']
                for (band, snap) in res['failed_frames']:
                    key = f"{gid}_{band}"
                    aspectnone_dict[key] = aspectnone_dict.get(key, 0) + 1
                    aspectnone_tiles_dict.setdefault(key, []).append(
                        f"{res['obsid']}_ext{snap}")
 
                if res['log']:
                    tqdm.write("\n".join(res['log']))
 
        # Fold ASPCORR successes into obs_table (main thread only). Keeps the
        # table consistent so the downstream SSS check sees freshly-corrected
        # extensions as DIRECT instead of skipping them.
        for (oid, band, snap) in attempt_successes:
            m = ((obs_table['ObsID'].astype(str) == str(oid)) &
                 (obs_table['Filter'] == band) &
                 (obs_table['Snapshot'].astype(int) == int(snap)))
            obs_table.loc[m, 'Extension_Status'] = 'DIRECT'
            obs_table.loc[m, 'AspCorr Flag'] = True
 
        total_remaining = sum(aspectnone_dict.values())
 
        print("\n" + "=" * 70)
        if attempt_num == 0:
            print("INITIAL ATTEMPT COMPLETE")
        else:
            print(f"RETRY ATTEMPT {attempt_num} COMPLETE")
        print("=" * 70)
        print(f" Attempted:  {att_attempted}")
        print(f" Successful: {att_successful}")
        print(f" Failed:     {att_failed}")
        print(f" Frames still needing correction: {total_remaining}")
 
        if total_remaining == 0:
            print("\n✅ All frames successfully corrected!")
            break
 
        failed_frames_to_retry = {k: list(v) for k, v in aspectnone_tiles_dict.items()}
        attempt_num += 1
 
        if manual_mode:
            print(f"\n{total_remaining} frame(s) failed.")
            print("Failed by group/band:")
            for key, count in aspectnone_dict.items():
                print(f" {key}: {count}")
            retry = input("\nRetry with different parameters? (yes/no): "
                          ).strip().lower()
            if retry not in ('yes', 'y'):
                print("Stopping correction process.")
                break
            try:
                new_sb = input(f"New side_buffer (current {current_side_buffer}, "
                               f"Enter to keep): ").strip()
                new_ns = input(f"New num_stars (current {current_num_stars}, "
                               f"Enter to keep): ").strip()
                new_sb = int(new_sb) if new_sb else current_side_buffer
                new_ns = int(new_ns) if new_ns else current_num_stars
                retry_sequence.append((new_sb, new_ns))
            except ValueError:
                print("Invalid input — stopping.")
                break
        else:
            if attempt_num < len(retry_sequence):
                print(f"\n⚠️ {total_remaining} frame(s) failed at "
                      f"({current_side_buffer}\", {current_num_stars} stars). "
                      f"Advancing to ({retry_sequence[attempt_num][0]}\", "
                      f"{retry_sequence[attempt_num][1]} stars)...")
 
    #######################################################################
    # FINAL SUMMARY
    #######################################################################
    print("\n" + "=" * 70)
    print("ASPECT CORRECTION FINAL SUMMARY")
    print("=" * 70)
    final_remaining = sum(aspectnone_dict.values()) if aspectnone_dict else 0
    print(f"Total frames still needing correction: {final_remaining}")
    print(f"Attempts made: {attempt_num + 1}")
    if final_remaining > 0:
        print("\nFailed frames will be moved to NotASPCORR/ by Step 3.5")
        print("Failed frames by group/band:")
        for key, count in aspectnone_dict.items():
            print(f"  {key}: {count}")
 
    return aspectnone_dict, aspectnone_tiles_dict




#############################################################################################################
# Ensure pfiles directory exists
os.makedirs("/tmp/pfiles", exist_ok=True)

try:
    get_ipython()
    IN_NOTEBOOK = True
except NameError:
    IN_NOTEBOOK = False

# Parse args differently based on environment
if IN_NOTEBOOK:
    # In Jupyter, ignore sys.argv and use defaults
    class NotebookArgs:
        source_name = None
        source_ra = None
        source_dec = None
        verbose = False
        use_wsl = True  # Set to True if on Windows to skip autodetection.
        make_plots = False
    
    args = NotebookArgs()
else:
    # Command line - parse normally
    args = parser.parse_args()



class DownloadError(Exception):
    """Raise when requests status quo does not return 200."""
    pass


#insert download products code here

def setup_data_directories():
    """
    setup for data and save directories/also downloads said areas if asked, gives
    (data_directory, save_directory) or (None, None) if user cancels
    """
    global DATA_DIRECTORY, SAVE_DIRECTORY

    print("\n" + "=" * 70)
    print("SWIFT UVOT DATA SETUP")
    print("=" * 70)

    # Ask if they have existing data
    print("\nHow would you like to provide data?")
    print("  1. Yes - I have existing data already downloaded")
    print("  2. No - I want to download data for one new target")
    print("  3. BATCH - I have a CSV/TXT list of targets to download and process")
    print("  4. BATCH DOWNLOAD-ONLY - CSV list: download only, skip processing")
    print("  5. BATCH PROCESS-ONLY  - CSV list: skip download, process existing data")
    print("  6. dataInit POOL - CSV list: shared obsid pool, process each target against it (dataInit/dataSRC)")
    print()
    print(" [Batch input file format]")
    print(" The file must have a header row with these columns")
    print(" (case-insensitive; multiple alias names accepted):")
    print(" Target    (or: Name, Source, Source_Name, Object)")
    print(" RA        (or: RA_deg, RA_obj, Right_Ascension)   in degrees)")
    print(" Dec       (or: De, Dec_deg, De_obj, Declination)  in degrees)")
    print(" Radius    (or: Search_Radius, R)  in degrees [OPTIONAL])")
    print(" Threshold (or: Detect_Threshold, Sigma)   sigma [OPTIONAL])")
    print(f" If no Threshold column is given, {DEFAULT_DETECT_THRESHOLD} sigma is used.")
    print(f" If no Radius column is given, {DEFAULT_SEARCH_RADIUS} deg is used.")
    print(" 3' will taget only observation directly targeting your source, while above 3' adds nearby targets")
    print(" with 15' you will begin adding targets where the source is on the edge with 17' being the whole FOV of the instrument.")
    print(" 'allframes'      (or: all_frames, perframe, per_frame) Will enable run type 'allframes' if set true")
    print(" This will cause the code to run UVOTSOURCE on all exposures instead of just individual exposures and summed exposures")
    print(" 'timeavg  (or:'time_avg', 'timeaveraged', 'time_averaged', 'allsummed', 'all_summed') Will enable run type 'timeavg' if set true")
    print(" This will cause the code to run UVOTIMSUM on all exposures to combine them into once source and then run")
    print(" UVOTSOURCE on these newly made co-added observations instead of just individual exposures and summed exposures")
    print(" Note: neither of these will not stop it from running summed exposures it will be addition to what is called 'mixed processing'")
    print(" if neither is not set to true (or:  '1', 'yes', 't', 'y', 'on') it will be disabled by default")
    print(" finderfov  (or:'finder_fov', 'fov', 'fov_arcmin', 'finder_zoom') is the hard set fov of the .IMG made")
    print(" by default the pipleine will produce a .IMG of the observation and will if not set with finderfov be 2' or large enough to contain the background region.")
    print(" .csv = comma-separated, .txt = tab-separated. Auto-detected.")

    while True:
        choice = input("\nEnter your choice (1, 2, 3, 4, 5, or 6): ").strip()
        if choice in ['1', '2', '3', '4', '5', '6']:
            break
        print("Invalid choice.")

    # Handle batch modes by going into the batch runner
    if choice == '3':
        print("\n--- BATCH FULL MODE ---")
        return {'_batch_mode': True, '_batch_mode_type': 'full'}
    elif choice == '4':
        print("\n--- BATCH DOWNLOAD-ONLY MODE ---")
        print("This mode will only download data. Processing can be run later")
        print("using BATCH PROCESS-ONLY with the same CSV.")
        return {'_batch_mode': True, '_batch_mode_type': 'download'}
    elif choice == '5':
        print("\n--- BATCH PROCESS-ONLY MODE ---")
        print("This mode assumes data is already downloaded in subfolders")
        print("named exactly like the CSV's Target column. Pipeline will skip")
        print("download and run cleanup → photometry on each target.")
        return {'_batch_mode': True, '_batch_mode_type': 'process'}
    elif choice == '6':
        print("\n--- dataInit POOL MODE ---")
        print("All targets in your CSV draw from ONE shared obsid pool")
        print("(dataInit). Each obsid is processed once; per-target")
        print("results go to dataSRC/<target>/UVOT/.")
        return {'_datainit_mode': True}

    # Initialize Tkinter for file dialogs
    root = tk.Tk()
    root.withdraw()

    data_directory = None
    save_directory = None

    if choice == '1':
        #######################################################################
        # OPTION 1: Use existing data
        #######################################################################
        print("\n" + "-" * 70)
        print("SELECT EXISTING DATA DIRECTORY")
        print("-" * 70)
        print("Please select the folder containing your data.")

        data_directory = filedialog.askdirectory(
            title="Select Existing SWIFT Data Directory"
        )

        if not data_directory:
            print("No directory selected. Aborting.")
            return None

        print(f"Data Directory: {data_directory}")

        # Ask for save directory
        print("\n" + "-" * 70)
        print("SELECT SAVE DIRECTORY FOR RESULTS")
        print("-" * 70)
        print("Please select where you want to save analysis results...")

        save_directory = filedialog.askdirectory(
            title="Select Save Directory for Results"
        )

        if not save_directory:
            print("No save directory selected. Using data directory as default.")
            save_directory = data_directory

        print(f"Save Directory: {save_directory}")

    else:
        #######################################################################
        # OPTION 2: Download new data
        #######################################################################
        print("\n" + "-" * 70)
        print("DOWNLOAD NEW SWIFT DATA")
        print("-" * 70)
        print("\nEnter the coordinates and search radius:")

        while True:
            try:
                ra = float(input("Right Ascension (decimal degrees): ").strip())
                break
            except ValueError:
                print("Invalid input. Please enter a number.")

        while True:
            try:
                dec = float(input("Declination (decimal degrees): ").strip())
                break
            except ValueError:
                print("Invalid input. Please enter a number.")

        while True:
            try:
                radius = float(input("Search radius (degrees): ").strip())
                break
            except ValueError:
                print("Invalid input. Please enter a number.")

        # Query the database
        print(f"\nSearching for observations at RA={ra}, Dec={dec}, "
              f"Radius={radius}°...")
        query = ObsQuery(ra=str(ra), dec=str(dec), radius=radius)

        total_obs = len(query)

        if total_obs == 0:
            print("No observations found with these parameters.")
            return None

        print(f"\n{'=' * 70}")
        print(f"FOUND {total_obs} OBSERVATION(S)")
        print(f"{'=' * 70}")

        preview_count = min(10, total_obs)
        print(f"\nPreview of first {preview_count} observation(s):")
        for i, q in enumerate(list(query)[:preview_count], start=1):
            print(f"  {i}. ObsID: {q.obsid} | Date: {q.begin}")

        if total_obs > preview_count:
            print(f"  ... and {total_obs - preview_count} more")

        print(f"\nThis will download {total_obs} observation(s) with UVOT data.")
        confirm = input("Do you want to proceed with the download? (yes/no): ").strip().lower()

        if confirm not in ['yes', 'y']:
            print("Download cancelled.")
            return None

        # Select download directory
        print("\n" + "-" * 70)
        print("SELECT DOWNLOAD DIRECTORY")
        print("-" * 70)
        print("Please select where you want to download the SWIFT data...")

        data_directory = filedialog.askdirectory(
            title="Select Download Directory"
        )

        if not data_directory:
            print("No directory selected. Aborting.")
            return None

        print(f"Data will be downloaded to: {data_directory}")

        # Select save directory
        print("\n" + "-" * 70)
        print("SELECT SAVE DIRECTORY FOR RESULTS")
        print("-" * 70)
        print("Please select where you want to save analysis results...")

        save_directory = filedialog.askdirectory(
            title="Select Save Directory for Results"
        )

        if not save_directory:
            print("No save directory selected. Using download directory as default.")
            save_directory = data_directory

        print(f"Save Directory: {save_directory}")

        os.makedirs(data_directory, exist_ok=True)

        # Download the data
        print(f"\n{'=' * 70}")
        print("DOWNLOADING DATA")
        print(f"{'=' * 70}")

        jobs = [(q.obsid, os.path.join(data_directory, f"{q.obsid}")) for q in query]
        _download_obsids_parallel(jobs, desc="Downloading", reporter=print)

        print(f"\n{'=' * 70}")
        print("DOWNLOAD COMPLETE")
        print(f"{'=' * 70}")
        print(f"Total observations downloaded: {total_obs}")
        print(f"Data location: {data_directory}")

    #######################################################################
    # Collect target coordinates upfront so the pipeline can run
    # unattended without prompting again later.
    #######################################################################
    print("\n" + "-" * 70)
    print("TARGET COORDINATES")
    print("-" * 70)
    print("Enter the target source coordinates (decimal degrees).")
    print("These will be used for source and background region generation.\n")

    while True:
        try:
            target_ra = float(input("Target RA (decimal degrees): ").strip())
            break
        except ValueError:
            print("  Invalid — please enter a number.")

    while True:
        try:
            target_dec = float(input("Target Dec (decimal degrees): ").strip())
            break
        except ValueError:
            print("  Invalid — please enter a number.")

    print(f"\nTarget: RA={target_ra:.6f}, Dec={target_dec:.6f}")

    #######################################################################
    # Save to globals and return dict
    #######################################################################
    DATA_DIRECTORY = data_directory
    SAVE_DIRECTORY = save_directory

    return {
        'data_directory': data_directory,
        'save_directory': save_directory,
        'target_ra': target_ra,
        'target_dec': target_dec,
    }








    #insert subfolder idenfitication code here


#insert aspect correction parameter initialization here

master_table = pd.DataFrame(columns=['ObsID', 'Filter', 'Snapshot', 'Group Type', 'Group Num', 'Smeared Flag', 'SSS Flag', 'AspCorr Flag'])





###########################################################################################################################################
#Bellow is testing for clean_up_data cross compatiablity code 



def clean_up_data(automation_mode=False, base_path=None, save_path=None, detect_threshold=3.0, only_obsids=None):
    """  
        automation_mode : If True, skips GUI and print statements, returns data
        base_path : Required if automation_mode=True
        save_path : Required if automation_mode=True
        
        If automation_mode=True, then it will return:
            - 'all_frames': DataFrame from IAC
            - 'summary': Summary DataFrame from IAC  
            - 'orphan_solutions': Dict of orphan solutions
            - 'smeared_list': List of smeared observation folders
            - 'observations_table': Detailed per-extension table
    """

# SETUP: Get paths from arguments, globals, or config file
# If paths not provided, try to use global variables
    if base_path is None:
        if DATA_DIRECTORY is not None:
            base_path = DATA_DIRECTORY
            if not automation_mode:
                print(f"Using global DATA_DIRECTORY: {base_path}")
        else:
            # Try loading from config file
            config_data, config_save = load_paths_from_config()
            if config_data:
                base_path = config_data
                if not automation_mode:
                    print(f"Loaded DATA_DIRECTORY from config: {base_path}")
    
    if save_path is None:
        if SAVE_DIRECTORY is not None:
            save_path = SAVE_DIRECTORY
            if not automation_mode:
                print(f"Using global SAVE_DIRECTORY: {save_path}")
        else:
            # Try loading from config file
            if base_path:  # Only if we found data path
                config_data, config_save = load_paths_from_config()
                if config_save:
                    save_path = config_save
                    if not automation_mode:
                        print(f"Loaded SAVE_DIRECTORY from config: {save_path}")
    
    # check that we have the paths before it crashes.
    if automation_mode:
        if not base_path or not save_path:
            raise ValueError(
                "automation_mode needs base_path and save_path.\n"
                "Either give them as arguments, set global variables, or run setup_data_directories() first."
            )
    else:
        # If still no paths, inform user they need to run setup
        if not base_path or not save_path:
            print("\n" + "="*70)
            print("NO DATA DIRECTORIES FOUND")
            print("="*70)
            print("You need to set up your data directories first.")
            print("="*70)
            return
    
    # Display configuration
    if not automation_mode:
        print(f"{'='*70}")
        print(f"Data Directory: {base_path}")
        print(f"Save Directory: {save_path}")
        print(f"{'='*70}")
        
    
    # Initialize results dictionary for automation mode
    results = {
        'all_frames': None,
        'summary': None,
        'orphan_solutions': None,
        'smeared_list': None,
        'observations_table': None
    }

    # 0. BUILD OBSERVATIONS TABLE SKELETON (before anything else)
    if not automation_mode:
        print("\n=== Building observations table skeleton ===")
    obs_table = build_observations_skeleton(base_path, only_obsids=only_obsids)
    results['observations_table'] = obs_table

    # 1. RUN UVOT DETECT
    if not automation_mode:
        print("\n=== Running UVOT Detect ===")
    try:
        batch_run_uvotdetect(base_path, threshold=detect_threshold, obs_table=obs_table, only_obsids=only_obsids)
    except Exception as e:
        print(f" UVOTDETECT failed: {e}")
        if not automation_mode:
            import traceback
            traceback.print_exc()
    
    
    # 2. SMEAR DETECTION (per-extension)
    if not automation_mode:
        print("\n=== Detecting Smeared Frames ===")
    try:
        smeared_list, smeared_extensions = detect_smeared_frames(base_path, obs_table=obs_table, only_obsids=only_obsids)
        results['smeared_list'] = smeared_list
        results['smeared_extensions'] = smeared_extensions
    except Exception as e:
        print(f" Smear detection failed: {e}")
        results['smeared_list'] = []
        results['smeared_extensions'] = []
    
    # 3. RUN IAC
    if not automation_mode:
        print("\n=== Running IAC Swift Automation ===")
        
    all_frames, summary = swift_automation_mode(base_path=base_path, save_path=save_path)
    results['all_frames'] = all_frames
    results['summary'] = summary
    
    # ADDED: Save the summary CSV (swift_automation_mode doesn't do this)
    if summary is not None:
        summary_path = os.path.join(save_path, "workload_summary.csv")
        summary.to_csv(summary_path, index=False)
        if not automation_mode:
            print(f"✅ Workload summary saved to: {summary_path}")
    
    if all_frames is None or summary is None:
        if not automation_mode:
            print("⚠️ IAC automation failed to generate data.")
    else:
        if not automation_mode:
            print("\n=== Populating Observations Table ===")
            
        # 3.5. FILL GROUPING INTO THE SKELETON TABLE
        # The skeleton already exists (built at step 0) with paths + ASPCORR.
        # Now that the IAC engine has produced all_frames/summary, fill in
        # Group_ID / Group_Status / RA / Dec, then apply smear flags.
        try:
            obs_table = update_skeleton_with_grouping(
                obs_table, all_frames, summary)

            # Apply both whole-obs and per-extension smearing flags
            if results['smeared_list'] or results.get('smeared_extensions'):
                obs_table = update_smeared_flags(
                    obs_table,
                    results['smeared_list'],
                    results.get('smeared_extensions', []),
                )
            results['observations_table'] = obs_table

            # Save table to CSV
            table_path = os.path.join(save_path, "observations_table.csv")
            obs_table.to_csv(table_path, index=False)

            if not automation_mode:
                print(f" Observations table saved to: {table_path}")
                print(f" Total entries: {len(obs_table)}")

        except Exception as e:
            print(f" Failed to update observations table: {e}")
            import traceback
            traceback.print_exc()

        
        # 4. SOLVE ORPHAN FRAMES
        if not automation_mode:
            print("\n=== Solving Orphan Frames ===")
        
        # In automation mode, return the data instead of saving CSVs
        orphan_solutions = solve_orphan_frames_by_group(
            base_path=base_path, 
            save_dir=save_path, 
            return_data=automation_mode,  # Return data in automation mode
            input_df=all_frames,
            input_summary=summary
        )
        results['orphan_solutions'] = orphan_solutions
    
    # 5. REMOVE SMEARED FRAMES
    if smeared_list:
        if not automation_mode:
            print("\n=== Removing Smeared Frames ===")
        remove_smeared(base_path, smeared_list)
    else:
        if not automation_mode:
            print("\n=== No Smeared Frames to Remove ===")
    
    if not automation_mode:
        print("\n=== Clean Up Data Process Complete ===")
        print(f"Results saved to: {save_path}")
        return None
    else:
        # Return all data for downstream processing
        return results



######################################
def _run_quarantine(data_dir, obs_table):
    """
    Helper that does Step 3.5 quarantine work. This is needed for the batch
    as there is no quaratine work in the normal pipeline for it call, that was done local.
    Moves orphan and fully-NONE observations to subdirectories.
    """
    not_aspcorr_dir = os.path.join(data_dir, "NotASPCORR")
    orphans_dir = os.path.join(data_dir, "Orphans")
    os.makedirs(not_aspcorr_dir, exist_ok=True)
    os.makedirs(orphans_dir, exist_ok=True)
    QUARANTINE_FOLDERS = {"Smeared", "NotASPCORR", "Orphans"}
    obsid_pattern = re.compile(r"(\d{11})")

    orphan_obsids = set()
    if obs_table is not None:
        orphan_mask = pd.Series(False, index=obs_table.index)
        if 'Group_Status' in obs_table.columns:
            orphan_mask |= (obs_table['Group_Status'] == 'ORPHAN')
            orphan_mask |= (obs_table['Group_Status'] == 'UNKNOWN')
        if 'Group_ID' in obs_table.columns:
            orphan_mask |= (obs_table['Group_ID'] == -1)
        orphan_obsids = set(obs_table.loc[orphan_mask, 'ObsID'].astype(str).unique())

    top_folders = [f for f in os.listdir(data_dir)
                   if os.path.isdir(os.path.join(data_dir, f))
                   and f not in QUARANTINE_FOLDERS]

    for folder in top_folders:
        m = obsid_pattern.search(folder)
        if not m:
            continue
        obsid = m.group(1)
        if obsid in orphan_obsids:
            folder_path = os.path.join(data_dir, folder)
            dest = os.path.join(orphans_dir, folder)
            if not os.path.exists(dest):
                try:
                    shutil.move(folder_path, dest)
                except Exception:
                    pass

    # Move fully-uncorrected
    top_folders = [f for f in os.listdir(data_dir)
                   if os.path.isdir(os.path.join(data_dir, f))
                   and f not in QUARANTINE_FOLDERS]
    for folder in top_folders:
        m = obsid_pattern.search(folder)
        if not m:
            continue
        folder_path = os.path.join(data_dir, folder)
        has_any_correction = False
        found_any_sk = False
        for root_d, _, fnames in os.walk(folder_path):
            for fname in fnames:
                if "_sk.img" not in fname or not any(b in fname for b in BANDS):
                    continue
                found_any_sk = True
                try:
                    with fits.open(os.path.join(root_d, fname)) as hdul:
                        for hdu in hdul:
                            if hdu.header.get('NAXIS', 0) < 2:
                                continue
                            v = str(hdu.header.get("ASPCORR", "NONE")).strip().upper()
                            if v in ("DIRECT", "UNICORR"):
                                has_any_correction = True
                                break
                    if has_any_correction:
                        break
                except Exception:
                    continue
            if has_any_correction:
                break
        if not found_any_sk or has_any_correction:
            continue
        dest = os.path.join(not_aspcorr_dir, folder)
        if not os.path.exists(dest):
            try:
                shutil.move(folder_path, dest)
            except Exception:
                pass



def _unquarantine_obsids(datainit_path, obsids, reporter=print):
    """
    Move any obsid in `obsids` that currently sits inside the pool's Orphans/
    or NotASPCORR/ quarantine folders back to the top-level pool location
    (datainit_path/<entry>/), so it is reprocessed as a normal pool obsid.
 
    Smeared/ is deliberately left alone (smearing is intrinsic).
 
    Returns the number of obsid folders moved.
    """
    obsid_set = set(str(o) for o in obsids)
    moved = 0
    obsid_re = re.compile(r"(\d{11})")
 
    for qfolder in ("Orphans", "NotASPCORR"):
        qpath = os.path.join(datainit_path, qfolder)
        if not os.path.isdir(qpath):
            continue
        try:
            entries = list(os.listdir(qpath))
        except OSError:
            continue
        for entry in entries:
            m = obsid_re.search(entry)
            if not m:
                continue
            if m.group(1) not in obsid_set:
                continue
            src = os.path.join(qpath, entry)
            if not os.path.isdir(src):
                continue
            dst = os.path.join(datainit_path, entry)
            if os.path.exists(dst):
                # Already present in the main pool (shouldn't normally happen).
                # Leave the quarantined copy rather than risk clobbering.
                reporter(f"  un-quarantine: {entry} already in pool — leaving "
                         f"quarantined copy in {qfolder}/")
                continue
            try:
                shutil.move(src, dst)
                moved += 1
            except Exception as e:
                reporter(f"  un-quarantine: could not move {entry} out of "
                         f"{qfolder}/: {str(e)[:120]}")
 
    if moved:
        reporter(f"  Un-quarantined {moved} obsid(s) from Orphans/NotASPCORR "
                 f"back into the pool for reprocessing.")
    return moved



##############################################################################
"""
StallWatchdog — pinpoint pipeline hangs 

It does NOT kill anything (that's the job of per-subprocess timeouts). It only *observes*
Like a watchdog, wink, wink.

dump_path="/home/<you>/pipeline_stall_dump.txt").start()

Reading the dump when it fires:
  - The "last activity" label = the work item that hung.
  - Thread stacks show the exact line each thread is on. A worker stuck in
    readline()/select on the warm shell pipe = waiting for a sentinel/output.import time
  - Process STAT column: 'S' (interruptible sleep) on a HEASoft/perl child
    usually means it's blocked on stdin -> a prompt (fix: stdin=/dev/null,
    mode=h). 'D' = uninterruptible I/O wait -> filesystem/HEASoft I/O stall.
    'Z' = zombie -> it died but wasn't reaped (sentinel never arrived case).
"""

class StallWatchdog:
    def __init__(self, stall_seconds=300, check_every=30, dump_path=None):
        self.stall_seconds = stall_seconds
        self.check_every = check_every
        self.dump_path = dump_path or os.path.expanduser(
            "~/pipeline_stall_dump.txt")
        self._last = time.time()
        self._label = "startup"
        self._dumped_for_this_stall = False
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = None

    def beat(self, label=None):
        """Record progress. Call around each unit of work."""
        with self._lock:
            self._last = time.time()
            if label is not None:
                self._label = label
            self._dumped_for_this_stall = False  # new progress -> re-arm

    def start(self):
        self._thread = threading.Thread(target=self._loop, daemon=True, name="StallWatchdog")
        self._thread.start()
        return self

    def stop(self):
        self._stop.set()

    def _loop(self):
        while not self._stop.wait(self.check_every):
            with self._lock:
                idle = time.time() - self._last
                label = self._label
                already = self._dumped_for_this_stall
            if idle >= self.stall_seconds and not already:
                self._dump(idle, label)
                with self._lock:
                    self._dumped_for_this_stall = True

    def _dump(self, idle, label):
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        try:
            with open(self.dump_path, "a") as f:
                f.write("\n" + "=" * 72 + "\n")
                f.write(f"STALL at {ts}: no progress for {idle:.0f}s\n")
                f.write(f"last activity label: {label}\n")
                f.write("-" * 72 + "\nALL THREAD STACKS:\n")
                f.flush()
                faulthandler.dump_traceback(file=f, all_threads=True)
                f.write("-" * 72 + "\nLIVE HEASOFT-ish PROCESSES "
                        "(pid stat etime cmd):\n")
                try:
                    if HEASOFT_BACKEND == "wsl":
                        ps_cmd = ["wsl", "bash", "-lc", "ps -eo pid,stat,etime,args"]
                    else:
                        ps_cmd = ["ps", "-eo", "pid,stat,etime,cmd"]
                    out = subprocess.run(
                        ps_cmd, capture_output=True, text=True, timeout=10).stdout
                    keys = ("uvot", "perl", "ftcopy", "fappend", "fhelp",
                            "pget", "pset", "uvotimsum", "uvotsource",
                            "uvotdetect", "uvotunicorr", "uvotskycorr")
                    hits = [ln for ln in out.splitlines()
                            if any(k in ln for k in keys)]
                    f.write(("\n".join(hits) if hits
                             else "(no matching child processes found)") + "\n")
                except Exception as e:
                    f.write(f"(ps failed: {e})\n")
                f.write("=" * 72 + "\n")
                f.flush()
        except Exception as e:
            # never let the watchdog itself crash the run
            print(f"StallWatchdog: dump failed: {e}")
        print(f"⚠️ STALL DETECTED: no progress for {idle:.0f}s "
              f"(last activity: {label}). Thread dump appended to "
              f"{self.dump_path}")


########################################## 
# Light Curve Generating Function

def plot_uvot_lightcurves(
    bands_to_plot=None,
    xlim=(54000, 61000),
    excel_file=None,
    ogle_file=None,
    xrt_files=None,
    per_band_plots=True,        # one plot per band (skips empty bands)
    offset_overlay=True,        # combined plot, bands offset by band_offset_step
    overlay_plot=True,         # original combined overlay (no offset)
    stacked_plot=False,         # DEPRECATED: removed; accepted but ignored
    band_offset_step=1.0,       # magnitude spacing between bands in the offset plot
    ul_arrow_frac=0.05,         # UL arrow length as a fraction of the axis y-span
    y_pad_frac=0.12,            # blank margin above/below the data (incl. error bars)
    save_prefix=None,
    Upperlimits=True,
    Lowerlimits=True,           # draw SATURATED points as bright (lower) limits
    target=None,                # observation/target name -> titles like "sxp5.05_UVOT"
):
    """
    Plot UVOT (+ optional OGLE / XRT) light curves.
 
    Produces up to 8 figures:
      - one plot per UVOT band (6), each skipped if that band has no data
      - one "offset" overlay where every band is shifted by band_offset_step
        magnitudes so the bands don't overlap; the per-band offset is shown
        in the legend (e.g. "uw1 (+3)")
      - one "Original" plot where their is no offset.
 
    Upper limits (when Upperlimits=True) are drawn the same way on every plot:
    a white-filled circle outlined in the band colour, with a downward arrow
    whose length is ul_arrow_frac of that axis's y-span (so it never runs long).
 
    Extra axes:
      - every plot gets a top x-axis relabelling MJD as calendar months/years.
      - single-band plots get a right y-axis in f_lambda (per-Angstrom flux),
        scaled directly from this band's own AB_FLUX_AA so it matches the
        pipeline's recorded conversion. The non-offset overlay instead gets a
        right y-axis in f_nu (band-independent cgs flux density from AB_MAG),
        but only when there's no XRT panel already using the right side. The
        offset overlay gets no flux axis (of  course per-band offsets make a single flux scale meaningless).
    """

 
    # MJD epoch (1858-11-17) expressed as a matplotlib date number.
    _MJD_OFFSET = mdates.date2num(datetime(1858, 11, 17))
 
    _AB_ZP = 48.6     # AB system zeropoint
    _TINY = 1e-300    # guard so log10 never sees 0 at an axis edge

    ##########################################################################
    # FONT SIZES — bump these to scale every number/label on the figures.
    ##########################################################################
    _FS_TICK = 14      # tick numbers (both axes, primary + secondary)
    _FS_LABEL = 17     # axis labels
    _FS_TITLE = 18     # plot title
    _FS_LEGEND = 13    # legend text


    ##########################################################################
    # UVOT band central wavelength and FWHM (Angstrom), Poole et al. 2008
    # (as tabulated by the HEASARC UVOTSSC catalogue). FWHM is the bandpass
    # width used to turn a flux DENSITY F_lambda into a band-integrated FLUX:
    #     F [erg s^-1 cm^-2]  =  F_lambda [erg s^-1 cm^-2 A^-1] * FWHM [A]
    # To instead show lambda*F_lambda (= nu*F_nu), swap FWHM for the
    # wavelength in _BAND_FLUX_WIDTH below.
    ##########################################################################
    _BAND_WL_AA = {            # band -> (effective_wavelength_AA, FWHM_AA)
        'uvv': (5468.0, 769.0),
        'ubb': (4392.0, 975.0),
        'uuu': (3465.0, 785.0),
        'uw1': (2600.0, 693.0),
        'um2': (2246.0, 498.0),
        'uw2': (1928.0, 657.0),
    }
 
    def _band_flux_width(band):
        """Multiplier that converts F_lambda -> band flux for `band`.
        Returns the FWHM (band-integrated flux). Switch the index from 1 to 0
        to return the effective wavelength instead, which gives lambda*F_lambda
        (= nu*F_nu) on the right axis."""
        rec = _BAND_WL_AA.get(str(band).lower())
        return rec[1] if rec else None
 
    def _style_axis(ax, log_right=False):
        """Apply consistent fonts + denser majors + minor subticks to a primary
        axis (left/bottom). Top/right belong to the secondary calendar/flux
        axes, so they're left off here to avoid double tick rows."""
        ax.tick_params(axis='both', which='major', labelsize=_FS_TICK,
                       length=7, width=1.2, direction='out',
                       top=False, right=False)
        ax.tick_params(axis='both', which='minor', length=4, width=0.9, direction='out', top=False, right=False)
        ax.xaxis.set_major_locator(ticker.MaxNLocator(nbins=10, min_n_ticks=6))
        ax.xaxis.set_minor_locator(ticker.AutoMinorLocator(5))
        ax.yaxis.set_major_locator(ticker.MaxNLocator(nbins=10, min_n_ticks=6))
        ax.yaxis.set_minor_locator(ticker.AutoMinorLocator(5))
        ax.xaxis.label.set_size(_FS_LABEL)
        ax.yaxis.label.set_size(_FS_LABEL)
        ax.title.set_size(_FS_TITLE)
 
    def _style_secondary(secax, log_axis=False, axis='y'):
        """Fonts + tick sizes for a secondary axis. It deliberately does NOT set
        any locators: the calendar axis sets Year/Month locators itself and the
        flux axes use _align_flux_ticks_to_mag, so touching locators here would clobber them"""
        secax.tick_params(axis='both', which='major', labelsize=_FS_TICK, length=7, width=1.2)
        secax.tick_params(axis='both', which='minor', length=4, width=0.9)
        lab = getattr(secax, f"{axis}axis").label
        if lab is not None:
            lab.set_size(_FS_LABEL)
 
    def _align_flux_ticks_to_mag(ax_mag, secax, mag_to_flux):
        """Place flux-axis ticks at the MAGNITUDE axis's own tick positions, so
        they line up with the mag grid, are evenly spaced. """
        try:
            ylo, yhi = ax_mag.get_ylim()
            mlo, mhi = min(ylo, yhi), max(ylo, yhi)
            majors = [t for t in ax_mag.yaxis.get_majorticklocs()
                      if mlo - 1e-9 <= t <= mhi + 1e-9]
            minors = [t for t in ax_mag.yaxis.get_minorticklocs()
                      if mlo - 1e-9 <= t <= mhi + 1e-9]
            fmaj = np.asarray(mag_to_flux(np.asarray(majors, dtype=float)))
            secax.yaxis.set_major_locator(ticker.FixedLocator(fmaj))
            if minors:
                fmin = np.asarray(mag_to_flux(np.asarray(minors, dtype=float)))
                secax.yaxis.set_minor_locator(ticker.FixedLocator(fmin))
 
            def _sci(x, pos):
                if not np.isfinite(x) or x <= 0:
                    return ""
                e = int(np.floor(np.log10(x)))
                m = x / 10.0 ** e
                return rf"${m:.1f}\times10^{{{e}}}$"
 
            secax.yaxis.set_major_formatter(ticker.FuncFormatter(_sci))
            secax.yaxis.set_minor_formatter(ticker.NullFormatter())
        except Exception:
            pass
 
            
    ##########################################################################
    # top calendar axis
    ##########################################################################
    def _add_calendar_top_axis(ax, fmt="%Y"):
        """Top x-axis labelled by YEAR only, with monthly subticks between the
        years (12 ticks/year for short baselines, thinned for long ones). Year
        labels never overlap and sit at regular year boundaries, unlike the old
        month-name labels which collided on narrow figures."""
        def mjd_to_dnum(mjd):
            return np.asarray(mjd, dtype=float) + _MJD_OFFSET
 
        def dnum_to_mjd(dnum):
            return np.asarray(dnum, dtype=float) - _MJD_OFFSET
 
        secax = ax.secondary_xaxis("top", functions=(mjd_to_dnum, dnum_to_mjd))
        secax.xaxis.set_major_locator(mdates.YearLocator())
        secax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
 
        # Monthly subticks between year labels, thin out for long baselines so
        # the top axis never turns into a solid ruler.
        try:
            span_yr = abs(ax.get_xlim()[1] - ax.get_xlim()[0]) / 365.25
        except Exception:
            span_yr = 2.0
        if span_yr <= 3:
            minor = mdates.MonthLocator()                       # every month (12/yr)
        elif span_yr <= 8:
            minor = mdates.MonthLocator(bymonth=(1, 4, 7, 10))  # quarterly
        else:
            minor = mdates.MonthLocator(bymonth=(1, 7))         # semiannual
        secax.xaxis.set_minor_locator(minor)
        secax.xaxis.set_minor_formatter(ticker.NullFormatter())
        secax.set_xlabel("")
        _style_secondary(secax, log_axis=False, axis='x')
 
        # Fallback. a window entirely within one calendar year contains no
        # Jan-1, so YearLocator would show no label at all. In that case put a
        # single year tick at the left edge so there's always a year reference.
        try:
            x0, x1 = sorted(ax.get_xlim())
            d0, d1 = x0 + _MJD_OFFSET, x1 + _MJD_OFFSET
            years = range(mdates.num2date(d0).year, mdates.num2date(d1).year + 1)
            jan1_mjd = [mdates.date2num(datetime(y, 1, 1)) - _MJD_OFFSET
                        for y in years]
            if not any(x0 <= m <= x1 for m in jan1_mjd):
                yr = mdates.num2date(d0).year
                secax.xaxis.set_major_locator(ticker.FixedLocator([d0]))
                secax.xaxis.set_major_formatter(ticker.FixedFormatter([str(yr)]))
        except Exception:
            pass
        return secax
        
    ##########################################################################
    # combined plot: band-independent f_nu (erg s^-1 cm^-2 Hz^-1)
    #  AB definition: m = -2.5 log10(f_nu) - 48.6  -> exact, wavelength-free.
    ##########################################################################
    def _mag_to_fnu(mag):
        return 10.0 ** (-(np.asarray(mag, dtype=float) + _AB_ZP) / 2.5)
 
    def _fnu_to_mag(fnu):
        fnu = np.clip(np.asarray(fnu, dtype=float), _TINY, None)
        return -2.5 * np.log10(fnu) - _AB_ZP
 
    def _add_fnu_right_axis(ax, label=r"$f_\nu$  [erg s$^{-1}$ cm$^{-2}$ Hz$^{-1}$]"):
        """Right y-axis showing AB flux density f_nu. Use on the COMBINED plot."""
        secax = ax.secondary_yaxis("right", functions=(_mag_to_fnu, _fnu_to_mag))
        secax.set_ylabel(label)
        _style_secondary(secax, log_axis=False, axis='y')
        _align_flux_ticks_to_mag(ax, secax, _mag_to_fnu)
        return secax
 
    ##########################################################################
    # single-band plot: f_lambda (erg s^-1 cm^-2 A^-1), K from the table
    # f_lambda = K * 10**(-0.4*mag); K = median(AB_FLUX_AA * 10**(0.4*mag))
    # over this band's rows, so the axis uses the pipeline's own conversion
    # with no hardcoded effective wavelength.
    ##########################################################################
    def _band_flux_constant(mag, flux, band=None):
        mag = np.asarray(mag, dtype=float)
        flux = np.asarray(flux, dtype=float)
        good = np.isfinite(mag) & np.isfinite(flux) & (flux > 0)
        if good.any():
            # match the pipeline's own AB_FLUX_AA<->AB_MAG scaling.
            return float(np.median(flux[good] * 10.0 ** (0.4 * mag[good])))
        # No (mag, flux) pair to calibrate from — e.g. an upper-limit-only
        # band. Fall back to the analytic AB zeropoint, which needs no
        # detection:  f_lambda(mag=0) = f_nu(AB=0) * c / lambda_eff^2, with
        # f_nu(AB=0) = 3631 Jy = 3.631e-20 erg s^-1 cm^-2 Hz^-1 and
        # c = 2.998e18 A/s. This is just the standard AB mag<->flux relation.
        if band is not None:
            rec = _BAND_WL_AA.get(str(band).lower())
            if rec:
                lam = float(rec[0])              # effective wavelength [A]
                if lam > 0:
                    return 3.631e-20 * 2.998e18 / (lam * lam)
        return None
 
    def _add_bandflux_right_axis(ax, band, mag, flux, label=r"$F$  [erg s$^{-1}$ cm$^{-2}$]"):
        """Right y-axis showing band-integrated FLUX for a SINGLE band:
        F = (K*10**(-0.4*mag)) * FWHM_band. Returns the axis, or None if K or
        the band width is unavailable (then the right axis is simply skipped)."""
        K = _band_flux_constant(mag, flux, band=band)
        width = _band_flux_width(band)
        if (K is None or not np.isfinite(K) or K <= 0
                or width is None or width <= 0):
            return None
        KF = K * width   # F = KF * 10**(-0.4*mag)
 
        def mag_to_flux(m):
            return KF * 10.0 ** (-0.4 * np.asarray(m, dtype=float))
 
        def flux_to_mag(fl):
            fl = np.clip(np.asarray(fl, dtype=float), _TINY, None)
            return -2.5 * np.log10(fl / KF)
 
        secax = ax.secondary_yaxis("right",
                                   functions=(mag_to_flux, flux_to_mag))
        secax.set_ylabel(label)
        _style_secondary(secax, log_axis=False, axis='y')
        _align_flux_ticks_to_mag(ax, secax, mag_to_flux)
        return secax
 
    if stacked_plot:
        print("  NOTE: stacked_plot has been removed; ignoring stacked_plot=True.")
 
    default_bands = ["uvv", "ubb", "uuu", "uw1", "uw2", "um2"]
    mag_col = 'AB_MAG'
    mag_err_col = 'AB_MAG_ERR'
 
    # Title stem named after the observation/target (e.g. "sxp5.05_UVOT"),
    # falling back to a plain "UVOT" when no target is supplied.
    title_stem = f"{target}_UVOT" if target else "UVOT"
 
    ##########################################################################
    # LOAD UVOT PHOTOMETRY DATA 
    ##########################################################################
    if excel_file is None:
        print("ERROR: excel_file parameter is required.")
        return
 
    print(f"Loading UVOT data from: {excel_file}")
    if excel_file.lower().endswith(".csv"):
        df = pd.read_csv(excel_file)
    else:
        df = pd.read_csv(excel_file, sep='\t')
 
    ##########################################################################
    # BUILD MJD COLUMN 
    ##########################################################################
    if 'MJD' in df.columns:
        df['MJD'] = pd.to_numeric(df['MJD'], errors='coerce')
        df = df[df['MJD'].notna()]
        print(f"  Using existing MJD column ({len(df)} rows)")
    elif 'TSTART' in df.columns:
        df['TSTART'] = pd.to_numeric(df['TSTART'], errors='coerce')
        df = df[df['TSTART'].notna()]
        df['MJD'] = df['TSTART'] / 86400.0 + 51910.0
        print(f"  Converted TSTART → MJD ({len(df)} rows)")
    elif 'DATE_TAG' in df.columns:
        df['OBS_DATE'] = pd.to_datetime(
            df['DATE_TAG'], errors='coerce', format='%Y-%m-%d_%H-%M-%S')
        df = df[pd.notnull(df['OBS_DATE'])]
        if len(df) > 0:
            df['MJD'] = Time(df['OBS_DATE']).mjd
            print(f"  Converted DATE_TAG → MJD ({len(df)} rows)")
        else:
            print("ERROR: DATE_TAG column exists but no dates could be parsed.")
            return
    else:
        found = None
        for c in df.columns:
            if 'mjd' in c.lower() or 'time' in c.lower():
                found = c
                break
        if found:
            df['MJD'] = pd.to_numeric(df[found], errors='coerce')
            df = df[df['MJD'].notna()]
            print(f"  Using column '{found}' as MJD ({len(df)} rows)")
        else:
            print("ERROR: Cannot find a time column (MJD, TSTART, or DATE_TAG).")
            return
 
    if len(df) == 0:
        print("ERROR: No data remaining after time conversion.")
        return
 
    ##########################################################################
    # FIND MAGNITUDE COLUMNS 
    ##########################################################################
    if mag_col not in df.columns:
        mag_col_actual = None
        for pattern in ['ab_mag', 'mag']:
            for c in df.columns:
                cl = c.lower().replace(' ', '_')
                if pattern in cl and 'err' not in cl and 'flux' not in cl and 'lim' not in cl:
                    mag_col_actual = c
                    break
            if mag_col_actual:
                break
        if mag_col_actual is None:
            print(f"ERROR: Cannot find a magnitude column. Available: {list(df.columns)}")
            return
        mag_col = mag_col_actual
        print(f"  Using '{mag_col}' as magnitude column")
 
    if mag_err_col not in df.columns:
        mag_err_col_actual = None
        for pattern_pair in [('ab_mag', 'err'), ('mag', 'err')]:
            for c in df.columns:
                cl = c.lower().replace(' ', '_')
                if pattern_pair[0] in cl and pattern_pair[1] in cl:
                    mag_err_col_actual = c
                    break
            if mag_err_col_actual:
                break
        if mag_err_col_actual is None:
            print(f"WARNING: Cannot find magnitude error column. Setting errors to 0.")
            df['_ERR'] = 0.0
            mag_err_col = '_ERR'
        else:
            mag_err_col = mag_err_col_actual
            print(f"  Using '{mag_err_col}' as magnitude error column")
 
    df[mag_col] = pd.to_numeric(df[mag_col], errors='coerce')
    df[mag_err_col] = pd.to_numeric(df[mag_err_col], errors='coerce')
 
    # AB_FLUX_AA (per-Angstrom flux) column, used for the single-band right
    # flux axis. Recorded by the pipeline alongside AB_MAG.
    flux_col = 'AB_FLUX_AA' if 'AB_FLUX_AA' in df.columns else None
    if flux_col is not None:
        df[flux_col] = pd.to_numeric(df[flux_col], errors='coerce')
 
    ##########################################################################
    # IDENTIFY BAND COLUMN 
    ##########################################################################
    band_col_name = 'BAND'
    if band_col_name not in df.columns:
        for c in df.columns:
            if 'band' in c.lower() or 'filter' in c.lower():
                band_col_name = c
                break
 
    ##########################################################################
    # IDENTIFY UPPER-LIMIT COLUMNS 
    ##########################################################################
    ul_col = None
    for c in df.columns:
        if c.lower().replace(' ', '').replace('_', '') == 'upperlimit':
            ul_col = c
            break
 
    ul_mag_col = None
    for cand in ['AB_MAG_LIM', 'PLOT_MAG', 'MAG_LIM']:
        if cand in df.columns:
            ul_mag_col = cand
            break
    if ul_mag_col is None:
        for c in df.columns:
            cl = c.lower().replace(' ', '_')
            if 'mag' in cl and 'lim' in cl:
                ul_mag_col = c
                break
    if ul_mag_col is not None:
        df[ul_mag_col] = pd.to_numeric(df[ul_mag_col], errors='coerce')
 
    ##########################################################################
    # IDENTIFY LOWER-LIMIT (SATURATION) COLUMN
    ##########################################################################
    # Saturated points (uvotsource SATURATED / a LowerLimit column carried
    # through compilation) are bright limits, the true magnitude is brighter
    # than (<=) the measured one, so they plot at AB_MAG with an up arrow.
    ll_col = None
    for cand in ['LowerLimit', 'SATURATED']:
        if cand in df.columns:
            ll_col = cand
            break
    if ll_col is None:
        for c in df.columns:
            if c.lower().replace(' ', '').replace('_', '') in ('lowerlimit', 'saturated'):
                ll_col = c
                break
 
    def _is_truthy(v):
        # Robust across the forms a SATURATED/limit flag arrives in, real bool,
        # int8 84/70 (ASCII 'T'/'F', how a FITS logical comes through the
        # whole-record byteswap path), and strings after a CSV trip.
        if isinstance(v, (bool, np.bool_)):
            return bool(v)
        if isinstance(v, (int, np.integer)):
            return int(v) in (1, 84)
        if isinstance(v, (bytes, bytearray)):
            return v.strip().lower() in (b't', b'true', b'1', b'yes')
        return str(v).strip().lower() in ('true', '1', 'yes', 't')
 
    ##########################################################################
    # SPLIT DETECTIONS FROM UPPER LIMITS  
    ##########################################################################
    if ul_col is not None:
        is_ul_mask = df[ul_col].apply(_is_truthy)
        det_df = df[~is_ul_mask].copy()
        ul_df = df[is_ul_mask].copy()
    else:
        det_df = df.copy()
        ul_df = df.iloc[0:0].copy()
        if Upperlimits:
            print(" NOTE: Upperlimits=True but no 'UpperLimit' column found "
                  "in the data — nothing to plot as upper limits.")
 
    ##########################################################################
    # SPLIT OFF LOWER LIMITS (SATURATION)
    ##########################################################################
    # Pull saturated rows out of the detection set BEFORE quality cleaning —
    # they carry a huge AB_MAG_ERR (~1 mag) that would otherwise survive the
    # err<=0.35*mag cut and plot as a normal point with a giant error bar,
    # which is exactly what the lower-limit treatment replaces. With
    # Lowerlimits=False they stay in det_df and plot normally (big bars).
    if Lowerlimits and ll_col is not None:
        is_ll_mask = det_df[ll_col].apply(_is_truthy)
        ll_df = det_df[is_ll_mask].copy()
        det_df = det_df[~is_ll_mask].copy()
    else:
        ll_df = det_df.iloc[0:0].copy()
 
    ##########################################################################
    # BASIC QUALITY CLEANING (detections only) 
    ##########################################################################
    det_df = det_df[np.isfinite(det_df[mag_col]) & np.isfinite(det_df[mag_err_col])]
    det_df = det_df[det_df[mag_err_col] <= 0.55 * det_df[mag_col].abs()]
    det_df.sort_values('MJD', inplace=True)
    merged = det_df
 
    if Upperlimits and ul_mag_col is not None and not ul_df.empty:
        ul_df = ul_df[np.isfinite(ul_df[ul_mag_col])]
        ul_df.sort_values('MJD', inplace=True)
 
    # Lower limits plot at AB_MAG (mag_col), keep only finite ones.
    if Lowerlimits and not ll_df.empty:
        ll_df = ll_df[np.isfinite(ll_df[mag_col])]
        ll_df.sort_values('MJD', inplace=True)
 
    print(f"  After quality cleaning: {len(merged)} detections"
          + (f", {len(ul_df)} upper limits" if Upperlimits else "")
          + (f", {len(ll_df)} lower limits (saturated)" if Lowerlimits else ""))
 
    # If UL plotting was requested but this target produced NO upper limits,
    # the resulting figures would be dentical to the non-UL
    # pass just under different filenames. So lets Skip the whole pass so the
    # directory isn't cluttered with duplicate plots. 
    if Upperlimits and (ul_mag_col is None or ul_df.empty):
        print(" No upper limits found for this target — skipping the "
              "upper-limit plot set (identical to the non-UL plots).")
        return
 
    ##########################################################################
    # LOAD OGLE DATA (optional)  
    ##########################################################################
    ogle_df = None
    if ogle_file is not None:
        try:
            ogle_df = pd.read_csv(ogle_file)
            if 'HJD' in ogle_df.columns:
                ogle_df['MJD'] = ogle_df['HJD'] - 2400000
            ogle_df[band_col_name] = 'OGLE'
            ogle_mag_col = None
            ogle_err_col = None
            for c in ogle_df.columns:
                cl = c.lower()
                if 'magnitude' in cl and 'error' not in cl and 'err' not in cl:
                    ogle_mag_col = c
                elif 'error' in cl or ('err' in cl and 'mag' in cl):
                    ogle_err_col = c
            if ogle_mag_col is None:
                for c in ogle_df.columns:
                    if 'mag' in c.lower() and 'err' not in c.lower():
                        ogle_mag_col = c
                        break
            if ogle_err_col is None:
                for c in ogle_df.columns:
                    if 'err' in c.lower():
                        ogle_err_col = c
                        break
            if ogle_mag_col and ogle_mag_col != mag_col:
                ogle_df.rename(columns={ogle_mag_col: mag_col}, inplace=True)
            if ogle_err_col and ogle_err_col != mag_err_col:
                ogle_df.rename(columns={ogle_err_col: mag_err_col}, inplace=True)
            if mag_col not in ogle_df.columns or mag_err_col not in ogle_df.columns:
                print(f" WARNING: OGLE columns could not be mapped. "
                      f"Has: {list(ogle_df.columns)}")
                ogle_df = None
            else:
                print(f" OGLE: {len(ogle_df)} data points")
        except Exception as e:
            print(f" WARNING: Could not load OGLE file: {e}")
            ogle_df = None
 
    ##########################################################################
    # LOAD XRT DATA (optional)   
    ##########################################################################
    def find_col(df_cols, name_sub):
        for c in df_cols:
            if name_sub.lower() in c.lower():
                return c
        return None
 
    xrt_list = []
    if xrt_files is not None:
        for label, path in xrt_files.items():
            try:
                xdf = pd.read_csv(path)
            except Exception as e:
                print(f" WARNING: could not read {label} ({path}): {e}")
                continue
            mjd_c = find_col(xdf.columns, 'mjd')
            rate_c = find_col(xdf.columns, 'count rate')
            err_pos_c = find_col(xdf.columns, 'positive')
            err_neg_c = find_col(xdf.columns, 'negative')
            if mjd_c is None or rate_c is None:
                if xdf.shape[1] >= 2:
                    mjd_c = xdf.columns[0]; rate_c = xdf.columns[1]
                else:
                    print(f" Skipping {label}: Cannot identify columns.")
                    continue
            is_ul = 'UL' in label.upper()
            if not is_ul and err_pos_c and err_neg_c:
                if xdf[err_pos_c].fillna(0).abs().max() == 0:
                    is_ul = True
            temp = pd.DataFrame()
            temp['MJD (days)'] = pd.to_numeric(xdf[mjd_c], errors='coerce')
            if is_ul:
                temp['Count_Rate_UL'] = pd.to_numeric(xdf[rate_c], errors='coerce')
                temp['Type'] = 'UL'
                temp = temp.dropna(subset=['MJD (days)', 'Count_Rate_UL'])
            else:
                temp['Count Rate (counts per second)'] = pd.to_numeric(xdf[rate_c], errors='coerce')
                temp['Count Rate Positive Error'] = (
                    pd.to_numeric(xdf[err_pos_c], errors='coerce').abs() if err_pos_c else np.nan)
                temp['Count Rate Negative Error'] = (
                    pd.to_numeric(xdf[err_neg_c], errors='coerce').abs() if err_neg_c else np.nan)
                temp['Type'] = 'Normal'
                temp = temp.dropna(subset=['MJD (days)', 'Count Rate (counts per second)'])
            temp['Label'] = label
            xrt_list.append(temp)
            print(f"  {label}: {len(temp)} data points")
    xrt_all = pd.concat(xrt_list, ignore_index=True, sort=False) if xrt_list else pd.DataFrame()
 
    ##########################################################################
    # DETERMINE BANDS + COLOURS
    ##########################################################################
    if bands_to_plot is None:
        plot_bands = [b for b in default_bands]
    elif bands_to_plot == 'auto':
        plot_bands = list(merged[band_col_name].str.lower().unique())
    else:
        plot_bands = [b.lower() for b in bands_to_plot]
 
    # add UL-only bands so their limits can still show
    if (Upperlimits and not ul_df.empty
            and (bands_to_plot is None or bands_to_plot == 'auto')):
        for b in ul_df[band_col_name].str.lower().unique():
            if b not in plot_bands and b != 'ogle':
                plot_bands.append(b)
 
    # add bands that exist ONLY as saturated lower limits (all frames bright)
    if (Lowerlimits and not ll_df.empty
            and (bands_to_plot is None or bands_to_plot == 'auto')):
        for b in ll_df[band_col_name].str.lower().unique():
            if b not in plot_bands and b != 'ogle':
                plot_bands.append(b)
 
    # colour map keyed by band, FIXED mapping so a band keeps the same
    # colour in every figure, every target, and the time-averaged SED.
    cmap = plt.get_cmap('tab10')
    _FIXED_BAND_ORDER = ['ubb', 'um2', 'uuu', 'uvv', 'uw1', 'uw2']
    band_colors = {b: cmap(i % 10) for i, b in enumerate(_FIXED_BAND_ORDER)}
    for b in plot_bands:                  # any extra series (e.g. ogle)
        if b not in band_colors:
            band_colors[b] = cmap(len(band_colors) % 10)
 
    #####################################################################
    # shared helpers
    #####################################################################
    def _band_det(band):
        """Detection rows for a band (lowercased match)."""
        return merged[merged[band_col_name].str.lower() == band.lower()]
 
    def _band_ul(band):
        """Upper-limit rows for a band, only when ULs are enabled/available."""
        if not Upperlimits or ul_mag_col is None or ul_df.empty:
            return ul_df.iloc[0:0]
        return ul_df[ul_df[band_col_name].str.lower() == band.lower()]
 
    def _band_ll(band):
        """Lower-limit (saturated) rows for a band, when enabled/available."""
        if not Lowerlimits or ll_df.empty:
            return ll_df.iloc[0:0]
        return ll_df[ll_df[band_col_name].str.lower() == band.lower()]
 
    def _draw_uls_on(ax, bands, offsets, label_uls=False):
        """
        Draw upper limits as white-filled, band-coloured circles with a
        downward arrow. Arrow length = ul_arrow_frac of the axis y-span, so it
        scales with the axis and never runs long. Must be called AFTER the
        axis y-limits are finalised (set_ylim + invert), since it reads them.
        """
        if not Upperlimits or ul_mag_col is None or ul_df.empty:
            return
        ylo, yhi = ax.get_ylim()
        span = abs(yhi - ylo)
        if span <= 0:
            span = 1.0
        arrow_len = ul_arrow_frac * span     # data units (mag); axis is inverted
        for band in bands:
            if band.lower() == 'ogle':
                continue
            sub = _band_ul(band)
            if len(sub) == 0:
                continue
            off = offsets.get(band, 0.0)
            yvals = (sub[ul_mag_col] + off).values
            xvals = sub['MJD'].values
            color = band_colors.get(band, 'gray')
            # circle: white inside, band-colour outline
            ax.scatter(xvals, yvals, marker='o', s=55,
                       facecolors='white', edgecolors=color,
                       linewidths=1.4, zorder=6,
                       label=(f'{band} (UL)' if label_uls else '_nolegend_'))
            # downward arrow (toward fainter = larger mag = down on inverted axis)
            for x, y in zip(xvals, yvals):
                ax.annotate('', xy=(x, y + arrow_len), xytext=(x, y),
                            arrowprops=dict(arrowstyle='-|>', color=color, lw=1.4, shrinkA=0, shrinkB=0),
                            zorder=5, annotation_clip=False)
 
    def _draw_lls_on(ax, bands, offsets, label_lls=False):
        """
        Draw lower limits (saturated, bright limits) as white-filled,
        band-coloured circles with an UPWARD arrow 
        """
        if not Lowerlimits or ll_df.empty:
            return
        ylo, yhi = ax.get_ylim()
        span = abs(yhi - ylo)
        if span <= 0:
            span = 1.0
        arrow_len = ul_arrow_frac * span     # data units (mag), axis is inverted
        for band in bands:
            if band.lower() == 'ogle':
                continue
            sub = _band_ll(band)
            if len(sub) == 0:
                continue
            off = offsets.get(band, 0.0)
            yvals = (sub[mag_col] + off).values
            xvals = sub['MJD'].values
            color = band_colors.get(band, 'gray')
            # circle: white inside, band-colour outline (same marker as ULs)
            ax.scatter(xvals, yvals, marker='o', s=55,
                       facecolors='white', edgecolors=color,
                       linewidths=1.4, zorder=6,
                       label=(f'{band} (sat. limit)' if label_lls else '_nolegend_'))
            # upward arrow (toward brighter = smaller mag = UP on inverted axis)
            for x, y in zip(xvals, yvals):
                ax.annotate('', xy=(x, y - arrow_len), xytext=(x, y),
                            arrowprops=dict(arrowstyle='-|>', color=color, lw=1.4, shrinkA=0, shrinkB=0),
                            zorder=5, annotation_clip=False)
 
    def _finalize_mag_axis(ax, y_values_list):
        """
        Set inverted magnitude y-limits from the supplied y-values, leaving
        a blank margin (y_pad_frac of the data span) above and below, plus
        extra room for UL/LL arrows on both edges, then invert.
        """
        ys = np.concatenate([np.asarray(v, dtype=float)
                             for v in y_values_list if len(v)]) \
            if any(len(v) for v in y_values_list) else np.array([])
        if ys.size == 0:
            ax.set_ylim(20.5, 14.5)  # empty default 
            return
        ymin, ymax = float(np.nanmin(ys)), float(np.nanmax(ys))
        data_span = (ymax - ymin) if ymax > ymin else 1.0
        # both edges get the requested margin, but also guarantee room for an
        # arrow (ul_arrow_frac of the span), UL arrows point down (bottom edge,
        # ymax side), LL/saturated arrows point up (top edge, ymin side).
        edge_pad = max(y_pad_frac * data_span, 1.5 * ul_arrow_frac * data_span, 0.30)
        top_pad = edge_pad if (Lowerlimits and not ll_df.empty) else y_pad_frac * data_span
        bot_pad = edge_pad
        ax.set_ylim(ymin - top_pad, ymax + bot_pad)
        ax.invert_yaxis()
 
    #####################################################################
    # PER-BAND PLOTS  — skip any band with no data
    #####################################################################
    if per_band_plots:
        # 3-sigma only when a single band was explicitly requested
        single_band = (isinstance(bands_to_plot, list)
                       and len(bands_to_plot) == 1
                       and bands_to_plot[0].lower() != 'ogle')
        for band in plot_bands:
            if band.lower() == 'ogle':
                continue
            det = _band_det(band)
            ul = _band_ul(band)
            ll = _band_ll(band)
            if len(det) == 0 and len(ul) == 0 and len(ll) == 0:
                continue  # no data for this band? no plot at all
 
            fig, ax = plt.subplots(figsize=(12, 5))
            color = band_colors.get(band, None)
            if len(det) > 0:
                ax.errorbar(det['MJD'], det[mag_col],
                            yerr=[det[mag_err_col].abs(), det[mag_err_col].abs()],
                            fmt='o', linestyle='none', capsize=2, markersize=4,
                            color=color, label=band)
 
            # 3-sigma lines (single explicit band only)
            if single_band and len(det) > 0:
                bd = det[(det['MJD'] >= xlim[0]) & (det['MJD'] <= xlim[1])]
                if len(bd) > 0:
                    mean_mag = bd[mag_col].mean()
                    mean_err = bd[mag_err_col].abs().mean()
                    ax.axhline(mean_mag, color='gray', linestyle='--', linewidth=1.2,
                               label=f'Mean ({mean_mag:.2f})')
                    ax.axhline(mean_mag + 3*mean_err, color='red', linestyle='-.',
                               linewidth=1.0, label=f'+3σ ({mean_mag + 3*mean_err:.2f})')
                    ax.axhline(mean_mag - 3*mean_err, color='blue', linestyle='-.',
                               linewidth=1.0, label=f'−3σ ({mean_mag - 3*mean_err:.2f})')
                    ax.axhspan(mean_mag - 3*mean_err, mean_mag + 3*mean_err,
                               color='gray', alpha=0.08)
 
            # y-limits from detections + ULs (no offsets on per-band plots).
            # Use the error-bar ENDPOINTS (mag +/- err) so a long bar near an
            # edge gets headroom instead of spilling off the axis.
            yv = []
            if len(det) > 0:
                _m = det[mag_col].values
                _e = det[mag_err_col].abs().values
                yv.append(_m + _e)
                yv.append(_m - _e)
            if len(ul) > 0:
                yv.append(ul[ul_mag_col].values)
            if len(ll) > 0:
                yv.append(ll[mag_col].values)
            _finalize_mag_axis(ax, yv)
            _draw_uls_on(ax, [band], offsets={band: 0.0}, label_uls=True)
            _draw_lls_on(ax, [band], offsets={band: 0.0}, label_lls=True)
 
            ax.set_xlim(*xlim)
            ax.set_xlabel('MJD')
            ax.set_ylabel('AB Magnitude')
            ax.set_title(f'{title_stem} — {band.upper()} light curve'
                         + (' + upper limits' if (Upperlimits and len(ul)) else '')
                         + (' + sat. limits' if (Lowerlimits and len(ll)) else ''))
            ax.grid(alpha=0.3)
            ax.grid(which='minor', alpha=0.12)
            if ax.get_legend_handles_labels()[1]:
                ax.legend(fontsize=_FS_LEGEND)
            # top calendar axis + per-band integrated-FLUX right axis.
            _style_axis(ax)
            _add_calendar_top_axis(ax)
            # Per-band integrated-FLUX right axis, always drawn. K is
            # calibrated from this band's detections when available, otherwise
            # from the analytic AB zeropoint, so an upper-limit-only band
            # (no detections) still gets its flux axis.
            if len(det) > 0 and flux_col is not None:
                _bf_mag, _bf_flux = det[mag_col].values, det[flux_col].values
            else:
                _bf_mag, _bf_flux = np.array([]), np.array([])
            _add_bandflux_right_axis(ax, band, _bf_mag, _bf_flux)
            plt.tight_layout()
            if save_prefix:
                fig.savefig(f'{save_prefix}_{band}.png', dpi=200)
            plt.show()
            plt.close(fig)
 
    #####################################################################
    # OFFSET OVERLAY — bands spaced by band_offset_step
    #####################################################################
    if offset_overlay:
        uvot_bands = [b for b in plot_bands if b.lower() != 'ogle']
        # assign an integer offset per band in display order
        offsets = {b: i * band_offset_step for i, b in enumerate(uvot_bands)}
 
        fig, ax = plt.subplots(figsize=(16, 7))
        all_y = []
        for band in uvot_bands:
            off = offsets[band]
            det = _band_det(band)
            color = band_colors.get(band, None)
            if len(det) > 0:
                yv = det[mag_col] + off
                ax.errorbar(det['MJD'], yv,
                            yerr=[det[mag_err_col].abs(), det[mag_err_col].abs()],
                            fmt='o', linestyle='none', capsize=2, markersize=4,
                            color=color, label=f'{band} (+{off:g})')
                _e = det[mag_err_col].abs().values
                all_y.append(yv.values + _e)
                all_y.append(yv.values - _e)
            else:
                # still show a legend entry so the offset is documented
                ax.scatter([], [], color=color, label=f'{band} (+{off:g})')
            ul = _band_ul(band)
            if len(ul) > 0:
                all_y.append((ul[ul_mag_col] + off).values)
            ll = _band_ll(band)
            if len(ll) > 0:
                all_y.append((ll[mag_col] + off).values)
 
        _finalize_mag_axis(ax, all_y)
        _draw_uls_on(ax, uvot_bands, offsets)
        _draw_lls_on(ax, uvot_bands, offsets)
 
        ax.set_xlim(*xlim)
        ax.set_xlabel('MJD')
        ax.set_ylabel(f'AB Magnitude (offset by {band_offset_step:g} mag/band — see legend)')
        ax.set_title(f'{title_stem} bands — artificially offset overlay'
                     + (' + upper limits' if (Upperlimits and not ul_df.empty) else '')
                     + (' + sat. limits' if (Lowerlimits and not ll_df.empty) else ''))
        ax.grid(alpha=0.3)
        ax.grid(which='minor', alpha=0.12)
        leg = ax.legend(ncol=3, title='Band (offset)', fontsize=_FS_LEGEND)
        if leg is not None and leg.get_title() is not None:
            leg.get_title().set_fontsize(_FS_LEGEND)
        # calendar top axis only, per-band offsets make one flux scale meaningless.
        _style_axis(ax)
        _add_calendar_top_axis(ax)
        plt.tight_layout()
        if save_prefix:
            fig.savefig(f'{save_prefix}_offset_overlay.png', dpi=200)
        plt.show()
        plt.close(fig)

    #####################################################################
    # NON-OFFSET OVERLAY 
    #####################################################################
    if overlay_plot:
        fig, ax_mag = plt.subplots(figsize=(16, 6))
        all_y = []
        for band in plot_bands:
            if band == 'OGLE' or band.lower() == 'ogle':
                sub = ogle_df
            else:
                sub = _band_det(band)
            # Plot detections if this band has any (don't `continue` on none
            # the band may be UL-only, and its limits still need to set the axis
            # range, otherwise faint ULs fall off the bottom).
            if sub is not None and len(sub) > 0:
                ax_mag.errorbar(sub['MJD'], sub[mag_col].values,
                                yerr=[np.abs(sub[mag_err_col].values),
                                      np.abs(sub[mag_err_col].values)],
                                fmt='o', linestyle='none', capsize=2,
                                label=band, markersize=4,
                                color=band_colors.get(band, None))
                _m = sub[mag_col].values
                _e = np.abs(sub[mag_err_col].values)
                all_y.append(_m + _e)
                all_y.append(_m - _e)
            # Upper limits ALWAYS feed the y-range (detections or not).
            ulb = _band_ul(band)
            if len(ulb) > 0:
                all_y.append(ulb[ul_mag_col].values)
            # Lower limits (saturated) likewise feed the y-range.
            llb = _band_ll(band)
            if len(llb) > 0:
                all_y.append(llb[mag_col].values)
        _finalize_mag_axis(ax_mag, all_y)
        _draw_uls_on(ax_mag, plot_bands, offsets={b: 0.0 for b in plot_bands},
                     label_uls=True)
        _draw_lls_on(ax_mag, plot_bands, offsets={b: 0.0 for b in plot_bands},
                     label_lls=True)
 
        if not xrt_all.empty:
            ax_xrt = ax_mag.twinx()
            normal = xrt_all[xrt_all['Type'] == 'Normal']
            if not normal.empty:
                ax_xrt.errorbar(
                    normal['MJD (days)'], normal['Count Rate (counts per second)'],
                    yerr=[normal['Count Rate Negative Error'].fillna(0).values,
                          normal['Count Rate Positive Error'].fillna(0).values],
                    fmt='s', linestyle='none', capsize=2, markersize=4,
                    color='black', label='XRT')
            ulx = xrt_all[xrt_all['Type'] == 'UL']
            if not ulx.empty:
                ax_xrt.scatter(ulx['MJD (days)'], ulx['Count_Rate_UL'],
                               marker='v', facecolors='none', edgecolors='black',
                               s=60, label='XRT UL')
            ax_xrt.set_ylabel('Count Rate (counts/s)')
            h1, l1 = ax_mag.get_legend_handles_labels()
            h2, l2 = ax_xrt.get_legend_handles_labels()
            by_label = dict(zip(l1 + l2, h1 + h2))
            if by_label:
                leg = ax_mag.legend(by_label.values(), by_label.keys(),
                                    ncol=3, title='Band/Series', fontsize=_FS_LEGEND)
                if leg is not None and leg.get_title() is not None:
                    leg.get_title().set_fontsize(_FS_LEGEND)
        else:
            if ax_mag.get_legend_handles_labels()[1]:
                leg = ax_mag.legend(ncol=3, title='Band', fontsize=_FS_LEGEND)
                if leg is not None and leg.get_title() is not None:
                    leg.get_title().set_fontsize(_FS_LEGEND)
 
        ax_mag.set_xlabel('MJD')
        ax_mag.set_ylabel('AB Magnitude')
        ax_mag.set_xlim(*xlim)
        ax_mag.set_title(f'{title_stem}'
                         + (' + upper limits' if (Upperlimits and not ul_df.empty) else '')
                         + (' + sat. limits' if (Lowerlimits and not ll_df.empty) else ''))
        ax_mag.grid(alpha=0.3)
        ax_mag.grid(which='minor', alpha=0.12)
        # calendar top axis always, f_nu right axis only when XRT isn't already
        # using the right side (band-independent AB flux density from AB_MAG).
        _style_axis(ax_mag)
        if not xrt_all.empty:
            ax_xrt.tick_params(axis='both', which='major', labelsize=_FS_TICK)
            ax_xrt.yaxis.label.set_size(_FS_LABEL)
        _add_calendar_top_axis(ax_mag)
        if xrt_all.empty:
            _add_fnu_right_axis(ax_mag)
        plt.tight_layout()
        if save_prefix:
            fig.savefig(f'{save_prefix}_overlay.png', dpi=200)
        plt.show()
        plt.close(fig)







################################################################################
# RUN-MODE ADDITION —  All-frames 
################################################################################

# Global default for the per-exposure ("All-frames") pass. A per-target CSV
# 'AllFrames' column overrides this when present. Mixed ALWAYS runs, this only
# adds the extra per-exposure pass on top.
RUN_ALLFRAMES = False

# Significance threshold for the per-exposure pass. uvotsource is run at this
# sigma, and a row is a DETECTION only if its measured NSIGMA >= this; below it
# the exposure is a non-detection and becomes an upper limit at AB_MAG_LIM.
# (uvotsource does NOT return AB_MAG=99 for a faint non-detection, it returns a
# low-significance magnitude and reports the real significance in NSIGMA, so
# NSIGMA, not AB_MAG==99, is the correct detection test.)
ALLFRAMES_SIGMA = 3.0

_MODE_SPEC = {
    'mixed':     {'subdir': 'Mixed',        'suffix': ''},
    'allframes': {'subdir': 'AllFrames',    'suffix': '_allframes'},
    'timeavg':   {'subdir': 'TimeAveraged', 'suffix': '_timeavg'},
}


def _mode_output_dir(save_path, mode):
    """Return (subdir_path, filename_suffix) for a run mode, creating the dir."""
    spec = _MODE_SPEC[mode]
    d = os.path.join(save_path, spec['subdir'])
    os.makedirs(d, exist_ok=True)
    return d, spec['suffix']
 
 
def _build_uvotsource_command(image, srcreg, bkgreg, expfile, outfile,
                              sigma=5, cd_dir=None):
    """
    The new uvotsource command string, shared by 
    All-frames, and later Time-Averaged so the flags
    can never drift apart.
 
    cd_dir given  -> NORMAL mode: prefixes "cd <dir> &&"; image/regions/outfile
                     are RELATIVE names resolved inside cd_dir.
    cd_dir None   -> SPLIT mode (dataInit): image/srcreg/bkgreg/expfile/outfile
                     must already be ABSOLUTE, prepare_path()'d strings.
    """
    core = (f"uvotsource image='{image}' "
            f"srcreg='{srcreg}' bkgreg='{bkgreg}' sigma={sigma} "
            f"expfile='{expfile}' "
            f"zerofile=CALDB coinfile=CALDB psffile=CALDB lssfile=CALDB "
            f"syserr=NO frametime=DEFAULT apercorr=NONE output=ALL "
            f"outfile='{outfile}' cleanup=YES clobber=YES chatter=1 "
            f"mode=h < /dev/null")
    if cd_dir is not None:
        cd = prepare_path(cd_dir) if HEASOFT_BACKEND == "wsl" else cd_dir
        return f"cd '{cd}' && {core}"
    return core
 
 
def _truthy(v):
    """the truth test for a SATURATED / limit flag across the three forms it
    can apparently arrive in: a bool (direct astropy access), int8 84/70 — ASCII
    'T'/'F' which is how a FITS logical 'L' column comes through the
    whole record byteswap path the compiler uses, and a string ('True'/'T'/'1')
    after a CSV trip. Plain str() would wrongly read int8 84 as false."""
    if isinstance(v, (bool, np.bool_)):
        return bool(v)
    if isinstance(v, (int, np.integer)):
        return int(v) in (1, 84)           # 84 == ord('T'); 0/70 == False
    if isinstance(v, (bytes, bytearray)):
        return v.strip().lower() in (b't', b'true', b'1', b'yes')
    return str(v).strip().lower() in ('true', '1', 'yes', 't')
 
 
def _finalsource_is_detection(fpath):
    """True only if a finalsource FITS holds a REAL detection (AB_MAG present,
    finite, not 99). The main uvotsource pass writes a finalsource even for a
    non-detection (AB_MAG=99) whenever a source region existed, such a file is
    NOT a detection, so the Mixed upper-limit pass should still compute a limit
    for it instead of skipping, otherwise that obsid/band falls into a crack
    (the 99 row is dropped at compile, and the UL pass skips because a
    finalsource exists) and appears in neither Mixed list. Returns False on any
    read error so the caller errs toward computing a limit."""
    try:
        with fits.open(fpath) as hdul:
            if len(hdul) < 2 or hdul[1].data is None or len(hdul[1].data) == 0:
                return False
            ab = float(hdul[1].data['AB_MAG'][0])
            return bool(np.isfinite(ab) and ab != 99.0)
    except Exception:
        return False
 
 
def _compile_and_plot_mode(obs_table, image_dirs, save_path, target,
                           output_root, mode, derive_flags=False, override_dirs=None):
    """
    Compile the finalsource FITS for ONE run mode into
    <save_path>/<ModeSubdir>/master_photometry{suffix}{tag}.txt and draw its light curves.

    derive_flags=False (Mixed): UpperLimit comes from the *_ul filename; rows
        with AB_MAG==99 (and not UL) are dropped as bad data. Saturation
        LowerLimit rides along from the SATURATED column.
    derive_flags=True (All-frames): each row's flags are decided from its OWN
        values, SATURATED -> lower limit; AB_MAG 99/non-finite -> upper limit
        (value from AB_MAG_LIM); otherwise a detection. The per-exposure
        snapshot number is parsed from the filename into a SNAPSHOT column.
    """
    split_mode = output_root is not None
    tlabel = f" [{target}]" if target else ""
    out_dir, suffix = _mode_output_dir(save_path, mode)
    tag = f"_{target}" if target else ""
 
    print("=" * 70)
    print(f"COMPILING PHOTOMETRY{tlabel} — mode={mode}")
    print("=" * 70)
 
    if mode == 'mixed':
        fs_suffix = (f"_finalsource_{target}.fits" if target else "_finalsource.fits")
        ul_suffix = (f"_finalsource_ul_{target}.fits" if target else "_finalsource_ul.fits")
        af_token = None
    else:  # allframes / timeavg share this branch (filename token differs)
        fs_suffix = (f"_{target}.fits" if target else ".fits")
        ul_suffix = None
        af_token = f"_finalsource{suffix}_ext"
 
    # where finalsource files live
    comp_dirs = []
    if override_dirs is not None:
        # Time-averaged mode: outputs live in ONE dedicated co-add dir, not
        # in per-obsid dirs, the caller hands (label, dir) pairs in directly.
        comp_dirs = list(override_dirs)
    elif split_mode:
        for obsid, _img in image_dirs:
            d = os.path.join(output_root, str(obsid))
            if os.path.isdir(d):
                comp_dirs.append((obsid, d))
    else:
        if obs_table is not None and not obs_table.empty and 'Full_Path' in obs_table.columns:
            _seen = set()
            for _, row in obs_table.iterrows():
                fp = row['Full_Path']
                if not isinstance(fp, str) or not fp:
                    continue
                d = os.path.dirname(fp)
                parts = set(os.path.normpath(d).replace("\\", "/").split("/"))
                if parts & {"Smeared", "NotASPCORR", "Orphans"}:
                    continue
                if d in _seen or not os.path.isdir(d):
                    continue
                _seen.add(d)
                m = re.search(r"(\d{11})", d)
                comp_dirs.append((m.group(1) if m else "UNKNOWN", d))
 
    # build the (obsid, filepath, band, is_ul, snapshot) job list
    comp_jobs = []
    for obsid, root_dir in comp_dirs:
        try:
            entries = os.listdir(root_dir)
        except OSError:
            continue
        for f in entries:
            band_match = re.match(r"([a-z0-9]+)_finalsource", f)
            if not band_match:
                continue
            band = band_match.group(1)
            if band not in BANDS:
                continue
            snapshot = None
            forced_ul = False
            if mode == 'mixed':
                is_ul = f.endswith(ul_suffix)
                is_det = f.endswith(fs_suffix) and not f.endswith(ul_suffix)
                if not (is_ul or is_det):
                    continue
            else:
                if af_token not in f or not f.endswith(fs_suffix):
                    continue
                mext = re.search(r"_ext(\d+)", f)
                snapshot = int(mext.group(1)) if mext else None
                # A '_lim' token right after the ext number means Mixed found NO
                # point source for this obsid (no source region), so every
                # exposure is a forced upper limit regardless of NSIGMA, this
                # is what keeps extended-galaxy contamination from registering
                # as detections, matching the Mixed's way.
                forced_ul = re.search(r"_ext\d+_lim(?:_|\.)", f) is not None
                is_ul = False  # decided per row at read time
            comp_jobs.append((obsid, os.path.join(root_dir, f), band, is_ul,
                              snapshot, forced_ul))
 
    def _read_one(obsid, filepath, band, is_ul, snapshot, forced_ul=False):
        try:
            with fits.open(filepath) as hdul:
                if len(hdul) < 2 or hdul[1].data is None:
                    return None
                # FITS is big-endian, convert to native (numpy 1.x & 2.x safe).
                _arr = np.asarray(hdul[1].data)
                _arr = _arr.view(_arr.dtype.newbyteorder()).byteswap()
                df = pd.DataFrame(_arr)
            if 'EXTNAME' in df.columns:
                df.drop(columns=['EXTNAME'], inplace=True)
            df["OBSID"] = obsid
            df["BAND"] = band
            df["SOURCE_FILE"] = filepath
            if snapshot is not None:
                df["SNAPSHOT"] = snapshot
            if target:
                df["TARGET"] = target
 
            abmag = (pd.to_numeric(df['AB_MAG'], errors='coerce')
                     if 'AB_MAG' in df.columns
                     else pd.Series(np.nan, index=df.index))
 
            if derive_flags:
                # All-frames, classify per row from the exposure's OWN values.
                # A faint non-detection comes back with a low NSIGMA (not just
                # AB_MAG=99), so NSIGMA is the new detection test, AB_MAG==99 /
                # non-finite is the hard bad-pixel non-detection. Either way the
                # value reported is AB_MAG_LIM. Saturated frames are bright (lower) limits.
                nsig = (pd.to_numeric(df['NSIGMA'], errors='coerce')
                        if 'NSIGMA' in df.columns
                        else pd.Series(np.nan, index=df.index))
                sat = (df['SATURATED'].apply(_truthy)
                       if 'SATURATED' in df.columns
                       else pd.Series(False, index=df.index))
                hard_nondet = (abmag == 99.0) | (~np.isfinite(abmag))
                low_sig = (~np.isfinite(nsig)) | (nsig < ALLFRAMES_SIGMA)
                if forced_ul:
                    # Mixed found no point source for this obsid, every exposure
                    # is an upper limit, no matter how significant the (extended
                    # galaxy) flux looks. NSIGNMA is meaningless here. IT HOLDS NO POWER HERE.
                    df["LowerLimit"] = pd.Series(False, index=df.index)
                    df["UpperLimit"] = pd.Series(True, index=df.index)
                else:
                    df["LowerLimit"] = sat & (~hard_nondet)
                    df["UpperLimit"] = (~df["LowerLimit"]) & (low_sig | hard_nondet)
                if 'AB_MAG_LIM' in df.columns:
                    lim = pd.to_numeric(df['AB_MAG_LIM'], errors='coerce')
                    df["PLOT_MAG"] = np.where(df["UpperLimit"], lim, abmag)
                else:
                    df["PLOT_MAG"] = abmag
            else:
                df["UpperLimit"] = is_ul
                if is_ul and 'AB_MAG_LIM' in df.columns:
                    df["PLOT_MAG"] = df["AB_MAG_LIM"]
                elif 'AB_MAG' in df.columns:
                    df["PLOT_MAG"] = df["AB_MAG"]
                if 'SATURATED' in df.columns:
                    df["LowerLimit"] = df['SATURATED'].apply(_truthy) & (~bool(is_ul))
                else:
                    df["LowerLimit"] = False
            return df
        except Exception as e:
            return {'_error': f"  Error reading {filepath}: {e}"}
 
    all_rows = []
    if comp_jobs:
        results_by_idx = {}
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_idx = {executor.submit(_read_one, *job): i
                             for i, job in enumerate(comp_jobs)}
            for fut in as_completed(future_to_idx):
                idx = future_to_idx[fut]
                try:
                    results_by_idx[idx] = fut.result()
                except Exception as e:
                    results_by_idx[idx] = {'_error': f"  compile worker crashed: {e}"}
        for i in range(len(comp_jobs)):
            res = results_by_idx.get(i)
            if res is None:
                continue
            if isinstance(res, dict) and '_error' in res:
                print(res['_error'])
                continue
            all_rows.append(res)
 
    if all_rows:
        df_all = pd.concat(all_rows, ignore_index=True)
        nan_cols = [c for c in df_all.columns if df_all[c].isna().all()]
        if nan_cols:
            df_all.drop(columns=nan_cols, inplace=True)
    else:
        df_all = pd.DataFrame()
 
    # Mixed drops AB_MAG==99 non-UL rows (bad data). All-frames already turned
    # those into upper limits above, so the drop is skipped there.
    if not df_all.empty and not derive_flags:
        is_ul_s = (df_all['UpperLimit'] == True if 'UpperLimit' in df_all.columns
                   else pd.Series(False, index=df_all.index))
        mask = pd.Series(False, index=df_all.index)
        if 'AB_MAG' in df_all.columns:
            mask |= (df_all['AB_MAG'] == 99.0) | (~np.isfinite(df_all['AB_MAG']))
        if 'AB_MAG_ERR' in df_all.columns:
            mask |= (df_all['AB_MAG_ERR'] == 99.0)
        mask &= ~is_ul_s
        if int(mask.sum()) > 0:
            df_all = df_all.loc[~mask].reset_index(drop=True)
 
    if df_all.empty:
        print(f" No photometry to write{tlabel} (mode={mode}).")
        return df_all
 
    txt_path = os.path.join(out_dir, f"master_photometry{suffix}{tag}.txt")
    df_all.to_csv(txt_path, sep='\t', index=False)
    print(f" Master photometry saved: {txt_path}  ({len(df_all)} rows)")
    if WRITE_CSV_COPY:
        df_all.to_csv(os.path.join(out_dir, f"master_photometry{suffix}{tag}.csv"),
                      index=False)
 
    try:
        if mode == 'timeavg':
            # ONE co-added point per band, a time axis is meaningless here.
            # Plot magnitude vs wavelength (SED-style) instead of a light curve.
            plot_uvot_timeavg_sed(
                excel_file=txt_path,
                save_prefix=os.path.join(out_dir, f"sed{suffix}{tag}"),
                target=target)
        else:
            if 'MJD' in df_all.columns:
                _mjd = pd.to_numeric(df_all['MJD'], errors='coerce').dropna()
            elif 'TSTART' in df_all.columns:
                _mjd = pd.to_numeric(df_all['TSTART'], errors='coerce').dropna() / 86400.0 + 51910.0
            else:
                _mjd = None
            plot_xlim = ((float(_mjd.min()) - 50, float(_mjd.max()) + 50)
                         if _mjd is not None and len(_mjd) else (53000, 62000))
            for sub, want_ul in (("no_ul", False), ("with_ul", True)):
                try:
                    plot_uvot_lightcurves(
                        excel_file=txt_path, xlim=plot_xlim,
                        ogle_file=None, xrt_files=None,
                        overlay_plot=True, stacked_plot=True,
                        save_prefix=os.path.join(out_dir, f"lightcurve{suffix}{tag}_{sub}"),
                        Upperlimits=want_ul, target=target)
                except Exception as e:
                    print(f"  light curve '{sub}' failed: {e}")
    except Exception as e:
        print(f" auto plotting step failed: {e}")
    return df_all
 
 
def run_allframes_for_target(obs_table, base_path, save_path, image_dirs,
                             src_reg_name, bkg_reg_name, target=None,
                             output_root=None, target_ra=None, target_dec=None):
    """
    ALL-FRAMES (per-exposure) photometry. Runs AFTER the Mixed pass has written
    the source/background regions. Re-photometers EACH good exposure (image
    extension) of every raw SK individually we are reusing the
    obsid's existing source region and the target background region (the WCS is
    aspect-corrected, so the sky position is stable exposure-to-exposure).
    """
    if not image_dirs:
        return None
    split_mode = output_root is not None
    tlabel = f" [{target}]" if target else ""
    suffix = _MODE_SPEC['allframes']['suffix']
 
    print("=" * 70)
    print(f"ALL-FRAMES (per-exposure) PHOTOMETRY{tlabel}")
    print("=" * 70)
 
    def _af_one_observation(obsid, img_dir):
        counts = dict(processed=0, skipped=0, failed=0)
        lines = []
        def log(msg): lines.append(msg)
        write_dir = _obsid_write_dir(output_root, img_dir, obsid)
        src_reg_path = os.path.join(write_dir, src_reg_name)
        bkg_reg_path = os.path.join(write_dir, bkg_reg_name)
        if not os.path.exists(bkg_reg_path):
            return {'obsid': obsid, 'counts': counts, 'log': lines}
        have_src = os.path.exists(src_reg_path)
 
        for band in BANDS:
            sk_img = f"sw{obsid}{band}_sk.img"
            sk_gz = f"sw{obsid}{band}_sk.img.gz"
            if os.path.exists(os.path.join(img_dir, sk_img)):
                sk_name = sk_img
            elif os.path.exists(os.path.join(img_dir, sk_gz)):
                sk_name = sk_gz
            else:
                continue
            sk_path = os.path.join(img_dir, sk_name)
 
            good = _resolve_good_extensions(sk_path, obsid, band, obs_table)['good']
            if not good:
                continue
 
            ex_img = f"sw{obsid}{band}_ex.img"
            ex_gz = f"sw{obsid}{band}_ex.img.gz"
            if os.path.exists(os.path.join(img_dir, ex_img)):
                ex_name = ex_img
            elif os.path.exists(os.path.join(img_dir, ex_gz)):
                ex_name = ex_gz
            else:
                ex_name = None
 
            # source region, reuse the obsid's region if Mixed wrote one, else
            # fall back to a target-position region so the exposure still yields something, a limit
            if have_src:
                src_use_name, src_use_path = src_reg_name, src_reg_path
            else:
                if target_ra is None or target_dec is None:
                    continue
                src_use_name = (f"auto_source_af_{target}.reg" if target
                                else "auto_source_af.reg")
                src_use_path = os.path.join(write_dir, src_use_name)
                if not os.path.exists(src_use_path):
                    with open(src_use_path, 'w') as fh:
                        fh.write('# Region file format: DS9 version 4.1\nfk5\n'
                                 f'circle({target_ra},{target_dec},5.000")\n')
 
            for ext in good:
                # No Mixed source region for this obsid -> mark every exposure
                # as a forced upper limit ('_lim'), so the compile step reports
                # limits instead of letting extended-galaxy flux read as a
                # detection (defers to Mixed's point-source verdict).
                lim_tok = "" if have_src else "_lim"
                out_name = (f"{band}_finalsource{suffix}_ext{ext:02d}{lim_tok}"
                            + (f"_{target}.fits" if target else ".fits"))
                out_path = os.path.join(write_dir, out_name)
                if os.path.exists(out_path):
                    counts['skipped'] += 1
                    continue
 
                if not split_mode:
                    cmd = _build_uvotsource_command(
                        image=f"{sk_name}[{ext}]", srcreg=src_use_name,
                        bkgreg=bkg_reg_name,
                        expfile=(f"{ex_name}[{ext}]" if ex_name else "NONE"),
                        outfile=out_name, sigma=ALLFRAMES_SIGMA, cd_dir=img_dir)
                else:
                    exp_abs = (prepare_path(os.path.join(img_dir, ex_name)) + f"[{ext}]"
                               if ex_name else "NONE")
                    cmd = _build_uvotsource_command(
                        image=prepare_path(sk_path) + f"[{ext}]",
                        srcreg=prepare_path(src_use_path),
                        bkgreg=prepare_path(bkg_reg_path),
                        expfile=exp_abs,
                        outfile=prepare_path(out_path), sigma=ALLFRAMES_SIGMA, cd_dir=None)
 
                run_heasoft_command(cmd, quiet=True)
                time.sleep(0.5)
                if os.path.exists(out_path):
                    counts['processed'] += 1
                    log(f"\u2705 {out_name}")
                else:
                    counts['failed'] += 1
                    log(f"\u274c all-frames uvotsource no output {obsid}/{band} ext{ext}")
        return {'obsid': obsid, 'counts': counts, 'log': lines}
 
    processed = skipped = failed = 0
    print(f"Running per-exposure uvotsource across up to {MAX_WORKERS} obs...\n")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_af_one_observation, obsid, img_dir): obsid
                   for obsid, img_dir in image_dirs}
        for fut in tqdm(as_completed(futures), total=len(futures),
                        desc="All-frames uvotsource", unit="obs"):
            oid = futures[fut]
            WATCHDOG.beat(f"All-frames: finished obsid {oid}")
            try:
                res = fut.result()
            except Exception as e:
                tqdm.write(f"\u274c all-frames worker crashed: {e}")
                continue
            c = res['counts']
            processed += c['processed']; skipped += c['skipped']; failed += c['failed']
            if res['log']:
                tqdm.write("\n".join(res['log']))
    print(f"\nALL-FRAMES SUMMARY{tlabel}: processed {processed}, "
          f"skipped {skipped}, failed {failed}\n")
 
    return _compile_and_plot_mode(
        obs_table=obs_table, image_dirs=image_dirs, save_path=save_path,
        target=target, output_root=output_root, mode='allframes',
        derive_flags=True)


# =============================================================================
# TIME-AVERAGED ("All summed")
# =============================================================================

# Global default for the time-averaged (co-added) pass. A per-target CSV
# 'TimeAvg' column overrides this when present.
RUN_TIMEAVG = False

# FRAMTIME consistency tolerance for the co-add, same value the summation
# step uses. Exposures whose frame time differs by more than this from the
# dominant group are dropped (uvotimsum shouldn't mix frame times).
TIMEAVG_FRAMETIME_TOL = 0.0004

# When Mixed found NO point source, the deep co-add gets its OWN detection
# attempt (a previously-invisible star can surface in the stack). These currently match
# the Mixed pass's criteria, uvotdetect at this threshold, and a source counts
# as "the target" if it centroids within this many arcsec of the target.
TIMEAVG_DETECT_THRESHOLD = 3.0
TIMEAVG_MAX_OFFSET_ARCSEC = 10.0


def run_timeavg_for_target(obs_table, base_path, save_path, image_dirs,
                           src_reg_name, bkg_reg_name, target=None,
                           output_root=None, target_ra=None, target_dec=None):
    """
    TIME-AVERAGED photometry: co-add EVERY good exposure of the target's
    obsids into ONE deep image per band, then run uvotsource once on it.

    "Good" is exactly _resolve_good_extensions' definition — ASPCORR
    DIRECT/UNICORR, not SSS-flagged, not smeared. so nothing bad ever
    enters the co-add (proably). Runs AFTER the Mixed pass (reuses its source and
    background regions). If Mixed found NO point source for the target, the
    deep point is photometered at the target position and force-flagged as
    an upper limit via the '_lim' filename token (the same extended-source
    guard All-frames uses), so bright galaxy flux can't read as a detection.
    """
    if not image_dirs:
        return None
    tlabel = f" [{target}]" if target else ""
    suffix = _MODE_SPEC['timeavg']['suffix']
    tag = f"_{target}" if target else ""

    print("=" * 70)
    print(f"TIME-AVERAGED (co-added) PHOTOMETRY{tlabel}")
    print("=" * 70)

    # work dir: masters, deep images and finalsource outputs live here
    if output_root is not None:
        work_dir = os.path.join(output_root, "TimeAvg")
    else:
        work_dir = os.path.join(save_path, _MODE_SPEC['timeavg']['subdir'],
                                "coadd")
    os.makedirs(work_dir, exist_ok=True)

    # regions: reuse the Mixed pass's per-target regions
    # Every obsid write_dir holds a copy of the same region content, so the
    # first one found is copied into the work dir.
    src_found = bkg_found = None
    for obsid, img_dir in image_dirs:
        wdir = _obsid_write_dir(output_root, img_dir, obsid)
        if bkg_found is None:
            p = os.path.join(wdir, bkg_reg_name)
            if os.path.exists(p):
                bkg_found = p
        if src_found is None:
            p = os.path.join(wdir, src_reg_name)
            if os.path.exists(p):
                src_found = p
        if src_found and bkg_found:
            break
    if bkg_found is None:
        print(" No background region found (Mixed pass must run first) — "
              "skipping time-averaged pass.")
        return None
    bkg_path = os.path.join(work_dir, bkg_reg_name)
    if os.path.abspath(bkg_found) != os.path.abspath(bkg_path):
        shutil.copy(bkg_found, bkg_path)
 
    have_src = src_found is not None
    if have_src:
        src_path = os.path.join(work_dir, src_reg_name)
        if os.path.abspath(src_found) != os.path.abspath(src_path):
            shutil.copy(src_found, src_path)
    else:
        # No Mixed source region: the deep image gets its own detection
        # attempt per band. Target coords are required for that.
        if target_ra is None or target_dec is None:
            print(" No source region and no target coords — skipping.")
            return None
        src_path = None
        print(" No Mixed source region — will re-detect on each band's deep "
              "co-add (a dim source can surface in the stack).")

    def _grab_image_ext(path, ext_num):
        """(data, header) of the ext_num-th IMAGE extension (counting only
        NAXIS>=2 HDUs, 1-based) the same numbering _resolve_good_extensions
        uses, so 'good' ext numbers map back to the right HDU."""
        with fits.open(path) as hdul:
            en = 0
            for hdu in hdul:
                if hdu.header.get('NAXIS', 0) < 2:
                    continue
                en += 1
                if en == ext_num:
                    return np.array(hdu.data), hdu.header.copy()
        return None, None

    
    def _tavg_one_band(band):
        """Co-add + photometer ONE band. Bands are fully independent (all
        filenames carry the band), so they run in parallel safely."""
        lines = []
        log = lines.append
        res = {'band': band, 'ok': False, 'skipped': False, 'n_frames': 0, 'log': lines}
 
        out_det_name = f"{band}_finalsource{suffix}_ext00{tag}.fits"
        out_lim_name = f"{band}_finalsource{suffix}_ext00_lim{tag}.fits"
        # rerun: either variant already existing means this band is done
        for nm in (out_det_name, out_lim_name):
            if os.path.exists(os.path.join(work_dir, nm)):
                res['ok'] = True
                res['skipped'] = True
                return res
 
        # gather every good exposure of this band across all obsids --
        frames = []   # {'sk','ex','ext','obsid','ft','exp'}
        for obsid, img_dir in image_dirs:
            sk = None
            for cand in (f"sw{obsid}{band}_sk.img",
                         f"sw{obsid}{band}_sk.img.gz"):
                p = os.path.join(img_dir, cand)
                if os.path.exists(p):
                    sk = p
                    break
            if sk is None:
                continue
            good = _resolve_good_extensions(sk, obsid, band, obs_table)['good']
            if not good:
                continue
            ex = None
            for cand in (f"sw{obsid}{band}_ex.img",
                         f"sw{obsid}{band}_ex.img.gz"):
                p = os.path.join(img_dir, cand)
                if os.path.exists(p):
                    ex = p
                    break
            try:
                with fits.open(sk) as hdul:
                    en = 0
                    for hdu in hdul:
                        if hdu.header.get('NAXIS', 0) < 2:
                            continue
                        en += 1
                        if en in good:
                            ft = hdu.header.get('FRAMTIME', None)
                            frames.append({
                                'sk': sk, 'ex': ex, 'ext': en,
                                'obsid': str(obsid),
                                'ft': (float(ft) if ft is not None else None),
                                'exp': float(hdu.header.get('EXPOSURE', 0.0)),
                            })
            except Exception as e:
                log(f"  {band}: could not read {os.path.basename(sk)} "
                    f"({str(e)[:120]})")
        if not frames:
            return res   # this band simply has no good data, no plot entry

        # FRAMTIME consistency (same rule as summation)
        # Group by frame time within tolerance; keep the dominant group
        # (most exposures, then most total exposure time).
        groups = []
        no_ft = [fr for fr in frames if fr['ft'] is None]
        for fr in frames:
            if fr['ft'] is None:
                continue
            placed = False
            for g in groups:
                if abs(fr['ft'] - g['ft']) <= TIMEAVG_FRAMETIME_TOL:
                    g['frames'].append(fr)
                    g['exp'] += fr['exp']
                    placed = True
                    break
            if not placed:
                groups.append({'ft': fr['ft'], 'frames': [fr],
                               'exp': fr['exp']})
        if len(groups) > 1:
            groups.sort(key=lambda g: (len(g['frames']), g['exp']),
                        reverse=True)
            dropped = sum(len(g['frames']) for g in groups[1:]) + len(no_ft)
            log(f"  {band}: mixed FRAMTIME — keeping {groups[0]['ft']:.5f}s "
                f"({len(groups[0]['frames'])} exposure(s)), "
                f"dropping {dropped}")
            frames = groups[0]['frames']
        elif groups:
            frames = groups[0]['frames'] + no_ft
        res['n_frames'] = len(frames)

        # build the stacked master SK (+ matching expmap master)
        # copy each good IMAGE extension (data + full header,
        # so WCS/FRAMTIME/EXPOSURE all ride along) into one multi-extension
        # file. EXTNAMEs are deduplicated (uvotimsum-safe).
        master_sk = os.path.join(work_dir, f"tavg_master_{band}{tag}.img")
        master_ex = os.path.join(work_dir, f"tavg_master_{band}{tag}_ex.img")
        summed_sk_name = f"{band}_timeavg_summed{tag}.fits"
        summed_ex_name = f"{band}_timeavg_expmap{tag}.fits"
        summed_sk = os.path.join(work_dir, summed_sk_name)
        summed_ex = os.path.join(work_dir, summed_ex_name)
 
        have_all_ex = all(fr['ex'] is not None for fr in frames)
        try:
            with fits.open(frames[0]['sk']) as h0:
                prim_hdr = h0[0].header.copy()
            sk_hdus = [fits.PrimaryHDU(header=prim_hdr)]
            ex_hdus = [fits.PrimaryHDU(header=prim_hdr.copy())]
            seen_names = set()
            for i, fr in enumerate(frames, 1):
                d, h = _grab_image_ext(fr['sk'], fr['ext'])
                if d is None:
                    raise ValueError(f"image ext {fr['ext']} not found in "
                                     f"{os.path.basename(fr['sk'])}")
                nm = str(h.get('EXTNAME', '')).strip() or f"TAVG{i:03d}"
                if nm in seen_names:              # EXTNAMEs must stay unique
                    nm = f"{nm}_{i}"
                seen_names.add(nm)
                h['EXTNAME'] = nm
                sk_hdus.append(fits.ImageHDU(data=d, header=h))
                if have_all_ex:
                    de, he = _grab_image_ext(fr['ex'], fr['ext'])
                    if de is None:
                        have_all_ex = False       # ex file too short? bail
                    else:
                        he['EXTNAME'] = nm
                        ex_hdus.append(fits.ImageHDU(data=de, header=he))
            fits.HDUList(sk_hdus).writeto(master_sk, overwrite=True)
            if have_all_ex:
                fits.HDUList(ex_hdus).writeto(master_ex, overwrite=True)
        except Exception as e:
            log(f" ❌ {band}: master build failed ({str(e)[:160]})")
            return res
        if not have_all_ex:
            log(f"  {band}: exposure map incomplete — running with " f"expfile=NONE")

        # co-add via uvotimsum (a single exposure skips the sum)
        wd = prepare_path(work_dir) if HEASOFT_BACKEND == "wsl" else work_dir
        if len(frames) > 1:
            cmd = (f"cd '{wd}' && uvotimsum "
                   f"infile='{os.path.basename(master_sk)}' "
                   f"outfile='{summed_sk_name}' exclude=NONE "
                   f"clobber=YES mode=h < /dev/null")
            run_heasoft_command(cmd, quiet=True)
            time.sleep(0.5)
            if not os.path.exists(summed_sk):
                log(f" ❌ {band}: uvotimsum failed on the co-add")
                return res
            img_use = summed_sk_name
            exp_use = "NONE"
            if have_all_ex:
                cmd = (f"cd '{wd}' && uvotimsum "
                       f"infile='{os.path.basename(master_ex)}' "
                       f"outfile='{summed_ex_name}' method=EXPMAP "
                       f"exclude=NONE clobber=YES mode=h < /dev/null")
                run_heasoft_command(cmd, quiet=True)
                time.sleep(0.5)
                if os.path.exists(summed_ex):
                    exp_use = summed_ex_name
        else:
            img_use = f"{os.path.basename(master_sk)}[1]"
            exp_use = (f"{os.path.basename(master_ex)}[1]"
                       if have_all_ex else "NONE")

        # source region for the DEEP image
        if have_src:
            # Mixed already located the point source, reuse its region.
            src_use = os.path.basename(src_path)
            lim_tok = ""
        else:
            # Mixed saw nothing in the shallow data, but the co-add is much
            # deeper (proably), re-detect HERE before assuming an upper limit. Same
            # criteria as the Mixed pass: uvotdetect, source counts if it
            # centroids within TIMEAVG_MAX_OFFSET_ARCSEC of the target.
            det_name = f"{band}_timeavg_detect{tag}.fits"
            det_path = os.path.join(work_dir, det_name)
            cmd = (f"cd '{wd}' && uvotdetect infile='{img_use}' "
                   f"outfile='{det_name}' expfile=NONE "
                   f"threshold={TIMEAVG_DETECT_THRESHOLD} clobber=YES "
                   f"mode=h < /dev/null")
            run_heasoft_command(cmd, quiet=True)
            time.sleep(0.5)
 
            hit = None   # (ra, dec, sep_arcsec) of the recovered source
            if os.path.exists(det_path):
                try:
                    ras = decs = None
                    with fits.open(det_path) as hdul:
                        if (len(hdul) >= 2 and hdul[1].data is not None
                                and len(hdul[1].data) > 0):
                            ras = np.array(hdul[1].data['RA'], dtype=float)
                            decs = np.array(hdul[1].data['DEC'], dtype=float)
                    if ras is not None and len(ras):
                        cat = SkyCoord(ras, decs, unit='deg', frame='fk5')
                        tgt = SkyCoord(target_ra, target_dec, unit='deg', frame='fk5')
                        seps = tgt.separation(cat).arcsecond
                        i = int(np.argmin(seps))
                        if seps[i] <= TIMEAVG_MAX_OFFSET_ARCSEC:
                            hit = (float(ras[i]), float(decs[i]),
                                   float(seps[i]))
                except Exception as e:
                    log(f"  {band}: deep detect catalog unreadable "
                        f"({str(e)[:100]}) — treating as non-detection")
 
            band_src_name = f"auto_source_tavg_{band}{tag}.reg"
            band_src_path = os.path.join(work_dir, band_src_name)
            if hit is not None:
                with open(band_src_path, 'w') as fh:
                    fh.write('# Region file format: DS9 version 4.1\n'
                             '# Source RECOVERED on the time-averaged co-add\n'
                             'fk5\n'
                             f'circle({hit[0]},{hit[1]},5.000")\n')
                lim_tok = ""
                log(f"  {band}: source RECOVERED on the deep co-add "
                    f"({hit[2]:.1f}\" from target) — measuring normally")
            else:
                with open(band_src_path, 'w') as fh:
                    fh.write('# Region file format: DS9 version 4.1\n'
                             '# No source on the deep co-add — target position\n'
                             'fk5\n'
                             f'circle({target_ra},{target_dec},5.000")\n')
                lim_tok = "_lim"
                log(f"  {band}: still no source within "
                    f"{TIMEAVG_MAX_OFFSET_ARCSEC:.0f}\" on the DEEP image — "
                    f"forced upper limit")
            src_use = band_src_name
 
        out_name = out_lim_name if lim_tok else out_det_name
        out_path = os.path.join(work_dir, out_name)
 
        # ONE uvotsource on the deep image 
        # sigma = ALLFRAMES_SIGMA so the run and the compile-time NSIGMA
        # classification (which uses ALLFRAMES_SIGMA) can never disagree.
        cmd = _build_uvotsource_command(
            image=img_use, srcreg=src_use,
            bkgreg=os.path.basename(bkg_path), expfile=exp_use,
            outfile=out_name, sigma=ALLFRAMES_SIGMA, cd_dir=work_dir)
        run_heasoft_command(cmd, quiet=True)
        time.sleep(0.5)
        if os.path.exists(out_path):
            log(f" ✅ {band}: {len(frames)} exposure(s) co-added → {out_name}")
            res['ok'] = True
        else:
            log(f" ❌ {band}: uvotsource produced no output on the co-add")
        return res
 
    # drive the bands in parallel (each band fully independent)
    n_ok = n_skip = 0
    workers = min(MAX_WORKERS, len(BANDS))
    print(f"Co-adding + photometering up to {len(BANDS)} band(s) "
          f"across {workers} worker(s)...\n")
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_tavg_one_band, b): b for b in BANDS}
        for fut in as_completed(futures):
            b = futures[fut]
            WATCHDOG.beat(f"Time-averaged: finished band {b}")
            try:
                res = fut.result()
            except Exception as e:
                print(f" ❌ time-avg worker crashed for {b}: {str(e)[:160]}")
                continue
            if res['log']:
                print("\n".join(res['log']))
            if res['ok']:
                n_ok += 1
                if res['skipped']:
                    n_skip += 1
    print(f"\nTIME-AVERAGED SUMMARY{tlabel}: {n_ok} band(s) with a co-added "
          f"point ({n_skip} reused from a previous run)\n")
    if n_ok == 0:
        return None
 
    # compile + plot
    # The co-add outputs live in ONE dedicated dir (not per-obsid dirs), so
    # it is handed to the compiler directly via override_dirs.
    return _compile_and_plot_mode(
        obs_table=obs_table, image_dirs=image_dirs, save_path=save_path,
        target=target, output_root=output_root, mode='timeavg',
        derive_flags=True, override_dirs=[("TIMEAVG", work_dir)])
 
 
def plot_uvot_timeavg_sed(excel_file, save_prefix=None, target=None):
    """
    Plot TIME-AVERAGED photometry as magnitude vs WAVELENGTH (SED-style).
 
    The time-averaged mode produces ONE co-added point per band, so a time
    axis is meaningless, instead each band's point is placed at its
    effective wavelength (Poole et al. 2008).
    """

    _FS_TICK, _FS_LABEL, _FS_TITLE = 14, 17, 18
    _AB_ZP = 48.6
    _TINY = 1e-300
    _WL = {'uw2': 1928.0, 'um2': 2246.0, 'uw1': 2600.0,
           'uuu': 3465.0, 'ubb': 4392.0, 'uvv': 5468.0}
 
    if excel_file.lower().endswith('.csv'):
        df = pd.read_csv(excel_file)
    else:
        df = pd.read_csv(excel_file, sep='\t')
    if df.empty:
        print("  SED: no rows to plot.")
        return
 
    band_col = 'BAND' if 'BAND' in df.columns else None
    if band_col is None:
        for c in df.columns:
            if 'band' in c.lower() or 'filter' in c.lower():
                band_col = c
                break
    if band_col is None:
        print(" SED: no band column found.")
        return
 
    def _truthy_local(v):
        if isinstance(v, (bool, np.bool_)):
            return bool(v)
        if isinstance(v, (int, np.integer)):
            return int(v) in (1, 84)
        if isinstance(v, (bytes, bytearray)):
            return v.strip().lower() in (b't', b'true', b'1', b'yes')
        return str(v).strip().lower() in ('true', '1', 'yes', 't')
 
    mag = pd.to_numeric(df.get('AB_MAG'), errors='coerce')
    magerr = pd.to_numeric(df.get('AB_MAG_ERR'), errors='coerce').abs()
    if 'PLOT_MAG' in df.columns:
        plotmag = pd.to_numeric(df['PLOT_MAG'], errors='coerce')
    elif 'AB_MAG_LIM' in df.columns:
        plotmag = pd.to_numeric(df['AB_MAG_LIM'], errors='coerce')
    else:
        plotmag = mag
    is_ul = (df['UpperLimit'].apply(_truthy_local)
             if 'UpperLimit' in df.columns
             else pd.Series(False, index=df.index))
    is_ll = (df['LowerLimit'].apply(_truthy_local)
             if 'LowerLimit' in df.columns
             else pd.Series(False, index=df.index))
 
    # bands present, ordered blue -> red by wavelength
    bands = [b for b in sorted(_WL, key=_WL.get)
             if (df[band_col].str.lower() == b).any()]
    if not bands:
        print("  SED: no recognised UVOT bands in the table.")
        return
    # FIXED band->colour mapping, identical to plot_uvot_lightcurves
    cmap = plt.get_cmap('tab10')
    _FIXED_BAND_ORDER = ['ubb', 'um2', 'uuu', 'uvv', 'uw1', 'uw2']
    band_colors = {b: cmap(i % 10) for i, b in enumerate(_FIXED_BAND_ORDER)}
 
    fig, ax = plt.subplots(figsize=(10, 6))
    yvals = []
    for b in bands:
        m = (df[band_col].str.lower() == b)
        wl = _WL[b]
        color = band_colors[b]
        det = m & (~is_ul) & (~is_ll) & np.isfinite(mag)
        if det.any():
            ax.errorbar(np.full(det.sum(), wl), mag[det],
                        yerr=magerr[det].fillna(0.0), fmt='o', capsize=3,
                        markersize=7, color=color, zorder=5)
            yvals.append((mag[det] + magerr[det].fillna(0)).values)
            yvals.append((mag[det] - magerr[det].fillna(0)).values)
        llm = m & is_ll & np.isfinite(mag)
        if llm.any():
            yvals.append(mag[llm].values)
        ulm = m & is_ul & np.isfinite(plotmag)
        if ulm.any():
            yvals.append(plotmag[ulm].values)
 
    if not yvals:
        print(" SED: nothing finite to plot.")
        plt.close(fig)
        return
    ys = np.concatenate(yvals)
    ymin, ymax = float(np.nanmin(ys)), float(np.nanmax(ys))
    span = (ymax - ymin) if ymax > ymin else 1.0
    pad = max(0.12 * span, 0.30)
    ax.set_ylim(ymin - pad, ymax + pad)
    ax.invert_yaxis()
 
    # limit arrows sized to the final axis span
    ylo, yhi = ax.get_ylim()
    arrow = 0.06 * abs(yhi - ylo)
    for b in bands:
        m = (df[band_col].str.lower() == b)
        wl = _WL[b]
        color = band_colors[b]
        ulm = m & is_ul & np.isfinite(plotmag)
        for y in plotmag[ulm]:
            ax.scatter([wl], [y], marker='o', s=70, facecolors='white',
                       edgecolors=color, linewidths=1.6, zorder=6)
            ax.annotate('', xy=(wl, y + arrow), xytext=(wl, y),
                        arrowprops=dict(arrowstyle='-|>', color=color, lw=1.5, shrinkA=0, shrinkB=0),
                        zorder=5, annotation_clip=False)
        llm = m & is_ll & np.isfinite(mag)
        for y in mag[llm]:
            ax.scatter([wl], [y], marker='o', s=70, facecolors='white',
                       edgecolors=color, linewidths=1.6, zorder=6)
            ax.annotate('', xy=(wl, y - arrow), xytext=(wl, y),
                        arrowprops=dict(arrowstyle='-|>', color=color, lw=1.5, shrinkA=0, shrinkB=0),
                        zorder=5, annotation_clip=False)
 
    wls = [_WL[b] for b in bands]
    ax.set_xlim(min(wls) - 400, max(wls) + 400)
    ax.set_xlabel(r'Effective wavelength [$\mathrm{\AA}$]', fontsize=_FS_LABEL)
    ax.set_ylabel('AB Magnitude', fontsize=_FS_LABEL)
    stem = f"{target}_UVOT" if target else "UVOT"
    ax.set_title(f'{stem} — time-averaged photometry (all good exposures ' f'co-added)', fontsize=_FS_TITLE)
    ax.grid(alpha=0.3)
    ax.tick_params(axis='both', which='major', labelsize=_FS_TICK, length=7, width=1.2, top=False, right=False)
    ax.yaxis.set_minor_locator(ticker.AutoMinorLocator(5))
    ax.grid(which='minor', alpha=0.12)
 
    # top axis: band names at their wavelengths
    top = ax.secondary_xaxis('top')
    top.xaxis.set_major_locator(ticker.FixedLocator(wls))
    top.xaxis.set_major_formatter(ticker.FixedFormatter(
        [b.upper() for b in bands]))
    top.tick_params(axis='x', labelsize=_FS_TICK, length=7, width=1.2)
 
    # right axis: band-independent AB flux density f_nu, ticks aligned to
    # the magnitude axis's own tick positions 
    def _mag_to_fnu(m):
        return 10.0 ** (-(np.asarray(m, dtype=float) + _AB_ZP) / 2.5)
 
    def _fnu_to_mag(f):
        f = np.clip(np.asarray(f, dtype=float), _TINY, None)
        return -2.5 * np.log10(f) - _AB_ZP
 
    secax = ax.secondary_yaxis('right', functions=(_mag_to_fnu, _fnu_to_mag))
    secax.set_ylabel(r'$f_\nu$  [erg s$^{-1}$ cm$^{-2}$ Hz$^{-1}$]', fontsize=_FS_LABEL)
    try:
        majors = [t for t in ax.yaxis.get_majorticklocs()
                  if min(ylo, yhi) <= t <= max(ylo, yhi)]
        secax.yaxis.set_major_locator(ticker.FixedLocator(
            list(_mag_to_fnu(np.asarray(majors)))))
 
        def _sci(x, pos):
            if not np.isfinite(x) or x <= 0:
                return ""
            e = int(np.floor(np.log10(x)))
            return rf"${x / 10.0 ** e:.1f}\times10^{{{e}}}$"
        secax.yaxis.set_major_formatter(ticker.FuncFormatter(_sci))
        secax.yaxis.set_minor_formatter(ticker.NullFormatter())
    except Exception:
        pass
    secax.tick_params(axis='y', labelsize=_FS_TICK, length=7, width=1.2)
 
    plt.tight_layout()
    if save_prefix:
        fig.savefig(f'{save_prefix}.png', dpi=200)
    plt.show()
    plt.close(fig)





################################################################################
# IMAGES

# Master switch: make a finder PNG for each target at the end of photometry.
MAKE_FINDER_IMAGES = True
# Which band(s) to render. 'ubb' = the UVOT B filter. Set to tuple(BANDS)
# for one finder per band.
FINDER_BANDS = ('ubb', 'uvv', 'uuu', 'um2', 'uw1', 'uw2',)
# Cutout size on sky (arcmin). UVOT plate scale is ~0.502"/px, so 2' ≈ 240 px.
FINDER_FOV_ARCMIN = 2.0
# Matplotlib colormap for the finder image. 'gray' = classic finder look;
# any matplotlib colormap name works, e.g. 'jet', 'viridis', 'inferno', etc
FINDER_CMAP = 'jet'


def _parse_region_circle(reg_path):
    """Read (ra_deg, dec_deg, radius_arcsec) from a DS9 fk5 circle region
    file. Returns None if the file is missing/unparseable."""
    try:
        with open(reg_path) as fh:
            txt = fh.read()
        m = re.search(
            r"circle\(([-+\d.eE]+)\s*,\s*([-+\d.eE]+)\s*,\s*([\d.eE+]+)\"?\)",
            txt)
        if m:
            return float(m.group(1)), float(m.group(2)), float(m.group(3))
    except Exception:
        pass
    return None


def make_target_finder_image(image_path, target_ra, target_dec, out_png,
                             src_reg_path=None, bkg_reg_path=None,
                             src_radius_arcsec=5.0,
                             fov_arcmin=None, title=None, cmap=None):
    """
    Render a log-scaled finder-chart PNG: a cutout of `image_path` centred on
    the target with the SOURCE region circled (solid green; from
    src_reg_path when given, else a src_radius_arcsec circle at the target
    position) and the BACKGROUND region dashed cyan when bkg_reg_path is
    given. Adds a 10" scale bar and an N/E compass derived from the WCS.

    Returns out_png on success, None if the image can't be used (no 2-D
    extension, or the target falls off the frame, callers can then try the
    next candidate image).
    """

    if fov_arcmin is None:
        fov_arcmin = FINDER_FOV_ARCMIN
    if cmap is None:
        cmap = FINDER_CMAP
 
    # first 2-D extension = the image
    data = hdr = None
    try:
        with fits.open(image_path) as hdul:
            for hdu in hdul:
                if hdu.header.get('NAXIS', 0) >= 2:
                    data = np.array(hdu.data, dtype=float)
                    hdr = hdu.header.copy()
                    break
    except Exception:
        return None
    if data is None:
        return None
 
    try:
        w = WCS(hdr)
        cd = hdr.get('CDELT2') or hdr.get('CD2_2') or (0.502 / 3600.0)
        plate = abs(float(cd)) * 3600.0          # arcsec / pixel
        tx, ty = w.all_world2pix(target_ra, target_dec, 0)
        tx, ty = float(tx), float(ty)
    except Exception:
        return None
 
    ny, nx = data.shape
    if not (0 <= tx < nx and 0 <= ty < ny):
        return None                              # target off this frame
 
    # regions are parsed BEFORE the cutout so the frame can grow to fit them
    src_circ = _parse_region_circle(src_reg_path) if src_reg_path else None
    bkg_circ = _parse_region_circle(bkg_reg_path) if bkg_reg_path else None
 
    half = (fov_arcmin * 60.0 / 2.0) / plate
    # AUTO ZOOM-OUT: if the source or background circle would fall outside
    # the requested field of view, expand the cutout so both
    # regions always land in frame, with a 15% margin.
    for circ in (src_circ, bkg_circ):
        if circ is None:
            continue
        try:
            cx, cy = w.all_world2pix(circ[0], circ[1], 0)
            need = (max(abs(float(cx) - tx), abs(float(cy) - ty))
                    + circ[2] / plate)
            half = max(half, 1.15 * need)
        except Exception:
            pass
 
    x0, x1 = max(0, int(tx - half)), min(nx, int(tx + half) + 1)
    y0, y1 = max(0, int(ty - half)), min(ny, int(ty + half) + 1)
    if (x1 - x0) < 10 or (y1 - y0) < 10:
        return None
    cut = data[y0:y1, x0:x1]
 
    # LOG scaling
    norm = ImageNormalize(cut, interval=PercentileInterval(99.5),
                          stretch=LogStretch())
 
    fig, ax = plt.subplots(figsize=(6.4, 6.4))
    fig.patch.set_facecolor('black')
    ax.set_facecolor('black')
    ax.imshow(cut, origin='lower', cmap=cmap, norm=norm, extent=(x0 - 0.5, x1 - 0.5, y0 - 0.5, y1 - 0.5))
 
    # SOURCE region (solid green)
    if src_circ is not None:
        sra, sdec, srad = src_circ
    else:
        sra, sdec, srad = target_ra, target_dec, float(src_radius_arcsec)
    try:
        sx, sy = w.all_world2pix(sra, sdec, 0)
        ax.add_patch(Circle((float(sx), float(sy)), srad / plate, fill=False, ec='lime', lw=2.0, zorder=5))
    except Exception:
        pass
 
    # BACKGROUND region (dashed cyan) 
    if bkg_circ is not None:
        try:
            bx, by = w.all_world2pix(bkg_circ[0], bkg_circ[1], 0)
            ax.add_patch(Circle((float(bx), float(by)), bkg_circ[2] / plate,
                                fill=False, ec='cyan', lw=1.5, ls='--', zorder=5))
        except Exception:
            pass
 
    # crosshair ticks at the catalog target position (offset so they never cover the source itself)
    g = 1.8 * srad / plate
    ax.plot([tx + g, tx + 2 * g], [ty, ty], color='red', lw=1.4, zorder=6)
    ax.plot([tx, tx], [ty + g, ty + 2 * g], color='red', lw=1.4, zorder=6)
 
    # 10" scale bar (bottom-left) 
    bar = 10.0 / plate
    bx0, by0 = x0 + 0.06 * (x1 - x0), y0 + 0.06 * (y1 - y0)
    ax.plot([bx0, bx0 + bar], [by0, by0], color='white', lw=2.2)
    ax.text(bx0 + bar / 2, by0 + 0.015 * (y1 - y0), '10"', color='white', ha='center', va='bottom', fontsize=11)
 
    # N/E compass from the WCS (top-right)
    try:
        import math
        pN = w.all_world2pix(target_ra, target_dec + 30.0 / 3600.0, 0)
        pE = w.all_world2pix(
            target_ra + 30.0 / 3600.0 /
            max(0.001, abs(math.cos(math.radians(target_dec)))),
            target_dec, 0)
        vN = np.array([float(pN[0]) - tx, float(pN[1]) - ty])
        vE = np.array([float(pE[0]) - tx, float(pE[1]) - ty])
        vN /= max(1e-9, np.hypot(*vN))
        vE /= max(1e-9, np.hypot(*vE))
        L = 0.09 * (x1 - x0)
        cx0 = x0 + 0.88 * (x1 - x0)
        cy0 = y0 + 0.84 * (y1 - y0)
        for v, lab in ((vN, 'N'), (vE, 'E')):
            ax.annotate('', xy=(cx0 + L * v[0], cy0 + L * v[1]), xytext=(cx0, cy0),
                        arrowprops=dict(arrowstyle='-|>', color='white', lw=1.4))
            ax.text(cx0 + 1.28 * L * v[0], cy0 + 1.28 * L * v[1], lab,
                    color='white', ha='center', va='center', fontsize=11)
    except Exception:
        pass
 
    ax.set_xticks([])
    ax.set_yticks([])
    for s in ax.spines.values():
        s.set_color('white')
    if title:
        ax.set_title(title, color='white', fontsize=14)
 
    plt.tight_layout()
    fig.savefig(out_png, dpi=150, facecolor=fig.get_facecolor(), bbox_inches='tight')
    plt.close(fig)
    return out_png


def make_finder_images_for_target(image_dirs, save_path, target_ra,
                                  target_dec, target=None, output_root=None,
                                  bands=None, fov_arcmin=None,
                                  src_reg_name=None, bkg_reg_name=None, cmap=None):
    """
    Produce PNG(s) for one target into <save_path>/FinderImages/.

    Per band, the DEEPEST available image is used, in priority order:
      1. the time-averaged co-add ({band}_timeavg_summed*.fits), if made
      2. any obsid's summed image ({band}_ex_summed.fits)
      3. any obsid's raw SK image
    A candidate is skipped (next one tried) when the target isn't on its
    frame. The source circle uses the Mixed source region when it exists,
    else the time-averaged pass's recovered/target region, else a plain 5"
    circle at the target position. Background region drawn when found.

    If NONE of the requested band(s) have any usable image (e.g. the target
    was never observed in B), the remaining UVOT bands are tried in turn so
    the target still gets ONE finder image, with a printed note saying which band substituted. 
    """
    if bands is None:
        bands = FINDER_BANDS
    tag = f"_{target}" if target else ""
    out_dir = os.path.join(save_path, "FinderImages")
    os.makedirs(out_dir, exist_ok=True)
 
    # region files: Mixed regions live in the per-obsid write dirs. Callers
    # inside the pipeline pass the names they already have; standalone calls
    # fall back to obs_file_name.
    src_name = (src_reg_name if src_reg_name is not None
                else obs_file_name(None, 'source_reg', target=target))
    bkg_name = (bkg_reg_name if bkg_reg_name is not None
                else obs_file_name(None, 'bkg_reg', target=target))
    src_reg = bkg_reg = None
    for obsid, img_dir in image_dirs:
        wdir = _obsid_write_dir(output_root, img_dir, obsid)
        if src_reg is None and os.path.exists(os.path.join(wdir, src_name)):
            src_reg = os.path.join(wdir, src_name)
        if bkg_reg is None and os.path.exists(os.path.join(wdir, bkg_name)):
            bkg_reg = os.path.join(wdir, bkg_name)
        if src_reg and bkg_reg:
            break
 
    # the time-averaged work dir (deep images + per-band recovered regions)
    if output_root is not None:
        tavg_dir = os.path.join(output_root, "TimeAvg")
    else:
        tavg_dir = os.path.join(save_path, "TimeAveraged", "coadd")
 
    def _try_band(band):
        # image candidates, deepest first
        candidates = []
        p = os.path.join(tavg_dir, f"{band}_timeavg_summed{tag}.fits")
        if os.path.exists(p):
            candidates.append(p)
        for obsid, img_dir in image_dirs:
            p = os.path.join(img_dir, f"{band}_ex_summed.fits")
            if os.path.exists(p):
                candidates.append(p)
        for obsid, img_dir in image_dirs:
            for nm in (f"sw{obsid}{band}_sk.img", f"sw{obsid}{band}_sk.img.gz"):
                p = os.path.join(img_dir, nm)
                if os.path.exists(p):
                    candidates.append(p)
        if not candidates:
            return None
 
        # per-band source region: Mixed's, else the timeavg recovered one
        band_src = src_reg
        if band_src is None:
            p = os.path.join(tavg_dir, f"auto_source_tavg_{band}{tag}.reg")
            if os.path.exists(p):
                band_src = p
 
        out_png = os.path.join(out_dir, f"finder_{band}{tag}.png")
        stem = f"{target} — {band.upper()}" if target else band.upper()
        for cand in candidates:
            ok = make_target_finder_image(
                cand, target_ra, target_dec, out_png,
                src_reg_path=band_src, bkg_reg_path=bkg_reg,
                fov_arcmin=fov_arcmin, title=stem, cmap=cmap)
            if ok:
                print(f"  finder image: {out_png} "
                      f"(from {os.path.basename(cand)})")
                return out_png
        return None
 
    made = []
    for band in bands:
        r = _try_band(band)
        if r:
            made.append(r)
        else:
            print(f" finder: no usable {band} image for this target")
    if not made:
        # requested band(s) had nothing, substitute the first band that works
        for band in [b for b in BANDS if b not in bands]:
            r = _try_band(band)
            if r:
                print(f"  finder: requested band(s) {tuple(bands)} "f"unavailable — used {band} instead")
                made.append(r)
                break
    if not made:
        print(" finder: could not produce any finder image (no usable images, or target off every frame)")
    return made
 


WATCHDOG = StallWatchdog(stall_seconds=300, dump_path="C:/Users/05ble/OneDrive/Desktop/Watchdog/pipeline_stall_dump.txt")
WATCHDOG.start()
