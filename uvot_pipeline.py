# -*- coding: utf-8 -*-
import os
import subprocess

import pandas as pd
import numpy as np

import math
import time
import gc

import re

import shutil
from astropy.wcs import WCS
from astropy.io import fits
from astropy.table import QTable, Table
import astropy.units as u
from astropy.coordinates import SkyCoord

from swifttools.swift_too import TOO, Resolve, ObsQuery, Data

from tqdm import tqdm

import requests
from requests.auth import HTTPBasicAuth

import tkinter as tk
from tkinter import filedialog

import contextlib

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
        out['Radius'].fillna(DEFAULT_SEARCH_RADIUS, inplace=True)
    else:
        out['Radius'] = DEFAULT_SEARCH_RADIUS

    threshold_col = _resolve_column(df, _BATCH_COL_ALIASES['threshold'])
    if threshold_col is not None:
        out['Threshold'] = pd.to_numeric(df[threshold_col], errors='coerce')
        out['Threshold'].fillna(DEFAULT_DETECT_THRESHOLD, inplace=True)
    else:
        out['Threshold'] = DEFAULT_DETECT_THRESHOLD

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


@contextlib.contextmanager    #I am not sure how this works?, Found this online and am using it, the @ is a decorator, which is something I dont fully understand.
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
###########################################################################################



def run_heasoft_command(command):
    """
    Runs HEASOFT commands through the appropriate backend.

    WSL: makes a WSL bash shell, activates conda env (WSL_CONDA_ENV)
         or sources WSL_HEASOFT_INIT_SCRIPT, then runs the command.

    NATIVE (Linux/macOS): Sources $HEADAS/headas-init.sh then runs the
         command.  Auto-detects $HEADAS and $CALDB from environment
         unless NATIVE_HEADAS_PATH / NATIVE_CALDB_PATH are set in the
         config block.
    """
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

        # CALDB: config var > env var
        caldb = NATIVE_CALDB_PATH or os.environ.get("CALDB", "")
        if not caldb:
            raise RuntimeError(
                "Native backend needs $CALDB set in the environment, "
                "or NATIVE_CALDB_PATH set in the config block."
            )

        # Derive CALDB config files (standard layout)
        caldb_config = (os.environ.get("CALDBCONFIG")
                        or f"{caldb}/software/tools/caldb.config")
        caldb_alias = (os.environ.get("CALDBALIAS")
                       or f"{caldb}/software/tools/alias_config.fits")

        # Build the full command:
        #   HEADASNOQUERY=1, HEADASPROMPT=/dev/null  -> tools won't prompt
        #   CALDB / CALDBCONFIG / CALDBALIAS         -> CALDB lookups work
        #   PFILES with per-PID dir                  -> avoid race conditions
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
        print("  [RESULT]: FAILED")
        print("--- Error Details ---")
        print(result.stderr)
    elif result.stderr and "ERROR" in result.stderr.upper():
        # HEASoft tools sometimes exit 0 even on internal failure.
        # Check stderr for "ERROR" string.
        print("  [RESULT]: FAILED (HEASoft error in stderr)")
        print("--- Error Details ---")
        print(result.stderr)
    else:
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

def find_obs_file(base_path, obsid, band, file_type='sk'):
    """
    Finds the actual path of a file even if the folder has extra date tags. This is 100% only becuase I have done that.
    """
    target_filename = f"sw{obsid}{band}_{file_type}.img.gz"
    for root, dirs, files in os.walk(base_path):
        # Check if we are in the correct subfolder structure
        if obsid in root and root.endswith(os.path.join("uvot", "image")):
            if target_filename in files:
                return os.path.join(root, target_filename)
    return None

#WSL UVOTDETECT version, Thomas if you so desire and think my logical bellow is good and would like to use it, you can edit to the code to add
# If WSL elements, As currently this is later called with a If WSL rather then being built in.
def batch_run_uvotdetect(base_path, threshold=3.0):
    
# The six UVOT filter bands we care about
    BANDS = ["uvv", "uuu", "ubb", "um2", "uw1", "uw2"]

    def get_extension_count(filepath):
        try:
            with fits.open(filepath) as hdul:
                return len(hdul) - 1
        except Exception as e:
            print(f"  Error reading FITS: {e}")
            return 0

    print("\n" + "=" * 70)
    print("BATCH UVOTDETECT")
    print("=" * 70)

    # Walk the entire directory tree under base_path, looking for directories named "image" 
    for root, dirs, files in os.walk(base_path):
        if os.path.basename(root) != "image":
            continue

        # Convert the Windows path to a WSL-compatible path
        img_dir_heasoft = prepare_path(root)

        print(f"\n Processing image directory:")
        print(f"{root}")

        # Regex to match UVOT sky image filenames:
        obsid_pattern = re.compile(r"sw(\d{11})([a-z0-9]+)_sk\.img\.gz")

        for file in files:
            # Try to match the filename against the expected pattern
            match = obsid_pattern.match(file)
            if not match:
                continue

            OBSID, band = match.groups()

            # Skip non-UVOT bands (e.g. XRT files that might match the pattern and I did install some of those by accident. Also safty first.)
            if band not in BANDS:
                continue

            print(f"\n Found SK image: {file}")

            # Use the helper function to get the full resolved path to the SK file
            sk_file_path = find_obs_file(base_path, OBSID, band, file_type='sk')

            if not sk_file_path:
                print(f" Could not find SK file for OBSID={OBSID}, band={band}")
                continue

            # Check how many image extensions the FITS file contains
            ext_count = get_extension_count(sk_file_path)
            print(f" {ext_count} image extension(s) found")

            # Get just the filename (no directory) for the HEASOFT command
            sk_filename = os.path.basename(sk_file_path)

            if ext_count > 1: #Multiextensions
                print(f" Creating detect files for {ext_count} extensions...")
                for ext in range(1, ext_count + 1):
                    detect_ext = f"{band}_detect_ext{ext}.fits"
                    detect_ext_path = os.path.join(root, detect_ext)
                    if os.path.exists(detect_ext_path):
                        print(f" Extension {ext} detect exists - skipping")
                        continue

                    print(f" Running detect on extension {ext}")

                    uvotdetect_cmd = (
                        f"cd '{img_dir_heasoft}' && "
                        f"uvotdetect "
                        f"infile='{sk_filename}[{ext}]' "
                        f"outfile='{detect_ext}' "
                        f"expfile=NONE threshold={threshold} clobber=YES"
                    )
                    run_heasoft_command(uvotdetect_cmd)
            else:  # Single extensions
                detect_base = f"{band}_detect.fits"
                detect_path = os.path.join(root, detect_base)
                if os.path.exists(detect_path):
                    print(" Detect file already exists - skipping")
                    continue

                print(" Running single-extension detect...")
                uvotdetect_cmd = (
                    f"cd '{img_dir_heasoft}' && "
                    f"uvotdetect "
                    f"infile='{sk_filename}' "
                    f"outfile='{detect_base}' "
                    f"expfile=NONE threshold={threshold} clobber=YES"
                )
                run_heasoft_command(uvotdetect_cmd)

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
        f"refreg='{ref_reg_filepath}'"
    )
    return command



def detect_smeared_frames(base_path):
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

    detect_files = []
    for root, dirs, files in os.walk(base_path):
        if os.path.basename(root) == "image":
            for file in files:
                # Skip the corrected detect files (they're post-summation)
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

    #open detect.fits and read header into dataframe
    with fits.open(detect_path) as hdul:
        detect_header = hdul[0].header
        
    #read header to find central pointing position
    center_ra = detect_header['RA_PNT'] * u.deg
    center_dec = detect_header['DEC_PNT']* u.deg

    #set up buffers
    center_coords = SkyCoord(ra=center_ra, dec=center_dec, frame='fk5')
    position_angle1 = 0 * u.deg
    position_angle2 = 90 * u.deg
    position_angle3 = 180 * u.deg
    position_angle4 = 270 * u.deg
    sep = side_buffer * u.arcmin

    #create upper and lower ra/dec bounds
    dec_max = center_coords.directional_offset_by(position_angle1, sep).dec.degree
    dec_min = center_coords.directional_offset_by(position_angle3, sep).dec.degree
    
    ra_max = center_coords.directional_offset_by(position_angle2, sep).ra.degree
    ra_min = center_coords.directional_offset_by(position_angle4, sep).ra.degree

    #extract sources from detect.fits
    stars = QTable.read(detect_path).to_pandas()
    stars = stars[(stars['RA'] >= ra_min) & (stars['RA'] <= ra_max)]
    stars = stars[(stars['DEC'] >= dec_min) & (stars['DEC'] <= dec_max)]

    #keep only the 15 brightest sources
    bright_stars = stars.sort_values('MAG', ascending=True)
    bright_stars = bright_stars.iloc[:num_stars+1, :]

    nearby_stars = []

    #loop over all bright central stars
    #use positions to calculate separation between each star
    #remove stars closer together than 1 arcminute
    for i in range(num_stars+1):
        for j in range(num_stars+1):
    
            if i != j:
                star1_ra = bright_stars.iloc[i, 0]
                star1_dec = bright_stars.iloc[i, 1]
                star1_coords  = SkyCoord(star1_ra, star1_dec, unit='deg', frame='fk5')
        
                star2_ra = bright_stars.iloc[j, 0]
                star2_dec = bright_stars.iloc[j, 1]
                star2_coords  = SkyCoord(star2_ra, star2_dec, unit='deg', frame='fk5')
        
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
                                bkg_reg_name="auto_bkg.reg"):
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
    BANDS = ["uvv", "uuu", "ubb", "um2", "uw1", "uw2"]
    QUARANTINE = {"Smeared", "NotASPCORR", "Orphans"}

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

    upper_limit_paths = []
    n_processed = 0
    n_skipped = 0
    n_failed = 0

    for root, dirs, files in os.walk(base_path):
        normalised = os.path.normpath(root)
        if not normalised.endswith(os.path.join("uvot", "image")):
            continue
        path_parts = normalised.split(os.sep)
        if any(q in path_parts for q in QUARANTINE):
            continue

        obsid_match = re.search(r"(\d{11})", root)
        obsid = obsid_match.group(1) if obsid_match else "?"

        # Reuse the background region the generator already produced.
        # If it's missing, we can't compute a limit here.
        bkg_reg_path = os.path.join(root, bkg_reg_name)
        if not os.path.exists(bkg_reg_path):
            continue

        current_files = os.listdir(root)
        detect_failed_dir = os.path.join(root, "DetectFailed")
        detect_failed_files = (os.listdir(detect_failed_dir)
                               if os.path.isdir(detect_failed_dir) else [])

        for band in BANDS:
            # If a normal detection exists for this band, this is NOT a
            # non-detection — skip it. The UL pass only handles leftovers.
            finalsource_file = f"{band}_finalsource.fits"
            if finalsource_file in current_files:
                continue

            # Already computed the upper limit on a previous run? Reuse it.
            ul_output = f"{band}_finalsource_ul.fits"
            ul_output_path = os.path.join(root, ul_output)
            if os.path.exists(ul_output_path):
                upper_limit_paths.append(ul_output_path)
                n_skipped += 1
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

            # Write the source region at the target position
            ul_src_name = "auto_source_ul.reg"
            with open(os.path.join(root, ul_src_name), 'w') as f:
                f.write(ul_src_text)

            print(f" [{obsid} / {band}] Non-detection — computing "
                  f"upper limit (input: {input_file})")

            # Normal uvotsource. The output's AB_MAG_LIM is the 3-sigma
            # limiting magnitude we report as the upper limit.
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
                    f"cleanup=YES clobber=YES chatter=1"
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
                    f"cleanup=YES clobber=YES chatter=1"
                )

            run_heasoft_command(cmd)
            time.sleep(1)

            if os.path.exists(ul_output_path):
                upper_limit_paths.append(ul_output_path)
                n_processed += 1
                print(f" ✅ Upper limit computed")
            else:
                n_failed += 1
                print(f" ❌ uvotsource failed to produce output")

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
        cmd = f"fappend '{ref_heasoft}' '{master_heasoft}'"
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
        f"exclude=NONE"
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
        f"expfile=NONE threshold=3"
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
                        f"refreg='ref.reg'"
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
        'ObsID', 'Filter', 'Snapshot', 'Smeared Flag', 'SSS Flag', 'AspCorr Flag',
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
                           output_name="auto_source.reg"):
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
    QUARANTINE = {"Smeared", "NotASPCORR", "Orphans"}
    BANDS = ["uvv", "uuu", "ubb", "um2", "uw1", "uw2"]

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

    for root, dirs, files in os.walk(base_path):
        normalised = os.path.normpath(root)
        if not normalised.endswith(os.path.join("uvot", "image")):
            continue
        path_parts = normalised.split(os.sep)
        if any(q in path_parts for q in QUARANTINE):
            continue

        obsid_match = re.search(r"(\d{11})", root)
        obsid = obsid_match.group(1) if obsid_match else "?"

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

            print(f"Running uvotdetect on {obsid}/{band} ({input_file})...")

            if HEASOFT_BACKEND == "wsl":
                wsl_dir = prepare_path(root)
                cmd = (f"cd '{wsl_dir}' && "
                       f"uvotdetect infile='{input_file}' "
                       f"outfile='{detect_file}' "
                       f"expfile=NONE threshold=3 clobber=YES")
            else:
                cmd = (f"cd '{root}' && "
                       f"uvotdetect infile='{input_file}' "
                       f"outfile='{detect_file}' "
                       f"expfile=NONE threshold=3 clobber=YES")

            run_heasoft_command(cmd)
            time.sleep(2)

            # Retry once if it failed
            if not os.path.exists(detect_path):
                print(f"Retrying {obsid}/{band}...")
                time.sleep(3)
                run_heasoft_command(cmd)
                time.sleep(2)

            if os.path.exists(detect_path):
                corrected_detects_run += 1
            else:
                # Failed twice — move the input file to a subfolder
                # so it can't be used with a mismatched source region
                print(f"❌ uvotdetect failed twice for {obsid}/{band}")
                print(f"Moving {input_file} to DetectFailed/")

                failed_dir = os.path.join(root, "DetectFailed")
                os.makedirs(failed_dir, exist_ok=True)

                src_path = os.path.join(root, input_file)
                dst_path = os.path.join(failed_dir, input_file)

                try:
                    shutil.move(src_path, dst_path)
                    print(f"Moved: {input_file}")
                except Exception as e:
                    print(f"Error moving: {e}")

                detect_failures.append({
                    'ObsID': obsid,
                    'Band': band,
                    'File': input_file,
                    'Directory': root,
                })

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

    for root, dirs, files in os.walk(base_path):
        normalised = os.path.normpath(root)
        if not normalised.endswith(os.path.join("uvot", "image")):
            continue
        path_parts = normalised.split(os.sep)
        if any(q in path_parts for q in QUARANTINE):
            continue

        obsid_match = re.search(r"(\d{11})", root)
        obsid = obsid_match.group(1) if obsid_match else "?"

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
                    reg_path = os.path.join(root, output_name)
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
    sss_count = 0
    checked = 0
    errored = 0

    # Group by (ObsID, Filter) so we process all extensions of one file together
    for (obsid, band), group in multi_ext_groups.groupby(['ObsID', 'Filter']):
        img_dir = os.path.dirname(group['Full_Path'].iloc[0])
        if not os.path.exists(img_dir):
            continue

        # Locate the SK image
        sk_img = f"sw{obsid}{band}_sk.img"
        sk_gz = f"sw{obsid}{band}_sk.img.gz"
        if os.path.exists(os.path.join(img_dir, sk_img)):
            sk_filename = sk_img
        elif os.path.exists(os.path.join(img_dir, sk_gz)):
            sk_filename = sk_gz
        else:
            continue

        # Write temp regions
        temp_src = os.path.join(img_dir, "_sss_src_tmp.reg")
        temp_bkg = os.path.join(img_dir, "_sss_bkg_tmp.reg")
        with open(temp_src, 'w') as f:
            f.write(src_reg_text)
        with open(temp_bkg, 'w') as f:
            f.write(bkg_reg_text)

        # Run uvotsource for each extension in the group
        for _, row in group.iterrows():
            ext = int(row['Snapshot'])
            if row['Extension_Status'] not in ('DIRECT', 'UNICORR'):
                # Skip uncorrected extensions — they'd fail anyway
                continue

            temp_out = os.path.join(img_dir, f"_sss_check_ext{ext}.fits")
            if os.path.exists(temp_out):
                os.remove(temp_out)

            if HEASOFT_BACKEND == "wsl":
                wsl_d = prepare_path(img_dir)
                cmd = (f"cd '{wsl_d}' && "
                       f"uvotsource image='{sk_filename}[{ext}]' "
                       f"srcreg='_sss_src_tmp.reg' "
                       f"bkgreg='_sss_bkg_tmp.reg' "
                       f"sigma=3 expfile=NONE "
                       f"zerofile=CALDB coinfile=CALDB psffile=CALDB lssfile=CALDB "
                       f"syserr=NO frametime=DEFAULT apercorr=NONE output=ALL "
                       f"outfile='_sss_check_ext{ext}.fits' "
                       f"cleanup=YES clobber=YES chatter=0")
            else:
                cmd = (f"cd '{img_dir}' && "
                       f"uvotsource image='{sk_filename}[{ext}]' "
                       f"srcreg='_sss_src_tmp.reg' "
                       f"bkgreg='_sss_bkg_tmp.reg' "
                       f"sigma=3 expfile=NONE "
                       f"zerofile=CALDB coinfile=CALDB psffile=CALDB lssfile=CALDB "
                       f"syserr=NO frametime=DEFAULT apercorr=NONE output=ALL "
                       f"outfile='_sss_check_ext{ext}.fits' "
                       f"cleanup=YES clobber=YES chatter=0")

            run_heasoft_command(cmd)
            checked += 1

            # Read result
            if not os.path.exists(temp_out):
                errored += 1
                continue

            try:
                with fits.open(temp_out) as hdul:
                    if len(hdul) >= 2 and hdul[1].data is not None and len(hdul[1].data) > 0:
                        ab_mag = float(hdul[1].data['AB_MAG'][0])
                        if ab_mag == 99.0 or not np.isfinite(ab_mag):
                            # Flag this extension as SSS-bad
                            mask = ((obs_table['ObsID'] == obsid) &
                                    (obs_table['Filter'] == band) &
                                    (obs_table['Snapshot'] == ext))
                            obs_table.loc[mask, 'SSS Flag'] = True
                            sss_count += 1
                            sss_failures.append({
                                'ObsID': obsid,
                                'Band': band,
                                'Snapshot': ext,
                                'AB_MAG': ab_mag,
                                'Directory': img_dir,
                            })
                            print(f"  [{obsid} / {band} ext{ext}] SSS-flagged (AB_MAG=99)")
            except Exception as e:
                print(f"  [{obsid} / {band} ext{ext}] Error reading result: {e}")
                errored += 1

            # Clean up the temp uvotsource output
            try:
                os.remove(temp_out)
            except Exception:
                pass

        # Clean up temp regions
        for tmp in (temp_src, temp_bkg):
            try:
                os.remove(tmp)
            except Exception:
                pass

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
    print(f"  Errored during check: {errored}")
    print(f"  observations_table.csv updated with SSS flags")
    print("=" * 70)

    return obs_table


    
def run_uvotsource_pipeline(obs_table, base_path, save_path, source_reg=None, bkg_reg=None, target_ra=None, target_dec=None, automation_mode=True):
    BANDS = ["uvv", "uuu", "ubb", "um2", "uw1", "uw2"]

    # Build a set of smeared OBSIDs from obs_table so we can skip them.
    # This is the ONLY thing we use obs_table for in 4c/4d, everything
    # else comes from walking the filesystem directly.
    smeared_obsids = set()
    if obs_table is not None and 'Smeared Flag' in obs_table.columns:
        smeared_rows = obs_table[obs_table['Smeared Flag'] == True]
        smeared_obsids = set(smeared_rows['ObsID'].astype(str).unique())
        if smeared_obsids:
            print(f"Will skip {len(smeared_obsids)} smeared OBSIDs")

    
    ###########################################################################
    # DISCOVER ALL uvot/image directories, this is more or less a safty thing
    # I dont want to rely on the obs_table paths which may be differnt after aspect correction as some frames are moved.
    # You know looking back on the code, THIS is 100% increasing the run time, Should proably change this around
    # I need to learn to trust the table again, even after my little incident.
    
    obsid_pattern = re.compile(r"(\d{11})")

    # Collect all (obsid, image_dir) pairs
    image_dirs = []  # list of (obsid_str, full_path_to_image_dir)

    for root_dir, dirs, files in os.walk(base_path):
        normalised = os.path.normpath(root_dir)
        if not normalised.endswith(os.path.join("uvot", "image")):
            continue

        # Extract OBSID from the path
        match = obsid_pattern.search(root_dir)
        if not match:
            continue
        obsid = match.group(1)

        # Skip smeared observations
        if obsid in smeared_obsids:
            continue

        # Skip if this is inside a quarantine folder
        # (Smeared, NotASPCORR, or Orphans)
        path_parts = root_dir.split(os.sep)
        if any(qf in path_parts for qf in ("Smeared", "NotASPCORR", "Orphans")):
            continue

        image_dirs.append((obsid, root_dir))

    print(f"Found {len(image_dirs)} uvot/image directories to process")
    print(f"Unique OBSIDs: {len(set(o for o, _ in image_dirs))}\n")

    if not image_dirs:
        print("No observation directories found — check base_path.")
        return None if automation_mode else None


    
    ##################################################################################
    # Sum Multi extensions files UVOTSUM 
    # 
    # For each image directory, for each band:
    #   1. Find the SK image (uncompressed .img first, then .img.gz)
    #   2. Open the FITS file and read ASPCORR per extension
    #   3. If ALL extensions are DIRECT/UNICORR -> sum normally (no exclude)
    #   4. If SOME extensions are NONE -> sum with exclude= to skip them (I think this is working but at this point this might be broken)
    #   5. If ALL extensions are NONE -> skip entirely (this should be getting qurentined, but something is happening)
    #   6. If only 1 usable extension -> no summation needed, uvotsource  can work on the SK file directly using that extension
    #
    print("=" * 70)
    print("SUMMING MULTI-EXTENSION FILES (uvotimsum) — ASPCORR-AWARE")
    print("=" * 70)

    summed_count = 0
    sum_skipped = 0
    sum_failed = 0
    sum_not_needed = 0
    sum_with_excludes = 0
    exp_summed_count = 0


    def _sum_exposure_map(obsid, band, img_dir, exclude_str=None):
        """
        Sum the exposure map for a given obsid/band using uvotimsum
        with expmap=yes.  This flag tells uvotimsum to preserve
        exposure times during pixel resampling (instead of treating pixel values as photon counts).
        Returns True if a summed exposure map exists after this call.
        """
        exp_summed_outfile = f"{band}_expmap_summed.fits"
        exp_summed_outpath = os.path.join(img_dir, exp_summed_outfile)
 
        if os.path.exists(exp_summed_outpath):
            return True
 
        # Find the raw exposure map
        ex_img = f"sw{obsid}{band}_ex.img"
        ex_gz = f"sw{obsid}{band}_ex.img.gz"
        ex_file = None
        if os.path.exists(os.path.join(img_dir, ex_img)):
            ex_file = ex_img
        elif os.path.exists(os.path.join(img_dir, ex_gz)):
            ex_file = ex_gz
        else:
            return False
 
        # Build uvotimsum command with method=EXPMAP
        # method=EXPMAP tells uvotimsum to sum as exposure maps,
        # preserving exposure times during pixel resampling instead
        # of treating pixel values as photon counts.
        if HEASOFT_BACKEND == "wsl":
            wsl_d = prepare_path(img_dir)
            ecmd = (f"cd '{wsl_d}' && "
                    f"uvotimsum infile='{ex_file}' "
                    f"outfile='{exp_summed_outfile}' method=EXPMAP")
        else:
            ecmd = (f"cd '{img_dir}' && "
                    f"uvotimsum infile='{ex_file}' "
                    f"outfile='{exp_summed_outfile}' method=EXPMAP")
        if exclude_str:
            ecmd += f" exclude={exclude_str}"
        run_heasoft_command(ecmd)
        time.sleep(1)
        return os.path.exists(exp_summed_outpath)
 
    for obsid, img_dir in tqdm(image_dirs, desc="Summing extensions", unit="obs"):
        for band in BANDS:
            # Check if a summed SK file already exists, skip if so
            summed_outfile = f"{band}_ex_summed.fits"
            summed_outpath = os.path.join(img_dir, summed_outfile)
            if os.path.exists(summed_outpath):
                sum_skipped += 1
                # SK already summed, but ensure exposure map is too
                if _sum_exposure_map(obsid, band, img_dir):
                    exp_summed_count += 1
                continue

            
            #################################################################################
            # Find the SK image.
            # Priority: uncompressed .img first, then compressed .img.gz.
            
            sk_img = f"sw{obsid}{band}_sk.img"
            sk_gz = f"sw{obsid}{band}_sk.img.gz"
 
            img_file = None
            img_full_path = None
            if os.path.exists(os.path.join(img_dir, sk_img)):
                img_file = sk_img
                img_full_path = os.path.join(img_dir, sk_img)
            elif os.path.exists(os.path.join(img_dir, sk_gz)):
                img_file = sk_gz
                img_full_path = os.path.join(img_dir, sk_gz)
            else:
                continue

            #################################################################################
            # Open the FITS file and check ASPCORR on each image extension.
            # Build a list of which extensions are good (DIRECT/UNICORR)
            # and which are bad (NONE).

            try:
                with fits.open(img_full_path) as hdul:
                    good_exts = []
                    bad_exts = []
                    ext_num = 0
                    for hdu in hdul:
                        if hdu.header.get('NAXIS', 0) < 2:
                            continue
                        ext_num += 1
                        val = str(hdu.header.get('ASPCORR', 'NONE')).strip().upper()
                        if val == 'DIRECT':
                            good_exts.append(ext_num)
                        else:
                            bad_exts.append(ext_num)
                    total_exts = len(good_exts) + len(bad_exts)

                # Quality-flag check: move SSS-flagged AND smeared extensions
                # into bad_exts so uvotimsum's exclude them.
                if obs_table is not None:
                    flagged_exts = set()

                    # SSS-flagged extensions
                    if 'SSS Flag' in obs_table.columns:
                        sss_mask = (
                            (obs_table['ObsID'].astype(str) == str(obsid)) &
                            (obs_table['Filter'] == band) &
                            (obs_table['SSS Flag'] == True)
                        )
                        flagged_exts.update(
                            obs_table.loc[sss_mask, 'Snapshot'].astype(int).tolist()
                        )

                    # Smeared extensions
                    if 'Smeared Flag' in obs_table.columns:
                        smear_mask = (
                            (obs_table['ObsID'].astype(str) == str(obsid)) &
                            (obs_table['Filter'] == band) &
                            (obs_table['Smeared Flag'] == True)
                        )
                        flagged_exts.update(
                            obs_table.loc[smear_mask, 'Snapshot'].astype(int).tolist()
                        )

                    if flagged_exts:
                        moved = []
                        for ext in flagged_exts:
                            if ext in good_exts:
                                good_exts.remove(ext)
                                bad_exts.append(ext)
                                moved.append(ext)
                        if moved:
                            print(f"  [{obsid} / {band}] Quality-flagged extensions "
                                  f"moved to exclude: {sorted(moved)}")
                            
            except Exception as e:
                print(f"  [{obsid} / {band}] Error reading FITS: {e}")
                continue

            
            # No image extensions at all, skip
            if total_exts == 0:
                continue
 
            # All extensions are NONE — skip (should have been quarantined, but safety check)
            if len(good_exts) == 0:
                print(f"  [{obsid} / {band}] All {total_exts} extensions are NONE — skipping")
                continue
 
            # Only 1 usable extension total, no summation needed,
            # uvotsource can work on the SK file directly
            if total_exts <= 2 and len(bad_exts) == 0:
                sum_not_needed += 1
                # Still sum the exposure map — it may have multiple
                # extensions even if the SK doesn't need summing
                _sum_exposure_map(obsid, band, img_dir)
                continue
 
            # If only 1 good extension out of many, still no point summing
            # a single frame — but we DO need to note this so uvotsource
            # knows to use that specific extension
            if len(good_exts) == 1 and len(bad_exts) > 0:
                print(f"  [{obsid} / {band}] Only 1 good extension (ext {good_exts[0]}) "
                      f"out of {total_exts} — no summation, uvotsource will use SK directly")
                sum_not_needed += 1
                continue

            #################################################################################
            # Build the uvotimsum command

            if bad_exts:
                exclude_str = ",".join(str(e) for e in bad_exts)
                print(f"  [{obsid} / {band}] {total_exts} extensions: "
                      f"{len(good_exts)} good, {len(bad_exts)} NONE "
                      f"→ summing with exclude={exclude_str}")
            else:
                exclude_str = None
                print(f"  [{obsid} / {band}] {total_exts} extensions, all corrected → summing")
 
            if HEASOFT_BACKEND == "wsl":
                wsl_img_dir = prepare_path(img_dir)
                if exclude_str:
                    sum_cmd = (f"cd '{wsl_img_dir}' && "
                               f"uvotimsum infile='{img_file}' "
                               f"outfile='{summed_outfile}' "
                               f"exclude={exclude_str}")
                else:
                    sum_cmd = (f"cd '{wsl_img_dir}' && "
                               f"uvotimsum '{img_file}' '{summed_outfile}'")
            else:
                if exclude_str:
                    sum_cmd = (f"cd '{img_dir}' && "
                               f"uvotimsum infile='{img_file}' "
                               f"outfile='{summed_outfile}' "
                               f"exclude={exclude_str}")
                else:
                    sum_cmd = (f"cd '{img_dir}' && "
                               f"uvotimsum '{img_file}' '{summed_outfile}'")
 
            result = run_heasoft_command(sum_cmd)
 
            # Short delay for WSL filesystem sync
            time.sleep(1)
 
            if os.path.exists(summed_outpath):
                if bad_exts:
                    print(f"✅ Created {summed_outfile} (excluded extensions: {exclude_str})")
                    sum_with_excludes += 1
                else:
                    print(f"✅ Created {summed_outfile}")
                summed_count += 1
                # The emojis will become frequency now, as it was the only quick way to vissually screen fails quickly.
                
                # Sum the exposure map with the SAME exclude list and
                # expmap=yes so uvotimsum treats it as exposure data.
                if _sum_exposure_map(obsid, band, img_dir, exclude_str):
                    print(f"✅ Created {band}_expmap_summed.fits")
                    exp_summed_count += 1
                else:
                    print(f"⚠️ No exposure map found (uvotsource will use expfile=NONE)")
            else:
                print(f"❌ uvotimsum failed for {obsid}/{band}")
                sum_failed += 1
 
    print(f"\nSummation results:")
    print(f" SK images created  : {summed_count}")
    print(f" (with excludes)  : {sum_with_excludes}")
    print(f" Exp maps summed    : {exp_summed_count}")
    print(f" Already existed    : {sum_skipped}")
    print(f" Not needed         : {sum_not_needed} (single extension or ≤2)")
    print(f" Failed             : {sum_failed}\n")






    #############################################################################
    # GET REGION FILES, TEMPORARY SOLUTION (WILL BE REPLACED, SOONISH?)
    
    # SOURCE REGION: Auto-generate per observation
    # Uses detect files to find the source centroid in each frame.
    # This is better than a single fixed region because the source
    # position shifts between observations due to pointing jitter.
    if source_reg is None:
        print("=" * 70)
        print("GENERATING SOURCE REGIONS")
        print("=" * 70)
 
        if target_ra is None or target_dec is None:
            raise ValueError(
                "target_ra and target_dec are required when source_reg is None. "
                "These should be collected in setup_data_directories() and "
                "passed through run_uvot_pipeline()."
            )
 
        src_reg_name = "auto_source.reg"
        write_source_reg_files(base_path, target_ra, target_dec,
                               save_path=save_path,
                               output_name=src_reg_name)
    else:
        if not os.path.exists(source_reg):
            raise FileNotFoundError(f"Source region file not found: {source_reg}")
        src_reg_name = os.path.basename(source_reg)
        print(f"Using provided source region: {source_reg}")
        print("Copying to observation directories...")
        for root_dir, dirs, files in os.walk(base_path):
            normalised = os.path.normpath(root_dir)
            if normalised.endswith(os.path.join("uvot", "image")):
                try:
                    shutil.copy2(source_reg, root_dir)
                except Exception as e:
                    print(f"  Warning — could not copy to {root_dir}: {e}")
 
    # --- BACKGROUND REGION (temporary — manual selection) ---
    if bkg_reg is None:
        # Auto generate background region
        # source free positions, then checks each candidate against
        # all observation exposure maps to pick the best one.
        bkg_result = generate_best_background(
            base_path, save_path, target_ra, target_dec
        )
        if bkg_result is None:
            print("Background generation failed — aborting.")
            return None
        bkg_reg_name = "auto_bkg.reg"
    else:
        # Manual background provided, copy to all directories
        if not os.path.exists(bkg_reg):
            raise FileNotFoundError(f"Background region file not found: {bkg_reg}")

        bkg_reg_name = os.path.basename(bkg_reg)
        print(f"Background region: {bkg_reg}")

        print("Copying background region to observation directories...")
        copy_count = 0
        for root_dir, dirs, files in os.walk(base_path):
            normalised = os.path.normpath(root_dir)
            if normalised.endswith(os.path.join("uvot", "image")):
                try:
                    shutil.copy2(bkg_reg, root_dir)
                    copy_count += 1
                except Exception as e:
                    print(f"  Warning — could not copy to {root_dir}: {e}")
        print(f"Copied background region to {copy_count} folders.\n")

    
    #################################################################################
    # rUN uvotsource on each  obsid and band

    print("=" * 70)
    print("RUNNING UVOTSOURCE")
    print("=" * 70)
 
    processed = 0
    skipped = 0
    failed = 0
 
    for obsid, img_dir in tqdm(image_dirs, desc="Running uvotsource", unit="obs"):
        for band in BANDS:
            finalsource_file = f"{band}_finalsource.fits"
            finalsource_path = os.path.join(img_dir, finalsource_file)
 
            # Skip if already processed
            if os.path.exists(finalsource_path):
                skipped += 1
                continue

            #################################################################################
            # Decide which input file to use.
            #   1. {band}_ex_summed.fits      — summed multi-extension file
            #   2. sw{OBSID}{band}_sk.img     — uncompressed SK (from uvotunicorr)
            #   3. sw{OBSID}{band}_sk.img.gz  — compressed SK (original download)
            
            summed_file = f"{band}_ex_summed.fits"
            sk_file_img = f"sw{obsid}{band}_sk.img"
            sk_file_gz = f"sw{obsid}{band}_sk.img.gz"
 
            input_file = None
            if os.path.exists(os.path.join(img_dir, summed_file)):
                input_file = summed_file
                # Summed files are trusted, the summation step already
                # excluded NONE extensions via the exclude parameter.
            elif os.path.exists(os.path.join(img_dir, sk_file_img)):
                input_file = sk_file_img
            elif os.path.exists(os.path.join(img_dir, sk_file_gz)):
                input_file = sk_file_gz
            else:
                # No file for this band in this observation — totally normal,
                # not every observation has every band.
                continue

            ################################################################
            # ASPCORR SAFETY CHECK AGAIN 
            # verify the file is actually corrected before running uvotsource on it.
            #
            # If the input is a summed file, we trust it (summation already
            # excluded NONE extensions). If it's a raw SK file, we open
            # it and check that ALL image extensions have DIRECT/UNICORR.
            # 
            if input_file != summed_file:
                input_full_path = os.path.join(img_dir, input_file)
                try:
                    all_good = True
                    has_image_ext = False
                    ext_statuses = []
                    with fits.open(input_full_path) as hdul:
                        for idx, hdu in enumerate(hdul):
                            naxis = hdu.header.get('NAXIS', 0)
                            if naxis < 2:
                                continue
                            has_image_ext = True
                            val = str(hdu.header.get('ASPCORR', 'NONE')).strip().upper()
                            ext_statuses.append((idx, val))
                            if val not in ('DIRECT', 'UNICORR'):
                                all_good = False

                    if not has_image_ext:
                        print(f"  [{obsid} / {band}] No image extensions — skipping")
                        skipped += 1
                        continue

                    if not all_good:
                        print(f"  [{obsid} / {band}] ASPCORR not fully corrected — skipping")
                        print(f"    File: {input_file}")
                        print(f"    Extensions: {ext_statuses}")
                        skipped += 1
                        continue
 
                except Exception as e:
                    print(f"  [{obsid} / {band}] Cannot verify ASPCORR ({e}) — skipping")
                    skipped += 1
                    continue
 
            # Verify region files are present in the directory
            if not os.path.exists(os.path.join(img_dir, src_reg_name)):
                print(f"  [{obsid} / {band}] Source region missing — skipping")
                skipped += 1
                continue
            if not os.path.exists(os.path.join(img_dir, bkg_reg_name)):
                print(f"  [{obsid} / {band}] Background region missing — skipping")
                skipped += 1
                continue
 
            print(f"  [{obsid} / {band}] Using {input_file}")



            ################################################################
            # Find the best available exposure map for this band.
            # Priority matches the SK file priority:
            #   1. {band}_expmap_summed.fits — summed exposure map
            #   2. sw{OBSID}{band}_ex.img    — uncompressed
            #   3. sw{OBSID}{band}_ex.img.gz — compressed
            #   4. NONE                       — no exposure map available
            # 
            exp_img = f"sw{obsid}{band}_ex.img"
            exp_gz = f"sw{obsid}{band}_ex.img.gz"
            exp_summed = f"{band}_expmap_summed.fits"

            exp_file = "NONE"
            if input_file == summed_file:
                # Summed SK, use summed exposure map (extension names match)
                if os.path.exists(os.path.join(img_dir, exp_summed)):
                    exp_file = exp_summed
            else:
                # Raw SK, use raw exposure map (extension names match) Because it can make sums
                # But if it tries to use them has a "2d error" which I dont 100% get, but I sure do know thats what im seeing.
                if os.path.exists(os.path.join(img_dir, exp_img)):
                    exp_file = exp_img
                elif os.path.exists(os.path.join(img_dir, exp_gz)):
                    exp_file = exp_gz

            if exp_file != "NONE":
                print(f"Exposure map: {exp_file}")
            else:
                print(f"Exposure map: NONE (not available)")

            print(f"Running uvotsource ...")
            
            #################################################################################
            # Build and run the uvotsource command

            if HEASOFT_BACKEND == "wsl":
                wsl_img_dir = prepare_path(img_dir)
                uvotsource_cmd = (
                    f"cd '{wsl_img_dir}' && "
                    f"uvotsource image='{input_file}' "
                    f"srcreg='{src_reg_name}' "
                    f"bkgreg='{bkg_reg_name}' "
                    f"sigma=5 "
                    f"expfile='{exp_file}' "
                    f"zerofile=CALDB coinfile=CALDB psffile=CALDB lssfile=CALDB "
                    f"syserr=NO frametime=DEFAULT apercorr=NONE output=ALL "
                    f"outfile='{finalsource_file}' "
                    f"cleanup=YES clobber=YES chatter=1"
                )
            else:
                uvotsource_cmd = (
                    f"cd '{img_dir}' && "
                    f"uvotsource image='{input_file}' "
                    f"srcreg='{src_reg_name}' "
                    f"bkgreg='{bkg_reg_name}' "
                    f"sigma=5 "
                    f"expfile='{exp_file}' "
                    f"zerofile=CALDB coinfile=CALDB psffile=CALDB lssfile=CALDB "
                    f"syserr=NO frametime=DEFAULT apercorr=NONE output=ALL "
                    f"outfile='{finalsource_file}' "
                    f"cleanup=YES clobber=YES chatter=1"
                )

            run_heasoft_command(uvotsource_cmd)
            time.sleep(1)

            # Check result
            if os.path.exists(finalsource_path):
                print(f" ✅ Created {finalsource_file}")
                processed += 1
            else:
                print(f" ❌ uvotsource did not produce output")
                failed += 1

    print(f"\n{'─' * 70}")
    print(f"UVOTSOURCE SUMMARY")
    print(f" Processed : {processed}")
    print(f" Skipped   : {skipped} (already existed or missing region)")
    print(f" Failed    : {failed}")
    print(f"{'─' * 70}\n")


    #################################################################################
    # Upper-limit pass for failed-detect frames
    # Run uvotsource at the catalog position with a small aperture for
    # any file that was moved to DetectFailed/ during region generation.
    # These yield upper-limit magnitudes which still tell us "the source
    # was at most this bright" — useful for light-curve analysis.
    #################################################################################
    if target_ra is not None and target_dec is not None:
        run_upper_limit_uvotsource(
            obs_table=obs_table,
            base_path=base_path,
            save_path=save_path,
            target_ra=target_ra,
            target_dec=target_dec,
        )
        
    #################################################################################
    # Read finalsource files and make Master CSV 
   
    print("=" * 70)
    print("COMPILING PHOTOMETRY DATA")
    print("=" * 70)
 
    all_rows = []
    band_tables = {b: [] for b in BANDS}
    files_found = 0
    files_loaded = 0
 
    for root_dir, dirs, files in os.walk(base_path):
        # Skip quarantine directories
        path_parts = set(root_dir.replace("\\", "/").split("/"))
        if path_parts & {"Smeared", "NotASPCORR", "Orphans"}:
            continue
 
        for f in files:
            # Match either normal finalsource OR upper-limit finalsource
            # Examples:
            # uw1_finalsource.fits     (normal detection)
            # uw1_finalsource_ul.fits  (upper limit)
            is_upper_limit = False
            if f.endswith("_finalsource_ul.fits"):
                is_upper_limit = True
            elif f.endswith("_finalsource.fits"):
                is_upper_limit = False
            else:
                continue
 
            files_found += 1
            filepath = os.path.join(root_dir, f)
 
            # Extract band from filename
            # "uw1_finalsource.fits" -> "uw1"
            # "uw1_finalsource_ul.fits" -> "uw1"
            if is_upper_limit:
                band_match = re.match(r"([a-z0-9]+)_finalsource_ul\.fits$", f)
            else:
                band_match = re.match(r"([a-z0-9]+)_finalsource\.fits$", f)
            if not band_match:
                continue
            band = band_match.group(1)
            if band not in BANDS:
                continue
 
            # Extract OBSID from the directory path
            obsid_match = re.search(r"(\d{11})", root_dir)
            obsid = obsid_match.group(1) if obsid_match else "UNKNOWN"
 
            try:
                with fits.open(filepath) as hdul:
                    if len(hdul) < 2 or hdul[1].data is None:
                        print(f"  Warning — no table data in {filepath}")
                        continue
 
                    data = hdul[1].data
                    df = pd.DataFrame(np.array(data).byteswap().newbyteorder())
 
                    # EXTNAME is a FITS header keyword (extension name), not
                    # photometry data. Some versions of astropy/numpy include
                    # it when converting the binary table to an array. If it
                    # ended up as a column, drop it — it becomes NaN during
                    # concat and causes downstream issues.
                    if 'EXTNAME' in df.columns:
                        df.drop(columns=['EXTNAME'], inplace=True)
 
                    # Attach metadata
                    df["OBSID"] = obsid
                    df["BAND"] = band
                    df["SOURCE_FILE"] = filepath
                    df["UpperLimit"] = is_upper_limit

                    # For upper-limit rows, the meaningful value is the
                    # 3-sigma limiting magnitude (AB_MAG_LIM), not AB_MAG
                    # (which is unreliable for a non-detection). Surface it
                    # in a dedicated column the plotting code can read
                    # uniformly: PLOT_MAG holds AB_MAG for detections and
                    # AB_MAG_LIM for upper limits.
                    if is_upper_limit and 'AB_MAG_LIM' in df.columns:
                        df["PLOT_MAG"] = df["AB_MAG_LIM"]
                    elif 'AB_MAG' in df.columns:
                        df["PLOT_MAG"] = df["AB_MAG"]

                    all_rows.append(df)
                    band_tables[band].append(df)
                    files_loaded += 1
                    flag = " [UL]" if is_upper_limit else ""
                    print(f"  Loaded {f}  (ObsID {obsid}){flag}")
 
            except Exception as e:
                print(f"  Error reading {filepath}: {e}")
 
    # Combine everything
    print(f"\n  Finalsource files found: {files_found}")
    print(f"  Successfully loaded: {files_loaded}")
 
    if all_rows:
        df_all = pd.concat(all_rows, ignore_index=True)
 
        # Drop any columns that are entirely NaN — these can appear when
        # concat merges DataFrames with slightly different column sets
        # (e.g. if one finalsource file has an extra header-derived column
        # like EXTNAME that others don't).
        all_nan_cols = [c for c in df_all.columns if df_all[c].isna().all()]
        if all_nan_cols:
            print(f"  Dropping all-NaN columns: {all_nan_cols}")
            df_all.drop(columns=all_nan_cols, inplace=True)
 
        print(f"  Master table: {len(df_all)} rows, {len(df_all.columns)} columns")
    else:
        df_all = pd.DataFrame()
        print("\n ⚠️ No finalsource files found — photometry table is empty.")
 
    band_dfs = {}
    for band, dfs in band_tables.items():
        band_dfs[band] = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    #################################################################################
    # Write Excel workbook (All_Data + per-band sheets + Summary)

    #################################################################################
    # FINAL SSS FILTER: drop any rows where AB_MAG=99 or AB_MAG_ERR=99
    # These are extensions where the source landed on a bad pixel 
    # whether caught by pre-summation SSS check (multi-ext) or surfacing
    # here for single-ext observations where pre-check couldn't help.
    # For review this is mkaing a "final_sss_dropped.txt" — rows that got dropped, for inspection
    # and "sss_failures.csv" — extensions flagged at the pre-summation check
    #################################################################################
    sss_dropped = 0
    if not df_all.empty:
        is_upper_limit = (df_all['UpperLimit'] == True
                          if 'UpperLimit' in df_all.columns
                          else pd.Series(False, index=df_all.index))

        mag_99_mask = pd.Series(False, index=df_all.index)
        if 'AB_MAG' in df_all.columns:
            mag_99_mask |= (df_all['AB_MAG'] == 99.0)
            mag_99_mask |= (~np.isfinite(df_all['AB_MAG']))
        if 'AB_MAG_ERR' in df_all.columns:
            mag_99_mask |= (df_all['AB_MAG_ERR'] == 99.0)

        # Don't drop upper-limit rows: their AB_MAG is legitimately 99
        # because there's no detection. Their value lives in AB_MAG_LIM.
        mag_99_mask &= ~is_upper_limit

        sss_dropped = int(mag_99_mask.sum())
        if sss_dropped > 0:
            # Save the dropped rows for diagnostic
            dropped = df_all.loc[mag_99_mask].copy()
            dropped_path = os.path.join(save_path, "final_sss_dropped.txt")
            dropped.to_csv(dropped_path, sep='\t', index=False)
            print(f"\n  Final SSS filter: dropped {sss_dropped} rows (AB_MAG=99)")
            print(f"  Dropped rows saved: {dropped_path}")
            df_all = df_all.loc[~mag_99_mask].reset_index(drop=True)

        # Same filter on per-band tables
        for band in list(band_dfs.keys()):
            band_df = band_dfs[band]
            if band_df.empty:
                continue
            band_mask = pd.Series(False, index=band_df.index)
            if 'AB_MAG' in band_df.columns:
                band_mask |= (band_df['AB_MAG'] == 99.0)
                band_mask |= (~np.isfinite(band_df['AB_MAG']))
            if 'AB_MAG_ERR' in band_df.columns:
                band_mask |= (band_df['AB_MAG_ERR'] == 99.0)
            band_dfs[band] = band_df.loc[~band_mask].reset_index(drop=True)

    #######################################################################
    # WRITE TAB-SEPARATED .TXT (universal output for code)
    # Also  I want a comma-separated .csv if WRITE_CSV_COPY is True
    # (human-readable for visual inspection)
    #######################################################################
    if not df_all.empty:
        txt_path = os.path.join(save_path, "master_photometry.txt")
        df_all.to_csv(txt_path, sep='\t', index=False)
        print(f"  Master photometry saved: {txt_path}")
        print(f"    Rows: {len(df_all)}, Columns: {len(df_all.columns)}")
        print(f"    Unique OBSIDs: {df_all['OBSID'].nunique()}")
        if sss_dropped > 0:
            print(f"    Dropped from final output: {sss_dropped} (SSS = AB_MAG=99)")
        # Optional human-readable CSV copy
        if WRITE_CSV_COPY:
            csv_path = os.path.join(save_path, "master_photometry.csv")
            df_all.to_csv(csv_path, index=False)
            print(f"  CSV copy saved (human-inspection): {csv_path}")
        #####################################################################
        # AUTO-GENERATE LIGHT CURVES

        try:
            # x-axis range from the data itself.
            # df_all carries TSTART (Swift MET secs); MJD = MET/86400 + 51910.
            try:
                if 'MJD' in df_all.columns:
                    _mjd = pd.to_numeric(df_all['MJD'], errors='coerce').dropna()
                elif 'TSTART' in df_all.columns:
                    _t = pd.to_numeric(df_all['TSTART'], errors='coerce').dropna()
                    _mjd = _t / 86400.0 + 51910.0
                else:
                    _mjd = None

                if _mjd is not None and len(_mjd) > 0:
                    pad = 50.0
                    plot_xlim = (float(_mjd.min()) - pad, float(_mjd.max()) + pad)
                else:
                    plot_xlim = (53000, 62000)
            except Exception:
                plot_xlim = (53000, 62000)

            for tag, want_ul in (("no_ul", False), ("with_ul", True)):
                try:
                    plot_uvot_lightcurves(
                        excel_file=txt_path,
                        xlim=plot_xlim,
                        ogle_file=None,        # UVOT-only auto plots
                        xrt_files=None,
                        overlay_plot=True,
                        stacked_plot=True,
                        save_prefix=os.path.join(save_path, f"lightcurve_{tag}"),
                        Upperlimits=want_ul,
                    )
                    print(f"  Saved light curve set: lightcurve_{tag}_*.png")
                except Exception as e:
                    print(f"  WARNING: light curve '{tag}' failed: {e}")

        except ImportError:
            print("  NOTE: plot_uvot_lightcurves.py not importable — "
                  "skipping auto light curves.")
        except Exception as e:
            print(f"  WARNING: auto light-curve step failed: {e}")

    else:
        print("\n  WARNING: No photometry to write.")

    #################################################################################
    # Return
    if automation_mode:
        return df_all
    else:
        print(f"\n{'=' * 70}")
        print("UVOTSOURCE PIPELINE COMPLETE")
        print(f"{'=' * 70}")
        return None



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

def generate_best_background(base_path, save_path, target_ra, target_dec, bkg_radius=8.0, n_candidates=10, threshold=1.0, output_name="auto_bkg.reg"):
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
    QUARANTINE = {"Smeared", "NotASPCORR", "Orphans"}
    BANDS = ["uvv", "uuu", "ubb", "um2", "uw1", "uw2"]

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

    for root, dirs, files in os.walk(base_path):
        normalised = os.path.normpath(root)
        if not normalised.endswith(os.path.join("uvot", "image")):
            continue
        path_parts = normalised.split(os.sep)
        if any(q in path_parts for q in QUARANTINE):
            continue

        obsid_match = re.search(r"(\d{11})", root)
        obsid = obsid_match.group(1) if obsid_match else "?"
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
        _write_background_regions(base_path, best, bkg_radius, output_name, QUARANTINE)
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

    n_written = _write_background_regions(
        base_path, best_cand, bkg_radius, output_name, QUARANTINE
    )
    print(f"\n  Wrote {n_written} background region files ({output_name})")

    return best_cand


def _write_background_regions(base_path, candidate, bkg_radius, output_name,
                              quarantine_folders):
    """Write background region file into every uvot/image directory."""
    count = 0

    for root, dirs, files in os.walk(base_path):
        normalised = os.path.normpath(root)
        if not normalised.endswith(os.path.join("uvot", "image")):
            continue
        path_parts = normalised.split(os.sep)
        if any(q in path_parts for q in quarantine_folders):
            continue

        reg_path = os.path.join(root, output_name)
        reg_text = (
            f'# Region file format: DS9 version 4.1\n'
            f'# Auto-generated background region\n'
            f'fk5\n'
            f'circle({candidate["ra"]},{candidate["dec"]},{bkg_radius}")\n'
        )
        with open(reg_path, 'w') as f:
            f.write(reg_text)
        count += 1

    return count


import time
import gc


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

    
    ###############################################################################
    # RETRY STATE VARIABLES
    # Determine the retry sequence based on mode
    if manual_mode:
        # Manual mode: use provided params, then prompt between attempts
        if side_buffer is None:
            side_buffer = ASPECT_RETRY_LADDER[0][0]
        if num_stars is None:
            num_stars = ASPECT_RETRY_LADDER[0][1]
        retry_sequence = [(side_buffer, num_stars)]  # Will extend via prompts
    else:
        # Automatic mode: use the full ladder
        retry_sequence = list(ASPECT_RETRY_LADDER)
    
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
        # need access to DIRECT reference frames 
        # Originally the error here was I did not keep the table open
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

        # DEBUG (The first on many... Unless I already deleted most of them): Print the group status on first attempt
        if attempt_num == 0:
            print("\nGroup Status Breakdown:")
            for status in ['COMPLETED', 'READY', 'ORPHAN', 'UNICORR']:
                count = len(working_table[working_table['Group_Status'] == status]['Group_ID'].unique())
                print(f"  {status}: {count} groups")
            print()

        ###############################################################################
        # The Main loop
        
        for group_id in unique_groups:
            group_data = working_table[working_table['Group_ID'] == group_id]
            group_status = group_data['Group_Status'].iloc[0]

            # Skip groups that don't need processing
            Orphans_Exist = 0
            # ORPHAN groups have no related observations to use as references, Thomas will take care of this. I think, Or he will tell me how to.
            if group_status == 'ORPHAN':
                # Record any remaining failures for this group+band combination
                Orphans_Exist += 1
                if attempt_num == 0:
                    print(f"\n[Group {group_id}] Status: ORPHAN - Skipping")
                continue

            # COMPLETED groups.... Are completed.
            if group_status == 'COMPLETED':
                if attempt_num == 0:
                    print(f"\n[Group {group_id}] Status: COMPLETED - Already done")
                continue

            # UNICORR groups Are also completed.
            if group_status == 'UNICORR':
                if attempt_num == 0:
                    print(f"\n[Group {group_id}] Status: UNICORR - Already done")
                continue

            if Orphans_Exist > 0:
                key = f"{group_id}_{band}"
                aspectnone_dict[key] = Orphans_Exist
                # Build list of Orphan frame identifiers for retry/manual inspection
                Orphan_obsids = []
                for idx, obs_row in corrections_needed.iterrows():
                    Orphan_obsids.append(f"{obs_row['ObsID']}_ext{obs_row['Snapshot']}")
                aspectnone_tiles_dict[key] = Orphan_obsids[:remaining]
                
            # Print out to know what the code is on.
            print(f"\n{'=' * 70}")
            print(f"Processing Group {group_id} (Status: {group_status})")
            print(f"{'=' * 70}")

            ###############################################################################
            # Go over bands within the group
            
            # CHANGE FROM clean_uvot_tiles.py:
            #   clean_uvot_tiles was hardcoded to only process 'uw1' (all
            #   file paths contained 'uw1'). This function processes every
            #   band present in the group.
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
                    # clean_uvot_tiles used a single global reference, I said it before.
                    suitable_ref = None

                    # Find any DIRECT reference for this band. uvotunicorr
                    # corrects the WCS regardless of which extension the
                    # reference came from, I was trying to match extensions
                    # On the old version, that was pointless.
                    for _, ref_candidate in ref_candidates.iterrows():
                        candidate_path = ref_candidate['Full_Path']
                        candidate_obsid = ref_candidate['ObsID']
                        candidate_snapshot = ref_candidate['Snapshot']

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
                                'snapshot': candidate_snapshot,
                                'full_path': os.path.join(ref_dir_check, ref_file_found),
                                'dir': ref_dir_check
                            }
                            print(f"    Using reference: ObsID {candidate_obsid}, "
                                  f"Extension {candidate_snapshot}")
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
                            num_stars=current_num_stars,
                            side_buffer=current_side_buffer
                        )

                        obs_bright_stars = find_brightest_central_stars(
                            obs_detect_file,
                            num_stars=current_num_stars,
                            side_buffer=current_side_buffer
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
                                if HEASOFT_BACKEND == "wsl":
                                    wsl_path = prepare_path(ref_full_path)
                                    run_heasoft_command(f"gunzip -k '{wsl_path}'")
                                else:
                                    run_heasoft_command(f"gunzip -k '{ref_full_path}'")
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
                                if HEASOFT_BACKEND == "wsl":
                                    wsl_path = prepare_path(obs_img_path)
                                    run_heasoft_command(f"gunzip -k '{wsl_path}'")
                                else:
                                    run_heasoft_command(f"gunzip -k '{obs_img_path}'")
                            obs_img_path = obs_img_unzipped

                        if not os.path.exists(obs_img_path):
                            print(" ❌ Failed to unzip obs image")
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
                        # Native and WSL both use the same command builder
                        unicorr_command = create_uvotunicorr_command(
                            ref_frame=ref_obsid,
                            obs_frame=obs_obsid,
                            band=band,
                            ref_snapshot=ref_snapshot,
                            obs_snapshot=obs_snapshot,
                            obspath=obs_dir
                        )

                        print(f" Running uvotunicorr...")
                        gc.collect()
                        corrections_attempted += 1
                        run_heasoft_command(unicorr_command)
                        time.sleep(5)

                            
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
                                        print(f"✅ Correction successful - ASPCORR = {aspcorr_after}")
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
                        corrections_failed += 1   

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
            print("\n✅ All frames successfully corrected!")
            break

        
        # Because that wont happen, retry prompt.
        # CHANGE FROM clean_uvot_tiles.py:
        #   clean_uvot_tiles had a nearly identical prompt:
        #       go_again = input('Do you wish to change the global parameters...? [Y/N]')
        #   The key difference is that on retry, clean_uvot_tiles re-ran
        #   EVERYTHING (download, detect, smear, unzip, correct). We only
        #   re-run the correction step on the specific failed frames.
        
        # Prepare for next attempt
        failed_frames_to_retry = aspectnone_tiles_dict.copy()
        attempt_num += 1
        
        # Manual mode: prompt user to decide next step
        if manual_mode:
            print(f"\n {total_remaining} frames failed.")
            print("\nFailed frames by group:")
            for key, count in aspectnone_dict.items():
                print(f" {key}: {count} frames")
            
            retry = input("\nRetry with different parameters? (yes/no): ").strip().lower()
            if retry not in ['yes', 'y']:
                print("Stopping correction process.")
                break
            
            try:
                new_sb = input(f"New side_buffer (current: {current_side_buffer}, Enter to keep): ").strip()
                new_ns = input(f"New num_stars (current: {current_num_stars}, Enter to keep): ").strip()
                new_sb = int(new_sb) if new_sb else current_side_buffer
                new_ns = int(new_ns) if new_ns else current_num_stars
                retry_sequence.append((new_sb, new_ns))
            except ValueError:
                print("Invalid input — stopping.")
                break
        else:
            # Automatic mode: just continue to next params in ladder
            if attempt_num < len(retry_sequence):
                print(f"\n⚠ {total_remaining} frames failed at "
                      f"({current_side_buffer}\", {current_num_stars} stars).")
                print(f"Advancing to next attempt with "
                      f"({retry_sequence[attempt_num][0]}\", "
                      f"{retry_sequence[attempt_num][1]} stars)...")

    #######################################################################################
    # The End
    print("\n" + "=" * 70)
    print("ASPECT CORRECTION FINAL SUMMARY")
    print("=" * 70)
    final_remaining = sum(aspectnone_dict.values()) if aspectnone_dict else 0
    print(f"Total frames still needing correction: {final_remaining}")
    print(f"Attempts made: {attempt_num + 1}")
    
    if final_remaining > 0:
        print(f"\nFailed frames will be moved to NotASPCORR/ by Step 3.5")
        print("\nFailed frames by group:")
        for key, count in aspectnone_dict.items():
            print(f"  {key}: {count} frames")
    
    return aspectnone_dict, aspectnone_tiles_dict



#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import subprocess
import sys
import platform

import math
import pandas as pd
import numpy as np
import re

import shutil
#import uvot_pipeline as up
import argparse
import warnings
import requests

from tqdm import tqdm
#from sh import gunzip

import astropy.units as u
from astropy.io import fits
from astropy.coordinates import SkyCoord
from astropy.time import Time
from astropy.wcs import WCS
from astropy.table import QTable, Table

from swifttools.swift_too import Clock,TOO, Resolve, ObsQuery, Data

import tkinter as tk
from tkinter import filedialog


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
    print()
    print(" [Batch input file format]")
    print(" The file must have a header row with these columns")
    print(" (case-insensitive; multiple alias names accepted):")
    print(" Target    (or: Name, Source, Source_Name, Object)")
    print(" RA        (or: RA_deg, RA_obj, Right_Ascension)   in degrees")
    print(" Dec       (or: De, Dec_deg, De_obj, Declination)  in degrees")
    print(" Radius    (or: Search_Radius, R)  in degrees [OPTIONAL]")
    print(" Threshold (or: Detect_Threshold, Sigma)   sigma [OPTIONAL]")
    print(f" If no Threshold column is given, {DEFAULT_DETECT_THRESHOLD} sigma is used.")
    print(f" If no Radius column is given, {DEFAULT_SEARCH_RADIUS} deg is used.")
    print(" 3' will taget only observation directly targeting your source, while above 3' adds nearby targets")
    print(" with 15' you will begin adding targets where the source is on the edge with 17' being the whole FOV of the instrument.")
    print(" .csv = comma-separated, .txt = tab-separated. Auto-detected.")

    while True:
        choice = input("\nEnter your choice (1, 2, 3, 4, or 5): ").strip()
        if choice in ['1', '2', '3', '4', '5']:
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

        for i, q in enumerate(query, start=1):
            start_time_str = str(q.begin).replace(":", "-").replace(" ", "_")
            obs_dir = os.path.join(data_directory, f"{q.obsid}_{start_time_str}")
            os.makedirs(obs_dir, exist_ok=True)

            if os.listdir(obs_dir):
                print(f"[{i}/{total_obs}] ObsID {q.obsid} already exists, skipping...")
                continue

            print(f"[{i}/{total_obs}] Downloading ObsID {q.obsid} "
                  f"(Date: {q.begin})...")
            try:
                Data(obsid=q.obsid, uvot=True, clobber=True, outdir=obs_dir)
                print(f"Successfully downloaded to {obs_dir}")
            except Exception as e:
                print(f"Failed to download: {e}")

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



def clean_up_data(automation_mode=False, base_path=None, save_path=None, detect_threshold=3.0):
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
    
    # 1. RUN UVOT DETECT
    if not automation_mode:
        print("\n=== Running UVOT Detect ===")
    try:
        batch_run_uvotdetect(base_path, threshold=detect_threshold)
    except Exception as e:
        print(f" UVOTDETECT failed: {e}")
        if not automation_mode:
            import traceback
            traceback.print_exc()
    
    
    # 2. SMEAR DETECTION (per-extension)
    if not automation_mode:
        print("\n=== Detecting Smeared Frames ===")
    try:
        smeared_list, smeared_extensions = detect_smeared_frames(base_path)
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
            
        # 3.5. POPULATE OBSERVATIONS TABLE
        try:
            obs_table = populate_observations_table(base_path, all_frames, summary)
            # Apply both wholesale-obs and per-extension smearing flags
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
            print(f" Failed to populate observations table: {e}")
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
    BANDS = ["uvv", "uuu", "ubb", "um2", "uw1", "uw2"]
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






########################################## 
# Light Curve Generating Function

def plot_uvot_lightcurves(
    bands_to_plot=None,
    xlim=(54000, 61000),
    excel_file=r"C:\Users\05ble\OneDrive\Desktop\UVOT2 - Orphan Testing 3\master_photometry.txt",
    ogle_file=r"C:/Users/05ble/OneDrive/Desktop/BEXRAY Stuff/CSV and Data/SXP5_05Ogle_clean.csv", #These are artifacts from my code I use, that I kept Incase I wanted to use them
    xrt_files={"XRT": r"C:/Users/05ble/OneDrive/Desktop/BEXRAY Stuff/CSV and Data/XRT/SC1966_XRT.csv",
            "XRT_UL": r"C:/Users/05ble/OneDrive/Desktop/BEXRAY Stuff/CSV and Data/XRT/SC1966_XRT_UL.csv",
            "XRT_TOO": r"C:/Users/05ble/OneDrive/Desktop/BEXRAY Stuff/CSV and Data/XRT/SC1966_XRT_TOO.csv",
            "XRT_WT": r"C:/Users/05ble/OneDrive/Desktop/BEXRAY Stuff/CSV and Data/XRT/SC1966_XRT_TOO_WT.csv"},
    overlay_plot=True,
    stacked_plot=True,
    save_prefix=None,
    Upperlimits=False,
):

    """
    Plot UVOT + OGLE magnitudes and XRT count rates.

    Parameters You Can Set!
    ----------
    bands_to_plot : list, "auto", or None
        Which UVOT bands to plot.  None = all 6 defaults.
        Single-element list like ["uw1"] enables 3-sigma lines.
        "auto" = plot whatever bands exist in the data.
    xlim : tuple
        (min_MJD, max_MJD) for the x-axis.
    excel_file : str
        Path to master_photometry.txt (tab-separated) or .csv.
    ogle_file : str or None
        Path to OGLE CSV.  None = no OGLE data plotted.
    xrt_files : dict or None
        Dictionary of {"label": "filepath"} for XRT CSVs.
        Pass whatever you have — e.g. just {"XRT": "path.csv"} is fine.
        Labels containing "UL" are treated as upper limits.
        None = no XRT data plotted.
    overlay_plot : bool
        Produce the combined overlay plot.
    stacked_plot : bool
        Produce the 3-panel stacked plot.
    save_prefix : str or None
        If set, saves PNGs with this prefix.
    Upperlimits : bool
        If True, draw non-detection upper limits as open downward
        triangles (in each band's colour) on both plots.  If False,
        upper limits are ignored entirely.  Default False.
    """

    import pandas as pd
    import numpy as np
    import matplotlib.pyplot as plt
    from astropy.time import Time

    default_bands = ["uvv", "ubb", "uuu", "uw1", "uw2", "um2"]
    mag_col = 'AB_MAG'
    mag_err_col = 'AB_MAG_ERR'

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
    # The pipeline's UVOT_Data_Analysis.xlsx may or may not have DATE_TAG.
    # uvotsource FITS output includes TSTART (MET seconds) which gets
    # carried into the Excel/CSV. We try multiple ways to get a time:
    #   1. If MJD column already exists, use it
    #   2. If TSTART exists (Swift MET), convert to MJD
    #   3. If DATE_TAG exists and is parseable, convert to MJD
    #   4. If a column name contains "mjd" or "time", try that
    ##########################################################################
    if 'MJD' in df.columns:
        df['MJD'] = pd.to_numeric(df['MJD'], errors='coerce')
        df = df[df['MJD'].notna()]
        print(f"  Using existing MJD column ({len(df)} rows)")

    elif 'TSTART' in df.columns:
        # TSTART is Swift Mission Elapsed Time (seconds since 2001-01-01 UTC)
        # Convert to MJD: MJD = TSTART/86400 + 51910.0
        # (51910.0 is the MJD of the Swift reference epoch 2001-01-01T00:00:00)
        df['TSTART'] = pd.to_numeric(df['TSTART'], errors='coerce')
        df = df[df['TSTART'].notna()]
        df['MJD'] = df['TSTART'] / 86400.0 + 51910.0
        print(f"  Converted TSTART → MJD ({len(df)} rows)")

    elif 'DATE_TAG' in df.columns:
        df['OBS_DATE'] = pd.to_datetime(
            df['DATE_TAG'], errors='coerce', format='%Y-%m-%d_%H-%M-%S'
        )
        df = df[pd.notnull(df['OBS_DATE'])]
        if len(df) > 0:
            df['MJD'] = Time(df['OBS_DATE']).mjd
            print(f"  Converted DATE_TAG → MJD ({len(df)} rows)")
        else:
            print("ERROR: DATE_TAG column exists but no dates could be parsed.")
            return
    else:
        # Last resort: look for any column with "mjd" or "time" in the name
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
    # uvotsource FITS output column names can vary depending on how astropy
    # reads them. The names are normally: AB_MAG, AB_MAG_ERR, AB_FLUX, AB_FLUX_ERR,
    # RATE, RATE_ERR, etc. 

    # Search for magnitude column.  Exclude 'lim' so we don't accidentally
    # grab AB_MAG_LIM as the detection magnitude.
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

    # Search for magnitude error column
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
            print(f"  Available columns: {list(df.columns)}")
            df['_ERR'] = 0.0
            mag_err_col = '_ERR'
        else:
            mag_err_col = mag_err_col_actual
            print(f"  Using '{mag_err_col}' as magnitude error column")

    df[mag_col] = pd.to_numeric(df[mag_col], errors='coerce')
    df[mag_err_col] = pd.to_numeric(df[mag_err_col], errors='coerce')

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
    # IDENTIFY UPPER-LIMIT COLUMNS (written by the pipeline)
    #   UpperLimit  : flag, True for non-detection rows
    #   AB_MAG_LIM  : the 3-sigma limiting magnitude (plotted for ULs)
    #   PLOT_MAG    : pipeline convenience col (= AB_MAG_LIM for ULs) — fallback
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
    if ul_mag_col is None:  # fuzzy fallback
        for c in df.columns:
            cl = c.lower().replace(' ', '_')
            if 'mag' in cl and 'lim' in cl:
                ul_mag_col = c
                break
    if ul_mag_col is not None:
        df[ul_mag_col] = pd.to_numeric(df[ul_mag_col], errors='coerce')

    def _is_truthy(v):
        return str(v).strip().lower() in ('true', '1', 'yes', 't')

    ##########################################################################
    # SPLIT DETECTIONS FROM UPPER LIMITS
    # The cleaning below is designed for detections.  Upper-limit rows carry
    # AB_MAG=99 and have no meaningful error, so we set them aside FIRST and
    # never run them through the detection cleaning (otherwise they'd be
    # dropped or mangled).
    ##########################################################################
    if ul_col is not None:
        is_ul_mask = df[ul_col].apply(_is_truthy)
        det_df = df[~is_ul_mask].copy()
        ul_df = df[is_ul_mask].copy()
    else:
        det_df = df.copy()
        ul_df = df.iloc[0:0].copy()  # empty
        if Upperlimits:
            print("  NOTE: Upperlimits=True but no 'UpperLimit' column found "
                  "in the data — nothing to plot as upper limits.")

    ##########################################################################
    # BASIC QUALITY CLEANING (detections only)
    ##########################################################################
    det_df = det_df[np.isfinite(det_df[mag_col]) & np.isfinite(det_df[mag_err_col])]
    det_df = det_df[det_df[mag_err_col] <= 0.35 * det_df[mag_col].abs()]

    det_df.sort_values('MJD', inplace=True)
    merged = det_df

    if Upperlimits and ul_mag_col is not None and not ul_df.empty:
        ul_df = ul_df[np.isfinite(ul_df[ul_mag_col])]
        ul_df.sort_values('MJD', inplace=True)

    print(f"  After quality cleaning: {len(merged)} detections"
          + (f", {len(ul_df)} upper limits" if Upperlimits else ""))

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

            # Rename mag columns to match UVOT names
            # OGLE typically has "Magnitude" and "Magnitude_Error"
            # We need to rename them to whatever mag_col / mag_err_col are
            ogle_mag_col = None
            ogle_err_col = None
            for c in ogle_df.columns:
                cl = c.lower()
                # Find the magnitude column (but not the error column)
                if 'magnitude' in cl and 'error' not in cl and 'err' not in cl:
                    ogle_mag_col = c
                # Find the error column
                elif 'error' in cl or ('err' in cl and 'mag' in cl):
                    ogle_err_col = c
            # Fallback: try simpler patterns
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

            # Verify the rename worked
            if mag_col not in ogle_df.columns or mag_err_col not in ogle_df.columns:
                print(f"  WARNING: OGLE columns could not be mapped. "
                      f"Has: {list(ogle_df.columns)}")
                print(f"  Need: '{mag_col}' and '{mag_err_col}'")
                ogle_df = None
            else:
                print(f"  OGLE: {len(ogle_df)} data points")
        except Exception as e:
            print(f"  WARNING: Could not load OGLE file: {e}")
            ogle_df = None

    ##########################################################################
    # LOAD XRT DATA (optional,)
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
                print(f"  WARNING: could not read {label} ({path}): {e}")
                continue

            mjd_c = find_col(xdf.columns, 'mjd')
            rate_c = find_col(xdf.columns, 'count rate')
            err_pos_c = find_col(xdf.columns, 'positive')
            err_neg_c = find_col(xdf.columns, 'negative')

            if mjd_c is None or rate_c is None:
                if xdf.shape[1] >= 2:
                    mjd_c = xdf.columns[0]
                    rate_c = xdf.columns[1]
                else:
                    print(f"  Skipping {label}: Cannot identify columns.")
                    continue

            # Upper limits if label contains "UL" or all errors are 0
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
                    pd.to_numeric(xdf[err_pos_c], errors='coerce').abs() if err_pos_c else np.nan
                )
                temp['Count Rate Negative Error'] = (
                    pd.to_numeric(xdf[err_neg_c], errors='coerce').abs() if err_neg_c else np.nan
                )
                temp['Type'] = 'Normal'
                temp = temp.dropna(subset=['MJD (days)', 'Count Rate (counts per second)'])

            temp['Label'] = label
            xrt_list.append(temp)
            print(f"  {label}: {len(temp)} data points")

    xrt_all = pd.concat(xrt_list, ignore_index=True, sort=False) if xrt_list else pd.DataFrame()

    ##########################################################################
    # DETERMINE BANDS TO PLOT
    ##########################################################################
    if bands_to_plot is None:
        plot_bands = default_bands.copy()
    elif bands_to_plot == 'auto':
        plot_bands = list(merged[band_col_name].str.lower().unique())
    else:
        plot_bands = [b.lower() for b in bands_to_plot]

    # If a band only has upper limits (no surviving detections) it would be
    # missing from 'auto'/None — add those bands so their ULs can show.
    if (Upperlimits and not ul_df.empty
            and (bands_to_plot is None or bands_to_plot == 'auto')):
        for b in ul_df[band_col_name].str.lower().unique():
            if b not in plot_bands and b != 'ogle':
                plot_bands.append(b)

    if ogle_df is not None and 'OGLE' not in [b.upper() for b in plot_bands]:
        plot_bands.append('OGLE')

    cmap = plt.get_cmap('tab10')
    band_colors = {b: cmap(i % 10) for i, b in enumerate(plot_bands)}

    ##########################################################################
    # 3-SIGMA CALCULATION (single UVOT band only)
    ##########################################################################
    show_sigma3 = False
    sigma3_band = None
    sigma3_mean_mag = None
    sigma3_upper = None
    sigma3_lower = None

    if (bands_to_plot is not None
            and bands_to_plot != 'auto'
            and isinstance(bands_to_plot, list)
            and len(bands_to_plot) == 1
            and bands_to_plot[0].lower() != 'ogle'):

        sigma3_band = bands_to_plot[0]
        band_data = merged[merged[band_col_name].str.lower() == sigma3_band.lower()]
        band_data = band_data[(band_data['MJD'] >= xlim[0]) & (band_data['MJD'] <= xlim[1])]

        if len(band_data) > 0:
            mean_mag = band_data[mag_col].mean()
            mean_err = band_data[mag_err_col].abs().mean()

            sigma3_mean_mag = mean_mag
            sigma3_upper = mean_mag + 3 * mean_err
            sigma3_lower = mean_mag - 3 * mean_err
            show_sigma3 = True

            outside = band_data[
                (band_data[mag_col] < sigma3_lower) | (band_data[mag_col] > sigma3_upper)
            ]
            print(f"\n── 3σ Summary for {sigma3_band.upper()} "
                  f"(MJD {xlim[0]}–{xlim[1]}) ──")
            print(f"  N points       : {len(band_data)}")
            print(f"  Mean AB_MAG    : {mean_mag:.4f}")
            print(f"  Mean AB_MAG_ERR: {mean_err:.4f}")
            print(f"  3σ range       : [{sigma3_lower:.4f}, {sigma3_upper:.4f}]")
            print(f"  Points outside : {len(outside)} / {len(band_data)}"
                  f"  ({100 * len(outside) / len(band_data):.1f}%)\n")

    def draw_sigma3_lines(ax):
        if not show_sigma3:
            return
        ax.axhline(sigma3_mean_mag, color='gray', linestyle='--',
                    linewidth=1.2, label=f'Mean ({sigma3_mean_mag:.2f})')
        ax.axhline(sigma3_upper, color='red', linestyle='-.',
                    linewidth=1.0, label=f'+3σ ({sigma3_upper:.2f})')
        ax.axhline(sigma3_lower, color='blue', linestyle='-.',
                    linewidth=1.0, label=f'−3σ ({sigma3_lower:.2f})')
        ax.axhspan(sigma3_lower, sigma3_upper, color='gray', alpha=0.08,
                    label='3σ region')

    ##########################################################################
    # UPPER-LIMIT DRAWING HELPER
    # Open downward triangles in each band's colour, no error bars.  This is
    # the standard convention for "the source was at most this bright."
    ##########################################################################
    def draw_upper_limits(ax):
        if not Upperlimits or ul_mag_col is None or ul_df.empty:
            return
        for band in plot_bands:
            if band.lower() == 'ogle':
                continue
            sub = ul_df[ul_df[band_col_name].str.lower() == band.lower()]
            if len(sub) == 0:
                continue
            ax.scatter(
                sub['MJD'], sub[ul_mag_col],
                marker='v', s=55,
                facecolors='none',
                edgecolors=band_colors.get(band, 'gray'),
                linewidths=1.2,
                label=f'{band} (UL)'
            )

    ##########################################################################
    # OVERLAY PLOT
    ##########################################################################
    if overlay_plot:
        fig, ax_mag = plt.subplots(figsize=(16, 6))

        for band in plot_bands:
            if band == 'OGLE':
                sub = ogle_df
            else:
                sub = merged[merged[band_col_name].str.lower() == band.lower()]

            if sub is None or len(sub) == 0:
                continue

            mags = sub[mag_col].values
            errs = np.abs(sub[mag_err_col].values)

            ax_mag.errorbar(
                sub['MJD'], mags, yerr=[errs, errs],
                fmt='o', linestyle='none', capsize=2,
                label=band, markersize=4,
                color=band_colors.get(band, None)
            )

        draw_sigma3_lines(ax_mag)
        draw_upper_limits(ax_mag)

        # XRT on right axis if available
        if not xrt_all.empty:
            ax_xrt = ax_mag.twinx()

            normal = xrt_all[xrt_all['Type'] == 'Normal']
            if not normal.empty:
                ax_xrt.errorbar(
                    normal['MJD (days)'],
                    normal['Count Rate (counts per second)'],
                    yerr=[normal['Count Rate Negative Error'].fillna(0).values,
                          normal['Count Rate Positive Error'].fillna(0).values],
                    fmt='s', linestyle='none', capsize=2, markersize=4,
                    color='black', label='XRT'
                )

            ul = xrt_all[xrt_all['Type'] == 'UL']
            if not ul.empty:
                ax_xrt.scatter(
                    ul['MJD (days)'], ul['Count_Rate_UL'],
                    marker='v', facecolors='none', edgecolors='black',
                    s=60, label='XRT UL'
                )

            ax_xrt.set_ylabel('Count Rate (counts/s)')

            h1, l1 = ax_mag.get_legend_handles_labels()
            h2, l2 = ax_xrt.get_legend_handles_labels()
            by_label = dict(zip(l1 + l2, h1 + h2))
            ax_mag.legend(by_label.values(), by_label.keys(),
                          ncol=3, title='Band/Series')
        else:
            ax_mag.legend(ncol=3, title='Band')

        ax_mag.set_xlabel('MJD')
        ax_mag.set_ylabel('AB Magnitude')
        ax_mag.invert_yaxis()
        ax_mag.set_xlim(*xlim)

        title_suffix = f' — {sigma3_band.upper()} with 3σ' if show_sigma3 else ''
        ul_suffix = ' + upper limits' if (Upperlimits and not ul_df.empty) else ''
        ax_mag.set_title(f'UVOT + OGLE (left) and XRT (right){title_suffix}{ul_suffix}')
        ax_mag.grid(alpha=0.3)

        plt.tight_layout()
        if save_prefix:
            fig.savefig(f'{save_prefix}_overlay.png', dpi=200)
        plt.show()

    ##########################################################################
    # STACKED PLOT 
    ##########################################################################
    if stacked_plot:
        has_ogle = ogle_df is not None and len(ogle_df) > 0
        has_xrt = not xrt_all.empty

        n_panels = 1
        if has_ogle:
            n_panels += 1
        if has_xrt:
            n_panels += 1

        fig, axes = plt.subplots(
            nrows=n_panels, ncols=1, sharex=True,
            figsize=(16, 3 * n_panels),
            gridspec_kw={'height_ratios': [2] * n_panels}
        )

        if n_panels == 1:
            axes = [axes]

        panel_idx = 0

        # UVOT panel
        ax_uvot = axes[panel_idx]
        for band in plot_bands:
            if band.lower() == 'ogle':
                continue
            sub = merged[merged[band_col_name].str.lower() == band.lower()]
            if len(sub) == 0:
                continue
            ax_uvot.errorbar(
                sub['MJD'], sub[mag_col],
                yerr=[sub[mag_err_col].abs(), sub[mag_err_col].abs()],
                fmt='o', linestyle='none', capsize=2, markersize=4,
                label=band, color=band_colors.get(band, None)
            )
        draw_sigma3_lines(ax_uvot)
        draw_upper_limits(ax_uvot)
        ax_uvot.set_ylabel('UVOT AB Mag')
        ax_uvot.invert_yaxis()
        ax_uvot.grid(alpha=0.25)
        ax_uvot.legend(ncol=4)
        panel_idx += 1

        # OGLE panel (if data exists)
        if has_ogle:
            ax_ogle = axes[panel_idx]
            ax_ogle.errorbar(
                ogle_df['MJD'], ogle_df[mag_col],
                yerr=[ogle_df[mag_err_col].abs(), ogle_df[mag_err_col].abs()],
                fmt='o', linestyle='none', capsize=2, markersize=4,
                color='black', label='OGLE'
            )
            ax_ogle.set_ylabel('OGLE Mag')
            ax_ogle.invert_yaxis()
            ax_ogle.grid(alpha=0.25)
            ax_ogle.legend()
            panel_idx += 1

        # XRT panel (if data exists)
        if has_xrt:
            ax_xrt_ax = axes[panel_idx]
            normal = xrt_all[xrt_all['Type'] == 'Normal']
            if not normal.empty:
                ax_xrt_ax.errorbar(
                    normal['MJD (days)'],
                    normal['Count Rate (counts per second)'],
                    yerr=[normal['Count Rate Negative Error'].fillna(0).abs().values,
                          normal['Count Rate Positive Error'].fillna(0).abs().values],
                    fmt='s', linestyle='none', capsize=2, markersize=4,
                    color='darkred', label='XRT'
                )
            ul = xrt_all[xrt_all['Type'] == 'UL']
            if not ul.empty:
                ax_xrt_ax.scatter(
                    ul['MJD (days)'], ul['Count_Rate_UL'],
                    marker='v', s=60, facecolors='none', edgecolors='black',
                    label='UL'
                )
            ax_xrt_ax.set_ylabel('Counts/s')
            ax_xrt_ax.grid(alpha=0.25)
            ax_xrt_ax.legend()

        axes[-1].set_xlabel('MJD')
        axes[-1].set_xlim(*xlim)

        plt.tight_layout()
        if save_prefix:
            fig.savefig(f'{save_prefix}_stacked.png', dpi=200)
        plt.show()
