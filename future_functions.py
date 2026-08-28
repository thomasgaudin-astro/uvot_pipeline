def create_uvotunicorr_too_bash_command(ref_frame, obs_frame, band, snapshot, obspath=None):

    if obspath:
        ref_filepath = obspath+f'/sw{ref_frame}{band}_sk.img[{snapshot}]'
        obs_filepath = obspath+f'/sw{obs_frame}{band}_sk.img[{snapshot}]'
        ref_reg_filepath = obspath+'/ref.reg'
        obs_reg_filepath = obspath+'/obs.reg'
    else:
        ref_filepath = f'sw{ref_frame}{band}_sk.img[{snapshot}]'
        obs_filepath = f'sw{obs_frame}{band}_sk.img[{snapshot}]'
        ref_reg_filepath = 'ref.reg'
        obs_reg_filepath = 'obs.reg'
    
    bash_command = f"""
        bash -c '
        source {os.environ['HEADAS']}/headas-init.sh
        uvotunicorr obsfile={obs_filepath} reffile={ref_filepath} obsreg={obs_reg_filepath} refreg={ref_reg_filepath}
        '
        """

    return bash_command

def create_uvotimsum_too_bash_command(source_name, obsid, band, file_type, exclude=None):
        
        infile_path = f'./{source_name}/TOO/{obsid}/uvot/image/sw{obsid}{band}_{file_type}.img.gz'

        if file_type == 'sk':
            outfile_path = f'./{source_name}/TOO/{obsid}/uvot/image/{band}_summed.fits'
        
        if file_type == 'ex':
            outfile_path = f'./{source_name}/TOO/{obsid}/uvot/image/{band}_ex_summed.fits'
        
        if exclude == None:
            bash_command = f"""
                bash -c '
                source {os.environ['HEADAS']}/headas-init.sh
                uvotimsum infile="{infile_path}" outfile="{outfile_path}"
                '
                """
        else:
            bash_command = f"""
                bash -c '
                source {os.environ['HEADAS']}/headas-init.sh
                uvotimsum infile="{infile_path}" outfile="{outfile_path}" exclude={exclude}
                '
                """

        return bash_command

def run_uvotimsum(uvotimsum_command):

    # Run the command
    result = subprocess.run(
        ['bash', '-i', '-c', uvotimsum_command],
        capture_output=True,
        text=True
    )

    # print("STDOUT:\n", result.stdout)
    # print("STDERR:\n", result.stderr)

    return result.stdout

def run_uvotimsum_verbose(uvotimsum_command):

    # Run the command
    result = subprocess.run(
        ['bash', '-i', '-c', uvotimsum_command],
        capture_output=True,
        text=True
    )

    print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)

    return result.stdout

def create_uvotsource_too_bash_command(source_name, obsid, band, snapshot, source_reg_file, bkg_reg_file):

    trunc_obs_filepath = f'./{source_name}/TOO/{obsid}/uvot/image/'
    obs_filepath = f'./{source_name}/TOO/{obsid}/uvot/image/sw{obsid}{band}_sk.img[{snapshot}]'
    exp_filepath  = f'./{source_name}/TOO/{obsid}/uvot/image/sw{obsid}{band}_ex.img.gz[{snapshot}]'

    if snapshot == 1:
    
        bash_command = f"""
            bash -c '
            source {os.environ['HEADAS']}/headas-init.sh
            uvotsource image="{obs_filepath}" srcreg="{source_reg_file}" bkgreg="{bkg_reg_file}" sigma=5 zerofile=CALDB coinfile=CALDB psffile=CALDB lssfile=CALDB expfile="{exp_filepath}" syserr=NO frametime=DEFAULT apercorr=NONE output=ALL outfile="{trunc_obs_filepath}{band}_source.fits" cleanup=YES clobber=YES chatter=1

            '
            """
    else:
        bash_command = f"""
            bash -c '
            source {os.environ['HEADAS']}/headas-init.sh
            uvotsource image="{obs_filepath}" srcreg="{source_reg_file}" bkgreg="{bkg_reg_file}" sigma=5 zerofile=CALDB coinfile=CALDB psffile=CALDB lssfile=CALDB expfile="{exp_filepath}" syserr=NO frametime=DEFAULT apercorr=NONE output=ALL outfile="{trunc_obs_filepath}{band}_source{snapshot}.fits" cleanup=YES clobber=YES chatter=1

            '
            """

    return bash_command

def create_uvotsource_summed_bash_command(source_name, obsid, band, source_reg_file, bkg_reg_file):

    trunc_obs_filepath = f'./{source_name}/TOO/{obsid}/uvot/image/'
    obs_filepath = f'./{source_name}/TOO/{obsid}/uvot/image/{band}_summed.fits'
    exp_filepath  = f'./{source_name}/TOO/{obsid}/uvot/image/{band}_ex_summed.fits'
    
    bash_command = f"""
        bash -c '
        source {os.environ['HEADAS']}/headas-init.sh
        uvotsource image="{obs_filepath}" srcreg="{source_reg_file}" bkgreg="{bkg_reg_file}" sigma=5 zerofile=CALDB coinfile=CALDB psffile=CALDB lssfile=CALDB expfile="{exp_filepath}" syserr=NO frametime=DEFAULT apercorr=NONE output=ALL outfile="{trunc_obs_filepath}{band}_source.fits" cleanup=YES clobber=YES chatter=1

        '
        """

    return bash_command

def parallel_uvotdetect(filepath, all_filepaths, verbose=False):
    with tqdm(total=len(all_filepaths)) as pbar:
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(single_uvotdetect, filepath, path, verbose) for path in all_filepaths]
            for future in as_completed(futures):
                pbar.update(1)
                yield future.result()

def parallel_uvotsource(all_filepaths, tile, source_name, source_reg, bkg_reg, verbose=False):
    with tqdm(total=len(all_filepaths)) as pbar:
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(single_uvotsource, tile, path, source_name, source_reg, bkg_reg, verbose) for path in all_filepaths]
            for future in as_completed(futures):
                pbar.update(1)
                yield future.result()