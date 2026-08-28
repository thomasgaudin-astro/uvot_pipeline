# -*- coding: utf-8 -*-
import os
import subprocess

class FTOOLSCommands():
    def __init__(self, sc_tile, obsid, source_name, detect=False, fkeyprint=False, unicorr=False, source=False, ref_frame=None, obs_frame=None):

        filepath = f'./S-CUBED/{sc_tile}/UVOT'
        
        subpath = os.path.join(filepath, obsid)
                
        sourcepath_fill = f'uvot/image/sw{obsid}uw1_sk.img.gz'
        outpath_fill = 'uvot/image/detect.fits'
        exppath_fill = f'uvot/image/sw{obsid}uw1_ex.img.gz'
        detectpath_fill = 'uvot/image/detect.reg'
        extracted_sourcepath_fill = f'uvot/image/sw{obsid}uw1_sk.img'
        source_outfile_fill = f'uvot/image/{obsid}_source.fits'

        self.source_path = os.path.join(subpath, sourcepath_fill)
        self.output_path = os.path.join(subpath, outpath_fill)
        self.exposure_path = os.path.join(subpath, exppath_fill)
        self.reg_path = os.path.join(subpath, detectpath_fill)
        self.extracted_source_path = os.path.join(subpath, extracted_sourcepath_fill)
        self.source_outfile_path = os.path.join(subpath, source_outfile_fill)

        self.ref_frame = ref_frame
        self.obs_frame = obs_frame
        self.obspath = os.path.join(filepath, obsid, 'uvot/image')

        self.source_reg = f'{source_name}_source.reg'
        self.bkg_reg = f'{source_name}_bkg.reg'

        if detect == True:
            self.uvotdetect_command = self.create_uvotdetect_bash_command(self.source_path, 
                                                                          self.output_path, 
                                                                          self.exposure_path, 
                                                                          self.reg_path
                                                                          )
        if fkeyprint == True:
            self.fkeyprint_command = self.create_fkeyprint_bash_command(self.source_path)

        if self.ref_frame and self.obsframe and unicorr == True:
            self.uvotunicorr_command = self.create_uvotunicorr_bash_command(self.ref_frame, 
                                                                            self.obs_frame, 
                                                                            self.obspath
                                                                            )

        if source == True:
            self.create_uvotsource_bash_command(self.extracted_source_path, 
                                                self.exposure_path, 
                                                self.source_reg, 
                                                self.bkg_reg, 
                                                self.source_outfile_path
                                                )
        

    def create_uvotdetect_bash_command(self, source_path, output_path, exposure_path, reg_path):

        # Construct bash command
        bash_command = f"""
        bash -c '
        source {os.environ['HEADAS']}/headas-init.sh
        uvotdetect \\
            infile={source_path} \\
            outfile={output_path} \\
            expfile={exposure_path} \\
            threshold=3 \\
            sexfile=DEFAULT \\
            plotsrc=NO \\
            regfile={reg_path} \\
            zerobkg=0.03 \\
            expopt=BETA \\
            calibrate=YES \\
            clobber=YES
        '
        """

        return bash_command

    def run_uvotdetect(self, uvotdetect_command):

        # Run the command
        result = subprocess.run(
            ['bash', '-i', '-c', uvotdetect_command],
            capture_output=True,
            text=True
        )

        # print("STDOUT:\n", result.stdout)
        # print("STDERR:\n", result.stderr)

        return result.stdout

    def run_uvotdetect_verbose(self, uvotdetect_command):

        # Run the command
        result = subprocess.run(
            ['bash', '-i', '-c', uvotdetect_command],
            capture_output=True,
            text=True
        )

        print("STDOUT:\n", result.stdout)
        print("STDERR:\n", result.stderr)

        return result.stdout
    
    def create_fkeyprint_bash_command(self, source_path):

        fits_path = os.path.abspath(source_path)
        
        # print("Absolute path:", fits_path)
        # print("Exists:", os.path.exists(fits_path))  # Confirm it actually exists!
        
        keyword = "ASPCORR"
        
        command = f"""
        source {os.environ['HEADAS']}/headas-init.sh
        fkeyprint "{fits_path}" {keyword}
        """
        
        return command

    def run_fkeyprint(self, fkeyprint_command):

        result = subprocess.run(
            ['bash', '-i', '-c', fkeyprint_command],
            capture_output=True,
            text=True
        )
        
        # print("STDOUT:\n", result.stdout)
        # print("STDERR:\n", result.stderr)

        return result.stdout

    def run_fkeyprint_verbose(self, fkeyprint_command):

        result = subprocess.run(
            ['bash', '-i', '-c', fkeyprint_command],
            capture_output=True,
            text=True
        )
        
        print("STDOUT:\n", result.stdout)
        print("STDERR:\n", result.stderr)

        return result.stdout

    def create_uvotunicorr_bash_command(self, ref_frame, obs_frame, obspath=None):

        if obspath:
            ref_filepath = obspath+f'/sw{ref_frame}uw1_sk.img[1]'
            obs_filepath = obspath+f'/sw{obs_frame}uw1_sk.img[1]'
            ref_reg_filepath = obspath+'/ref.reg'
            obs_reg_filepath = obspath+'/obs.reg'
        else:
            ref_filepath = f'sw{ref_frame}uw1_sk.img[1]'
            obs_filepath = f'sw{obs_frame}uw1_sk.img[1]'
            ref_reg_filepath = 'ref.reg'
            obs_reg_filepath = 'obs.reg'
        
        bash_command = f"""
            bash -c '
            source {os.environ['HEADAS']}/headas-init.sh
            uvotunicorr obsfile={obs_filepath} reffile={ref_filepath} obsreg={obs_reg_filepath} refreg={ref_reg_filepath}
            '
            """

        return bash_command

    def run_uvotunicorr(self, uvotunicorr_command):

        # Run the command
        result = subprocess.run(
            ['bash', '-i', '-c', uvotunicorr_command],
            capture_output=True,
            text=True
        )

        # print("STDOUT:\n", result.stdout)
        # print("STDERR:\n", result.stderr)

        return result.stdout

    def run_uvotunicorr_verbose(self, uvotunicorr_command):

        # Run the command
        result = subprocess.run(
            ['bash', '-i', '-c', uvotunicorr_command],
            capture_output=True,
            text=True
        )

        print("STDOUT:\n", result.stdout)
        print("STDERR:\n", result.stderr)

        return result.stdout

    def create_uvotsource_bash_command(self, source_path, exposure_path, source_reg_file, bkg_reg_file, source_outfile):
        
        bash_command = f"""
            bash -c '
            source {os.environ['HEADAS']}/headas-init.sh
            uvotsource image="{source_path}" srcreg="{source_reg_file}" bkgreg="{bkg_reg_file}" sigma=5 zerofile=CALDB coinfile=CALDB psffile=CALDB lssfile=CALDB expfile="{exposure_path}" syserr=NO frametime=DEFAULT apercorr=NONE output=ALL outfile="{source_outfile}" cleanup=YES clobber=YES chatter=1

            '
            """

        return bash_command

    def run_uvotsource(self, uvotsource_command):

        # Run the command
        result = subprocess.run(
            ['bash', '-i', '-c', uvotsource_command],
            capture_output=True,
            text=True
        )

        # print("STDOUT:\n", result.stdout)
        # print("STDERR:\n", result.stderr)

        return result.stdout

    def run_uvotsource_verbose(self, uvotsource_command):

        # Run the command
        result = subprocess.run(
            ['bash', '-i', '-c', uvotsource_command],
            capture_output=True,
            text=True
        )

        print("STDOUT:\n", result.stdout)
        print("STDERR:\n", result.stderr)

        return result.stdout
