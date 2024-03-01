#!/usr/bin/env python3

# Copyright (c) 2010-2019 CEA
#
# This software is governed by the CeCILL license under French law and
# abiding by the rules of distribution of free software. You can use,
# modify and/ or redistribute the software under the terms of the CeCILL
# license as circulated by CEA, CNRS and INRIA at the following URL
# "http://www.cecill.info".
#
# As a counterpart to the access to the source code and rights to copy,
# modify and redistribute granted by the license, users are provided only
# with a limited warranty and the software's author, the holder of the
# economic rights, and the successive licensors have only limited
# liability.
#
# In this respect, the user's attention is drawn to the risks associated
# with loading, using, modifying and/or developing or reproducing the
# software by the user in light of its specific status of free software,
# that may mean that it is complicated to manipulate, and that also
# therefore means that it is reserved for developers and experienced
# professionals having in-depth computer knowledge. Users are therefore
# encouraged to load and test the software's suitability as regards their
# requirements in conditions enabling the security of their systems and/or
# data to be ensured and, more generally, to use and operate it in the
# same conditions as regards security.
#
# The fact that you are presently reading this means that you have had
# knowledge of the CeCILL license and that you accept its terms.

import os
import zipfile
import zlib
import tempfile
from datetime import datetime
import shutil
import subprocess
from imagen_databank import PSC2_FROM_PSC1
import json
import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


QUARANTINE_PATH = '/imagen/FU3/RAW/QUARANTINE'
BIDS_PATH = '/neurospin/tmp/imagen/dcm2niix'
SKIP_PATH = '/imagen/mri_skip.json'


def quarantine_filename_semantics(filename):
    root, ext = os.path.splitext(filename)

    if (ext != '.zip'):
        logger.debug('%s: filename without ".zip" extension', filename)

    increment, suffix = root.split('_data_')
    increment = int(increment)

    psc1 = suffix[:-6]  # last 6 characters added by the upload portal
    if len(psc1) > 12:
        timepoint = psc1[12:]
        psc1 = psc1[:12]
    else:
        logger.error('%s: missing timepoint', psc1)

    return increment, psc1, timepoint


def timestamps(top, include_dirs=True):
    min_timestamp = datetime.max
    max_timestamp = datetime.min

    for root, dirs, files in os.walk(top):
        if include_dirs:
            for dirname in dirs:
                path = os.path.join(root, dirname)
                timestamp = datetime.fromtimestamp(os.path.getmtime(path))
                min_timestamp = min(timestamp, min_timestamp)
                max_timestamp = max(timestamp, max_timestamp)
        for filename in files:
            path = os.path.join(root, filename)
            timestamp = datetime.fromtimestamp(os.path.getmtime(path))
            min_timestamp = min(timestamp, min_timestamp)
            max_timestamp = max(timestamp, max_timestamp)

    return (min_timestamp, max_timestamp)


def list_datasets(path):
    datasets = {}

    for zip_file in os.listdir(path):
        zip_path = os.path.join(path, zip_file)
        root, ext = os.path.splitext(zip_file)

        if (ext != '.zip'):
            logger.debug('%s: this is not a ZIP file ', zip_file)
            continue
        elif not zipfile.is_zipfile(zip_path):
            logger.warn('%s: skip invalid ZIP file ', zip_file)
            continue

        # Unix timestamp of the ZIP file
        timestamp = os.path.getmtime(zip_path)

        # semantics of ZIP file name
        increment, psc1, timepoint = quarantine_filename_semantics(zip_file)

        # compare increment/timestamp of ZIP files, keep most recent
        timepoint = datasets.setdefault(timepoint, {})
        if psc1 in timepoint:
            old_zip_path, old_increment, old_timestamp = timepoint[psc1]
            if (increment <= old_increment or timestamp <= old_timestamp):
                if (increment >= old_increment or timestamp >= old_timestamp):
                    logger.error('%s: inconsistent timestamps', zip_file)
                continue
        timepoint[psc1] = (zip_path, increment, timestamp)

    return datasets


def dcm2nii(src, dst, comment):
    status = 0

    logger.info('%s: running dcm2niix: %s', src, dst)

    dcm2niix = ['dcm2niix',
                '-z', 'y', '-9'
                '-c', comment,
                '-o', dst,
                src]
    completed = subprocess.run(dcm2niix,
                               capture_output=True)
    if completed.returncode:
        logger.error('%s: dcm2niix failed: %s',
                     src, completed.stdout)
        status = completed.returncode

    return status


def deidentify(timepoint, psc1, zip_path, bids_path):
    logger.info('%s/%s: deidentify', psc1, timepoint)

    psc2 = PSC2_FROM_PSC1[psc1]
    out_sub_path = os.path.join(bids_path, 'sub-' + psc2)
    out_ses_path = os.path.join(out_sub_path, 'ses-' + timepoint)

    # skip ZIP files that have already been processed
    if os.path.isdir(out_ses_path):
        zip_timestamp = datetime.fromtimestamp(os.path.getmtime(zip_path))
        min_timestamp, max_timestamp = timestamps(out_ses_path)
        if min_timestamp > zip_timestamp:
            return
        else:
            shutil.rmtree(out_ses_path)
            os.makedirs(out_ses_path)

    status = 0
    prefix = 'cveda-mri-' + psc1
    with tempfile.TemporaryDirectory(prefix=prefix) as tempdir:
        # unpack ZIP file into temporary directory
        zip_file = zipfile.ZipFile(zip_path)
        try:
            zip_file.extractall(tempdir)
        except (zipfile.BadZipFile, OSError, EOFError, zlib.error) as e:
            logger.error('%s/%s: corrupt ZIP file: %s',
                         psc1, timepoint,  str(e))
            return

        os.makedirs(out_ses_path)
        status = dcm2nii(tempdir, out_ses_path,
                         psc2 + '/' + timepoint)

    if status:
        shutil.rmtree(out_ses_path)
        if not os.listdir(out_sub_path):  # empty directory
            os.rmdir(out_sub_path)

    return status


def main():
    datasets = list_datasets(QUARANTINE_PATH)

    for timepoint, timepoint_datasets in datasets.items():
        for psc1, (zip_path, increment, timestamp) in timepoint_datasets.items():
            with open(SKIP_PATH) as skip_file:
                skip = json.load(skip_file)
                if timepoint in skip and psc1 in skip[timepoint]:
                    continue
            deidentify(timepoint, psc1, zip_path, BIDS_PATH)


if __name__ == "__main__":
    main()
