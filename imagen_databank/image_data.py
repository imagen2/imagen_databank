# Copyright (c) 2014-2018 CEA
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
import re
import time
import datetime

from .core import (LONDON, NOTTINGHAM, DUBLIN, BERLIN,
                   HAMBURG, MANNHEIM, PARIS, DRESDEN,
                   SOUTHAMPTON, AACHEN)
from .dicom_utils import read_metadata
from .dicom_utils import InvalidDicomError

import logging
logger = logging.getLogger(__name__)

__all__ = ['SEQUENCE_LOCALIZER_CALIBRATION', 'SEQUENCE_T2',
           'SEQUENCE_T2_FLAIR', 'SEQUENCE_ADNI_MPRAGE',
           'SEQUENCE_MID', 'SEQUENCE_FT', 'SEQUENCE_SST',
           'SEQUENCE_B0_MAP', 'SEQUENCE_DTI',
           'SEQUENCE_RESTING_STATE',
           'SEQUENCE_NODDI',
           'SEQUENCE_NAME',
           'NONSTANDARD_DICOM',
           'series_type_from_description',
           'walk_image_data', 'report_image_data']


#
# information sent by Anna Cattrell to Dimitri on 13 June 2014:
# Standard Operating Procedure IMAGEN Follow-up 2 study
#
# 2.2.1 Overview of Imaging Session:
#
#   2. 3 plane localizer / Parallel imaging calibration
#   3. Axial T2 slices (site specific duration)
#   4. Axial T2 Flair slices (site specific duration)
#   5. 3D Sagittal ADNI MPRAGE (Long)
#   7. Monetary Incentive Delay Task (MID)
#   9. Face task (FT)
#  11. Stop-signal task (SST)
#  12. B0 Map
#  13. DTI (duration is heart-rate dependent at sites with cardiac gating)
#  14. Resting State
#  15. Short MPRAGE (baseline only)
#  16. EPI Global (JBP suggestion followed by a few centres at baseline)
#  17. NODDI (optional, added in Follow-up 3)
#
# the following constants attempt to describe each of these sequences
#
SEQUENCE_LOCALIZER_CALIBRATION = 2
SEQUENCE_T2 = 3
SEQUENCE_T2_FLAIR = 4
SEQUENCE_ADNI_MPRAGE = 5
SEQUENCE_MID = 7
SEQUENCE_FT = 9
SEQUENCE_SST = 11
SEQUENCE_B0_MAP = 12
SEQUENCE_DTI = 13
SEQUENCE_RESTING_STATE = 14
SEQUENCE_SHORT_MPRAGE = 15
SEQUENCE_GLOBAL = 16
SEQUENCE_NODDI = 17

#
# from sequence ID to sequence name
#
SEQUENCE_NAME = {
    SEQUENCE_LOCALIZER_CALIBRATION: 'Localizer/Calibration',
    SEQUENCE_T2: 'T2',
    SEQUENCE_T2_FLAIR: 'T2 Flair',
    SEQUENCE_ADNI_MPRAGE: 'ADNI MPRAGE',
    SEQUENCE_MID: 'EPI MID',
    SEQUENCE_FT: 'EPI Faces',
    SEQUENCE_SST: 'EPI Signal Stop',
    SEQUENCE_B0_MAP: 'B0 Map',
    SEQUENCE_DTI: 'DTI',
    SEQUENCE_RESTING_STATE: 'Resting State',
    SEQUENCE_SHORT_MPRAGE: 'Short MPRAGE',
    SEQUENCE_GLOBAL: 'EPI Global',
    SEQUENCE_NODDI: 'NODDI',
}

#
# check sequence names against these regex'es when trying to identify
# the type of a sequence from its name
#
# in some case order is important, for example:
# - first match 'FLAIR' and 'short MPRAGE'
# - then match 'T2' and 'MPRAGE'
#
_LOOSE_IMAGE_DATA_REGEXES = (
    (re.compile(r'LOCALI[ZS]ER', re.IGNORECASE), SEQUENCE_LOCALIZER_CALIBRATION),
    # LONDON calibration
    (re.compile(r'ASSET[- ]Cal', re.IGNORECASE), SEQUENCE_LOCALIZER_CALIBRATION),
    # NOTTINGHAM 3-plane scout
    (re.compile(r'Survey_SHC'), SEQUENCE_LOCALIZER_CALIBRATION),
    # LONDON FU3 3-plane Localizer
    (re.compile(r'3Plane'), SEQUENCE_LOCALIZER_CALIBRATION),
    # first search for "FLAIR" then for "T2"
    (re.compile(r'FLAIR', re.IGNORECASE), SEQUENCE_T2_FLAIR),
    (re.compile(r'T2', re.IGNORECASE), SEQUENCE_T2),
    (re.compile(r'short MPRAGE', re.IGNORECASE), SEQUENCE_SHORT_MPRAGE),
    (re.compile(r'MPRAGE', re.IGNORECASE), SEQUENCE_ADNI_MPRAGE),
    (re.compile(r'MID', re.IGNORECASE), SEQUENCE_MID),
    # "EPI short reward" and "EPI reward short" are the same as "EPI short MID"
    (re.compile(r'reward', re.IGNORECASE), SEQUENCE_MID),
    (re.compile(r'face', re.IGNORECASE), SEQUENCE_FT),
    (re.compile(r'stop[- ]signal', re.IGNORECASE), SEQUENCE_SST),
    # LONDON stop signal DICOM files contain "SST"
    (re.compile(r'SST', re.IGNORECASE), SEQUENCE_SST),
    (re.compile(r'global', re.IGNORECASE), SEQUENCE_GLOBAL),
    (re.compile(r'B0'), SEQUENCE_B0_MAP),
    # LONDON B0 maps made of 3 DICOM files containing "FIELDMAP"
    (re.compile(r'FIELDMAP', re.IGNORECASE), SEQUENCE_B0_MAP),
    (re.compile(r'DTI'), SEQUENCE_DTI),
    (re.compile(r'REST', re.IGNORECASE), SEQUENCE_RESTING_STATE),
)

#
# some acquisition centers may send nonstandard DICOM files
#
# for example Hamburg have sent DICOM files without "PART 10" headers
#
NONSTANDARD_DICOM = {
    LONDON: False,
    NOTTINGHAM: False,
    DUBLIN: False,
    BERLIN: False,
    HAMBURG: True,
    MANNHEIM: False,
    PARIS: False,
    DRESDEN: False,
    SOUTHAMPTON: False,
    AACHEN: False,
}

#
# the SOP Class UIDs we expect to find while scanning DICOM files:
# - those we process
# - those we discard silently
#
# any other SOP Class UID generates a warning
#
_ALLOWED_SOP_CLASS_UIDS = {
    'MR Image Storage',
    'Enhanced MR Image Storage',
}
_IGNORED_SOP_CLASS_UIDS = {
    'Grayscale Softcopy Presentation State Storage SOP Class',
    'Raw Data Storage',
    'Enhanced SR Storage',
    'Philips Private Gyroscan MR Serie Data',
    'Private MR Series Data Storage', '1.3.46.670589.11.0.0.12.2',
    'Private MR Examcard Storage', '1.3.46.670589.11.0.0.12.4',
    'Secondary Capture Image Storage',
}


def series_type_from_description(series_description):
    """Match series description to those listed in Imagen FU2 SOPs.

    This matching function is empirical and based on experimentation.

    Parameters
    ----------
    series_description : unicode
        The series description to match.

    Returns
    -------
    str
        If the series description loosely matches a series type listed
        in the SOPs, return this series type, else return None.

    """
    for regex, series_type in _LOOSE_IMAGE_DATA_REGEXES:
        if regex.search(series_description):
            return series_type
    return None


def walk_image_data(path, force=False):
    """Generate information on DICOM files in a directory.

    File that cannot be read are skipped and an error message is logged.

    Parameters
    ----------
    path : unicode
        Directory to read DICOM files from.
    force : bool
        Try reading nonstandard DICOM files, typically without "PART 10" headers.

    Yields
    ------
    tuple
        Yields a pair (metadata, relpath) where metadata is a dictionary
        of extracted DICOM metadata.

    """
    n = 0
    start = time.time()

    logger.info('start processing files under: %s', path)

    for root, dummy_dirs, files in os.walk(path):
        n += len(files)
        for filename in files:
            abspath = os.path.join(root, filename)
            relpath = os.path.normpath(os.path.relpath(abspath, path))
            # skip DICOMDIR since we are going to read all DICOM files anyway
            # beware, Nottigham had sent a DICOMDIR2 file!
            if filename.startswith('DICOMDIR'):
                continue
            logger.debug('read file: %s', relpath)
            try:
                metadata = read_metadata(abspath, force=force)
            except IOError as e:
                logger.error('cannot read file (%s): %s', str(e), relpath)
            except InvalidDicomError as e:
                logger.error('cannot read nonstandard DICOM file: %s: %s', str(e), relpath)
            except AttributeError as e:
                logger.error('missing attribute: %s: %s', str(e), relpath)
            else:
                yield (metadata, relpath)

    elapsed = time.time() - start
    logger.info('processed %d files in %.2f s: %s', n, elapsed, path)


def report_image_data(path, force=False):
    """Find DICOM files loosely organized according to the Imagen FU2 SOPs.

    The Imagen FU2 SOPs define a precise file organization for Image Data. In
    practice we have found the SOPs are only loosely followed. A method to find
    DICOM files while adapting to local variations is to read all DICOM files,
    then filter and break them down into series based on their contents.

    This function scans the directory where we expect to find the Image Data
    of a dataset and reports series of valid DICOM files.

    Parameters
    ----------
    path : unicode
        Directory to read DICOM files from.
    force : bool
        Try reading nonstandard DICOM files, typically without "PART 10" headers.

    Returns
    -------
    dict
        The key identifies a series while the value is a pair
        (series_data, images).

    """
    series_dict = {}

    for (image_data, relpath) in walk_image_data(path, force=force):
        if str(image_data['SOPClassUID']) in _IGNORED_SOP_CLASS_UIDS:
            continue
        # extract DICOM tags of interest, throw exceptions if missing tags!
        series_uid = image_data['SeriesInstanceUID']
        image_uid = image_data['SOPInstanceUID']
        series_number = image_data['SeriesNumber']
        series_description = image_data['SeriesDescription']
        image_types = image_data.get('ImageType', [])
        station_name = image_data.get('StationName', None)
        manufacturer = image_data.get('Manufacturer', None)
        manufacturer_model_name = image_data.get('ManufacturerModelName', None)
        software_versions = image_data.get('SoftwareVersions', None)
        device_serial_number = image_data.get('DeviceSerialNumber', None)
        if 'AcquisitionDate' in image_data:
            acquisition_date = image_data['AcquisitionDate']
            if 'AcquisitionTime' in image_data:
                acquisition_time = image_data['AcquisitionTime']
                timestamp = datetime.datetime.combine(acquisition_date,
                                                      acquisition_time)
            else:
                timestamp = datetime.datetime(acquisition_date.year,
                                              acquisition_date.month,
                                              acquisition_date.day)
        else:
            logger.error('missing acquisition time: %s', relpath)
        # FIXME: this is obviously wrong! # find PSC1 code
        if 'CommentsOnThePerformedProcedureStep' in image_data:  # DUBLIN
            psc1 = image_data['CommentsOnThePerformedProcedureStep']
        elif 'ImageComments' in image_data:  # HAMBURG, DRESDEN
            psc1 = image_data['ImageComments']
        elif 'PatientID' in image_data:  # LONDON, NOTTINGHAM, BERLIN, MANNHEIM, PARIS
            psc1 = image_data['PatientID']
        elif 'PatientName' in image_data:  # LONDON, NOTTINGHAM, BERLIN, MANNHEIM, PARIS
            psc1 = image_data['PatientName']
        else:
            psc1 = None
        # build the dictionary of series using 'SeriesInstanceUID' as a key
        if series_uid not in series_dict:
            series_data = {
                'SeriesNumber': series_number,
                'SeriesDescription': series_description,
                'ImageType': set(image_types),
                'MinAcquisitionDateTime': timestamp,
                'MaxAcquisitionDateTime': timestamp,
            }
            if station_name:
                series_data['StationName'] = station_name
            if manufacturer:
                series_data['Manufacturer'] = manufacturer
            if manufacturer_model_name:
                series_data['ManufacturerModelName'] = manufacturer_model_name
            if software_versions:
                series_data['SoftwareVersions'] = software_versions
            if device_serial_number:
                series_data['DeviceSerialNumber'] = device_serial_number
            if psc1:
                series_data['PSC1'] = psc1
            # populate series with relative path to DICOM files
            series_dict[series_uid] = (series_data, {image_uid: relpath})
        else:
            series_dict[series_uid][0]['ImageType'] |= set(image_types)
            # check consistency within series:
            if series_number != series_dict[series_uid][0]['SeriesNumber']:
                logger.error('inconsistent series number '
                             '"%s" / "%s":\n  %s\n  %s',
                             series_dict[series_uid][0]['SeriesNumber'],
                             series_number,
                             next(iter(series_dict[series_uid][1].values())),
                             relpath)
            elif series_description != series_dict[series_uid][0]['SeriesDescription']:
                logger.error('inconsistent series description '
                             '"%s" / "%s":\n  %s\n  %s',
                             series_dict[series_uid][0]['SeriesDescription'],
                             series_description,
                             next(iter(series_dict[series_uid][1].values())),
                             relpath)
            if station_name:
                if 'StationName' in series_dict[series_uid][0]:
                    if station_name != series_dict[series_uid][0]['StationName']:
                        logger.error('inconsistent station name '
                                     '"%s" / "%s":\n  %s\n  %s',
                                     series_dict[series_uid][0]['StationName'],
                                     station_name,
                                     next(iter(series_dict[series_uid][1].values())),
                                     relpath)
                else:
                    series_dict[series_uid][0]['StationName'] = station_name
            if manufacturer:
                if 'Manufacturer' in series_dict[series_uid][0]:
                    if manufacturer != series_dict[series_uid][0]['Manufacturer']:
                        logger.error('inconsistent manufacturer '
                                     '"%s" / "%s":\n  %s\n  %s',
                                     series_dict[series_uid][0]['Manufacturer'],
                                     manufacturer,
                                     next(iter(series_dict[series_uid][1].values())),
                                     relpath)
                else:
                    series_dict[series_uid][0]['Manufacturer'] = manufacturer
            if manufacturer_model_name:
                if 'ManufacturerModelName' in series_dict[series_uid][0]:
                    if manufacturer_model_name != series_dict[series_uid][0]['ManufacturerModelName']:
                        logger.error('inconsistent manufacturer model name '
                                     '"%s" / "%s":\n  %s\n  %s',
                                     series_dict[series_uid][0]['ManufacturerModelName'],
                                     manufacturer_model_name,
                                     next(iter(series_dict[series_uid][1].values())),
                                     relpath)
                else:
                    series_dict[series_uid][0]['ManufacturerModelName'] = manufacturer_model_name
            if software_versions:
                if 'SoftwareVersions' in series_dict[series_uid][0]:
                    if software_versions != series_dict[series_uid][0]['SoftwareVersions']:
                        logger.error('inconsistent software versions '
                                     '"%s" / "%s":\n  %s\n  %s',
                                     series_dict[series_uid][0]['SoftwareVersions'],
                                     software_versions,
                                     next(iter(series_dict[series_uid][1].values())),
                                     relpath)
                else:
                    series_dict[series_uid][0]['SoftwareVersions'] = software_versions
            if device_serial_number:
                if 'DeviceSerialNumber' in series_dict[series_uid][0]:
                    if device_serial_number != series_dict[series_uid][0]['DeviceSerialNumber']:
                        logger.error('inconsistent device serial number '
                                     '"%s" / "%s":\n  %s\n  %s',
                                     series_dict[series_uid][0]['DeviceSerialNumber'],
                                     device_serial_number,
                                     next(iter(series_dict[series_uid][1].values())),
                                     relpath)
                else:
                    series_dict[series_uid][0]['DeviceSerialNumber'] = device_serial_number

            if psc1:
                if 'PSC1' in series_dict[series_uid][0]:
                    if psc1 != series_dict[series_uid][0]['PSC1']:
                        logger.error('inconsistent PSC1 code '
                                     '"%s" / "%s":\n  %s\n  %s',
                                     series_dict[series_uid][0]['PSC1'],
                                     psc1,
                                     next(iter(series_dict[series_uid][1].values())),
                                     relpath)
                else:
                    series_dict[series_uid][0]['PSC1'] = psc1
            # populate series with relative path to DICOM files
            if image_uid not in series_dict[series_uid][1]:
                series_dict[series_uid][1][image_uid] = relpath
            else:
                logger.error('duplicate image in series (%s):'
                             '\n  %s\n  %s',
                             series_description,
                             series_dict[series_uid][1][image_uid],
                             relpath)
            # update acquisition date/time range by series
            if timestamp < series_dict[series_uid][0]['MinAcquisitionDateTime']:
                series_dict[series_uid][0]['MinAcquisitionDateTime'] = timestamp
            if timestamp > series_dict[series_uid][0]['MaxAcquisitionDateTime']:
                series_dict[series_uid][0]['MaxAcquisitionDateTime'] = timestamp

    return series_dict
