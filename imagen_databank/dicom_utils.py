# -*- coding: utf-8 -*-

# Copyright (c) 2014-2017 CEA
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

import re
import datetime
import dateutil.tz
import dicom
from dicom.filereader import InvalidDicomError

import logging
logger = logging.getLogger(__name__)

__all__ = ['read_metadata']


#
# parse DICOM DateTime and Time tags
#
_REGEX_DT = re.compile(r"((\d{4,14})(\.(\d{1,6}))?)([+-]\d{4})?")
_REGEX_TM = re.compile(r"(\d{2,6})(\.(\d{1,6}))?")


def _datetime_from_dt(dt):
    """Convert DICOM DateTime to Python datetime.

    Parameters
    ----------
    dt : str
        DateTime tag from DICOM image.

    Returns
    -------
    datetime

    """
    match = _REGEX_DT.match(dt)
    if match and len(dt) <= 26:
        dt_match = match.group(2)
        year = int(dt_match[0:4])
        if len(dt_match) < 6:
            month = 1
        else:
            month = int(dt_match[4:6])
        if len(dt_match) < 8:
            day = 1
        else:
            day = int(dt_match[6:8])
        if len(dt_match) < 10:
            hour = 0
        else:
            hour = int(dt_match[8:10])
        if len(dt_match) < 12:
            minute = 0
        else:
            minute = int(dt_match[10:12])
        if len(dt_match) < 14:
            second = 0
            microsecond = 0
        else:
            second = int(dt_match[12:14])
            ms_match = match.group(4)
            if ms_match:
                microsecond = int(ms_match.rstrip().ljust(6, '0'))
            else:
                microsecond = 0
        tz_match = match.group(5)
        if tz_match:
            offset = (int(tz_match[1:3]) * 60 + int(tz_match[3:5])) * 60
            if tz_match[0] == '-':
                offset = -offset
            tzinfo = dateutil.tz.tzoffset(tz_match, offset)
        else:
            tzinfo = None
        return datetime.datetime(year, month, day,
                                 hour, minute, second, microsecond,
                                 tzinfo)
    else:
        logger.error('incorrect DICOM DT: %s', dt)
        return None


def _date_from_da(da):
    """Convert DICOM Date to Python date.

    Parameters
    ----------
    da : str
        Date tag from DICOM image.

    Returns
    -------
    date

    """
    if len(da) == 8:
        year = int(da[0:4])
        month = int(da[4:6])
        day = int(da[6:8])
        return datetime.date(year, month, day)
    elif len(da) == 10 and da[4] == '.' and da[7] == '.':
        # ACR-NEMA Standard 300, predecessor to DICOM - for compatibility
        year = int(da[0:4])
        month = int(da[5:7])
        day = int(da[8:10])
        return datetime.date(year, month, day)
    else:
        logger.error('incorrect DICOM DA: %s', da)
        return None


def _time_from_tm(tm):
    """Convert DICOM Time to Python time.

    Parameters
    ----------
    tm : str
        Time tag from DICOM image.

    Returns
    -------
    time

    """
    match = _REGEX_TM.match(tm)
    if match and len(tm) <= 16:
        tm_match = match.group(1)
        hour = int(tm_match[0:2])
        if len(tm_match) < 4:
            minute = 0
        else:
            minute = int(tm_match[2:4])
        if len(tm_match) < 6:
            second = 0
            microsecond = 0
        else:
            second = int(tm_match[4:6])
            ms_match = match.group(3)
            if ms_match:
                microsecond = int(ms_match.rstrip().ljust(6, '0'))
            else:
                microsecond = 0
        return datetime.time(hour, minute, second, microsecond)
    else:
        logger.error('incorrect DICOM TM: %s', tm)
        return None


def read_metadata(path, force=False):
    """Read select metadata from a DICOM file.

    We always attempt to read the following DICOM tags. An exception is raised
    if one of the tags cannot be read:
        - SOPClassUID
        - SeriesInstanceUID
        - SeriesNumber
        - SeriesDescription
        - SOPInstanceUID

    We also attempt to read the following DICOM tags if they are present:
        - ImageType
        - AcquisitionDateTime
        - AcquisitionDate
        - AcquisitionTime
        - StationName
        - Manufacturer
        - ManufacturerModelName
        - DeviceSerialNumber
        - SoftwareVersions
        - PatientID

    Parameters
    ----------
    path : str
        Path name of the DICOM file.
    force : bool
        If True read nonstandard files, typically without "Part 10" headers.

    Returns
    -------
    dict

    """
    dataset = dicom.read_file(path, force=force)

    # missing compulsory tags will raise exceptions
    if 'SeriesDescription' in dataset:
        description = dataset.SeriesDescription
    elif 'ProtocolName' in dataset:
        description = dataset.ProtocolName
    else:
        description = dataset.SeriesDescription  # raise an exception!

    metadata = {
        'SOPClassUID': dataset.SOPClassUID,
        'SOPInstanceUID': dataset.SOPInstanceUID,
        'SeriesInstanceUID': dataset.SeriesInstanceUID,
        'SeriesNumber': dataset.SeriesNumber,
        'SeriesDescription': description,
    }

    # optional tags
    if 'ImageType' in dataset:
        metadata['ImageType'] = dataset.ImageType
    if 'AcquisitionDateTime' in dataset:
        dt = _datetime_from_dt(dataset.AcquisitionDateTime)
        metadata['AcquisitionDate'] = dt.date()
        metadata['AcquisitionTime'] = dt.time()
    else:
        if 'AcquisitionDate' in dataset:
            metadata['AcquisitionDate'] = _date_from_da(dataset.AcquisitionDate)
        if 'AcquisitionTime' in dataset:
            metadata['AcquisitionTime'] = _time_from_tm(dataset.AcquisitionTime)
    if 'StationName' in dataset:
        metadata['StationName'] = dataset.StationName
    if 'Manufacturer' in dataset:
        metadata['Manufacturer'] = dataset.Manufacturer
    if 'ManufacturerModelName' in dataset:
        metadata['ManufacturerModelName'] = dataset.ManufacturerModelName
    if 'DeviceSerialNumber' in dataset:
        metadata['DeviceSerialNumber'] = dataset.DeviceSerialNumber
    if 'SoftwareVersions' in dataset:
        if dicom.dataelem.isMultiValue(dataset.SoftwareVersions):
            # usually the last part is the more informative
            # for example on Philips scanners:
            # ['3.2.1', '3.2.1.1'] → '3.2.1.1'
            metadata['SoftwareVersions'] = dataset.SoftwareVersions[-1]
        else:
            metadata['SoftwareVersions'] = dataset.SoftwareVersions
    if 'StudyComments' in dataset:  # DUBLIN
        metadata['StudyComments'] = dataset.StudyComments
    if 'PatientName' in dataset:  # BERLIN, NOTTINGHAM
        metadata['PatientName'] = dataset.PatientName
    if 'ImageComments' in dataset:  # HAMBURG, DRESDEN
        metadata['ImageComments'] = dataset.ImageComments
    if 'StudyDescription' in dataset:  # LONDON
        metadata['StudyDescription'] = dataset.StudyDescription
    if 'PerformedProcedureStepDescription' in dataset:  # LONDON
        metadata['PerformedProcedureStepDescription'] = dataset.PerformedProcedureStepDescription
    if 'PatientID' in dataset:  # BERLIN, MANNHEIM, PARIS
        metadata['PatientID'] = dataset.PatientID

    return metadata
