# -*- coding: utf-8 -*-

import re
import datetime
import dateutil.tz
try:
    import dicom
except ImportError:
    HAS_DICOM = False
else:
    HAS_DICOM = True

import logging
logger = logging.getLogger(__name__)

__all__ = ['read_metadata']


def _decode(attribute):
    """Decode DICOM data elements from ISO_IR 100.

    DICOM strings are routinely encoded with ISO_IR 100 which is
    equivalent to IS0 8859-1.

    We currently expect DICOM strings to be encoded using ISO_IR 100.
    In this context DICOM strings returned by pydicom are 8-bit strings
    encoded with ISO_IR 100.

    Parameters
    ----------
    attribute  : str
        The 8-bit string to decode from ISO_IR 100.

    Returns
    -------
    unicode
        The decoded string.

    """
    return attribute.decode('latin_1')


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
        - ImageType
        - SOPInstanceUID

    We also attempt to read the following DICOM tags if they are present:
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
    if HAS_DICOM:
        dataset = dicom.read_file(path, force=force)
    else:
        return {}

    # missing compulsary tags will raise exceptions
    if 'SeriesDescription' in dataset:
        description = dataset.SeriesDescription
    elif 'ProtocolName' in dataset:
        description = dataset.ProtocolName
    else:
        description = dataset.SeriesDescription  # raise an exception!

    metadata = {
        'SOPInstanceUID': dataset.SOPInstanceUID,
        'SeriesInstanceUID': dataset.SeriesInstanceUID,
        'SeriesNumber': dataset.SeriesNumber,
        'SeriesDescription': _decode(description),
        'ImageType': [_decode(x) for x in dataset.ImageType],
    }

    # optional tags
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
        metadata['StationName'] = _decode(dataset.StationName)
    if 'Manufacturer' in dataset:
        metadata['Manufacturer'] = _decode(dataset.Manufacturer)
    if 'ManufacturerModelName' in dataset:
        metadata['ManufacturerModelName'] = _decode(dataset.ManufacturerModelName)
    if 'DeviceSerialNumber' in dataset:
        metadata['DeviceSerialNumber'] = _decode(dataset.DeviceSerialNumber)
    if 'SoftwareVersions' in dataset:
        if dicom.dataelem.isMultiValue(dataset.SoftwareVersions):
            # usually the last part is the more informative
            # for example on Philips scanners:
            # ['3.2.1', '3.2.1.1'] → '3.2.1.1'
            metadata['SoftwareVersions'] = _decode(dataset.SoftwareVersions[-1])
        else:
            metadata['SoftwareVersions'] = _decode(dataset.SoftwareVersions)
    if 'StudyComments' in dataset:  # DUBLIN
        metadata['StudyComments'] = _decode(dataset.StudyComments)
    if 'ImageComments' in dataset:  # HAMBURG, DRESDEN
        metadata['ImageComments'] = _decode(dataset.ImageComments)
    if 'PatientID' in dataset:  # LONDON, NOTTINGHAM, BERLIN, MANNHEIM, PARIS
        metadata['PatientID'] = _decode(dataset.PatientID)
    if 'PatientName' in dataset:  # LONDON, NOTTINGHAM, BERLIN, MANNHEIM, PARIS
        metadata['PatientName'] = _decode(dataset.PatientName)

    return metadata
