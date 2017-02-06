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

import os
try:
    from zipfile import BadZipFile
except ImportError:
    from zipfile import BadZipfile as BadZipFile

from ..cantab import (read_cant, read_datasheet, read_detailed_datasheet,
                      read_report)
from ..core import PSC2_FROM_PSC1
from ..core import Error

import logging
logger = logging.getLogger(__name__)

__all__ = ['check_cant_name', 'check_datasheet_name',
           'check_detailed_datasheet_name', 'check_report_name',
           'check_cant_content', 'check_datasheet_content',
           'check_detailed_datasheet_content', 'check_report_content']


_COLUMN_NAMES = {
    'FU2': {
        'REQUIRED': (
            # demographics...
            "Subject ID",
            "Age",
            "NART",
            "Gender",
            "Session start time",
            # Affective Go/No-go (AGN)
            "AGN Mean correct latency (positive)",
            "AGN Mean correct latency (negative)",
            "AGN Mean correct latency (neutral)",
            # Cambridge Guessing (Gambling) Task (CGT)
            "CGT Delay aversion",
            "CGT Deliberation time",
            "CGT Overall proportion bet",
            "CGT Quality of decision making",
            "CGT Risk adjustment",
            "CGT Risk taking",
        ),
        'OPTIONAL': (
            # Affective Go/No-go (AGN)
            "AGN Total omissions (neutral)",
            "AGN Total omissions (negative)",
            "AGN Total omissions (positive)",
            "AGN Affective response bias (Mean)",
            # Pattern Recognition Memory (PRM)
            'PRM Percent correct',
            # Rapid Visual Information Processing (RVP)
            "RVP A'",
            # Spatial Working Memory (SWM)
            "SWM Between errors",
            "SWM Strategy",
            # Warnings: skip Warning1, Warning2, ... Hardcoded in the code!
        )
    },
    'FU3': {
        'REQUIRED': (
            # demographics...
            "Subject ID",
            "Age",
            "NART",
            "Gender",
            "Session start time",
            # Cambridge Guessing (Gambling) Task (CGT)
            "CGT Delay aversion",
            "CGT Deliberation time",
            "CGT Overall proportion bet",
            "CGT Quality of decision making",
            "CGT Risk adjustment",
            "CGT Risk taking",
            # Intra-Extra Dimensional Set Shift (IED)
            "IED Total trials",
            "IED Total trials (adjusted)",
            "IED Completed stage trials",
            "IED Pre-ED errors",
            "IED EDS errors",
            "IED Total errors",
            "IED Total errors (adjusted)",
            "IED Completed stage errors",
            "IED Errors (block 1)",
            "IED Errors (block 2)",
            "IED Errors (block 3)",
            "IED Errors (block 4)",
            "IED Errors (block 5)",
            "IED Errors (block 6)",
            "IED Errors (block 7)",
            "IED Errors (block 8)",
            "IED Errors (block 9)",
            "IED Stages completed",
            # Spatial Working Memory (SWM)
            "SWM Between errors",
            "SWM Strategy",
        ),
        'OPTIONAL': (
            # Warnings: skip Warning1, Warning2, ... Hardcoded in the code!
        )
    }
}


def _check_psc1(subject_id, suffix=None, psc1=None):
    """Check a subject identifier against an expected value and yield errors.

    Parameters
    ----------
    subject_id : str
        Expected subject identifier: PSC1 code possibly followed by suffix.
    suffix : str, optional
        Time point identifier, typically appended to the PSC1 code.
    psc1 : str, optional
        Expected 12-digit PSC1 code.

    Yields
    -------
    error: Error

    """
    if suffix:
        if subject_id.endswith(suffix):
            subject_id = subject_id[:-len(suffix)]
        elif len(subject_id) <= 12 or subject_id.isdigit():
            yield 'PSC1 code "{0}" should end with suffix "{1}"'.format(subject_id, suffix)
    if subject_id.isdigit():
        if len(subject_id) != 12:
            yield 'PSC1 code "{0}" contains {1} digits instead of 12'.format(subject_id, len(subject_id))
    elif len(subject_id) > 12 and subject_id[:12].isdigit() and not subject_id[12].isdigit():
        yield 'PSC1 code "{0}" ends with unexpected suffix "{1}"'.format(subject_id, subject_id[12:])
        subject_id = subject_id[:12]
    if not subject_id.isdigit():
        yield 'PSC1 code "{0}" should contain 12 digits'.format(subject_id)
    elif len(subject_id) != 12:
        yield 'PSC1 code "{0}" contains {1} characters instead of 12'.format(subject_id, len(subject_id))
    elif subject_id not in PSC2_FROM_PSC1:
        yield 'PSC1 code "{0}" is not valid'.format(subject_id)
    elif psc1:
        if suffix and psc1.endswith(suffix):
            psc1 = psc1[:-len(suffix)]
        if subject_id != psc1:
            yield'PSC1 code "{0}" was expected to be "{1}"'.format(subject_id, psc1)


def _check_name(path, prefix, extension, suffix=None, psc1=None):
    """
    """
    error_list = []

    basename = os.path.basename(path)
    if basename.startswith(prefix) and basename.endswith(extension):
        subject_id = basename[len(prefix):-len(extension)]
        error_list = [Error(basename, 'Incorrect file name: ' + message)
                      for message in _check_psc1(subject_id, suffix, psc1)]
        return subject_id, error_list
    else:
        return None, [Error(basename, 'Not a valid Cantab file name')]


def check_cant_name(path, timepoint=None, psc1=None):
    """Check correctness of a Cantab cant_*.cclar filename.

    Parameters
    ----------
    path : str
        Pathname or filename of the Cantab file.
    timepoint : str, optional
        Time point identifier, found as a suffix in subject identifiers.
    psc1 : str, optional
        Expected 12-digit PSC1 code.

    Returns
    -------
    result: tuple
        In case of errors, return the tuple (psc1, errors) where psc1 is
        a collection of PSC1 codes found in the ZIP file and errors is an
        empty list if the ZIP file passes the check and a list of errors
        otherwise.

    """
    return _check_name(path, 'cant_', '.cclar', timepoint, psc1)


def check_datasheet_name(path, timepoint=None, psc1=None):
    """Check correctness of a Cantab datasheet_*.csv filename.

    Parameters
    ----------
    path : str
        Pathname or filename of the Cantab file.
    timepoint : str, optional
        Time point identifier, found as a suffix in subject identifiers.
    psc1 : str, optional
        Expected 12-digit PSC1 code.

    Returns
    -------
    result: tuple
        In case of errors, return the tuple (psc1, errors) where psc1 is
        a collection of PSC1 codes found in the ZIP file and errors is an
        empty list if the ZIP file passes the check and a list of errors
        otherwise.

    """
    return _check_name(path, 'datasheet_', '.csv', timepoint, psc1)


def check_detailed_datasheet_name(path, timepoint=None, psc1=None):
    """Check correctness of a Cantab detailed_datasheet_*.csv filename.

    Parameters
    ----------
    path : str
        Pathname or filename of the Cantab file.
    timepoint : str, optional
        Time point identifier, found as a suffix in subject identifiers.
    psc1 : str, optional
        Expected 12-digit PSC1 code.

    Returns
    -------
    result: tuple
        In case of errors, return the tuple (psc1, errors) where psc1 is
        a collection of PSC1 codes found in the ZIP file and errors is an
        empty list if the ZIP file passes the check and a list of errors
        otherwise.

    """
    return _check_name(path, 'detailed_datasheet_', '.csv', timepoint, psc1)


def check_report_name(path, timepoint=None, psc1=None):
    """Check correctness of a Cantab report_*.html filename.

    Parameters
    ----------
    path : str
        Pathname or filename of the Cantab file.
    timepoint : str, optional
        Time point identifier, found as a suffix in subject identifiers.
    psc1 : str, optional
        Expected 12-digit PSC1 code.

    Returns
    -------
    result: tuple
        In case of errors, return the tuple (psc1, errors) where psc1 is
        a collection of PSC1 codes found in the ZIP file and errors is an
        empty list if the ZIP file passes the check and a list of errors
        otherwise.

    """
    return _check_name(path, 'report_', '.html', timepoint, psc1)


def _simple_check_subject_id(path, subject_ids, suffix=None, psc1=None):
    """Helper function checks identifiers found in Cantab files against expected values.

    Parameters
    ----------
    path : str
        Path to the Cantab file to read and check.
    subject_ids : set
        Expected subject identifiers: PSC1 codes possibly followed by suffix.
    suffix : str, optional
        Time point identifier, typically appended to PSC1 codes.
    psc1 : str, optional
        Expected 12-digit PSC1 code.

    Yields
    -------
    error: Error

    """
    basename = os.path.basename(path)
    if len(subject_ids) < 1:
        yield Error(basename, 'Unable to find a PSC1 code inside file')
    else:
        if len(subject_ids) > 1:
            yield Error(basename,
                        'Multiple PSC1 codes inside file: {0}'
                        .format(', '.join(subject_ids)))
        for subject_id in subject_ids:
            for message in _check_psc1(subject_id, suffix, psc1):
                yield Error(basename, message)


def _simple_check_content(path, function, suffix=None, psc1=None):
    """Helper function to check the contents of a Cantab file.

    Used for those Cantab files types for which we chack only the
    Subjects IDs found in the file.

    Parameters
    ----------
    path : str
        Path to the Cantab file to check.
    function : function
        Specialized read function for this file type.
    suffix : str, optional
        Time point identifier, found as a suffix in subject identifiers.
    psc1 : str
        Expected PSC1 code.

    Returns
    -------
    result: tuple
        In case of errors, return the tuple (psc1, errors) where psc1 is
        a collection of PSC1 codes found in the ZIP file and errors is an
        empty list if the ZIP file passes the check and a list of errors
        otherwise.

    """
    error_list = []

    basename = os.path.basename(path)
    if os.path.getsize(path) == 0:
        return ([], [Error(basename, 'File is empty')])
    subject_ids = function(path)

    if len(subject_ids) < 1:
        error_list.append(Error(basename, 'Unable to find a PSC1 code inside file'))
    else:
        if len(subject_ids) > 1:
            error_list.append(Error(basename,
                                    'Multiple PSC1 codes inside file: {0}'
                                    .format(', '.join(subject_ids))))
        for subject_id in subject_ids:
            for message in _check_psc1(subject_id, suffix, psc1):
                error_list.append(Error(basename, message))

    return (subject_ids, error_list)


def _zip_check_content(path, function, suffix=None, psc1=None):
    """Helper function to check the contents of a zip'ped Cantab file.

    Parameters
    ----------
    path : str
        Path to the Cantab file to check.
    function : function
        Specialized read function for this file type.
    suffix : str, optional
        Time point identifier, found as a suffix in subject identifiers.
    psc1 : str
        Expected PSC1 code.
    date : datetime.date, optional
        Date of acquisition.

    Returns
    -------
    result: tuple
        In case of errors, return the tuple (psc1, errors) where psc1 is
        a collection of PSC1 codes found in the ZIP file and errors is an
        empty list if the ZIP file passes the check and a list of errors
        otherwise.

    """
    try:
        return _simple_check_content(path, function, suffix, psc1)
    except BadZipFile as e:
        basename = os.path.basename(path)
        return ([], [Error(basename, 'Cannot unzip file: {0}'.format(e))])


def _datasheet_check_content(path, function, suffix='FU2', psc1=None, date=None):
    """Helper function to check the contents of a datasheet_*.csv file.

    Parameters
    ----------
    path : str
        Path to the Cantab file to check.
    function : function
        Specialized read function for this file type.
    suffix : str, optional
        Time point identifier, found as a suffix in subject identifiers.
    psc1 : str
        Expected PSC1 code.
    date : datetime.date, optional
        Date of acquisition.

    Returns
    -------
    result: tuple
        In case of errors, return the tuple (psc1, errors) where psc1 is
        a collection of PSC1 codes found in the ZIP file and errors is an
        empty list if the ZIP file passes the check and a list of errors
        otherwise.

    """
    basename = os.path.basename(path)
    try:
        contents = function(path)
    except TypeError as e:  # "delimiter" must be an 1-character string
        return (None, [Error(basename, 'Cannot read CSV file: {0}'.format(e))])
    except UnicodeDecodeError as e:
        return ([], [Error(basename,
                           'File is seriously damaged: {0}'
                           .format(e))])
    errors = list(_simple_check_subject_id(path, contents[0], suffix, psc1))
    if date and date not in set(x.date() for x in contents[1]):
        errors.append(Error(basename,
                            'Date {0} was expected to be {1}'
                            .format('/'.join(str(x.date()) for x in contents[1]), date)))
    rows = contents[2]
    if rows != 2:
        errors.append(Error(basename,
                            'Found {0} rows instead of 2'
                            .format(rows)))
    columns = set(contents[4])
    if suffix in _COLUMN_NAMES:
        for column in _COLUMN_NAMES[suffix]['REQUIRED']:
            if column not in columns:
                errors.append(Error(basename,
                                    'Missing required column "{0}"'
                                    .format(column)))
        for column in columns:
            if column and column not in (_COLUMN_NAMES[suffix]['REQUIRED'] +
                                         _COLUMN_NAMES[suffix]['OPTIONAL']):
                if column.startswith('Warning'):  # FIXME: hardcoded!
                    continue
                errors.append(Error(basename,
                                    'Found unknown column "{0}"'
                                    .format(column)))
    else:
        errors.append(Error(basename,
                            'We are unable to check datasheet_*.csv files for timepoint "{0}"'
                            .format(suffix)))
    return (contents, errors)


def check_cant_content(path, timepoint=None, psc1=None, date=None):
    """Sanity check of a Cantab cant_*.cclar file.

    Parameters
    ----------
    path : str
        Path to the ZIP file.
    timepoint : str, optional
        Time point identifier, found as a suffix in subject identifiers.
    psc1 : str, optional
        Expected 12-digit PSC1 code.
    date : datetime.date, optional
        Date of acquisition.

    Returns
    -------
    result: tuple
        In case of errors, return the tuple (psc1, errors) where psc1 is
        a collection of PSC1 codes found in the ZIP file and errors is an
        empty list if the ZIP file passes the check and a list of errors
        otherwise.

    Raises
    ------
    FileNotFoundError
        If the file does not exist.

    """
    return _zip_check_content(path, read_cant, timepoint, psc1)


def check_datasheet_content(path, timepoint=None, psc1=None, date=None):
    """Sanity check of a Cantab datasheet_*.csv file.

    Parameters
    ----------
    path : str
        Path to the ZIP file.
    timepoint : str, optional
        Time point identifier, found as a suffix in subject identifiers.
    psc1 : str, optional
        Expected 12-digit PSC1 code.
    date : datetime.date, optional
        Date of acquisition.

    Returns
    -------
    result: tuple
        In case of errors, return the tuple (psc1, errors) where psc1 is
        a collection of PSC1 codes found in the ZIP file and errors is an
        empty list if the ZIP file passes the check and a list of errors
        otherwise.

    Raises
    ------
    FileNotFoundError
        If the file does not exist.

    """
    return _datasheet_check_content(path, read_datasheet, timepoint, psc1, date)


def check_detailed_datasheet_content(path, timepoint=None, psc1=None, date=None):
    """Sanity check of a Cantab detailed_datasheet_*.csv file.

    Parameters
    ----------
    path : str
        Path to the ZIP file.
    timepoint : str, optional
        Time point identifier, found as a suffix in subject identifiers.
    psc1 : str, optional
        Expected 12-digit PSC1 code.
    date : datetime.date, optional
        Date of acquisition.

    Returns
    -------
    result: tuple
        In case of errors, return the tuple (psc1, errors) where psc1 is
        a collection of PSC1 codes found in the ZIP file and errors is an
        empty list if the ZIP file passes the check and a list of errors
        otherwise.

    Raises
    ------
    FileNotFoundError
        If the file does not exist.

    """
    return _simple_check_content(path, read_detailed_datasheet, timepoint, psc1)


def check_report_content(path, timepoint=None, psc1=None, date=None):
    """Sanity check of a Cantab report_*.html file.

    Parameters
    ----------
    path : str
        Path to the ZIP file.
    timepoint : str, optional
        Time point identifier, found as a suffix in subject identifiers.
    psc1 : str, optional
        Expected 12-digit PSC1 code.
    date : datetime.date, optional
        Date of acquisition.

    Returns
    -------
    result: tuple
        In case of errors, return the tuple (psc1, errors) where psc1 is
        a collection of PSC1 codes found in the ZIP file and errors is an
        empty list if the ZIP file passes the check and a list of errors
        otherwise.

    Raises
    ------
    FileNotFoundError
        If the file does not exist.

    """
    return _simple_check_content(path, read_report, timepoint, psc1)
