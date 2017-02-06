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
import re

from .cantab import (CANTAB_CCLAR, DETAILED_DATASHEET_CSV, DATASHEET_CSV,
                     REPORT_HTML,
                     read_cant, read_datasheet, read_detailed_datasheet,
                     read_report)
from .behavioral import (MID_CSV, FT_CSV, SS_CSV, RECOG_CSV,
                         read_mid, read_ft, read_ss, read_recog)

import logging
logger = logging.getLogger(__name__)

__all__ = ['walk_additional_data', 'report_additional_data']


#
# check filenames against these regex'es when exploring Additional Data
#
# in some case order is important, for example:
# - first match 'detailed_datasheet'
# - then match 'datasheet'
#
_LOOSE_ADDITIONAL_DATA_REGEXES = (
    (re.compile(r'(\w+_)?cant(_\w+)?\.cclar', re.IGNORECASE), CANTAB_CCLAR),
    # Mannheim send 'detailed datasheet' files (space instead of underscore)
    (re.compile(r'(\w+_)?detailed[_ ]datasheet(_\w+)?\.csv', re.IGNORECASE),
     DETAILED_DATASHEET_CSV),
    (re.compile(r'(\w+_)?datasheet(_\w+)?\.csv', re.IGNORECASE), DATASHEET_CSV),
    (re.compile(r'(\w+_)?report(_\w+)?\.html', re.IGNORECASE), REPORT_HTML),
    (re.compile(r'ft_\w+\.csv', re.IGNORECASE), FT_CSV),
    (re.compile(r'mid_\w+\.csv', re.IGNORECASE), MID_CSV),
    (re.compile(r'recog_\w+\.csv', re.IGNORECASE), RECOG_CSV),
    (re.compile(r'ss_\w+\.csv', re.IGNORECASE), SS_CSV),
)

_EXACT_ADDITIONAL_DATA_REGEXES = (
    (re.compile(r'cant_\d{12}(fu|FU)?\.cclar'), CANTAB_CCLAR),
    (re.compile(r'detailed_datasheet_\d{12}(fu|FU)?\.csv'), DETAILED_DATASHEET_CSV),
    (re.compile(r'datasheet_\d{12}(fu|FU)?\.csv'), DATASHEET_CSV),
    (re.compile(r'report_\d{12}(fu|FU)?\.html'), REPORT_HTML),
    (re.compile(r'ft_\d{12}(fu|FU)?\.csv'), FT_CSV),
    (re.compile(r'mid_\d{12}(fu|FU)?\.csv'), MID_CSV),
    (re.compile(r'recog_\d{12}(fu|FU)?\.csv'), RECOG_CSV),
    (re.compile(r'ss_\d{12}(fu|FU)?\.csv', re.IGNORECASE), SS_CSV),
)


def _match_additional_data_sops(filename, exact=False):
    """Compare filename to filenames defined in Imagen FU2 SOPs.

    Compare actual filename to expected filenames expected for Additionnal
    Data in SOPs, either in a strict way or a loose way. This matching
    function is empirical and based on experimentation.

    Parameters
    ----------
    filename : unicode
        The file basename to match.

    exact : bool
        Exact match if True else loose match.

    Returns
    -------
    str
        If the filename loosely matches a file type defined in the SOPs,
        return the type file type, else return None.

    """
    if exact:
        regex_list = _EXACT_ADDITIONAL_DATA_REGEXES
    else:
        regex_list = _LOOSE_ADDITIONAL_DATA_REGEXES
    for regex, filetype in regex_list:
        if regex.match(filename):
            logger.debug('assign type "%s" to filename: %s',
                         filetype, filename)
            return filetype
    logger.info('filename does not match any known type: %s', filename)
    return None


def walk_additional_data(path):
    """Generate information on Additional Data files in a directory.

    Parameters
    ----------
    path : unicode
        The directory to look for files into.

    Returns
    -------
    tuple
        Yield a 2-tuple: the name and the path of each file relative to path.

    """

    for root, dirs, files in os.walk(path):
        for filename in files:
            relpath = os.path.relpath(os.path.join(root, filename), path)
            yield filename, relpath


def report_additional_data(path, psc1, exact=False):
    """Find Additional Data files that fit the Imagen FU2 SOPs.

    The Imagen FU2 SOPs define a precise file organization for Additional
    Data. In practice we have found the SOPs are only loosely followed by
    acquisition centres, hence the tolerant optional argument.

    This function scans the directory where we expect to find the Additional
    Data of a dataset and builds a collection of files identified as the
    files described in the SOPs.

    Parameters
    ----------
    path : unicode
        The directory to look for Additional Data into.

    psc1 : str
        PSC1 code of the subject.

    exact : bool
        Exact match if True, else loose match.

    Returns
    -------
    dict
        The key identifies the type of identified files and the value
        lists the relative path of the files.

    """
    additional_files = {}

    for filename, relpath in walk_additional_data(path):
        filetype = _match_additional_data_sops(filename, exact)
        if filetype:
            logger.debug('assign type "%s" to file: %s',
                         filetype, relpath)
            additional_files.setdefault(filetype, []).append(relpath)
        else:
            logger.warning('cannot match any known type: %s', relpath)

    additional_data = {}

    # read cant_*.cclar where available
    if CANTAB_CCLAR in additional_files:
        for f in additional_files[CANTAB_CCLAR]:
            f_path = os.path.join(path, f)
            subject_ids = read_cant(f_path)
            if psc1 in subject_ids:
                subject_ids.remove(psc1)
            additional_data.setdefault(CANTAB_CCLAR, {})[f] = subject_ids
    # read datasheet_*.csv where available
    if DATASHEET_CSV in additional_files:
        for f in additional_files[DATASHEET_CSV]:
            f_path = os.path.join(path, f)
            subject_ids, _rows, _columns_min, _start_times = read_datasheet(f_path)
            if psc1 in subject_ids:
                subject_ids.remove(psc1)
            additional_data.setdefault(DATASHEET_CSV, {})[f] = subject_ids
    # read detailed_datasheet_*.csv where available
    if DETAILED_DATASHEET_CSV in additional_files:
        for f in additional_files[DETAILED_DATASHEET_CSV]:
            f_path = os.path.join(path, f)
            subject_ids = read_detailed_datasheet(f_path)
            if psc1 in subject_ids:
                subject_ids.remove(psc1)
            additional_data.setdefault(DETAILED_DATASHEET_CSV, {})[f] = subject_ids
    # read report_*.html where available
    if REPORT_HTML in additional_files:
        for f in additional_files[REPORT_HTML]:
            f_path = os.path.join(path, f)
            subject_ids = read_report(f_path)
            if psc1 in subject_ids:
                subject_ids.remove(psc1)
            additional_data.setdefault(REPORT_HTML, {})[f] = subject_ids
    # read Scanning/ft_*.csv where available
    if FT_CSV in additional_files:
        for f in additional_files[FT_CSV]:
            f_path = os.path.join(path, f)
            subject_ids = read_ft(f_path)
            if psc1 in subject_ids:
                subject_ids.remove(psc1)
            additional_data.setdefault(FT_CSV, {})[f] = subject_ids
    # read Scanning/mid_*.csv where available
    if MID_CSV in additional_files:
        for f in additional_files[MID_CSV]:
            f_path = os.path.join(path, f)
            subject_ids = read_mid(f_path)
            if psc1 in subject_ids:
                subject_ids.remove(psc1)
            additional_data.setdefault(MID_CSV, {})[f] = subject_ids
    # read Scanning/recog_*.csv where available
    if RECOG_CSV in additional_files:
        for f in additional_files[RECOG_CSV]:
            f_path = os.path.join(path, f)
            subject_ids = read_recog(f_path)
            if psc1 in subject_ids:
                subject_ids.remove(psc1)
            additional_data.setdefault(RECOG_CSV, {})[f] = subject_ids
    # read Scanning/ss_*.csv where available
    if SS_CSV in additional_files:
        for f in additional_files[SS_CSV]:
            f_path = os.path.join(path, f)
            subject_ids = read_ss(f_path)
            if psc1 in subject_ids:
                subject_ids.remove(psc1)
            additional_data.setdefault(SS_CSV, {})[f] = subject_ids

    return additional_data
