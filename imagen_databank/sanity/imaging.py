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
import tempfile
import shutil
from zipfile import ZipFile
try:
    from zipfile import BadZipFile
except ImportError:
    from zipfile import BadZipfile as BadZipFile  # Python 2

from ..core import PSC2_FROM_PSC1
from ..core import Error
from ..behavioral import (MID_CSV, FT_CSV, SS_CSV, RECOG_CSV)
from ..behavioral import (read_mid, read_ft, read_ss, read_recog)
from ..image_data import (SEQUENCE_T2, SEQUENCE_T2_FLAIR,
                          SEQUENCE_ADNI_MPRAGE,
                          SEQUENCE_MID, SEQUENCE_FT, SEQUENCE_SST,
                          SEQUENCE_B0_MAP, SEQUENCE_DTI,
                          SEQUENCE_RESTING_STATE)
from ..dicom_utils import read_metadata

import logging
logger = logging.getLogger(__name__)

__all__ = ['check_zip_name', 'check_zip_content', 'ZipTree']


_BEHAVIORAL_PREFIX_EXTENSION = {
    MID_CSV: ('mid_', '.csv'),
    FT_CSV: ('ft_', '.csv'),
    SS_CSV: ('ss_', '.csv'),
    RECOG_CSV: ('recog_', '.csv'),
}


def _check_psc1(subject_id, suffix=None, psc1=None):
    """Check a subject identifier against an expected value and.

    Parameters
    ----------
    subject_id : str
        Expected subject identifier: PSC1 code possibly followed by suffix.
    suffix : str, optional
        Time point identifier, typically appended to PSC1 codes.
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
            yield 'PSC1 code "{0}" was expected to be "{1}"'.format(subject_id, psc1)


def check_zip_name(path, timepoint=None, psc1=None):
    """Check correctness of a ZIP filename.

    Parameters
    ----------
    path : str
        Pathname or filename of the ZIP file.
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
    basename = os.path.basename(path)
    if basename.endswith('.zip'):
        subject_id = basename[:-len('.zip')]
        error_list = [Error(basename, 'Incorrect ZIP file name: ' + message)
                      for message in _check_psc1(subject_id, timepoint, psc1)]
        return subject_id, error_list
    else:
        return None, [Error(basename, 'Not a valid ZIP file name')]


class ZipTree:
    """Node of a tree structure to represent ZipFile contents.

    Attributes
    ----------
    directories : dict
        Dictionnary of subdirectories.
    files : str
        Dictionnary of files under this node.

    """

    def __init__(self, filename=''):
        self.filename = filename
        self.directories = {}
        self.files = {}

    @staticmethod
    def create(path):
        ziptree = ZipTree()
        with ZipFile(path, 'r') as z:
            for zipinfo in z.infolist():
                ziptree._add(zipinfo)  # pylint: disable=W0212
        return ziptree

    def _add(self, zipinfo):
        d = self
        if zipinfo.filename.endswith('/'):  # directory
            parts = zipinfo.filename.rstrip('/').split('/')
            filename = ''
            for part in parts:
                filename += part + '/'
                d = d.directories.setdefault(part, ZipTree(filename))
        else:  # file
            parts = zipinfo.filename.split('/')
            basename = parts.pop()
            for part in parts:
                d = d.directories.setdefault(part, ZipTree())
            if basename not in d.files:
                d.files[basename] = zipinfo
            else:
                raise BadZipFile('duplicate file entry in zipfile')

    def pprint(self, indent=''):
        self._print_children(indent)

    def _print_children(self, indent=''):
        directories = list(self.directories.items())
        if directories:
            last_directory = directories.pop()
            for d, ziptree in directories:
                ziptree._print(d, indent, False)  # pylint: disable=W0212
        else:
            last_directory = None
        files = list(self.files.items())
        if files:
            if last_directory:
                d, ziptree = last_directory
                ziptree._print(d, indent, False)  # pylint: disable=W0212
            last_file = files.pop()
            for f, dummy_zipinfo in files:
                print(indent + '├── ' + f)
            f, dummy_zipinfo = last_file
            print(indent + '└── ' + f)
        elif last_directory:
            d, ziptree = last_directory
            ziptree._print(d, indent, True)  # pylint: disable=W0212

    def _print(self, name, indent='', last=True):
        if last:
            print(indent + '└── ' + name)
            indent += '    '
        else:
            print(indent + '├── ' + name)
            indent += '│   '
        self._print_children(indent)


class TemporaryDirectory(object):
    """Backport from Python 3.
    """
    def __init__(self, suffix="", prefix=tempfile.gettempprefix(), dir=None):
        self.pathname = tempfile.mkdtemp(suffix, prefix, dir)

    def __repr__(self):
        return "<{} {!r}>".format(self.__class__.__name__, self.name)

    def __enter__(self):
        return self.pathname

    def __exit__(self, exc, value, tb):
        shutil.rmtree(self.pathname)


def _check_behavioral_name(filename):
    for (key, (prefix, extension)) in _BEHAVIORAL_PREFIX_EXTENSION.items():
        if filename[:len(prefix)] == prefix and filename[-len(extension):] == extension:
            subject_id = filename[len(prefix):-len(extension)]
            return key, subject_id
    return None, None


def _check_physiological_name(filename):
    try:
        root, ext = filename.rsplit('.', 1)
    except ValueError:
        root, ext = filename, None
    if ext in {'ecg', 'ext', 'puls', 'resp'}:  # BERLIN, HAMBURG, MANNHEIM
        if root[-len('_rest'):] == '_rest':  # MANNHEIM
            root = root[:-len('_rest')]
        return ext, root
    elif ext in {'log'}:  # NOTTINGHAM
        # SCANPHYSLOG20140409112224.log
        # where 20140409112224 is the date/time 2014-04-09 11:22:24
        if root[:len('SCANPHYSLOG')] == 'SCANPHYSLOG':
            root = root[len('SCANPHYSLOG'):]
            if root.isdigit() and len(root) == len('20140409112224'):
                return ext, None
    elif ext in {'txt'}:  # LONDON
        # ImagenBRest_Resting_1_Raw_010000123456FU3.txt
        # ImagenBRest_Resting_1_Times_010000123456FU3.txt
        if root[:len('ImagenBRest_Resting_1_Raw_')] == 'ImagenBRest_Resting_1_Raw_':
            root = root[len('ImagenBRest_Resting_1_Raw_'):]
            return ext, root
        elif root[:len('ImagenBRest_Resting_1_Times_')] == 'ImagenBRest_Resting_1_Times_':
            root = root[len('ImagenBRest_Resting_1_Times_'):]
            return ext, root

    return None, None


def _check_scanning(path, ziptree, suffix, psc1, date, expected):
    """Check the "Scanning" folder of a ZipTree.

    Parameters
    ----------
    path : str
        Path to the ZIP file.
    ziptree : ZipTree
        "Scanning" branch with the meta-data read from the ZIP file.
    suffix : str, optional
        Time point identifier, found as a suffix in subject identifiers.
    psc1 : str, optional
        Expected 12-digit PSC1 code.
    date : datetime.date, optional
        Date of acquisition.
    expected : dict
        Behavioral files expected to be found in this folder, associated to MRI sequences or not.

    Yields
    -------
    error: Error

    """
    subject_ids = set()
    expected_tests = set([x for x in (MID_CSV, FT_CSV, SS_CSV, RECOG_CSV)
                          if expected[x] != 'Missing']) if expected else None
    actual_tests = set()
    error_list = []

    if ziptree.directories:
        for z in ziptree.directories.values():
            error_list.append(Error(z.filename,
                                    'Folder "Scanning" should not contain subfolders'))

    for f, z in ziptree.files.items():
        behavioral_type, subject_id = _check_behavioral_name(f)
        if behavioral_type:
            if subject_id:
                subject_ids.add(subject_id)
                error_list.extend([Error(z.filename, 'Incorrect behavioral file name: ' + message)
                                   for message in _check_psc1(subject_id, suffix, psc1)])
            else:
                error_list.append(Error(z.filename, 'Unexpected behavioral file name'))
        else:
            physiological_type, subject_id = _check_physiological_name(f)
            if physiological_type:
                if subject_id:
                    subject_ids.add(subject_id)
                    error_list.extend([Error(z.filename, 'Incorrect physiological file name: ' + message)
                                       for message in _check_psc1(subject_id, suffix, psc1)])
            else:
                error_list.append(Error(z.filename, 'Unexpected file name in "Scanning"'))
            continue  # TODO: make a function of the code below this line

        with TemporaryDirectory() as temp_directory:
            with ZipFile(path, 'r') as zip_file:
                if expected_tests and behavioral_type not in expected_tests:
                    error_list.append(Error(z.filename, 'Unexpected behavioral file'))
                behavioral_path = zip_file.extract(z.filename, path=temp_directory)
                if behavioral_type == MID_CSV:
                    subject_id, timestamp, trials, errors = read_mid(behavioral_path)
                    if subject_id:
                        error_list.extend([Error(z.filename, 'Incorrect behavioral file content: ' + message)
                                           for message in _check_psc1(subject_id, suffix, psc1)])
                        actual_tests.add(behavioral_type)
                    else:
                        error_list.append(Error(z.filename, 'Missing subject ID'))
                    subject_ids.add(subject_id)
                    if trials[-1] != 42:
                        error_list.append(Error(z.filename, 'Behavioral file contains {0} trials instead of 42'
                                                            .format(trials[-1])))
                    if date and date != timestamp.date():
                        error_list.append(Error(z.filename, 'Date was expected to be "{0}" instead of "{1}"'
                                                            .format(date, timestamp.date())))
                    error_list.extend(errors)
                elif behavioral_type == FT_CSV:
                    subject_id, timestamp, trials, errors = read_ft(behavioral_path)
                    if subject_id:
                        error_list.extend([Error(z.filename, 'Incorrect behavioral file content: ' + message)
                                           for message in _check_psc1(subject_id, suffix, psc1)])
                        actual_tests.add(behavioral_type)
                    else:
                        error_list.append(Error(z.filename, 'Missing subject ID'))
                    subject_ids.add(subject_id)
                    if len(trials) != 24:
                        error_list.append(Error(z.filename, 'Behavioral file contains {0} trials instead of 24'
                                                            .format(len(trials))))
                    if date and date != timestamp.date():
                        error_list.append(Error(z.filename, 'Date was expected to be "{0}" instead of "{1}"'
                                                            .format(date, timestamp.date())))
                    error_list.extend(errors)
                elif behavioral_type == SS_CSV:
                    subject_id, timestamp, trials, errors = read_ss(behavioral_path)
                    if subject_id:
                        error_list.extend([Error(z.filename, 'Incorrect behavioral file content: ' + message)
                                           for message in _check_psc1(subject_id, suffix, psc1)])
                        actual_tests.add(behavioral_type)
                    else:
                        error_list.append(Error(z.filename, 'Missing subject ID'))
                    subject_ids.add(subject_id)
                    if trials[-1] != 360:
                        error_list.append(Error(z.filename, 'Behavioral file contains {0} trials instead of 360'
                                                            .format(trials[-1])))
                    if date and date != timestamp.date():
                        error_list.append(Error(z.filename, 'Date was expected to be "{0}" instead of "{1}"'
                                                            .format(date, timestamp.date())))
                    error_list.extend(errors)
                elif behavioral_type == RECOG_CSV:
                    subject_id, timestamp, trials, errors = read_recog(behavioral_path)
                    if subject_id:
                        error_list.extend([Error(z.filename, 'Incorrect behavioral file content: ' + message)
                                           for message in _check_psc1(subject_id, suffix, psc1)])
                        actual_tests.add(behavioral_type)
                    else:
                        error_list.append(Error(z.filename, 'Missing subject ID'))
                    subject_ids.add(subject_id)
                    if len(trials) != 5:
                        error_list.append(Error(z.filename, 'Behavioral file contains {0} trials instead of 5'
                                                            .format(len(trials))))
                    if date and date != timestamp.date():
                        error_list.append(Error(z.filename, 'Date was expected to be "{0}" instead of "{1}"'
                                                            .format(date, timestamp.date())))
                    error_list.extend(errors)

    if expected_tests:
        missing_tests = expected_tests - actual_tests
        for x in missing_tests:
            error_list.append(Error(ziptree.filename,
                                    "Missing behavioral file '{}_*.csv'".format(x)))

    return subject_ids, error_list


def _check_additional_data(path, ziptree, suffix, psc1, date, expected):
    """Check the "AdditionalData" folder of a ZipTree.

    Parameters
    ----------
    path : str
        Path to the ZIP file.
    ziptree : ZipTree
        "AdditionalData" branch with the meta-data read from the ZIP file.
    suffix : str, optional
        Time point identifier, found as a suffix in subject identifiers.
    psc1 : str, optional
        Expected 12-digit PSC1 code.
    date : datetime.date, optional
        Date of acquisition.
    expected : dict.
        Sequences or tests expected to be found in this folder.

    Returns
    -------
    result: tuple
        In case of errors, return the tuple (psc1, errors) where psc1 is
        a collection of PSC1 codes found in the ZIP file and errors is an
        empty list if the ZIP file passes the check and a list of errors
        otherwise.

    """
    subject_ids = set()
    error_list = []

    if len(ziptree.directories) > 1:
        for d, z in ziptree.directories.items():
            if d != 'Scanning':
                error_list.append(Error(z.filename,
                                        'Folder "AdditionalData" should contain only a "Scanning" folder'))
    if 'Scanning' in ziptree.directories:
        s, e = _check_scanning(path, ziptree.directories['Scanning'],
                               suffix, psc1, date, expected)
        subject_ids.update(s)
        error_list.extend(e)
    else:
        error_list.append(Error(ziptree.filename + 'Scanning/',
                                'Folder "Scanning" is missing'))

    return subject_ids, error_list


def _files(ziptree):
    """List files in a ZipTree.

    Parameters
    ----------
    ziptree : ZipTree

    Yields
    -------
    f: str

    """
    for f in ziptree.files:
        yield ziptree.filename + f
    for ziptree in ziptree.directories.values():
        for f in _files(ziptree):
            yield f


def _check_empty_files(ziptree):
    """Recursively check for empty files in a ZipTree.

    Parameters
    ----------
    ziptree : ZipTree

    Yields
    -------
    error: Error

    """
    for zipinfo in ziptree.files.values():
        if zipinfo.file_size == 0:
            yield Error(zipinfo.filename, 'File is empty')
    for ziptree in ziptree.directories.values():
        for error in _check_empty_files(ziptree):
            yield error


def _check_image_data(path, ziptree, suffix, psc1, date, expected):
    #pylint: disable=unused-argument
    """Check the "ImageData" folder of a ZipTree.

    Parameters
    ----------
    path : str
        Path to the ZIP file.
    ziptree : ZipTree
        "ImageData" branch with the meta-data read from the ZIP file.
    suffix : str, optional
        Time point identifier, found as a suffix in subject identifiers.
    psc1 : str, optional
        Expected 12-digit PSC1 code.
    date : datetime.date, optional
        Date of acquisition.
    expected : dict.
        Sequences expected to be found in this folder.

    Returns
    -------
    result: tuple
        In case of errors, return the tuple (psc1, errors) where psc1 is
        a collection of PSC1 codes found in the ZIP file and errors is an
        empty list if the ZIP file passes the check and a list of errors
        otherwise.

    """
    subject_ids = set()
    error_list = []

    # check zip tree is not empty and does not contain empty files
    files = list(_files(ziptree))
    if len(files) < 1:
        error_list.append(Error(ziptree.filename, 'Folder is empty'))
    else:
        error_list.extend(_check_empty_files(ziptree))

        # choose a file from zip tree and check its DICOM tags
        with TemporaryDirectory('imagen') as tempdir:
            for f in files:
                with ZipFile(path, 'r') as z:
                    dicom_file = z.extract(f, tempdir)
                    try:
                        metadata = read_metadata(dicom_file, force=True)
                    except IOError:
                        continue
                    else:
                        for x in ('StudyComments',  # DUBLIN
                                  'ImageComments',  # HAMBURG, DRESDEN
                                  'PatientID'):  # LONDON, NOTTINGHAM, BERLIN, MANNHEIM, PARIS
                            if x in metadata:
                                subject_id = metadata[x]
                                if subject_id[-len(suffix):] == suffix:
                                    subject_id = subject_id[:-len(suffix)]
                                subject_ids.add(subject_id)
                                if subject_id != psc1:
                                    error_list.append(Error(f, 'PSC1 code "{0}" was expected to be "{1}"'
                                                               .format(subject_id, psc1)))
                                break
                        else:
                            subject_id = None
                            error_list.append(Error(f, 'Missing PSC1 code "{0}"'
                                                       .format(psc1)))
                        break
            else:
                error_list.append(Error(f, 'Unable to read DICOM files in dataset "{}"'
                                           .format(psc1)))

    return subject_ids, error_list


def _check_ziptree(path, ziptree, suffix=None, psc1=None, date=None, expected=None):
    """Check the uppermost folder of a ZipTree.

    Parameters
    ----------
    path : str
        Path to the ZIP file.
    ziptree : ZipTree
        Meta-data read from the ZIP file.
    suffix : str, optional
        Time point identifier, found as a suffix in subject identifiers.
    psc1 : str, optional
        Expected 12-digit PSC1 code.
    date : datetime.date, optional
        Date of acquisition.
    expected : dict, optional
        Which MRI sequences and tests to expect.

    Returns
    -------
    result: tuple
        In case of errors, return the tuple (psc1, errors) where psc1 is
        a collection of PSC1 codes found in the ZIP file and errors is an
        empty list if the ZIP file passes the check and a list of errors
        otherwise.

    """
    subject_ids = set()
    error_list = []

    basename = os.path.basename(path)
    for f, zipinfo in ziptree.files.items():
        error_list.append(Error(zipinfo.filename,
                                'Unexpected file at the root of the ZIP file'))

    if len(ziptree.directories) < 1:
        error_list.append(Error(basename,
                                'ZIP file lacks an uppermost folder'))

    for d, z in ziptree.directories.items():
        # uppermost directory
        subject_id = d
        error_list.extend([Error(z.filename, 'Incorrect uppermost folder name: ' + message)
                           for message in _check_psc1(subject_id, suffix, psc1)])
        for f in z.files:
            error_list.append(Error(f, 'Unexpected file in the uppermost folder'))
        for d in z.directories:
            if d != 'AdditionalData' and d != 'ImageData':
                error_list.append(Error(z.filename,
                                        'Unexpected folder subfolder in the uppermost folder'))
        # AdditionalData
        if 'AdditionalData' in z.directories:
            if psc1:
                expected_psc1 = psc1
            else:
                expected_psc1 = subject_id
                if expected_psc1.endswith(suffix):
                    expected_psc1 = expected_psc1[:-len(suffix)]
            s, e = _check_additional_data(path, z.directories['AdditionalData'],
                                          suffix, expected_psc1, date, expected)
            subject_ids.update(s)
            error_list.extend(e)
        else:
            error_list.append(Error(z.filename + 'AdditionalData/',
                                    'Folder "AdditionalData" is missing'))
        # ImageData
        if 'ImageData' in z.directories:
            s, e = _check_image_data(path, z.directories['ImageData'],
                                     suffix, psc1, date, expected)
            subject_ids.update(s)
            error_list.extend(e)
        else:
            error_list.append(Error(z.filename + 'ImageData/',
                                    'Folder "ImageData" is missing'))

    return subject_ids, error_list


def check_zip_content(path, timepoint=None, psc1=None, date=None, expected=None):
    """Rapid sanity check of a ZIP file containing imaging data for a subject.

    Expected sequences and tests are described as a dict:

    {
        SEQUENCE_T2: 'Good',
        SEQUENCE_T2_FLAIR: 'Good',
        SEQUENCE_ADNI_MPRAGE: 'Good',
        SEQUENCE_MID: 'Doubtful',
        SEQUENCE_FT: 'Bad',
        SEQUENCE_SST: 'Good',
        SEQUENCE_B0_MAP, 'Good',
        SEQUENCE_DTI, 'Good',
        SEQUENCE_RESTING_STATE, 'Missing',
    }

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
    expected : dict, optional
        Which MRI sequences and tests to expect.

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
    basename = os.path.basename(path)
    # is the file empty?
    if os.path.getsize(path) == 0:
        error_list = [Error(basename, 'ZIP file is empty')]
        return (set(), error_list)
    else:
        # read the ZIP file into a tree structure
        try:
            ziptree = ZipTree.create(path)
        except BadZipFile as e:
            error_list = [Error(basename, 'Cannot read ZIP file: {0}'.format(e))]
            return (set(), error_list)
        # check tree structure
        return _check_ziptree(path, ziptree, timepoint, psc1, date, expected)
