# -*- coding: utf-8 -*-

import os
import re
import datetime
import xlrd

import logging
logger = logging.getLogger(__name__)

__all___ = ['LONDON', 'NOTTINGHAM', 'DUBLIN', 'BERLIN',
            'HAMBURG', 'MANNHEIM', 'PARIS', 'DRESDEN',
            'CENTER_NAME',
            'PSC2_FROM_PSC1', 'PSC2_FROM_DAWBA',
            'PSC1_FROM_PSC2', 'DOB_FROM_PSC2',
            'detect_psc1', 'detect_psc2', 'guess_psc1',
            'Error']


#
# numerical ID of all 8 acquisition centers of Imagen
#
LONDON = 1
NOTTINGHAM = 2
DUBLIN = 3
BERLIN = 4
HAMBURG = 5
MANNHEIM = 6
PARIS = 7
DRESDEN = 8

#
# from numerical ID to standard name of all 8 acquisition centers of Imagen
#
CENTER_NAME = {
    LONDON: 'LONDON',
    NOTTINGHAM: 'NOTTINGHAM',
    DUBLIN: 'DUBLIN',
    BERLIN: 'BERLIN',
    HAMBURG: 'HAMBURG',
    MANNHEIM: 'MANNHEIM',
    PARIS: 'PARIS',
    DRESDEN: 'DRESDEN',
}

#
# file that maps PSC1 and DAWBA codes to PSC2 codes
#
_PSC2PSC = '/neurospin/imagen/src/scripts/psc_tools/psc2psc.csv'

#
# file that maps PSC2 codes to date of birth
#
_DOB = '/neurospin/imagen/src/scripts/psc_tools/DOB.csv'


def _initialize_psc1_dawba_psc2():
    """Returns dictionnaries to map PSC1 and DAWBA codes to PSC2.

    Parameters
    ----------
    path  : unicode
        File containing PSC1=DAWBA=PSC2 mappings.

    Returns
    -------
    tuple
        Pair of PSC1→PSC2 and DAWBA→PSC2 dictionnaries.

    """
    psc2_from_psc1 = {}
    psc2_from_dawba = {}
    with open(_PSC2PSC, 'rU') as f:
        for line in f:
            psc1, dawba, psc2 = line.strip('\n').split('=')
            # 1st line is: PSC1=DAWBA=PSC2
            if psc1 == 'PSC1' and dawba == 'DAWBA' and psc2 == 'PSC2':
                continue
            if psc2 in psc2_from_psc1:
                if psc2_from_psc1[psc1] != psc2:
                    logger.critical('inconsistent PSC1/PSC2 mapping: %s', _PSC2PSC)
                    raise Exception('inconsistent PSC1/PSC2 mapping')
            else:
                psc2_from_psc1[psc1] = psc2
            psc2_from_dawba[dawba] = psc2
    return psc2_from_psc1, psc2_from_dawba


_REGEX_DOB = re.compile(r'(\d{1,2})[./](\d{1,2})[./](\d{2,4})')


def _initialize_dob():
    """Returns dictionnary to map PSC1 code to date of birth.

    Parameters
    ----------
    path  : unicode
        DOB.csv file left over by initial Imagen team.

    Returns
    -------
    dict
        Dictionnary map PSC1 code to date of birth.

    """
    dob_from_psc2 = {}
    with open(_DOB, 'rU') as f:
        for line in f:
            psc2, dob, when = line.strip('\n').split('=')
            match = _REGEX_DOB.match(dob)
            if match:
                day = int(match.group(1))
                month = int(match.group(2))
                year = int(match.group(3))
                if year < 100:
                    year += 1900
                if year > 2012 or year < 1900:
                    raise Exception('unexpected date of birth: {0}'.format(dob))
                dob_from_psc2[psc2] = datetime.date(year, month, day)
            else:
                raise Exception('unexpected line in DOB.csv: {0}'.format(line))
    return dob_from_psc2


PSC2_FROM_PSC1, PSC2_FROM_DAWBA = _initialize_psc1_dawba_psc2()
PSC1_FROM_PSC2 = {v: k for k, v in PSC2_FROM_PSC1.items()}
DOB_FROM_PSC2 = _initialize_dob()


#
# the heuristic to detect a PSC1 code is that:
# - it starts with 0 followed by the digit associated to each center
# - it is a series of 12 digits
#
_PSC1_REGEX = re.compile('(0[' +
                         ''.join([str(c) for c in CENTER_NAME]) +
                         ']\d{10})[^d]?')


def detect_psc1(string):
    """Find potential PSC1 codes in a filename.

    PSC1 codes are sequences of 12 digits starting with 0 followed by a
    different digit for each center, followed by 10 digits.

    Parameters
    ----------
    filename : str
        The string to search for PSC1.

    Returns
    -------
    str
        Potential PSC1 code or None.

    """
    match = _PSC1_REGEX.search(string)
    if match:
        return match.group(1)
    else:
        return None


#
# the heuristic to detect a PSC2 code is that:
# - it starts with 0 followed by a different digit for each center
# - it a series of 12 digits
#
_PSC2_REGEX = re.compile('(0\d{11})[^d]?')


def detect_psc2(string):
    """Find potential PSC2 codes in a filename.

    PSC2 codes are sequences of 12 digits starting with 0.

    Parameters
    ----------
    filename : str
        The string to search for PSC2.

    Returns
    -------
    str
        Potential PSC2 code or None.

    """
    match = _PSC2_REGEX.search(string)
    if match:
        return match.group(1)
    else:
        return None


def guess_psc1(subject_id, center):
    subject_id = subject_id.split('_')[0]
    if subject_id.upper().startswith('FU2'):
        subject_id = subject_id[3:]
    if subject_id.upper().endswith('FU3'):
        subject_id = subject_id[:-3]
    elif subject_id.upper().endswith('FU2'):
        subject_id = subject_id[:-3]
    elif subject_id.upper().endswith('FU'):
        subject_id = subject_id[:-2]
    # this is very empirical and based on cases seen so far!
    if len(subject_id) < 10:
        subject_id = '0' + str(center) + subject_id.rjust(10, '0')
    elif len(subject_id) < 11:
        if len(subject_id) < 10:
            subject_id = subject_id.rjust(10, '0')
        subject_id = '0' + str(center) + subject_id
    elif len(subject_id) < 12:
        subject_id = subject_id[0:2] + '0' + subject_id[2:]
    # check this is an existing PSC1 code
    if subject_id in PSC2_FROM_PSC1:
        return subject_id
    return None


class Error:
    """Error while parsing files.

    Returned by functions that parse Cantab and behavioral files.

    Attributes
    ----------
    path : str
        File name.
    message : str
        Message explaining the error.
    sample : str
        Part of the file that generated the error.

    """
    _SAMPLE_LEN = 30

    def __init__(self, path, message, sample=None):
        self.path = path
        self.message = message
        self.sample = sample

    def __str__(self):
        if self.path:
            if self.sample:
                sample = repr(self.sample)
                if len(sample) > self._SAMPLE_LEN:
                    sample = sample[:self._SAMPLE_LEN] + '...'
                return '{0}: <{1}>: {2}'.format(self.message, sample, self.path)
            else:
                return '{0}: {1}'.format(self.message, self.path)
        else:
            return '{0}'.format(self.message)
