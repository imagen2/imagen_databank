# -*- coding: utf-8 -*-

import csv
from datetime import datetime

from .core import detect_psc1
from .core import PSC2_FROM_PSC1
from .core import Error

import logging
logger = logging.getLogger(__name__)

__all__ = ['MID_COLUMNS', 'FT_COLUMNS', 'SS_COLUMNS', 'RECOG_COLUMNS',
           'read_mid', 'read_ft_', 'read_ss', 'read_recog']

#
# types of files we expect to be find under AdditionalData/Scanning
#
FT_CSV = 'ft'
MID_CSV = 'mid'
SS_CSV = 'ss'
RECOG_CSV = 'recog'


def _parse_behavioral_datetime(date_string):
        """Read date in the format found in CSV files.

        * LONDON      01/02/2015 01:02:03
        * NOTTINGHAM  01/02/2015 01:02:03
        * DUBLIN      01/02/2015 01:02:03  2/1/2015 1:02:03 AM
        * BERLIN      01.02.2015 01:02:03
        * HAMBURG     01.02.2015 01:02:03
        * MANNHEIM    01.02.2015 01:02:03
        * PARIS       01/02/2015 01:02:03
        * DRESDEN     01.02.2015 01:02:03

        """
        DATE_FORMATS = (
            '%d.%m.%Y %H:%M:%S',
            '%d/%m/%Y %H:%M:%S',
            '%m/%d/%Y %I:%M:%S %p',
        )
        for date_format in DATE_FORMATS:
            try:
                dt = datetime.strptime(date_string, date_format)
                return dt
            except ValueError:
                pass
        return None


def _fix_spurious_quotes(s):
    if s.startswith('"'):
        last = s.rfind('"')
        if last > 0:
            main = s[1:last]
            last += 1
            tail = s[last:]
            if tail.isspace():
                s = main + tail
    return s


def _fix_terminal_tab(s):
    last = s.rfind('\t')
    if last > 0:
        main = s[:last]
        last += 1
        tail = s[last:]
        if tail.isspace():
            s = main + tail
    return s


MID_COLUMNS = (
    'Trial',
    'Trial Category',
    'Trial Start Time (Onset)',
    'Pre-determined Onset',
    'Cue Presented',
    'Anticipation Phase Start Time',
    'Anticipation Phase Duration',
    'Target Phase Start Time',
    'Target Phase Duration',
    'Response Made by Subject',
    'Response time',
    'Feedback Phase Start Time',
    'Outcome',
    'Amount',
    'Fixation Phase Start Time (Lasts until next trial start time)',
    'Success Rate',
    'Scanner Pulse',
)

FT_COLUMNS = (
    'Trial Start Time (Onset)',
    'Video Clip Name',
)

SS_COLUMNS = (
    'Trial',
    'Trial Category',
    'Trial Start Time (Onset)',
    'Pre-determined/randomised onset',
    'Go Stimulus Presentation Time',  # 'Go Stimulus Presentation Time '
    'Stimulus Presented',
    'Delay',
    'Stop Stimulus Presentation Time',
    'Response made by subject',
    'Absolute Response Time',
    'Relative Response Time',
    'Response Outcome',
    'Real Jitter',
    'Pre-determined Jitter',
    'Success Rate of Variable Delay Stop Trials',
    'Scanner Pulse',
)

RECOG_COLUMNS = (
    'TimePassed',
    'UserResponse',
    'ImageFileName',
)

# for each of the 4 tasks we provide a tuple:
# * first word in the behavioral file that identifies the task
# * list of columns in the 2nd line
# * column from which to extract the last ascending numerical sequence
# * True if the numerical sequence is strictly ascending
_TASK_SPECIFICS = {
    MID_CSV: ('MID_TASK', MID_COLUMNS, 0, True),
    FT_CSV: ('FACE_TASK', FT_COLUMNS, 0, True),
    SS_CSV: ('STOP_SIGNAL_TASK', SS_COLUMNS, 0, False),
    RECOG_CSV: ('RECOGNITION_TASK', RECOG_COLUMNS, 0, True),
}


def _read_generic_behavioral(path, task, strict=True):
    """Read behavioral files and return part of the contents and errors.

    Sometimes complete lines are enclosed in quotes. Such quotes
    must be fixed before the contents can be read as CSV.

    Parameters
    ----------
    path : str
        Path to the behavioral file to read from.

    task : ?
        Type of task.

    strict : bool
        Be more lenient and let wholly quoted lines through if False,
        else do report the error.

    Returns
    -------
    psc1 : str
        PSC1 code.
    timestamp : datetime
        Time stamp extracted from the header.
    trials : array_like
        Last ascending sequence of trials.
    errors : array_like
        List of Error.

    Raises
    ------
    FileNotFoundError
        If path does not exist.

    """
    psc1 = None
    timestamp = None
    sequence = []
    errors = []

    with open(path, 'r') as behavioral:  # add newline='' in Python 3
        lines = behavioral.readlines()

    # attempt to handle broken CSV files with fully quoted lines
    reader = csv.reader(lines, delimiter='\t')
    if not strict and max(len(row) for row in reader) < 2:
        lines = [_fix_spurious_quotes(line) for line in lines]

    # remove spurious terminal tab
    lines = [_fix_terminal_tab(line) for line in lines]

    # now re-read file contents
    reader = csv.reader(lines, delimiter='\t')

    # 1st line
    header = next(reader)
    if header:
        header = [x.strip() for x in header]
        if len(header) != 4:
            errors.append(Error(path, 'Line 1 contains {0} columns instead of 4'
                                      .format(len(header)), header))
        if len(header) > 3:
            COLUMN = 'Task type: Scanning'
            if header[3] != COLUMN:
                    errors.append(Error(path, 'Column 4 of line 1 must be "{0}" '
                                              'instead of "{1}"'
                                              .format(COLUMN, header[3]), header))
        if len(header) > 2:
            COLUMN = 'Subject ID:'
            if header[2].startswith(COLUMN):
                psc1 = header[2][len(COLUMN):].lstrip()
            else:
                errors.append(Error(path, 'Column 3 of line 1 "{0}" must start '
                                          'with "{1}"'
                                          .format(header[2], COLUMN), header))
        if len(header) > 1:
            timestamp = _parse_behavioral_datetime(header[1])
            if not timestamp:
                errors.append(Error(path, 'Column 2 of line 1 "{0}" is not a standard time stamp'
                                          .format(header[1]), header))
        if len(header) > 0:
            COLUMN = '{0} task'.format(_TASK_SPECIFICS[task][0])
            if header[0] != COLUMN:
                errors.append(Error(path, 'Column 1 of line 1 must be "{0}" '
                                          'instead of "{1}"'
                                          .format(COLUMN, header[0]), header))
    else:
        errors.append(Error(path, 'Empty file'))

    # 2nd line
    try:
        header = next(reader)
        header = [x.strip() for x in header]
        COLUMNS = _TASK_SPECIFICS[task][1]
        if len(header) != len(COLUMNS):
            errors.append(Error(path, 'Line 2 contains {0} columns instead of {1}'
                                      .format(len(header), len(COLUMNS)),
                                      header))
        for i, (h, c) in enumerate(zip(header, COLUMNS)):
            if h != c:
                errors.append(Error(path, 'Column {0} of line 2 must be {1} instead of {2}'
                                          .format(i + 1, c, h), header))
                break
    except StopIteration:
        errors.append(Error(path, 'Missing 2nd line'))

    # data
    last = None
    for n, row in enumerate(reader, 3):
        row = [x.strip() for x in row]
        COLUMNS = _TASK_SPECIFICS[task][1]
        if not any(row):  # get rid of empty rows
            continue
        elif (len(row) != len(COLUMNS)):
            errors.append(Error(path, 'Line {0} contains {1} columns instead of {2}'
                                      .format(n, len(row), len(COLUMNS)),
                                      row))
        # column to check for ascending numerical sequence
        current = row[_TASK_SPECIFICS[task][2]].strip()
        try:
            # expect ascending numerical sequences
            current = int(current)
            if last:
                if _TASK_SPECIFICS[task][3]:  # strictly ascending
                    if current <= last:
                        sequence = []  # start new ascending sequence
                else:
                    if current < last:
                        sequence = []  # start new ascending sequence
            sequence.append(current)
            last = current
        except ValueError as e:
            errors.append(Error(path, 'Column {0} of line {1} "{2}" should contain '
                                      'only numbers'
                                      .format(_TASK_SPECIFICS[task][2] + 1, n, current), row))
            if last:
                last = None

    return psc1, timestamp, sequence, errors


def read_mid(path, strict=True):
    """Return "Subject ID" and other information extracted from mid_*.csv.

    Sometimes complete lines are enclosed in quotes. In that case
    mid_*.csv content must be fixed before it can be read as CSV.

    Parameters
    ----------
    path : unicode
        Path to the mid_*.csv file to read from.

    strict : bool
        Be more lenient and let wholly quoted lines through if False,
        else do report the error.

    Returns
    -------
    psc1 : str
        PSC1 code.
    timestamp : datetime
        Time stamp extracted from the header.
    trials : array_like
        The last ascending sequence of trials ('Trials' column).
    errors : array_like
        List of Error.

    Raises
    ------
    FileNotFoundError
        If path does not exist.

    """
    return _read_generic_behavioral(path, MID_CSV, strict)


def read_ft(path, strict=True):
    """Return "Subject ID" and other information extracted from ft_*.csv.

    Sometimes complete lines are enclosed in quotes. In that case
    ft_*.csv content must be fixed before it can be read as CSV.

    Parameters
    ----------
    path : unicode
        Path to the ft_*.csv file to read from.

    strict : bool
        Be more lenient and let wholly quoted lines through if False,
        else do report the error.

    Returns
    -------
    psc1 : str
        PSC1 code.
    timestamp : datetime
        Time stamp extracted from the header.
    trials : array_like
        The last ascending sequence of trials ('Trials' column).
    errors : array_like
        List of Error.

    Raises
    ------
    FileNotFoundError
        If path does not exist.

    """
    return _read_generic_behavioral(path, FT_CSV, strict)


def read_ss(path, strict=True):
    """Return "Subject ID" and other information extracted from ss_*.csv.

    Sometimes complete lines are enclosed in quotes. In that case
    ss_*.csv content must be fixed before it can be read as CSV.

    Parameters
    ----------
    path : unicode
        Path to the ss_*.csv file to read from.

    strict : bool
        Be more lenient and let wholly quoted lines through if False,
        else do report the error.

    Returns
    -------
    psc1 : str
        PSC1 code.
    timestamp : datetime
        Time stamp extracted from the header.
    trials : array_like
        The last ascending sequence of trials ('Trials' column).
    errors : array_like
        List of Error.

    Raises
    ------
    FileNotFoundError
        If path does not exist.

    """
    return _read_generic_behavioral(path, SS_CSV, strict)


def read_recog(path, strict=True):
    """Return "Subject ID" and other information extracted from recog_*.csv.

    Sometimes complete lines are enclosed in quotes. In that case
    recog_*.csv content must be fixed before it can be read as CSV.

    Parameters
    ----------
    path : unicode
        Path to the recog_*.csv file to read from.

    strict : bool
        Be more lenient and let wholly quoted lines through if False,
        else do report the error.

    Returns
    -------
    psc1 : str
        PSC1 code.
    timestamp : datetime
        Time stamp extracted from the header.
    times : array_like
        The last ascending sequence of trials ('TimePassed' column).
    errors : array_like
        List of Error.

    Raises
    ------
    FileNotFoundError
        If path does not exist.

    """
    return _read_generic_behavioral(path, RECOG_CSV, strict)


def main():
    import os.path

    ROOT_DIR = '/neurospin/imagen/FU2/RAW/PSC1'
    for center in os.listdir(ROOT_DIR):
        center_path = os.path.join(ROOT_DIR, center)
        for subject in os.listdir(center_path):
            subject_path = os.path.join(center_path, subject)
            behavioral_path = os.path.join(subject_path,
                                           'AdditionalData', 'Scanning')
            if os.path.isdir(behavioral_path):
                #~ mid_files = tuple(os.path.join(behavioral_path, b)
                                 #~ for b in os.listdir(behavioral_path)
                                 #~ if 'mid_' in b)
                #~ for mid_file in mid_files:
                    #~ (psc1, timestamp, onsets, errors) = read_mid(mid_file, False)
                    #~ print('▸ {0} MID {1}'.format(psc1, len(onsets)))
                    #~ for error in errors:
                        #~ print('  ✗ {0}: {1}'.format(error.message,
                              #~ os.path.relpath(error.path, ROOT_DIR)))
                #~ ft_files = tuple(os.path.join(behavioral_path, b)
                                 #~ for b in os.listdir(behavioral_path)
                                 #~ if 'ft_' in b)
                #~ for ft_file in ft_files:
                    #~ (psc1, timestamp, onsets, errors) = read_ft(ft_file, False)
                    #~ print('▸ {0} FT {1}'.format(psc1, len(onsets)))
                    #~ for error in errors:
                        #~ print('  ✗ {0}: {1}'.format(error.message,
                              #~ os.path.relpath(error.path, ROOT_DIR)))
                ss_files = tuple(os.path.join(behavioral_path, b)
                                 for b in os.listdir(behavioral_path)
                                 if 'ss_' in b)
                for ss_file in ss_files:
                    (psc1, timestamp, onsets, errors) = read_ss(ss_file, False)
                    print('▸ {0} SS {1}'.format(psc1, len(onsets)))
                    for error in errors:
                        print('  ✗ {0}: {1}'.format(error.message,
                              os.path.relpath(error.path, ROOT_DIR)))
                #~ recog_files = tuple(os.path.join(behavioral_path, b)
                                 #~ for b in os.listdir(behavioral_path)
                                 #~ if 'recog_' in b)
                #~ for recog_file in recog_files:
                    #~ (psc1, timestamp, onsets, errors) = read_recog(recog_file, False)
                    #~ print('▸ {0} RECOG {1}'.format(psc1, len(onsets)))
                    #~ for error in errors:
                        #~ print('  ✗ {0}: {1}'.format(error.message,
                              #~ os.path.relpath(error.path, ROOT_DIR)))


if __name__ == '__main__':
    main()
