# -*- coding: utf-8 -*-

from zipfile import ZipFile
from lxml import etree
import datetime
import csv
import re
import sys

if sys.version_info < (3, 0):
    from codecs import open

from .core import detect_psc1

import logging
logger = logging.getLogger(__name__)

__all___ = ['CANTAB_CCLAR', 'DETAILED_DATASHEET_CSV', 'DATASHEET_CSV',
            'REPORT_HTML',
            'read_cant', 'read_datasheet', 'read_detailed_datasheet',
            'read_report']


#
# types of files we expect to be find under AdditionalData
#
CANTAB_CCLAR = 'cantab'
DETAILED_DATASHEET_CSV = 'detailed_datasheet'
DATASHEET_CSV = 'datasheet'
REPORT_HTML = 'report'

_ID_XPATH = ".//{http://www.camcog.com/proteus/entity/xml}attribute[@name='ID']"


def read_cant(path):
    """Return "Subject ID" values found in a cant_*.cclar file.

    Parameters
    ----------
    path : unicode
        Path to the cant_*.cclar file to read from.

    Returns
    -------
    list
        "Subject ID" values found in the file.

    """
    subject_ids = set()
    cantfile = ZipFile(path, 'r')
    for name in cantfile.namelist():
        if name.endswith('index.xml'):
            root = etree.fromstring(cantfile.read(name))
            for element in root.findall(_ID_XPATH):
                subject_ids.add(element.attrib['value'])
    cantfile.close()
    return subject_ids


def _parse_csv_datetime(date_string):
        """Read date in the format found in CSV files.

        * LONDON      01-Feb-2015 12:34:56
        * NOTTINGHAM  01-Feb-2015 12:34:56   01/02/2015 12:34
        * DUBLIN      01-Feb-2015 12:34:56
        * BERLIN      01.02.2015 12:34:56
        * HAMBURG     01.02.2015 12:34:56
        * MANNHEIM    01.02.2015 12:34:56
        * PARIS       01 Feb 2015 12:34:56
        * DRESDEN     12:34:56 01.02.2015

        """
        DATE_FORMATS = (
            '%d-%b-%Y %H:%M:%S',  # 01-Feb-2015 12:34:56
            '%d/%m/%Y %H:%M',     # 01/02/2015 12:34
            '%d.%m.%Y %H:%M:%S',  # 01.02.2015 12:34:56
            '%d %b %Y %H:%M:%S',  # 01 Feb 2015 12:34:56
            '%H:%M:%S %d.%m.%Y',  # 12:34:56 01.02.2015
        )
        for date_format in DATE_FORMATS:
            try:
                dt = datetime.datetime.strptime(date_string, date_format)
                return dt
            except ValueError:
                pass
        return None


def read_datasheet(path):
    """Return "Subject ID" and other information extracted from datasheet_*.csv.

    Parameters
    ----------
    path : unicode
        Path to the datasheet_*.csv file to read from.

    Returns
    -------
    list
        * "Subject ID" values found in the file.
        * "Session start time" values found in the file.
        * number of rows.
        * minimal number of columns.
        * list of column titles.

    """
    with open(path) as csvfile:
        # read header
        dialect = csv.Sniffer().sniff(csvfile.read())
        csvfile.seek(0)
        reader = csv.reader(csvfile, dialect)
        rows = 0
        columns_max = columns_min = 0
        fields = {}
        header = next(reader)
        if header:
            fields = {v: i for i, v in enumerate(header)}
            columns_max = columns_min = len(header)
            rows += 1
        subject_ids = set()
        session_start_times = set()
        # read values from the rest of the table
        for row in reader:
            if len(row) > 0:
                if "Subject ID" in fields:
                    subject_id = row[fields["Subject ID"]]
                else:
                    subject_id = row[0]
                subject_ids.add(subject_id)
            if "Session start time" in fields:
                session_start_time = _parse_csv_datetime(row[fields["Session start time"]])
                if session_start_time is not None:
                    if session_start_time < datetime.datetime(2007, 1, 1):
                        logger.warning('"Session start time" for {0} anterior to 2007: {1}'
                                       .format(subject_id, session_start_time.date()))
                    session_start_times.add(session_start_time)
            columns_min = min(len(row), columns_min)
            columns_max = max(len(row), columns_max)
            rows += 1
        return (subject_ids, session_start_times, rows, columns_min, fields)


#
# match lines with "Subject ID"
#
_DETAILED_DATASHEET_REGEX = re.compile(r'"?Subject ID : (\w*)"?')


def read_detailed_datasheet(path):
    """Return "Subject ID" values found in a detailed_datasheet_*.csv file.

    Parameters
    ----------
    path : unicode
        Path to the detailed_datasheet_*.csv file to read from.

    Returns
    -------
    list
        "Subject ID" values found in the file.

    """
    with open(path, encoding='latin1') as f:
        subject_ids = set()
        for line in f:
            match = _DETAILED_DATASHEET_REGEX.match(line)
            if match:
                subject_ids.add(match.group(1))
        return subject_ids


_REPORT_REGEX = re.compile('<th>Subject ID</th><td>(.*)</td><th>Gender</th><td>(.*)</td>')


def read_report(path):
    """Return "Subject ID" values found in a report_*.html file.

    Parameters
    ----------
    path : unicode
        Path to the report_*.html to read from.

    Returns
    -------
    list
        "Subject ID" values found in the file.

    """
    with open(path, encoding='latin-1') as report_html:
        subject_ids = set()
        for line in report_html:
            match = _REPORT_REGEX.match(line)
            if match:
                subject_ids.add(match.group(1))
        return subject_ids
