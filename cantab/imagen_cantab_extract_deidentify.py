#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import csv
import tarfile
import zipfile
import locale
from contextlib import contextmanager
from datetime import datetime
from tempfile import TemporaryDirectory
from collections import OrderedDict
from multiprocessing import Pool
from imagen_databank import PSC2_FROM_PSC1, DOB_FROM_PSC1
import logging

logging.basicConfig(level=logging.WARNING)

WORKER_PROCESSES = 8

BL_DATASETS = '/neurospin/imagen/BL/RAW/PSC1'
FU2_DATASETS = '/neurospin/imagen/FU2/RAW/PSC1'
FU2_NIFTI_DATASETS = '/neurospin/imagen/FU2/RAW/PSC1_NIFTI'
FU3_CANTAB = '/neurospin/imagen/FU3/RAW/PSC1'
SB_CANTAB = '/neurospin/imagen/STRATIFY/RAW/PSC1'

FEMALE = 'F'
MALE = 'M'

_BROKEN_SEPARATOR = ';"'


def _fix_broken_mannheim_csv_hack(value):
    if _BROKEN_SEPARATOR in value and value.endswith('"'):
        value = value.replace(_BROKEN_SEPARATOR, '.')[:-1]
    return value


def _process_row(row):
    ret = OrderedDict()

    for k, v in row.items():
        if k and 'Warning' not in k:
            v = _fix_broken_mannheim_csv_hack(v)
            ret[k] = v

    return ret


def process_cantab(path):
    """Extract data from a single Cantab file.

    Parameters
    ----------
    path  : str
        Path to Cantab datasheet_*.csv file.

    Returns
    -------
    list of OrderedDict
        List of datasheet_*.csv rows. Unless ill-formed, datasheet_*.csv
        files contain a single row of data.

    """
    logging.info('%s: processing Cantab file...', path)

    with open(path, encoding='latin1', newline='') as csvfile:
        ret = []

        dialect = csv.Sniffer().sniff(csvfile.read())
        csvfile.seek(0)
        cantab = csv.DictReader(csvfile, dialect=dialect)

        rows = list(cantab)
        if len(rows) > 1:
            logging.error('%s: %d lines', path, len(rows))
        else:
            for i, row in enumerate(rows, 1):
                if len(row) < 3:
                    logging.error('%s: %d columns in line %d', path, len(row), i)
                    break
            else:
                for row in rows:
                    ret.append(_process_row(row))

    return ret


def _extract_psc1_timestamp_BL(path):
    """Extract time stamp from BL tarballs.

    Parameters
    ----------
    path : unicode
        Tarball file name.

    Returns
    -------
    tuple (str, datetime.datetime)
        PSC1 code and date/time extracted from tarball file name.

    """
    basename = os.path.basename(path)

    # extract PSC1
    psc1 = basename[:12]
    if len(psc1) < 12 or not psc1.isdigit():
        return None, None

    # extract time stamp
    try:
        timestamp = datetime.strptime(basename[13:32],
                                      '%Y-%m-%d_%H:%M:%S')
    except ValueError:
        try:  # wrong date format for 070000166420 and 070000174119
            timestamp = datetime.strptime(basename[13:32],
                                          '%Y-%d-%m_%H:%M:%S')
        except ValueError:
            timestamp = None
    if timestamp is None:
        logging.error('incorrect date/time: %s', basename[13:32])
    elif basename[32:34] != '.0':
        logging.error('unexpected index: %s', basename[32:34])

    return psc1, timestamp


def process_dataset_BL(arguments):
    (psc1, path) = arguments  # unpack multiple arguments

    logging.info('%s: processing compressed dataset...', psc1)

    with TemporaryDirectory(prefix='imagen_cantab_') as tmp:
        with tarfile.open(path) as tar:
            members = tar.getmembers()
            for member in members:
                if member.name.endswith('BehaviouralData/datasheet_' + psc1 + '.csv'):
                    tar.extract(member, path=tmp)
                    datasheet_path = os.path.join(tmp, member.name)
                    return process_cantab(datasheet_path)
            else:
                logging.warning('%s: missing Cantab file', psc1)
                return None


def process_dataset_FU2(arguments):
    (psc1, path) = arguments  # unpack multiple arguments

    logging.info('%s: processing dataset...', psc1)

    additional_data_path = os.path.join(path, 'AdditionalData')
    if os.path.isdir(additional_data_path):
        for f in os.listdir(additional_data_path):
            if 'datasheet' in f and 'detailed' not in f:
                datasheet_path = os.path.join(additional_data_path, f)
                return process_cantab(datasheet_path)
        else:
            logging.warning('%s: missing Cantab file', psc1)
            return None
    else:
        logging.warning('%s: missing Cantab file', psc1)
        return None


def process_cantab_FU3_SB(arguments):
    (psc1, path) = arguments  # unpack multiple arguments

    logging.info('%s: processing Cantab file...', psc1)

    return process_cantab(path)


def list_datasets_BL(path):
    # list tarballs to process
    # for subjects with multiple tarballs, keep the most recent one
    datasets = {}
    for center in ('LONDON', 'NOTTINGHAM', 'DUBLIN', 'BERLIN',
                   'HAMBURG', 'MANNHEIM', 'PARIS', 'DRESDEN'):
        center_path = os.path.join(path, center)
        for dataset in os.listdir(center_path):
            psc1, timestamp = _extract_psc1_timestamp_BL(dataset)

            if psc1 and timestamp:
                dataset_path = os.path.join(center_path, dataset)
                datasets.setdefault(psc1, {})[timestamp] = dataset_path
            else:
                logging.error('%d: skipping: %s', psc1, dataset)

    logging.info('found %d compressed BL datasets', len(datasets))

    return[(psc1, timestamps[max(timestamps.keys())])  # keep latest dataset
           for (psc1, timestamps) in datasets.items()]


def list_datasets_FU2(path):
    # list directories to process
    datasets = []
    for center in ('LONDON', 'NOTTINGHAM', 'DUBLIN', 'BERLIN',
                   'HAMBURG', 'MANNHEIM', 'PARIS', 'DRESDEN'):
        center_path = os.path.join(path, center)
        if os.path.isdir(center_path):  # NIFTI datasets
            for dataset in os.listdir(center_path):
                psc1 = dataset[:12]
                dataset_path = os.path.join(center_path, dataset)
                datasets.append((psc1, dataset_path))

    logging.info('found %d FU2 datasets', len(datasets))

    return datasets


def list_cantab_FU3_SB(path):
    # list Cantab files to process
    cantabs = []
    for center in ('LONDON', 'NOTTINGHAM', 'DUBLIN', 'BERLIN',
                   'HAMBURG', 'MANNHEIM', 'PARIS', 'DRESDEN',
                   'SOUTHAMPTON', 'AACHEN'):
        center_path = os.path.join(path, center)
        if not os.path.isdir(center_path):
            continue
        for psc1 in os.listdir(center_path):
            additional_data_path = os.path.join(center_path, psc1,
                                                'AdditionalData')
            if not os.path.isdir(additional_data_path):  # skip ZIP imaging datasets
                continue
            for f in os.listdir(additional_data_path):
                if 'datasheet' in f and 'detailed' not in f:
                    cantab_path = os.path.join(additional_data_path, f)
                    cantabs.append((psc1, cantab_path))

    logging.info('found %d FU3 Cantab files', len(cantabs))

    return cantabs


def dataset_BL(path):
    todo_list = list(list_datasets_BL(path))

    pool = Pool(WORKER_PROCESSES)
    results = pool.map(process_dataset_BL, todo_list)
    pool.close()
    pool.join()

    psc1, path = zip(*todo_list)
    return dict(zip(psc1, results))


def dataset_FU2(path):
    todo_list = list(list_datasets_FU2(path))

    pool = Pool(WORKER_PROCESSES)
    results = pool.map(process_dataset_FU2, todo_list)
    pool.close()
    pool.join()

    psc1, path = zip(*todo_list)
    return dict(zip(psc1, results))


def cantab_FU3_SB(path):
    todo_list = list(list_cantab_FU3_SB(path))

    pool = Pool(WORKER_PROCESSES)
    results = pool.map(process_cantab_FU3_SB, todo_list)
    pool.close()
    pool.join()

    psc1, path = zip(*todo_list)
    return dict(zip(psc1, results))


_CANTAB_GENDER_MAPPING = {
    'Female': FEMALE,
    'Male': MALE,
}


@contextmanager
def setlocale(name):
    saved = locale.setlocale(locale.LC_ALL)
    try:
        if name is None:
            yield saved
        else:
            yield locale.setlocale(locale.LC_ALL, name)
    finally:
        if name is not None:
            locale.setlocale(locale.LC_ALL, saved)


def _parse_csv_datetime(date_string):
    """Read date in the format found in CSV files.

    * LONDON      01-Feb-2015 12:34:56   01-Feb-2015 12:34
    * NOTTINGHAM  01-Feb-2015 12:34:56   01/02/2015 12:34
    * DUBLIN      01-Feb-2015 12:34:56
    * BERLIN      01.02.2015 12:34:56
    * HAMBURG     01.02.2015 12:34:56
    * MANNHEIM    01.02.2015 12:34:56
    * PARIS       01 Feb 2015 12:34:56
    * DRESDEN     12:34:56 01.02.2015

    """
    DATE_FORMATS = (
        ('%d-%b-%Y %H:%M:%S', 'en_GB.UTF-8'),  # 01-Feb-2015 12:34:56
        ('%d-%b-%Y %H:%M', 'en_GB.UTF-8'),     # 01-Feb-2015 12:34
        ('%d %b %Y %H:%M:%S', 'en_US.UTF-8'),  # 01 Feb 2015 12:34:56
        ('%d/%m/%Y %H:%M', None),              # 01/02/2015 12:34
        ('%d.%m.%Y %H:%M:%S', None),           # 01.02.2015 12:34:56
        ('%H:%M:%S %d.%m.%Y', None),           # 12:34:56 01.02.2015
        ('%d.%m.%Y %H:%M', None),              # 01.02.2015 12:34
    )
    for date_format, locale_name in DATE_FORMATS:
        with setlocale(locale_name):
            try:
                return datetime.strptime(date_string, date_format)
            except ValueError:
                pass
    return None


def cleanup(timepoint, data, fields):
    result = {}

    for psc1 in data:
        cantab = data[psc1]

        # skip subjects datasets with missing Cantab file
        if cantab is None:
            logging.error('%s: %s: missing Cantab file', timepoint, psc1)
            continue

        # discard subjects with ill-formed multiline Cantab files
        if len(cantab) > 1:
            logging.error('%s: %s: multiple data rows in Cantab file', timepoint, psc1)
            continue
        elif len(cantab) < 1:
            logging.error('%s: %s: missing data in Cantab file', timepoint, psc1)
            continue
        else:
            cantab = next(iter(cantab))  # single line in Cantab file

        for field, name, required in fields:
            if required and field not in cantab:
                logging.error("%s: %s: missing required field '%s'",
                              timepoint, psc1, field)

        # remove time point after PSC1 pseudonym
        if cantab['Subject ID'][:12] != psc1:
            logging.error('%s: %s: incorrect PSC1 code in Cantab file (expected %s)',
                          timepoint, cantab['Subject ID'], psc1)
            continue
        else:
            cantab['Subject ID'] = psc1

        # standardize sex of subject: F/M
        if 'Gender' in cantab:
            gender = cantab['Gender']
            if gender in _CANTAB_GENDER_MAPPING:
                cantab['Gender'] = _CANTAB_GENDER_MAPPING[gender]
            elif gender != '':
                logging.error("%s: %s: invalid value for 'Gender': %s",
                              timepoint, psc1, gender)
                cantab['Gender'] = ''

        # standardize date/time format
        for field in ('Session start time', 'Test start time'):
            if field in cantab and cantab[field]:
                session_start_time = _parse_csv_datetime(cantab[field])
                if session_start_time is None:
                    cantab[field] = ''
                elif psc1 in DOB_FROM_PSC1:
                    dob = DOB_FROM_PSC1[psc1]
                    dob = datetime(dob.year, dob.month, dob.day)
                    cantab[field] = str((session_start_time - dob).days)
                else:
                    logging.error('%s: date of birth is missing', psc1)
                    cantab[field] = ''

        # standardize decimal separators and general cleanup
        for key in cantab:
            value = cantab[key]
            if value is None:
                pass  #FIXME: remove from dict?
            else:
                # remove quotes from quoted cell
                #FIXME: why doesn't the csv module handle these cells?
                value = value.strip('"')
                # decimal separator
                if set(value).issubset('-0123456789,'):  # continental-style
                    value = value.replace(',', '.')  # English-style
                cantab[key] = value

        result[psc1] = cantab

    return result


def write(path, data, fields):
    with open(path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_MINIMAL)

        fieldnames = (name if name is not None else field
                      for field, name, required in fields)
        writer.writerow(fieldnames)

        for psc2 in sorted(data):
            cantab = data[psc2]
            row = [psc2]
            for field, name, required in fields[1:]:  # skip 'Subject ID'
                row.append(cantab[field] if field in cantab else None)
            writer.writerow(row)


def main():
    # BL
    fields_BL = (
        # demographics
        ('Subject ID', 'PSC2', True),
        ('Age', None, True),
        ('NART', None, True),
        ('Gender', 'Sex', True),
        ('Session start time', None, True),
        # Pattern Recognition Memory (PRM)
        ('PRM Percent correct', None, False),
        ("RVP A'", None, True),
        # Spatial Working Memory (SWM)
        ('SWM Between errors', None, True),
        ('SWM Strategy', None, True),
        # Affective Go/No-go (AGN)
        ('AGN Mean correct latency (positive)', None, False),
        ('AGN Mean correct latency (negative)', None, False),
        ('AGN Mean correct latency (neutral)', None, False),
        ('AGN Total omissions (neutral)', None, False),
        ('AGN Total omissions (negative)', None, False),
        ('AGN Total omissions (positive)', None, False),
        ('AGN Affective response bias (Mean)', None, False),
        # Cambridge Guessing (Gambling) Task (CGT)
        ('CGT Delay aversion', None, False),
        ('CGT Deliberation time', None, False),
        ('CGT Overall proportion bet', None, False),
        ('CGT Quality of decision making', None, False),
        ('CGT Risk adjustment', None, False),
        ('CGT Risk taking', None, False),
        # ...
        ###('User name', None, False),
        ###('Subject notes', None, False),
        ###('Mode', None, False),
        ###('Test start time', None, False),
        ###('Test duration', None, False),
    )
    results = dataset_BL(BL_DATASETS)
    results = cleanup('BL', results, fields_BL)
    results = {PSC2_FROM_PSC1[psc1]: data
               for psc1, data in results.items()}
    write('CANTAB_BL_PSC2.csv', results, fields_BL)

    # FU2
    fields_FU2 = (
        # demographics
        ('Subject ID', 'PSC2', True),
        ('Age', None, True),
        ('NART', None, True),
        ('Gender', 'Sex', True),
        ('Session start time', None, True),
        # Affective Go/No-go (AGN)
        ('AGN Mean correct latency (positive)', None, True),
        ('AGN Mean correct latency (negative)', None, True),
        ('AGN Mean correct latency (neutral)', None, True),
        ('AGN Total omissions (neutral)', None, False),
        ('AGN Total omissions (negative)', None, False),
        ('AGN Total omissions (positive)', None, False),
        ('AGN Affective response bias (Mean)', None, False),
        # Cambridge Guessing (Gambling) Task (CGT)
        ('CGT Delay aversion', None, True),
        ('CGT Deliberation time', None, True),
        ('CGT Overall proportion bet', None, True),
        ('CGT Quality of decision making', None, True),
        ('CGT Risk adjustment', None, True),
        ('CGT Risk taking', None, True),
        # Pattern Recognition Memory (PRM)
        ('PRM Percent correct', None, False),
        # Rapid Visual Information Processing (RVP)
        ("RVP A'", None, False),
        # Spatial Working Memory (SWM)
        ('SWM Between errors', None, False),
        ('SWM Strategy', None, False),
    )
    results_DICOM = dataset_FU2(FU2_DATASETS)  # DICOM
    results_NIfTI = dataset_FU2(FU2_NIFTI_DATASETS) # NIfTI
    duplicates = results_DICOM.keys() & results_NIfTI.keys()
    for duplicate in duplicates:
        logging.warn('%s: duplicate DICOM and NIfTI data', duplicate)
    results = results_NIfTI.copy()
    results.update(results_DICOM)
    results = cleanup('FU2', results, fields_FU2)
    results = {PSC2_FROM_PSC1[psc1]: data
               for psc1, data in results.items()}
    write('CANTAB_FU2_PSC2.csv', results, fields_FU2)

    # FU3
    fields_FU3_SB = (
        # demographics
        ('Subject ID', 'PSC2', True),
        ('Age', None, True),
        ('NART', None, True),
        ('Gender', 'Sex', True),
        ('Session start time', None, True),
        # Cambridge Guessing (Gambling) Task (CGT)
        ('CGT Delay aversion', None, True),
        ('CGT Deliberation time', None, True),
        ('CGT Overall proportion bet', None, True),
        ('CGT Quality of decision making', None, True),
        ('CGT Risk adjustment', None, True),
        ('CGT Risk taking', None, True),
        # Intra-Extra Dimensional Set Shift (IED)
        ('IED Total trials', None, True),
        ('IED Total trials (adjusted)', None, True),
        ('IED Completed stage trials', None, True),
        ('IED Pre-ED errors', None, True),
        ('IED EDS errors', None, True),
        ('IED Total errors', None, True),
        ('IED Total errors (adjusted)', None, True),
        ('IED Completed stage errors', None, True),
        ('IED Errors (block 1)', None, True),
        ('IED Errors (block 2)', None, True),
        ('IED Errors (block 3)', None, True),
        ('IED Errors (block 4)', None, True),
        ('IED Errors (block 5)', None, True),
        ('IED Errors (block 6)', None, True),
        ('IED Errors (block 7)', None, True),
        ('IED Errors (block 8)', None, True),
        ('IED Errors (block 9)', None, True),
        ('IED Stages completed', None, True),
        # Spatial Working Memory (SWM)
        ('SWM Between errors', None, True),
        ('SWM Strategy', None, True),
    )
    results = cantab_FU3_SB(FU3_CANTAB)
    results = cleanup('FU3', results, fields_FU3_SB)
    results = {PSC2_FROM_PSC1[psc1]: data
               for psc1, data in results.items()}
    write('CANTAB_FU3_PSC2.csv', results, fields_FU3_SB)

    # Stratify
    results = cantab_FU3_SB(SB_CANTAB)
    results = cleanup('SB', results, fields_FU3_SB)
    results = {PSC2_FROM_PSC1[psc1]: data
               for psc1, data in results.items()}
    write('CANTAB_SB_PSC2.csv', results, fields_FU3_SB)


if __name__ == "__main__":
    main()
