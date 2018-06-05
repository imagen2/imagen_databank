#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import zipfile
from datetime import datetime
from tempfile import TemporaryDirectory
from multiprocessing import Pool
from imagen_databank import PSC2_FROM_PSC1, DOB_FROM_PSC1
import logging

logging.basicConfig(level=logging.WARNING)

WORKER_PROCESSES = 8

FU3_DATASETS = '/neurospin/imagen/FU3/RAW/QUARANTINE'
BEHAVIOURAL = '/neurospin/imagen/FU3/RAW/PSC2/onsets'


def _parse_onsets_datetime(date_string):
    """Read date in the format found in CSV files.

    """
    DATE_FORMATS = (
        '%d.%m.%Y %H:%M:%S',
        '%d/%m/%Y %H:%M:%S',
    )
    for date_format in DATE_FORMATS:
        try:
            dt = datetime.strptime(date_string, date_format)
            return dt
        except ValueError:
            pass
    return None


def _extract_psc1_timestamp_FU3(path):
    """Extract time stamp from FU3 zip files in QUARANTINE.

    Parameters
    ----------
    path : unicode
        Zip file name.

    Returns
    -------
    tuple (str, int)
        PSC1 code and database increment number from tarball file name.

    """
    path = os.path.basename(path)
    root, ext = os.path.splitext(path)

    # extract database increment number and PSC1
    increment, data, psc1 = root.split('_', 2)
    assert(increment.isdigit())
    increment = int(increment)
    while not psc1[:12].isdigit():
        split = psc1.split('_', 1)
        if len(split) > 1:
            psc1 = split[-1]
        else:
            psc1 = None
            break
    else:
        psc1 = psc1[:12]
    
    return psc1, increment


def process_behavioural(path, prefix, psc1, psc2):
    logging.info('%s: processing behavioural file...', path)

    with open(path, encoding='latin-1', newline='') as content:
        output = os.path.join(BEHAVIOURAL, prefix + '_' + psc2 + 'FU3.csv')
        with open(output, 'w') as output:
            # de-identify 1st line
            line = next(iter(content))
            column = line.split('\t')
            column[1] = str((_parse_onsets_datetime(column[1]).date() -
                             DOB_FROM_PSC1[psc1]).days)
            column[2] = column[2].replace(psc1, psc2)
            line = '\t'.join(column)
            # write to target file
            output.write(line)
            for line in content:
                output.write(line)


def process_dataset_FU3(arguments):
    (psc1, psc2, dataset_path) = arguments  # unpack multiple arguments

    logging.info('%s: processing zipped FU3 dataset...', psc1)

    with TemporaryDirectory(prefix='imagen_behavioural_') as tmp:
        with zipfile.ZipFile(dataset_path) as dataset_zipfile:
            members = dataset_zipfile.infolist()
    
            for prefix in ('ft', 'mid', 'recog', 'ss'):
                for member in members:
                    if member.filename == (psc1 + 'FU3/AdditionalData/Scanning/' +
                                           prefix + '_' + psc1 + 'FU3.csv'):
                        dataset_zipfile.extract(member, path=tmp)
                        behavioural_path = os.path.join(tmp, member.filename)
                        process_behavioural(behavioural_path, prefix, psc1, psc2)
                        break
                else:
                    logging.warning('%s: missing %s_*.csv file', psc1, prefix)

    logging.info('%s: processed zipped FU3 dataset', psc1)


def list_datasets_FU3(path):
    # list zip files to process
    # for subjects with multiple zip files, keep the most recent one
    datasets = {}
    for dataset in os.listdir(path):
        root, ext = os.path.splitext(dataset)
        if ext != '.zip':
            continue
        increment, data, psc1 = root.split('_', 2)
        assert(increment.isdigit() and data == 'data' and
               psc1[:12].isdigit())
        if psc1[12:15] != 'FU3':
            continue

        psc1, timestamp = _extract_psc1_timestamp_FU3(dataset)
        dataset_path = os.path.join(path, dataset)
        datasets.setdefault(psc1, {})[timestamp] = dataset_path

    logging.info('found %d zipped FU3 datasets', len(datasets))

    return[(psc1, timestamps[max(timestamps.keys())])  # keep latest dataset
           for (psc1, timestamps) in datasets.items()]


def dataset_FU3(path):
    todo_list = list(list_datasets_FU3(path))
    todo_list = [(psc1, PSC2_FROM_PSC1[psc1], path) for (psc1, path) in todo_list]
    
    pool = Pool(WORKER_PROCESSES)
    results = pool.map(process_dataset_FU3, todo_list)
    pool.close()
    pool.join()


def main():
    results = dataset_FU3(FU3_DATASETS)


if __name__ == "__main__":
    main()
