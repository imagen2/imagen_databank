#!/usr/bin/env python3

import os
import csv
import tarfile
import zipfile
from datetime import datetime
from tempfile import TemporaryDirectory
from collections import Counter
from multiprocessing import Pool
import dicom
from imagen_databank import PSC2_FROM_PSC1, DOB_FROM_PSC2
from imagen_databank import PSC1_FROM_PSC2
import logging

logging.basicConfig(level=logging.INFO)

WORKER_PROCESSES = 8

BL_DATASETS = '/neurospin/imagen/BL/RAW/PSC1'
FU2_DATASETS = '/neurospin/imagen/FU2/RAW/PSC1'
FU3_DATASETS = '/neurospin/imagen/FU3/RAW/QUARANTINE'
FU3_CANTAB = '/neurospin/imagen/FU3/RAW/PSC1'

FEMALE = 'F'
MALE = 'M'


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
    path = os.path.basename(path)

    # extract PSC1
    psc1 = path[:12]
    if len(psc1) < 12 or not psc1.isdigit():
        return None, None

    # extract time stamp
    try:
        timestamp = datetime.strptime(path[13:32],
                                      '%Y-%m-%d_%H:%M:%S')
    except ValueError:
        try:  # wrong date format for 070000166420 and 070000174119
            timestamp = datetime.strptime(path[13:32],
                                          '%Y-%d-%m_%H:%M:%S')
        except ValueError:
            timestamp = None
    if timestamp is None:
        logging.error('incorrect date/time: %s', path[13:32])
    elif path[32:34] != '.0':
        logging.error('unexpected index: %s', path[32:34])

    return psc1, timestamp


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


_CANTAB_GENDER_MAPPING = {
    'Female': FEMALE,
    'Male': MALE,
}


def _sex_from_cantab(path):
    """Extract sex from a single Cantab file.

    Parameters
    ----------
    path  : unicode
        Path to Cantab datasheet_*.csv file.

    Returns
    -------
    tuple (str, int)
        Sex, confidence as percentage of total occurrences.

    """
    result = []

    with open(path, encoding='latin1', newline='') as csvfile:
        path = os.path.basename(path)

        dialect = csv.Sniffer().sniff(csvfile.read())
        csvfile.seek(0)
        cantab = csv.DictReader(csvfile, dialect=dialect)
        for row in cantab:
            if 'Gender' in row:
                gender = row['Gender']
                if gender in _CANTAB_GENDER_MAPPING:
                    result.append(_CANTAB_GENDER_MAPPING[gender])
                    logging.debug("%s: value of 'Gender': %s",
                                  path, gender)
                elif gender != '':
                    logging.error("%s: invalid value for 'Gender': %s",
                                  path, gender)

    if len(result):
        sex = result[-1]
    else:
        sex = None

    return sex


_QUALITY_REPORT_GENDER_MAPPING = {
    'Female': FEMALE,
    'Male': MALE,
}


def _sex_from_quality_report(path):
    """Extract sex from QualityReport.txt file.

    Parameters
    ----------
    path  : unicode
        Path to QualityReport.txt.

    Returns
    -------
    str
        Sex extracted from QualityReport.txt.

    """
    result = None

    with open(path, 'r', encoding='latin1') as f:
        for line in f:
            line = [x.strip() for x in line.split('=')]
            if line[0] == 'Gender':
                gender = line[1]
                if gender in _QUALITY_REPORT_GENDER_MAPPING:
                    result = _QUALITY_REPORT_GENDER_MAPPING[gender]
                    logging.info("%s: value of 'Gender': %s",
                                 path, gender)
                else:
                    logging.error("%s: invalid value for 'Gender': %s",
                                  path, gender)
                break

    return result


_DICOM_PATIENT_SEX_MAPPING = {
    'F': FEMALE,
    'M': MALE,
    'W': FEMALE,
}

_DICOM_PATIENT_SEX_VOID = {
    'O',
    '',
}


def process_dataset_BL(arguments):
    (psc1, dataset_path) = arguments  # unpack multiple arguments

    logging.info('%s: processing compressed dataset...', psc1)

    with TemporaryDirectory(prefix='imagen_sex_') as tmp:
        with tarfile.open(dataset_path) as tar:
            members = tar.getmembers()

            # Cantab datasheet_*.csv
            cantab_sex = None
            for member in members:
                if member.name.endswith('BehaviouralData/datasheet_' + psc1 + '.csv'):
                    logging.debug('%s: found Cantab file: %s', psc1, member.name)
                    tar.extract(member, path=tmp)
                    path = os.path.join(tmp, member.name)
                    cantab_sex = _sex_from_cantab(path)
                    break
            else:
                logging.warn('%s: missing Cantab file', psc1)

            # QualityReport.txt
            quality_report_sex = None
            for member in members:
                if member.name.endswith('QualityReport.txt'):
                    logging.debug('%s: found QualityReport.txt: %s', psc1, member.name)
                    tar.extract(member, path=tmp)
                    path = os.path.join(tmp, member.name)
                    quality_report_sex = _sex_from_quality_report(path)
                    break
            else:
                logging.warn('%s: missing QualityReport.txt', psc1)

            # MRI DICOM files
            dicom_sex = None
            for member in members:
                if member.isfile() and 'ImageData/' in member.name:
                    if member.name.rsplit('/', 1)[-1].startswith('DICOMDIR'):
                        continue
                    else:
                        logging.debug('%s: found DICOM file: %s',
                                      psc1, member.name)
                        tar.extract(member, path=tmp)
                        dicom_path = os.path.join(tmp, member.name)
                        try:
                            dataset = dicom.read_file(dicom_path, force=True)
                        except IOError as e:
                            logging.error('%s: cannot read file: %s',
                                          psc1, str(e))
                        except dicom.filereader.InvalidDicomError as e:
                            logging.error('%s: cannot read nonstandard DICOM file: %s',
                                          psc1, str(e))
                        else:
                            if 'PatientSex' in dataset:
                                patient_sex = dataset.PatientSex
                                if patient_sex in _DICOM_PATIENT_SEX_MAPPING:
                                    dicom_sex = _DICOM_PATIENT_SEX_MAPPING[patient_sex]
                                    logging.info('%s: patient sex in DICOM file: %s',
                                                 dicom_path, patient_sex)
                                elif patient_sex in _DICOM_PATIENT_SEX_VOID:
                                    logging.info('%s: indeterminate patient sex in DICOM file: %s',
                                                 dicom_path, patient_sex)
                                else:
                                    logging.error('%s: invalid patient sex in DICOM file: %s',
                                                  dicom_path, patient_sex)
                            else:
                                    logging.warn('%s: missing patient sex in DICOM file',
                                                 dicom_path)
                            break
            else:
                logging.warn('%s: missing DICOM file', psc1)

    logging.info('%s: processed compressed BL dataset', psc1)

    return quality_report_sex, dicom_sex, cantab_sex


def process_dataset_FU2(arguments):
    (psc1, dataset_path) = arguments  # unpack multiple arguments

    logging.info('%s: processing dataset...', psc1)

    # Cantab datasheet_*.csv
    cantab_sex = None
    additional_data_path = os.path.join(dataset_path, 'AdditionalData')
    for f in os.listdir(additional_data_path):
        if 'datasheet' in f and 'detailed' not in f:
            datasheet_path = os.path.join(additional_data_path, f)
            cantab_sex = _sex_from_cantab(datasheet_path)
            logging.info('%s: sex in Cantab file: %s',
                         psc1, cantab_sex)
            break
    else:
        logging.warn('%s: missing Cantab file', psc1)

    # MRI DICOM files
    dicom_sex = None
    image_data_path = os.path.join(dataset_path, 'ImageData')
    for dirpath, dirnames, filenames in os.walk(image_data_path):
        for filename in filenames:
            if filename.startswith('DICOMDIR'):
                continue
            else:
                dicom_path = os.path.join(dirpath, filename)
                logging.debug('%s: found DICOM file: %s',
                              psc1, dicom_path)
                try:
                    dataset = dicom.read_file(dicom_path, force=True)
                except IOError as e:
                    logging.error('%s: cannot read file: %s',
                                  psc1, str(e))
                except dicom.filereader.InvalidDicomError as e:
                    logging.error('%s: cannot read nonstandard DICOM file: %s',
                                  psc1, str(e))
                else:
                    if 'PatientSex' in dataset:
                        patient_sex = dataset.PatientSex
                        if patient_sex in _DICOM_PATIENT_SEX_MAPPING:
                            dicom_sex = _DICOM_PATIENT_SEX_MAPPING[patient_sex]
                            logging.info('%s: patient sex in DICOM file: %s',
                                         psc1, patient_sex)
                        elif patient_sex in _DICOM_PATIENT_SEX_VOID:
                            logging.info('%s: indeterminate patient sex in DICOM file: %s',
                                         psc1, patient_sex)
                        else:
                            logging.error('%s: invalid patient sex in DICOM file: %s',
                                          psc1, patient_sex)
                    else:
                            logging.warn('%s: missing patient sex in DICOM file',
                                         psc1)
                    break
    else:
        logging.warn('%s: missing DICOM file', psc1)

    logging.info('%s: processed FU2 dataset', psc1)

    return dicom_sex, cantab_sex


def process_dataset_FU3(arguments):
    (psc1, dataset_path) = arguments  # unpack multiple arguments

    logging.info('%s: processing zipped FU3 dataset...', psc1)

    with TemporaryDirectory(prefix='imagen_sex_') as tmp:
        with zipfile.ZipFile(dataset_path) as dataset_zipfile:
            dicom_sex = None
            members = dataset_zipfile.infolist()
            for member in members:
                if not member.filename.endswith('/') and 'ImageData/' in member.filename:
                    if member.filename.rsplit('/', 1)[-1].startswith('DICOMDIR'):
                        continue
                    else:
                        logging.debug('%s: found DICOM file: %s',
                                      psc1, member.filename)
                        dataset_zipfile.extract(member, path=tmp)
                        dicom_path = os.path.join(tmp, member.filename)
                        try:
                            dataset = dicom.read_file(dicom_path, force=True)
                        except IOError as e:
                            logging.error('%s: cannot read file: %s',
                                          psc1, str(e))
                        except dicom.filereader.InvalidDicomError as e:
                            logging.error('%s: cannot read nonstandard DICOM file: %s',
                                          psc1, str(e))
                        else:
                            if 'PatientSex' in dataset:
                                patient_sex = dataset.PatientSex
                                if patient_sex in _DICOM_PATIENT_SEX_MAPPING:
                                    dicom_sex = _DICOM_PATIENT_SEX_MAPPING[patient_sex]
                                    logging.info('%s: patient sex in DICOM file: %s',
                                                 dicom_path, patient_sex)
                                elif patient_sex in _DICOM_PATIENT_SEX_VOID:
                                    logging.info('%s: indeterminate patient sex in DICOM file: %s',
                                                 dicom_path, patient_sex)
                                else:
                                    logging.error('%s: invalid patient sex in DICOM file: %s',
                                                  dicom_path, patient_sex)
                            else:
                                    logging.warn('%s: missing patient sex in DICOM file',
                                                 dicom_path)
                            break
            else:
                logging.warn('%s: missing DICOM file', psc1)

    logging.info('%s: processed zipped FU3 dataset', psc1)

    return dicom_sex


def process_cantab_FU3(arguments):
    (psc1, cantab_path) = arguments  # unpack multiple arguments

    logging.info('%s: processing FU3 Cantab file...', psc1)

    cantab_sex = _sex_from_cantab(cantab_path)
    logging.info('%s: sex in Cantab file: %s',
                 psc1, cantab_sex)

    return cantab_sex


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
        for dataset in os.listdir(center_path):
            psc1 = dataset[:12]
            dataset_path = os.path.join(center_path, dataset)
            datasets.append((psc1, dataset_path))

    logging.info('found %d FU2 datasets', len(datasets))

    return datasets


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


def list_cantab_FU3(path):
    # list Cantab files to process
    cantabs = []
    for center in ('LONDON', 'NOTTINGHAM', 'DUBLIN', 'BERLIN',
                   'HAMBURG', 'MANNHEIM', 'PARIS', 'DRESDEN'):
        center_path = os.path.join(path, center)
        for psc1 in os.listdir(center_path):
            additional_data_path = os.path.join(center_path, psc1,
                                                'AdditionnalData')
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


def dataset_FU3(path):
    todo_list = list(list_datasets_FU3(path))

    pool = Pool(WORKER_PROCESSES)
    results = pool.map(process_dataset_FU3, todo_list)
    pool.close()
    pool.join()

    psc1, path = zip(*todo_list)
    return dict(zip(psc1, results))


def cantab_FU3(path):
    todo_list = list(list_cantab_FU3(path))

    pool = Pool(WORKER_PROCESSES)
    results = pool.map(process_cantab_FU3, todo_list)
    pool.close()
    pool.join()

    psc1, path = zip(*todo_list)
    return dict(zip(psc1, results))


def main():
    results_BL = dataset_BL(BL_DATASETS)
    results_FU2 = dataset_FU2(FU2_DATASETS)
    results_FU3 = dataset_FU3(FU3_DATASETS)
    results_cantab_FU3 = cantab_FU3(FU3_CANTAB)
    results = (results_BL, results_FU2, results_FU3)

    with open('imagen_sex_dataset.csv', 'w', newline='') as csvfile:
        sex = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
        sex.writerow(['PSC1',
                       'QualityReport.txt','BL MRI', 'BL Cantab',
                       'FU2 MRI', 'FU2 Cantab',
                       'FU3 MRI', 'FU3 Cantab'])
        psc1s = set()
        for timepoint in results:
            psc1s = psc1s.union(set(timepoint.keys()))
        for psc1 in sorted(psc1s):
            row = [psc1]
            if psc1 in results_BL:
                row.extend(results_BL[psc1])
            else:
                row.extend((None, None, None))
            if psc1 in results_FU2:
                row.extend(results_FU2[psc1])
            else:
                row.extend((None, None))
            if psc1 in results_FU3:
                row.append(results_FU3[psc1])
            else:
                row.append(None)
            if psc1 in results_cantab_FU3:
                row.append(results_cantab_FU3[psc1])
            else:
                row.append(None)
            sex.writerow(row)


if __name__ == "__main__":
    main()
