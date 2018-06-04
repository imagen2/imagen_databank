#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from multiprocessing import Pool
import csv
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)

BL_RECRUITMENT_INFO = os.path.join('/neurospin/imagen/BL/RAW/PSC1/recruitment')

WORKER_PROCESSES = 16


FEMALE = 'F'
MALE = 'M'

_RECRUITMENT_SEX_MAPPING = {
    'f': FEMALE,
    'F': FEMALE,
    'm': MALE,
    'M': MALE,
    'w': FEMALE,
}

_RECRUITMENT_SEX_VOID = {
    '',
    '0',
    '0.0',
    'Test',
    'not known',
}


def _recruitment_center(s):
    s = s.strip()

    if set(s).issubset('12345678.0'):
        if '.' in s:
            try:
                s = float(s)
            except ValueError:
                logging.info('%s: cannot interpret as center code', s)
                return None
            else:
                s = str(int(s // 1))  # integral part
        if len(s) == 1:
            return s
        else:
            logging.error('%s: incorrect center code', s)
    else:
        logging.debug('%s: skipping center code', s)

    return None


def _recruitment_psc1(s, center):
    s = s.strip()

    if s.isdigit():
        if len(s) < 7:
            s = '0' + center + s.zfill(10)
        if len(s) == 12:
            return s
        else:
            logging.error('%s: incorrect PSC1 code', s)
    elif s:
        logging.warn('%s: cannot interpret as PSC1 code', s)
    else:
        logging.debug('empty PSC1 code')

    return None


def _recruitment_choice(psc1, timestamps):
    # use data with most recent time stamp
    counter = Counter(timestamps[max(timestamps.keys())])

    female = counter[FEMALE]
    male = counter[MALE]
    if female and male:
        logging.error('%s: inconsistent information about sex', psc1)
        return None
    elif female:
        return FEMALE
    elif male:
        return MALE
    else:
        logging.error('%s: cannot find information about sex', psc1)
        sex = None


def list_recruitment_BL(path):
    """List recruitment CSV files sent by recruitment centres.

    Parameters
    ----------
    path : str
        Directory to read CSV recruitment files from.

    Yields
    ------
    str
        Path to CSV file.

    """
    for f in os.listdir(path):
        root, ext = os.path.splitext(f)
        if ext == '.csv':
            yield os.path.join(path, f)


def process_recruitment_BL(path):
    timestamp = os.path.getmtime(path)

    recruitment_sex = {}

    with open(path, encoding='latin1', newline='') as csvfile:
        recruitment = csv.reader(csvfile, delimiter=',')
        for row in recruitment:
            center = _recruitment_center(row[0])
            if center:
                psc1 = _recruitment_psc1(row[1], center)
                if psc1:
                    gender = row[2].strip()
                    if gender in _RECRUITMENT_SEX_MAPPING:
                        sex = _RECRUITMENT_SEX_MAPPING[gender]
                        if psc1 in recruitment_sex:
                            if recruitment_sex[psc1] != sex:
                                logging.error('%s: inconsistent duplicate line',
                                              psc1)
                            else:
                                logging.error('%s: duplicate line',
                                              psc1)
                        else:
                            recruitment_sex[psc1] = sex
                    elif gender not in _RECRUITMENT_SEX_VOID:
                        logging.error("%s: incorrect 'gender': %s",
                                      psc1, gender)

    return timestamp, recruitment_sex


def recruitment_BL(path):
    """Process CSV recruitment files sent by recruitment centres at baseline.

    First list the files to process, then read these files in parallel.

    Parameters
    ----------
    path : str
        Directory to read CSV recruitment files from.

    Returns
    -------
    dict
        Key is PSC1 and value a pair (xnat_sex, xnat_experiment_sex).

    """
    todo_list = list(list_recruitment_BL(path))

    pool = Pool(WORKER_PROCESSES)
    results = pool.map(process_recruitment_BL, todo_list)
    pool.close()
    pool.join()

    sex_by_timestamp = {}
    for timestamp, result in results:
        for psc1, sex in result.items():
            sex_by_timestamp.setdefault(psc1, {})[timestamp] = result[psc1]

    recruitment_sex = {}
    for psc1, timestamps in sex_by_timestamp.items():
        max_timestamp = max(timestamps)
        sex = timestamps[max_timestamp]
        for k, v in timestamps.items():
            if v != sex:
                logging.error("%s: inconsistent 'gender' across time stamps\n"
                              '\t%s: %s\n'
                              '\t%s: %s',
                              psc1,
                              datetime.fromtimestamp(k).date(), v,
                              datetime.fromtimestamp(max_timestamp).date(), sex)
        recruitment_sex[psc1] = sex

    return recruitment_sex


def main():
    recruitment = recruitment_BL(BL_RECRUITMENT_INFO)

    with open('imagen_sex_recruitment.csv', 'w', newline='') as csvfile:
        sex = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
        sex.writerow(['PSC1', 'Recruitment'])
        for psc1 in sorted(recruitment):
            row = [psc1]
            row.append(recruitment[psc1])
            sex.writerow(row)


if __name__ == "__main__":
    main()
