#!/usr/bin/env python3

import os
from multiprocessing import Pool
import csv
from collections import Counter
import logging

logging.basicConfig(level=logging.INFO)

BL_PSYTOOLS = '/neurospin/imagen/BL/RAW/PSC1/psytools'
FU1_PSYTOOLS = '/neurospin/imagen/FU1/RAW/PSC1/psytools'
FU2_PSYTOOLS = '/neurospin/imagen/FU2/RAW/PSC1/psytools'
FU3_PSYTOOLS = '/neurospin/imagen/FU3/RAW/PSC1/psytools'

WORKER_PROCESSES = 24


FEMALE = 'F'
MALE = 'M'

_CSV_ID_CHECK_GENDER_MAPPING = {
    '1': MALE,
    '2': FEMALE,
    'female': FEMALE,
    'male': MALE,
}

_LSRC2_ID_CHECK_GENDER_MAPPING = {
    'F': FEMALE,
    'M': MALE,
}


def _psytools_choice(psc1, counter):
    female = counter[FEMALE]
    male = counter[MALE]
    total = female + male
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


def list_psytools_timepoint(path):
    """List Psytools CSV files exported from Delosis.

    Parameters
    ----------
    path : str
        Directory to read Psytools CSV files from.

    Yields
    ------
    str
        Path to Psytools CSV file.

    """
    CSV_PREFIX = ('IMAGEN-IMGN_', 'IMAGEN-cVEDA_')
    LSRC2_PREFIX = ('Imagen_', 'STRATIFY_')

    for f in os.listdir(path):
        root, ext = os.path.splitext(f)
        if ext == '.csv':
            if any(root.startswith(prefix) for prefix in CSV_PREFIX):
                yield (False, os.path.join(path, f))
            elif any(root.startswith(prefix) for prefix in LSRC2_PREFIX):
                yield (True, os.path.join(path, f))
            else:
                logging.error('skipping unknown CSV file: %s', f)


def process_psytools_timepoint(arguments):
    (lsrc2, path) = arguments  # unpack multiple arguments

    result = {}

    with open(path, 'r') as f:
        reader = csv.DictReader(f, dialect='excel')
        for row in reader:
            if lsrc2:
                psc1 = row['id']
                if psc1.endswith('FU3'):
                    psc1 = psc1[:-len('FU3')]
                elif psc1.endswith('FU2'):  # Parent questionnaires
                    psc1 = psc1[:-len('FU2')]
                if psc1.isdigit() and len(psc1) == 12:
                    if 'IdCheckGender' in row:
                        id_check_gender = row['IdCheckGender']
                        if id_check_gender in _LSRC2_ID_CHECK_GENDER_MAPPING:
                            sex = _LSRC2_ID_CHECK_GENDER_MAPPING[id_check_gender]
                            result.setdefault(psc1, []).append(sex)
                        else:
                            logging.error("%s: invalid 'IdCheckGender': %s",
                                          psc1, id_check_gender)
                else:
                    logging.info('%s: cannot interpret as PSC1 code', psc1)
            else:
                completed = row['Completed']
                trial = row['Trial']
                if completed == 't' and trial == "id_check_gender":
                    psc1_suffix = row['User code'].rsplit('-', 1)
                    psc1 = psc1_suffix[0]
                    if psc1.isdigit() and len(psc1) == 12:
                        trial_result = row['Trial result']
                        if trial_result in _CSV_ID_CHECK_GENDER_MAPPING:
                            sex = _CSV_ID_CHECK_GENDER_MAPPING[trial_result]
                            result.setdefault(psc1, []).append(sex)
                        else:
                            logging.error("%s: invalid 'id_check_gender': %s",
                                          psc1, trial_result)
                    else:
                        logging.info('%s: cannot interpret as PSC1 code', psc1)

    return result


def _decide_from_counter(counter):
    female = counter[FEMALE]
    male = counter[MALE]
    total = sum(counter.values())
    if total:
        if female > male:
            sex = FEMALE
            percentage = ((200 * female) // total + 1) // 2  # closest integer percentage
        elif male > female:
            sex = MALE
            percentage = ((200 * male) // total + 1) // 2  # closest integer percentage
        else:
            sex = None
            percentage = 50
    else:
        sex = None
        percentage = None

    return sex, percentage


def psytools_timepoint(path):
    todo_list = list(list_psytools_timepoint(path))

    pool = Pool(WORKER_PROCESSES)
    results = pool.map(process_psytools_timepoint, todo_list)
    pool.close()
    pool.join()

    sex_counter = {}
    for result in results:
        for psc1, sex in result.items():
            sex_counter.setdefault(psc1, Counter()).update(sex)

    return {psc1: _decide_from_counter(counter)
            for psc1, counter in sex_counter.items()}


def main():
    psytools_BL = psytools_timepoint(BL_PSYTOOLS)
    psytools_FU1 = psytools_timepoint(FU1_PSYTOOLS)
    psytools_FU2 = psytools_timepoint(FU2_PSYTOOLS)
    psytools_FU3 = psytools_timepoint(FU3_PSYTOOLS)
    psytools = (psytools_BL, psytools_FU1, psytools_FU2, psytools_FU3)

    with open('imagen_sex_psytools.csv', 'w', newline='') as csvfile:
        sex = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
        sex.writerow(['PSC1',
                      'Psytools BL', 'Psytools FU1',
                      'Psytools FU2', 'Psytools FU3'])
        psc1s = set()
        for timepoint in psytools:
            psc1s = psc1s.union(set(timepoint.keys()))
        for psc1 in sorted(psc1s):
            row = [psc1]
            for timepoint in psytools:
                if psc1 in timepoint:
                    row.append(timepoint[psc1][0])
                else:
                    row.append(None)
            sex.writerow(row)

            if any(psc1 in timepoint and timepoint[psc1][1] != 100
                   for timepoint in psytools):
                s = '%s: inconsistent sex:'
                if psc1 in psytools_BL:
                    s += '\n\tBL:  {} {}%%'.format(psytools_BL[psc1][0], psytools_BL[psc1][1])
                if psc1 in psytools_FU1:
                    s += '\n\tFU1: {} {}%%'.format(psytools_FU1[psc1][0], psytools_FU1[psc1][1])
                if psc1 in psytools_FU2:
                    s += '\n\tFU2: {} {}%%'.format(psytools_FU2[psc1][0], psytools_FU2[psc1][1])
                if psc1 in psytools_FU3:
                    s += '\n\tFU3: {} {}%%'.format(psytools_FU3[psc1][0], psytools_FU3[psc1][1])
                logging.warning(s, psc1)


if __name__ == "__main__":
    main()
