#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from multiprocessing import Pool
import csv
from datetime import datetime, date
from collections import Counter
import logging

logging.basicConfig(level=logging.INFO)

STRATIFY_PSYTOOLS = '/neurospin/imagen/STRATIFY/RAW/PSC1/psytools'
STRATIFY_DOB = '/neurospin/imagen/STRATIFY/RAW/PSC1/meta_data/dob_validation.csv'
STRATIFY_SEX = '/neurospin/imagen/STRATIFY/RAW/PSC1/meta_data/sex_validation.csv'

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

_CANTAB_GENDER_MAPPING = {
    'Female': FEMALE,
    'Male': MALE,
}


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
    CSV_PREFIX = ('IMAGEN-', 'STRATIFY-')
    LSRC2_PREFIX = ('Imagen_', 'STRATIFY_Core')  # exclude STRATIFY_Screening

    for f in os.listdir(path):
        root, ext = os.path.splitext(f)
        if ext == '.csv':
            if any(root.startswith(prefix) for prefix in CSV_PREFIX):
                yield (False, os.path.join(path, f), root)
            elif any(root.startswith(prefix) for prefix in LSRC2_PREFIX):
                yield (True, os.path.join(path, f), root)
            else:
                logging.error('skipping unknown CSV file: %s', f)


def process_psytools_timepoint(arguments):
    (lsrc2, path, name) = arguments  # unpack multiple arguments

    sex_counter = {}
    dob_counter = {}

    with open(path, 'r') as f:
        reader = csv.DictReader(f, dialect='excel')
        for row in reader:
            if lsrc2:
                psc1 = row['id']
                if psc1.endswith('SB'):
                    psc1 = psc1[:-len('SB')]
                if psc1.endswith('FU'):
                    psc1 = psc1[:-len('FU')]
                if psc1.isdigit() and len(psc1) == 12:
                    if 'IdCheckGender' in row:
                        id_check_gender = row['IdCheckGender']
                        if id_check_gender in _LSRC2_ID_CHECK_GENDER_MAPPING:
                            id_check_gender = _LSRC2_ID_CHECK_GENDER_MAPPING[id_check_gender]
                            sex_counter.setdefault(psc1, {}).setdefault(id_check_gender, Counter()).update(('IdCheckGender',))
                        elif id_check_gender:
                            logging.error("%s: %s: invalid 'IdCheckGender': %s",
                                          name, psc1, id_check_gender)
                        else:
                            logging.debug("%s: %s: empty 'IdCheckGender': %s",
                                          name, psc1, id_check_gender)
                    if 'IdCheckDob' in row:
                        id_check_dob = row['IdCheckDob']
                        try:
                            id_check_dob = datetime.strptime(id_check_dob, '%Y-%m-%d %H:%M:%S')
                        except ValueError as e:
                            if id_check_dob:
                                logging.error("%s: %s: invalid 'IdCheckDob': %s",
                                              name, psc1, id_check_dob)
                            else:
                                logging.debug("%s: %s: empty 'IdCheckDob': %s",
                                              name, psc1, id_check_dob)
                        else:
                            id_check_dob = id_check_dob.date()
                            if id_check_dob.year > 2012 or id_check_dob.year < 1990:
                                logging.error("%s: %s: skip 'IdCheckDob': %d",
                                              name, psc1, id_check_dob.year)
                            else:
                                dob_counter.setdefault(psc1, {}).setdefault(id_check_dob, Counter()).update(('IdCheckDob',))
                else:
                    logging.info('%s: %s: cannot interpret as PSC1 code', name, psc1)
            else:
                psc1_suffix = row['User code'].rsplit('-', 1)
                psc1 = psc1_suffix[0]
                if psc1.endswith('SB'):
                    psc1 = psc1[:-len('SB')]
                completed = row['Completed']
                if completed == 't':
                    trial = row['Trial']
                    if trial == 'id_check_gender':
                        if psc1.isdigit() and len(psc1) == 12:
                            trial_result = row['Trial result']
                            if trial_result in _CSV_ID_CHECK_GENDER_MAPPING:
                                id_check_gender = _CSV_ID_CHECK_GENDER_MAPPING[trial_result]
                                sex_counter.setdefault(psc1, {}).setdefault(id_check_gender, Counter()).update((trial,))
                            else:
                                logging.error("%s: %s: invalid 'id_check_gender': %s",
                                              name, psc1, trial_result)
                        else:
                            logging.info('%s: %s: cannot interpret as PSC1 code', name, psc1)
                    elif trial == 'ni_gender':
                        if psc1.isdigit() and len(psc1) == 12:
                            trial_result = row['Trial result']
                            if trial_result in _LSRC2_ID_CHECK_GENDER_MAPPING:
                                id_check_gender = _LSRC2_ID_CHECK_GENDER_MAPPING[trial_result]
                                sex_counter.setdefault(psc1, {}).setdefault(id_check_gender, Counter()).update((trial,))
                            else:
                                logging.error("%s: %s: invalid 'ni_gender': %s",
                                              name, psc1, trial_result)
                        else:
                            logging.info('%s: %s: cannot interpret as PSC1 code', name, psc1)
                    elif trial == 'id_check_dob':
                        if psc1.isdigit() and len(psc1) == 12:
                            trial_result = row['Trial result']
                            try:
                                month, year = trial_result.rsplit('_')
                                month = int(month)
                                year = int(year)
                            except ValueError as e:
                                logging.error("%s: invalid 'id_check_dob': %s",
                                              psc1, id_check_dob)
                            else:
                                if year > 2012 or year < 1990:
                                    logging.error("%s: skip 'id_check_dob': %d",
                                                  psc1, year)
                                else:
                                    dob_counter.setdefault(psc1, {}).setdefault((year, month), Counter()).update((trial,))
                        else:
                            logging.info('%s: %s: cannot interpret as PSC1 code', name, psc1)

    return sex_counter, dob_counter


def psytools_timepoint(path):
    todo_list = list(list_psytools_timepoint(path))

    pool = Pool(WORKER_PROCESSES)
    results = pool.map(process_psytools_timepoint, todo_list)
    pool.close()
    pool.join()

    sex = {}
    dob = {}
    for (sex_counter, dob_counter), (lsrc2, path, name) in zip(results, todo_list):
        for psc1, values in sex_counter.items():
            for value, variables in values.items():
                for variable, count in variables.items():
                    sex.setdefault(psc1, {}).setdefault(value, {}).setdefault(variable, Counter()).update({name: count})
        for psc1, values in dob_counter.items():
            for value, variables in values.items():
                for variable, count in variables.items():
                    dob.setdefault(psc1, {}).setdefault(value, {}).setdefault(variable, Counter()).update({name: count})

    clean_dob = {}
    for psc1, values in dob.items():
        exact_dates = set()
        for value, variables in values.items():
            if type(value) == date:
                for variable, counter in variables.items():
                    exact_dates.add(value)
                    clean_dob.setdefault(psc1, {}).setdefault(value, {}).setdefault(variable, Counter()).update(counter)
        for value, variables in values.items():
            if type(value) == tuple:
                year, month = value
                for variable, counter in variables.items():
                    for d in exact_dates:
                        if d.year == year and d.month == month:
                            clean_dob.setdefault(psc1, {}).setdefault(d, {}).setdefault(variable, Counter()).update(counter)
                            break
                    else:
                        clean_dob.setdefault(psc1, {}).setdefault(value, {}).setdefault(variable, Counter()).update(counter)

    return sex, clean_dob


def cantab_timepoint(path):
    sex = {}
    for center in os.listdir(path):
        center_path = os.path.join(path, center)
        if os.path.isdir(center_path):
            for psc1 in os.listdir(center_path):
                psc1_path = os.path.join(center_path, psc1)
                if os.path.isdir(psc1_path):
                    if psc1.isdigit() and len(psc1) == 12:
                        additional_data_path = os.path.join(psc1_path, 'AdditionalData')
                        for f in os.listdir(additional_data_path):
                            if f.startswith('datasheet_'):
                                if f == ('datasheet_' + psc1 + 'SB.csv'):
                                    f_path = os.path.join(additional_data_path, f)
                                    with open(f_path, newline='') as csvfile:
                                        reader = csv.DictReader(csvfile)
                                        if 'Gender' not in reader.fieldnames:
                                            csvfile.seek(0)
                                            reader = csv.DictReader(csvfile, delimiter=';')
                                            if 'Gender' not in reader.fieldnames:
                                                reader = None
                                        for row in reader:
                                            if 'Gender' in row:
                                                if row['Gender']:
                                                    sex[psc1] = _CANTAB_GENDER_MAPPING[row['Gender']]
                                                else:
                                                    logging.warning('%s: missing Gender value: %s', psc1, f)
                                            else:
                                                logging.warning('%s: missing Gender column (%s): %s', psc1, reader.fieldnames, f)
                                else:
                                    logging.error('%s: incorrect file name: %s', psc1, f)
                    else:
                        logging.info('%s: not a directory', psc1)
                else:
                        logging.debug('%s: not a PSC1 code', psc1)

    return sex


def main():
    sex, dob = psytools_timepoint(STRATIFY_PSYTOOLS)
    cantab_sex = cantab_timepoint('/neurospin/imagen/STRATIFY/RAW/PSC1')

    validated_dob = {}
    with open(STRATIFY_DOB, 'r') as f:
        reader = csv.reader(f, dialect='excel')
        for row in reader:
            validated_dob[row[0]] = datetime.strptime(row[1], '%Y-%m-%d').date()

    validated_sex = {}
    with open(STRATIFY_SEX, 'r') as f:
        reader = csv.reader(f, dialect='excel')
        for row in reader:
            validated_sex[row[0]] = row[1]

    for psc1 in cantab_sex:
        if psc1 in sex:
            sex[psc1].setdefault(cantab_sex[psc1], {}).setdefault('Gender', Counter()).update({'datasheet_' + psc1 + 'SB': 1})
        else:
            logging.error('%s: found in Cantab but missing from Psytools', psc1)

    today = datetime.today()

    with open('STRATIFY_SEX_' + today.strftime('%Y-%m-%d') + '.txt', 'w') as f:
        for psc1, values in sex.items():
            if psc1 in validated_sex:
                print(','.join((psc1, validated_sex[psc1])), file=f)
            elif len(values) > 1:
                message = '{}: multiple sex values:\n'.format(psc1)
                for value, variables in values.items():
                    count_value = 0
                    message_variable = ''
                    for variable, counters in variables.items():
                        count_variable = 0
                        message_name = ''
                        for name, count in counters.items():
                            message_name += '\t\t\t{}\n'.format(name)
                            count_variable += count
                        message_variable += '\t\t{} ({})\n'.format(variable, count_variable) + message_name
                        count_value += count_variable
                    message_value = '\t{} ({})\n'.format(value, count_value) + message_variable
                    message += message_value
                logging.error(message)
            else:
                value = next(iter(values.keys()))
                print(','.join((psc1, value)), file=f)

    with open('STRATIFY_DOB_' + today.strftime('%Y-%m-%d') + '.txt', 'w') as f:
        for psc1, values in dob.items():
            if psc1 in validated_dob:
                print(','.join((psc1, validated_dob[psc1].strftime('%Y-%m-%d'),
                                today.strftime('%Y-%m-%d_%H:%M:%S.0'))),
                      file=f)
            elif len(values) > 1:
                message = '{}: multiple date of birth values:\n'.format(psc1)
                for value, variables in values.items():
                    count_value = 0
                    message_variable = ''
                    for variable, counters in variables.items():
                        count_variable = 0
                        message_name = ''
                        for name, count in counters.items():
                            message_name += '\t\t\t{} ({})\n'.format(name, count)
                            count_variable += count
                        message_variable += '\t\t{} ({})\n'.format(variable, count_variable) + message_name
                        count_value += count_variable
                    message_value = '\t{} ({})\n'.format(value, count_value) + message_variable
                    message += message_value
                logging.error(message)
            else:
                value = next(iter(values.keys()))
                if type(value) == date:
                    value = value.strftime('%Y-%m-%d')
                    print(','.join((psc1, value,
                                    today.strftime('%Y-%m-%d_%H:%M:%S.0'))),
                          file=f)
                else:
                    logging.error('%s: skipping incomplete date: %s', psc1, str(value))


if __name__ == "__main__":
    main()
