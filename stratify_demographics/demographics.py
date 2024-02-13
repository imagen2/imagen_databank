#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from csv import reader
from csv import DictWriter
import xlrd
from imagen_databank import PSC2_FROM_PSC1, CENTER_NAME

import logging
logging.basicConfig(level=logging.ERROR)


_DEBUG_PSYTOOLS_SEX = 'STRATIFY_SEX_2021-05-31.txt'

_DEMOGRAPHIC_RECORDS_DIR = '/imagen/STRATIFY/RAW/PSC1/meta_data'
_DEMOGRAPHIC_RECORDS = [
    os.path.join(_DEMOGRAPHIC_RECORDS_DIR, 'STRATIFY_recruitment_file_SOUTHAMPTON_2019-05-23.xlsx'),
    os.path.join(_DEMOGRAPHIC_RECORDS_DIR, 'STRATIFY_recruitment_file_LONDON_2020-07-24.xlsx'),
    os.path.join(_DEMOGRAPHIC_RECORDS_DIR, 'ESTRA_recruitment_file_LONDON_2019-07-24.xlsx'),
    os.path.join(_DEMOGRAPHIC_RECORDS_DIR, 'STRATIFY_recruitment_file_LONDON_CONTROLS_2019-09-09.xlsx'),
    os.path.join(_DEMOGRAPHIC_RECORDS_DIR, 'STRATIFY_recruitment_file_BERLIN_2020-11-03.xlsx'),
]

_FINAL_COLUMNS = (
    'PSC2',
    'sex',
    'recruitment site',
    'scanning site',
    'patient group',
    'complete',
    'missing data',
)

_DEMOGRAPHIC_COLUMNS = {
    # handle separately 'PSC1 Code'
    # Stratify
    'Sex': _FINAL_COLUMNS[1],
    'Acquisition Centre (and Scanning Site)': _FINAL_COLUMNS[3],
    'Acquisition Centre': _FINAL_COLUMNS[3],
    'Patient Group': _FINAL_COLUMNS[4],
    'Fully Complete? Y/N': _FINAL_COLUMNS[5],
    'Missing Data (Please Specify)': _FINAL_COLUMNS[6],
    # ESTRA
    # (skip 'Recruitment Centre')
    'Scanning Site': _FINAL_COLUMNS[3],
    'Gender ': _FINAL_COLUMNS[1],
    'Diagnosis ': _FINAL_COLUMNS[4],
    'Diagnosis': _FINAL_COLUMNS[4],
    # Stratify 20 additional controls
    'Site': _FINAL_COLUMNS[3],
    'Group': _FINAL_COLUMNS[4],
    'Gender': _FINAL_COLUMNS[1],
    # LONDON CONTROLS
    
    # BERLIN
    'sex': _FINAL_COLUMNS[1],
    'scanning site': _FINAL_COLUMNS[3],
    'patient group': _FINAL_COLUMNS[4],
    'complete': _FINAL_COLUMNS[5],
    'missing data': _FINAL_COLUMNS[6],
}

_CONTROL_GROUP = 'Control'
_ADHD_GROUP = 'ADHD'
_AUD_GROUP = 'AUD'
_AN_GROUP = 'AN'
_RECAN_GROUP = 'recAN'
_BN_GROUP = 'BN'
_RECBN_GROUP = 'recBN'
_MDD_GROUP = 'MDD'
_PSYCHOSIS_GROUP = 'Psychosis'

_PATIENT_GROUPS = {
    _CONTROL_GROUP,
    _ADHD_GROUP,
    _AUD_GROUP,
    _AN_GROUP,
    _RECAN_GROUP,
    _BN_GROUP,
    _RECBN_GROUP,
    _MDD_GROUP,
    _PSYCHOSIS_GROUP,
}


def normalize_patient_group(s):
    table = {
        'control': _CONTROL_GROUP,
        'depression': _MDD_GROUP,
        'psychosis': _PSYCHOSIS_GROUP,
        'Alcohol Use Disorder': _AUD_GROUP,
        'Major Depressive Disorder': _MDD_GROUP,
        'Healthy Control': _CONTROL_GROUP,
    }
    if s in table:
       s = table[s]

    return s


def normalize_scanning_site(s):
    table = {
        # LONDON: 'CNS' or 'Invicro'
        'KCL': 'CNS',
        'Denmark Hill': 'CNS',
        # SOUTHAMPTON
        'Southampton': None,
        # BERLIN
        'BERLIN': None,
    }
    if s in table:
       s = table[s]

    return s


def normalize_sex(s):
    s = s.upper()

    table = {
        'FEMALE': 'F',
        'MALE': 'M',
    }
    if s in table:
       s = table[s]

    return s


def strip_cell(s):
    try:
        s = s.strip()
    except AttributeError:  # floats and other types
        pass
    return s


def read_demographic_record(path):
    demographics = {}

    with xlrd.open_workbook(path) as workbook:
        worksheet = workbook.sheet_by_index(0)

        # read header
        psc1_index = None
        index = {}
        row = [strip_cell(x) for x in worksheet.row_values(0)]
        print(path)
        for i, value in enumerate(row):
            if value in _DEMOGRAPHIC_COLUMNS:
                index[_DEMOGRAPHIC_COLUMNS[value]] = i
                print(i, value, '→', _DEMOGRAPHIC_COLUMNS[value])
            elif value == 'PSC1 Code' or value == 'PSC1':
                psc1_index = i
            else:
                print(i, value, '→', '?????')

        if psc1_index is None:
            logging.error('%s: cannot find PSC1 code', path)
            return demographics

        # read data
        for i in range(1, worksheet.nrows):
            row = [strip_cell(x) for x in worksheet.row_values(i)]

            psc1 = row[psc1_index]
            psc1 = psc1[:12]  # remove trailing FU3 or SB
            if psc1 not in PSC2_FROM_PSC1:
                logging.error('%s: invalid PSC1 code', psc1)
                continue

            demographics[psc1] = {}

            for name, i in index.items():
                value = row[i]
                if name == 'sex':
                    value = normalize_sex(value)
                    if value not in {'F', 'M'}:
                        logging.error('%s: invalid sex: %s', psc1, value)
                        continue
                elif name == 'patient group':
                    value = normalize_patient_group(value)
                    if value not in _PATIENT_GROUPS:
                        logging.error('%s: invalid patient group: %s',
                                      psc1, value)
                        continue
                elif name == 'scanning site':
                    value = normalize_scanning_site(value)
                elif name == 'complete':
                    if value not in {'Y', 'N', ''}:
                        logging.error('%s: invalid completeness: %s',
                                      psc1, value)
                        continue
                elif name == 'missing data':
                    value = value.rstrip(',.')
                    if value.lower() == 'none':
                        value = None
                demographics[psc1][name] = value

    return demographics


def read_demographic_records(paths):
    demographic_records = {}

    for path in paths:
        demographic_records.update(read_demographic_record(path))

    return demographic_records


def main():
    demographics = read_demographic_records(_DEMOGRAPHIC_RECORDS)

    with open(_DEBUG_PSYTOOLS_SEX, 'r') as sex_file:
        sex_reader = reader(sex_file, dialect='excel')

        with open('demographics.csv', 'w') as demographics_file:
            demographics_writer = DictWriter(demographics_file,
                                             _FINAL_COLUMNS,
                                              dialect='excel')
            demographics_writer.writeheader()
            for row in sex_reader:
                psc1 = row[0]
                psc2 = PSC2_FROM_PSC1[psc1]
                center = int(psc1[1])
                if center > 8:
                    center = int(psc1[1:3])
                center = CENTER_NAME[center]
                sex = row[1]
                if psc1 in demographics:
                    data = demographics[psc1]
                    data['PSC2'] = psc2
                    data['recruitment site'] = center
                    if 'sex' in data:
                        if data['sex'] != sex:
                            logging.error('%s: inconsistent sex between Psytools and recruitment file', psc1)
                        data['sex'] = sex
                else:
                    data = {
                        'PSC2': psc2,
                        'sex': sex, 
                        'recruitment site': center,
                    }
                row = {x: data[x] if x in data else None
                       for x in _FINAL_COLUMNS}
                demographics_writer.writerow(row)


if __name__ == "__main__":
    main()
