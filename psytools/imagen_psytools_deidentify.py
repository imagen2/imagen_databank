#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Re-encode and pseudonymize Psytools CSV files (BL, FU1, FU2, FU3 and Stratify).

This script replaces the Scito pseudonymization pipeline.

==========
Attributes
==========

Input
-----

PSYTOOLS_BL_DERIVED_DIR : str
    Location of BL PSC1-encoded files.
PSYTOOLS_FU1_DERIVED_DIR : str
    Location of FU1 PSC1-encoded files.
PSYTOOLS_FU2_DERIVED_DIR : str
    Location of FU2 PSC1-encoded files.
PSYTOOLS_FU3_DERIVED_DIR : str
    Location of FU3 PSC1-encoded files.
PSYTOOLS_STRATIFY_DERIVED_DIR : str
    Location of Stratify PSC1-encoded files.

Output
------

PSYTOOLS_BL_PSC2_DIR : str
    Location of BL PSC2-encoded files.
PSYTOOLS_FU1_PSC2_DIR : str
    Location of FU1 PSC2-encoded files.
PSYTOOLS_FU2_PSC2_DIR : str
    Location of FU2 PSC2-encoded files.
PSYTOOLS_FU3_PSC2_DIR : str
    Location of FU3 PSC2-encoded files.
PSYTOOLS_STRATIFY_PSC2_DIR : str
    Location of Stratify PSC2-encoded files.

"""

PSYTOOLS_BL_DERIVED_DIR = '/tmp/imagen/BL/processed/psytools'
PSYTOOLS_BL_PSC2_DIR = '/neurospin/imagen/BL/processed/psytools'
PSYTOOLS_FU1_DERIVED_DIR = '/tmp/imagen/FU1/processed/psytools'
PSYTOOLS_FU1_PSC2_DIR = '/neurospin/imagen/FU1/processed/psytools'
PSYTOOLS_FU2_DERIVED_DIR = '/tmp/imagen/FU2/processed/psytools'
PSYTOOLS_FU2_PSC2_DIR = '/neurospin/imagen/FU2/processed/psytools'
PSYTOOLS_FU3_DERIVED_DIR = '/tmp/imagen/FU3/processed/psytools'
PSYTOOLS_FU3_PSC2_DIR = '/neurospin/imagen/FU3/processed/psytools'
PSYTOOLS_STRATIFY_DERIVED_DIR = '/tmp/imagen/STRATIFY/processed/psytools'
PSYTOOLS_STRATIFY_PSC2_DIR = '/neurospin/imagen/STRATIFY/processed/psytools'
PSYTOOLS_IMACOV19_BL_DERIVED_DIR = '/tmp/imagen/IMACOV19_BL/processed/psytools'
PSYTOOLS_IMACOV19_BL_PSC2_DIR = '/neurospin/imagen/IMACOV19_BL/processed/psytools'
PSYTOOLS_IMACOV19_FU_DERIVED_DIR = '/tmp/imagen/IMACOV19_FU/processed/psytools'
PSYTOOLS_IMACOV19_FU_PSC2_DIR = '/neurospin/imagen/IMACOV19_FU/processed/psytools'
PSYTOOLS_STRATICO19_BL_DERIVED_DIR = '/tmp/imagen/STRATICO19_BL/processed/psytools'
PSYTOOLS_STRATICO19_BL_PSC2_DIR = '/neurospin/imagen/STRATICO19_BL/processed/psytools'
PSYTOOLS_STRATICO19_FU_DERIVED_DIR = '/tmp/imagen/STRATICO19_FU/processed/psytools'
PSYTOOLS_STRATICO19_FU_PSC2_DIR = '/neurospin/imagen/STRATICO19_FU/processed/psytools'


import os
from csv import DictReader
from csv import DictWriter
from datetime import datetime
import logging
logging.basicConfig(level=logging.INFO)

# import ../imagen_databank
import sys
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..'))
from imagen_databank import PSC2_FROM_PSC1
from imagen_databank import DOB_FROM_PSC1


def _deidentify_legacy(psc2_from_psc1, psytools_path, psc2_path):
    """Anonymize and re-encode a legacy Psytools questionnaire from PSC1 to PSC2.

    Legacy questionnaires are in long format.

    Parameters
    ----------
    psc2_from_psc1: map
        Conversion table, from PSC1 to PSC2.
    psytools_path: str
        Input: PSC1-encoded Psytools file.
    psc2_path: str
        Output: PSC2-encoded Psytools file.

    """
    with open(psytools_path, 'r') as psc1_file:
        psc1_reader = DictReader(psc1_file, dialect='excel')

        # de-identify columns with timestamps
        ANONYMIZED_COLUMNS = {
            'Completed Timestamp': ('%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S'),
            'Processed Timestamp': ('%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S'),
        }
        convert = [fieldname for fieldname in psc1_reader.fieldnames
                   if fieldname in ANONYMIZED_COLUMNS]

        # discard other columns with dates
        DISCARDED_COLUMNS = {
            'id_check_dob', 'id_check_gender', 'id_check_relation',
            # FU3 / NI DATA
            'DATE_BIRTH_1', 'DATE_BIRTH_2', 'DATE_BIRTH_3',
            'TEST_DATE_1', 'TEST_DATE_2', 'TEST_DATE_3'
        }

        # read/process each row and save for later writing
        rows = {}
        for row in psc1_reader:
            psc1, suffix = row['User code'][:12], row['User code'][12:]
            if psc1 in PSC2_FROM_PSC1:
                psc2 = PSC2_FROM_PSC1[psc1]
                if suffix in {'-C', '-P', '-I'}:
                    # keep the suffix of Imagen subject IDs
                    #   -C  Child
                    #   -P  Parent
                    #   -I  Institute
                    row['User code'] = psc2 + suffix
                else:
                    # remove "FU3 and "SB" suffixes in Stratify and LimeSurvey-derived files
                    if suffix not in {'FU3', 'SB'}:
                        logging.error('unknown suffix %s in user code %s',
                                      suffix, row['User code'])
                    row['User code'] = psc2
            else:
                logging.error('unknown PSC1 code %s in user code %s',
                              psc1, row['User code'])
                continue

            # de-identify columns with timestamps
            for fieldname in convert:
                if psc1 in DOB_FROM_PSC1:
                    birth = DOB_FROM_PSC1[psc1]
                    for timestamp_format in ANONYMIZED_COLUMNS[fieldname]:
                        try:
                            timestamp = datetime.strptime(row[fieldname],
                                                          timestamp_format).date()
                        except ValueError:
                            continue
                        else:
                            age = timestamp - birth
                            row[fieldname] = str(age.days)
                            break
                    else:
                        logging.error('%s: invalid "%s": %s',
                                      psc1, fieldname, row[fieldname])
                        row[fieldname] = None
                else:
                    row[fieldname] = None

            # convert to age in days at date of birth - should be 0 if correct!
            # FU2 / ESPAD CHILD
            # FU2 / NI DATA
            for column in ('education_end', 'ni_period', 'ni_date'):
                if column in psc1_reader.fieldnames:
                    if psc1 in DOB_FROM_PSC1:
                        birth = DOB_FROM_PSC1[psc1]
                        try:
                            d = datetime.strptime(row[column],
                                                  '%d-%m-%Y').date()
                        except ValueError:
                            row[column] = None
                        else:
                            age = d - birth
                            row[column] = str(age.days)
                    else:
                        row[column] = None

            # convert to age of parents in days at assessment
            # BL/FU1 / PBQ
            for column in ('pbq_01', 'pbq_02'):
                if column in psc1_reader.fieldnames:
                    try:
                        birth = datetime.strptime(row[column],
                                                      '%d-%m-%Y').date()
                    except ValueError:
                        row[column] = None
                    else:
                        # last 'timestamp' ought to be 'Processed timestamp'
                        age = timestamp - birth
                        row[column] = str(age.days)

            # discard other columns with dates
            for column in DISCARDED_COLUMNS:
                if column in psc1_reader.fieldnames:
                    del row[column]

            rows.setdefault(psc2, []).append(row)

        # save rows into output file, sort by PSC2
        with open(psc2_path, 'w') as psc2_file:
            fieldnames = [fieldname for fieldname in psc1_reader.fieldnames
                          if fieldname not in DISCARDED_COLUMNS]
            psc2_writer = DictWriter(psc2_file, fieldnames, dialect='excel')
            psc2_writer.writeheader()
            for psc2 in sorted(rows):
                for row in rows[psc2]:
                    psc2_writer.writerow(row)


def _psc1(psc1, psc2_from_psc1):
    if 'TEST' in psc1.upper():
        # skip test subjects
        logging.debug('skipping test subject "%s"', psc1)
    else:
        # find and skip subjects with invalid identifier
        if psc1[-3:] in {'FU2', 'FU3'}:
            psc1 = psc1[:-3]
        elif psc1[-2:] == 'SB':
            psc1 = psc1[:-2]
        if psc1 in psc2_from_psc1:
            return psc1
        elif psc1 in {'0x0000xxxxxx'}:
            logging.info('skipping known invalid subject identifier "%s"',
                         psc1)
        else:
            logging.error('invalid subject identifier "%s"', psc1)
    return None


def _deidentify_lsrc2(psc2_from_psc1, psytools_path, psc2_path):
    """Anonymize and re-encode an LSRC2 Psytools questionnaire from PSC1 to PSC2.

    LSRC2 questionnaires are in wide format.

    Parameters
    ----------
    psc2_from_psc1: map
        Conversion table, from PSC1 to PSC2.
    psytools_path: str
        Input: PSC1-encoded Psytools file.
    psc2_path: str
        Output: PSC2-encoded Psytools file.

    """
    COLUMNS_TO_REMOVE = {
        'token',
        'ipaddr',
        'IdCheckGender',
        'IdCheckDob',
        'geoLoc_search',  # Covid-19 questionnaires
    }
    COLUMNS_WITH_DATE = {
        'startdate',
        'datestamp',
        'submitdate',
    }

    with open(psytools_path, 'r') as psc1_file:
        psc1_reader = DictReader(psc1_file, dialect='excel')
        # columns to remove entirely
        fieldnames = [x for x in psc1_reader.fieldnames
                      if x not in COLUMNS_TO_REMOVE]
        with open(psc2_path, 'w') as psc2_file:
            psc2_writer = DictWriter(psc2_file, fieldnames, dialect='excel')
            psc2_writer.writeheader()
            for row in psc1_reader:
                # skip test and invalid subjects
                psc1 = _psc1(row['id'], psc2_from_psc1)
                if psc1:
                    psc2 = psc2_from_psc1[psc1]
                    # columns to remove entirely
                    for x in COLUMNS_TO_REMOVE:
                        if x in row:
                            del row[x]
                    # columns to de-identify
                    row['id'] = psc2
                    for x in COLUMNS_WITH_DATE:
                        if x in row and row[x]:
                            date = datetime.strptime(row[x],
                                                     '%Y-%m-%d %H:%M:%S').date()
                            if psc1 in DOB_FROM_PSC1:
                                birth = DOB_FROM_PSC1[psc1]
                                age = date - birth
                                row[x] = age.days
                            else:
                                logging.error('unknown date of birth: "%s"',
                                              psc1)
                                row[x] = None
                    psc2_writer.writerow(row)


def deidentify(psc2_from_psc1, master_dir, psc2_dir):
    """Anonymize and re-encode Psytools questionnaires within a directory.

    PSC1-encoded files are read from `master_dir`, anoymized and converted
    from PSC1 codes to PSC2, and the result is written in `psc2_dir`.

    Parameters
    ----------
    psc2_from_psc1: map
        Conversion table, from PSC1 to PSC2.
    master_dir: str
        Input directory with PSC1-encoded questionnaires.
    psc2_dir: str
        Output directory with PSC2-encoded and anonymized questionnaires.

    """
    CURRENTLY_NOT_PROPERLY_DEIDENTIFIED = {
        'IMAGEN-IMGN_RELIABILITY_PI_FU2-BASIC_DIGEST.csv',
        'IMAGEN-IMGN_RELIABILITY_FU3-BASIC_DIGEST.csv',
        'STRATIFY_screening_(London).csv',
        'STRATIFY_screening_(Southampton).csv',
        'STRATIFY_screening_(ED).csv',
    }

    for filename in os.listdir(master_dir):
        if filename in CURRENTLY_NOT_PROPERLY_DEIDENTIFIED:
            continue
        master_path = os.path.join(master_dir, filename)
        psc2_path = os.path.join(psc2_dir, filename)
        if filename.startswith('IMAGEN-') or filename.startswith('STRATIFY-') or filename.startswith('IMACOV19-') or filename.startswith('STRATICO19-'):
            _deidentify_legacy(psc2_from_psc1, master_path, psc2_path)
        elif filename.startswith('Imagen_') or filename.startswith('STRATIFY_'):
            _deidentify_lsrc2(psc2_from_psc1, master_path, psc2_path)
        else:
            logging.error('skipping unknown file: %s', filename)


def main():
    deidentify(PSC2_FROM_PSC1,
               PSYTOOLS_BL_DERIVED_DIR, PSYTOOLS_BL_PSC2_DIR)
    deidentify(PSC2_FROM_PSC1,
               PSYTOOLS_FU1_DERIVED_DIR, PSYTOOLS_FU1_PSC2_DIR)
    deidentify(PSC2_FROM_PSC1,
               PSYTOOLS_FU2_DERIVED_DIR, PSYTOOLS_FU2_PSC2_DIR)
    deidentify(PSC2_FROM_PSC1,
               PSYTOOLS_FU3_DERIVED_DIR, PSYTOOLS_FU3_PSC2_DIR)
    deidentify(PSC2_FROM_PSC1,
               PSYTOOLS_STRATIFY_DERIVED_DIR, PSYTOOLS_STRATIFY_PSC2_DIR)
    deidentify(PSC2_FROM_PSC1,
               PSYTOOLS_IMACOV19_BL_DERIVED_DIR, PSYTOOLS_IMACOV19_BL_PSC2_DIR)
    deidentify(PSC2_FROM_PSC1,
               PSYTOOLS_IMACOV19_FU_DERIVED_DIR, PSYTOOLS_IMACOV19_FU_PSC2_DIR)
    deidentify(PSC2_FROM_PSC1,
               PSYTOOLS_STRATICO19_BL_DERIVED_DIR, PSYTOOLS_STRATICO19_BL_PSC2_DIR)
    deidentify(PSC2_FROM_PSC1,
               PSYTOOLS_STRATICO19_FU_DERIVED_DIR, PSYTOOLS_STRATICO19_FU_PSC2_DIR)


if __name__ == "__main__":
    main()
