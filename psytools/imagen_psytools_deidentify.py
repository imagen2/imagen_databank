#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Re-encode and anonymize Psytools CSV files (BL, FU1, FU2 and FU3).

This script replaces the Scito anoymization pipeline.

==========
Attributes
==========

Input
-----

PSYTOOLS_BL_MASTER_DIR : str
    Location of BL PSC1-encoded files.
PSYTOOLS_FU1_MASTER_DIR : str
    Location of FU1 PSC1-encoded files.
PSYTOOLS_FU2_MASTER_DIR : str
    Location of FU2 PSC1-encoded files.
PSYTOOLS_FU3_MASTER_DIR : str
    Location of FU3 PSC1-encoded files.

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

"""

PSYTOOLS_BL_MASTER_DIR = u'/neurospin/imagen/BL/RAW/PSC1/psytools'
PSYTOOLS_BL_PSC2_DIR = u'/neurospin/imagen/BL/RAW/PSC2/psytools'
PSYTOOLS_FU1_MASTER_DIR = u'/neurospin/imagen/FU1/RAW/PSC1/psytools'
PSYTOOLS_FU1_PSC2_DIR = u'/neurospin/imagen/FU1/RAW/PSC2/psytools'
PSYTOOLS_FU2_MASTER_DIR = u'/neurospin/imagen/FU2/RAW/PSC1/psytools'
PSYTOOLS_FU2_PSC2_DIR = u'/neurospin/imagen/FU2/RAW/PSC2/psytools'
PSYTOOLS_FU3_MASTER_DIR = u'/neurospin/imagen/FU3/RAW/PSC1/psytools'
PSYTOOLS_FU3_PSC2_DIR = u'/neurospin/imagen/FU3/RAW/PSC2/psytools'

import logging
logging.basicConfig(level=logging.INFO)

import os
from csv import DictReader
from csv import DictWriter
from datetime import datetime

# import ../imagen_databank
import sys
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..'))
from imagen_databank import PSC2_FROM_PSC1
from imagen_databank import DOB_FROM_PSC2


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

        # de-identify columns that contain dates
        ANONYMIZED_COLUMNS = {
            'Completed Timestamp': '%Y-%m-%d %H:%M:%S.%f',
            'Processed Timestamp': '%Y-%m-%d %H:%M:%S.%f',
        }
        convert = [fieldname for fieldname in psc1_reader.fieldnames
                   if fieldname in ANONYMIZED_COLUMNS]

        with open(psc2_path, 'w') as psc2_file:
            psc2_writer = DictWriter(psc2_file, psc1_reader.fieldnames, dialect='excel')
            psc2_writer.writeheader()
            for row in psc1_reader:
                trial = row['Trial']
                # Psytools files contain identifying data,
                # specifically lines containing items:
                # - id_check_dob
                # - id_check_gender
                #
                # As the name implies, the purpose of these items is
                # cross-checking and error detection. They should not
                # be used for scientific purposes.
                #
                # These items should therefore not be published in the
                # Imagen database.
                #
                # The Scito anoymization pipeline used not to filter
                # these items out. Since the Imagen V2 server exposes raw
                # Psytools files to end users, we need to remove these
                # items sooner, before importing the data into the
                # CubicWeb database.
                if 'id_check_' in trial:
                    logging.debug('skipping line with "id_check_" for %s',
                                  row['User code'])
                    continue

                # subject ID is PSC1 followed by either of:
                #   -C  Child
                #   -P  Parent
                #   -I  Institute
                psc1_suffix = row['User code'].rsplit('-', 1)
                psc1 = psc1_suffix[0]
                if psc1 in PSC2_FROM_PSC1:
                    psc2 = PSC2_FROM_PSC1[psc1]
                    if len(psc1_suffix) > 1:
                        psc2_suffix = '-'.join((psc2, psc1_suffix[1]))
                    else:
                        psc2_suffix = psc2
                    logging.debug('converting from %s to %s',
                                  row['User code'], psc2_suffix)
                    row['User code'] = psc2_suffix
                else:
                    u = psc1.upper()
                    if ('FOLLOWUP' in u or 'TEST' in u or 'MAREN' in u
                            or 'THOMAS_PRONK' in u):
                        logging.debug('skipping test subject %s',
                                      row['User code'])
                    else:
                        logging.error('unknown PSC1 code %s in user code %s',
                                      psc1, row['User code'])
                    continue

                # de-identify columns that contain dates
                for fieldname in convert:
                    if psc2 in DOB_FROM_PSC2:
                        birth = DOB_FROM_PSC2[psc2]
                        timestamp = datetime.strptime(row[fieldname],
                                                      ANONYMIZED_COLUMNS[fieldname]).date()
                        age = timestamp - birth
                        row[fieldname] = str(age.days)
                    else:
                        row[fieldname] = None

                # de-identify rows that contain dates
                #
                # FU2 / ESPAD CHILD
                # FU2 / NI DATA
                if trial in {'education_end', 'ni_period', 'ni_date'}:
                    if psc2 and psc2 in DOB_FROM_PSC2:
                        try:
                            event = datetime.strptime(row['Trial result'],
                                                      '%d-%m-%Y').date()
                        except ValueError:
                            row['Trial result'] = None
                        else:
                            birth = DOB_FROM_PSC2[psc2]
                            age = event - birth
                            row['Trial result'] = str(age.days)
                    else:
                        row['Trial result'] = None

                psc2_writer.writerow(row)


def _psc2(psc1):
    if 'TEST' in psc1.upper():
        # skip test subjects
        logging.debug('Skipping test subject "%s"', psc1)
    else:
        # find and skip subjects with invalid identifier
        if psc1[-3:] in {'FU2', 'FU3'}:
            psc1 = psc1[:-3]
        elif psc1[-2:] == 'SB':
            psc1 = psc1[:-2]
        if psc1 in PSC2_FROM_PSC1:
            return PSC2_FROM_PSC1[psc1]
        else:
            logging.error('Invalid subject identifier "%s"', psc1)
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
                psc2 = _psc2(row['id'])
                if psc2:
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
                            birth = DOB_FROM_PSC2[psc2]
                            age = date - birth
                            row[x] = age.days
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
    for filename in os.listdir(master_dir):
        master_path = os.path.join(master_dir, filename)
        psc2_path = os.path.join(psc2_dir, filename)
        if filename.startswith('IMAGEN-IMGN_'):
            _deidentify_legacy(psc2_from_psc1, master_path, psc2_path)
        elif filename.startswith('Imagen_') or filename.startswith('STRATIFY_'):
            _deidentify_lsrc2(psc2_from_psc1, master_path, psc2_path)
        else:
            logging.error('skipping unknown file: %s', filename)


def main():
    deidentify(PSC2_FROM_PSC1,
               PSYTOOLS_BL_MASTER_DIR, PSYTOOLS_BL_PSC2_DIR)
    deidentify(PSC2_FROM_PSC1,
               PSYTOOLS_FU1_MASTER_DIR, PSYTOOLS_FU1_PSC2_DIR)
    deidentify(PSC2_FROM_PSC1,
               PSYTOOLS_FU2_MASTER_DIR, PSYTOOLS_FU2_PSC2_DIR)
    deidentify(PSC2_FROM_PSC1,
               PSYTOOLS_FU3_MASTER_DIR, PSYTOOLS_FU3_PSC2_DIR)


if __name__ == "__main__":
    main()
