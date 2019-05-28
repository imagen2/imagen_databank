#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Re-encode and anonymize DAWBA files (BL, FU1, FU2 and FU3).

This script replaces the Scito anoymization pipeline which does not
seem to be working anymore for DAWBA files.

==========
Attributes
==========

Input
-----

DAWBA_BL_MASTER_DIR : str
    Location of BL PSC1-encoded files.
DAWBA_FU1_MASTER_DIR : str
    Location of FU1 PSC1-encoded files.
DAWBA_FU2_MASTER_DIR : str
    Location of FU2 PSC1-encoded files.
DAWBA_FU3_MASTER_DIR : str
    Location of FU3 PSC1-encoded files.
DAWBA_SB_MASTER_DIR : str
    Location of Stratify PSC1-encoded files.

Output
------

DAWBA_BL_PSC2_DIR : str
    Location of BL PSC2-encoded files.
DAWBA_FU1_PSC2_DIR : str
    Location of FU1 PSC2-encoded files.
DAWBA_FU2_PSC2_DIR : str
    Location of FU2 PSC2-encoded files.
DAWBA_FU3_PSC2_DIR : str
    Location of FU3 PSC2-encoded files.
DAWBA_SB_PSC2_DIR : str
    Location of Stratify PSC2-encoded files.

"""

DAWBA_BL_MASTER_DIR = '/neurospin/imagen/BL/RAW/PSC1/dawba'
DAWBA_BL_PSC2_DIR = '/neurospin/imagen/BL/RAW/PSC2/dawba'
DAWBA_FU1_MASTER_DIR = '/neurospin/imagen/FU1/RAW/PSC1/dawba'
DAWBA_FU1_PSC2_DIR = '/neurospin/imagen/FU1/RAW/PSC2/dawba'
DAWBA_FU2_MASTER_DIR = '/neurospin/imagen/FU2/RAW/PSC1/dawba'
DAWBA_FU2_PSC2_DIR = '/neurospin/imagen/FU2/RAW/PSC2/dawba'
DAWBA_FU3_MASTER_DIR = '/neurospin/imagen/FU3/RAW/PSC1/dawba'
DAWBA_FU3_PSC2_DIR = '/neurospin/imagen/FU3/RAW/PSC2/dawba'
DAWBA_SB_MASTER_DIR = '/neurospin/imagen/STRATIFY/RAW/PSC1/dawba'
DAWBA_SB_PSC2_DIR = '/neurospin/imagen/STRATIFY/RAW/PSC2/dawba'

MISSING_DAWBA1_CODES = {
    # DAWBA1 codes, missing for some reason - just ignore them...
    '19042',
    '19044',
    '19045',
    '19046',
    '19047',
    '19048',
    '19049',
    '19050',
    '19051',
    '23094',
    '23095',
    '23096',
    '23097',
    '23098',
    '23099',
    '23100',
    '23101',
    '23102',
    '23103',
    '23104',
    '23105',
    '23106',
    '23107',
    '23108',
    '23109',
    '23110',
    '23112',
    '23881',
    '27361',
    '27512',
    '28117',
    '28694',
    '31469',
    '31470',
    '31471',
    '31473',
    '38297',
    '38298',
    '38299',
    '38300',
    '38301',
}
WITHDRAWN_DAWBA_CODES = {
    # see thread "DAWBA3 codes conversion table" from 2015-05-18
    '127657',
    # see thread "DAWBA3 codes conversion table" from 2015-12-15
    '128847',
    '127658',
    '132983',
    '129716',
    '129500',
    # see thread "Imagen: Dawba data 201490 acquired on 13 September 2015" on 2019-05-27
    '201490',
    # see thread "Imagen FU3 Dawba code 221867" on 2019-05-08
    '221867',
    # see thread "token management in Imagen FU3" on 2019-05-03
    '228686',
    '228691',
    # see thread "token management in Imagen FU3" on 2019-05-03
    '239204',
    '239230',
    # see thread "Imagen FU3 Dawba code 252346" on 2019-05-04
    '252346',
}

import os
from datetime import datetime

# import ../imagen_databank
import sys
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..'))
from imagen_databank import PSC1_FROM_DAWBA
from imagen_databank import PSC2_FROM_PSC1
from imagen_databank import DOB_FROM_PSC1

import logging
logging.basicConfig(level=logging.INFO)


def _create_psc2_file(dawba_path, psc2_path):
    """Anonymize and re-encode a DAWBA questionnaire from DAWBA to PSC2.

    DAWBA questionnaire files are CSV files.

    Columns containing a date will be modified and the date will converted to
    the age of the subject in days, as required by the anonymization process.

    Parameters
    ----------
    psc2_from_dawba: map
        Conversion table, from DAWBA to PSC2.
    dawba_path: str
        Input: DAWBA-encoded CSV file.
    psc2_path: str
        Output: PSC2-encoded CSV file.

    """
    with open(dawba_path, 'r') as dawba_file:
        # identify columns to anonymize/remove in header
        header = next(iter(dawba_file))
        items = header.split('\t')
        convert = {i for i, item in enumerate(items)
                   if 'sstartdate' in item or 'p1startdate' in item}
        skip = {i for i, item in enumerate(items)
                if 'ratername' in item or 'ratedate' in item}

        with open(psc2_path, 'w') as psc2_file:
            # write header
            items = [item for i, item in enumerate(items)
                     if i not in skip]
            psc2_file.write('\t'.join(items))
            if not items[-1].endswith('\n'):
                psc2_file.write('\n')

            # write data
            for line in dawba_file:
                items = line.split('\t')
                dawba = items[0]
                if dawba not in PSC1_FROM_DAWBA:
                    if dawba in WITHDRAWN_DAWBA_CODES:
                        logging.info('withdrawn DAWBA code: %s', dawba)
                    elif dawba in MISSING_DAWBA1_CODES:
                        logging.warning('missing DAWBA1 codes: %s', dawba)
                    else:
                        logging.error('DAWBA code missing from conversion table: %s',
                                      dawba)
                    continue
                psc1 = PSC1_FROM_DAWBA[dawba]
                if psc1 not in PSC2_FROM_PSC1:
                    logging.error('PSC1 code missing from conversion table: %s',
                                  psc1)
                    continue
                psc2 = PSC2_FROM_PSC1[psc1]
                logging.info('converting subject %s from DAWBA to PSC2',
                             psc1)
                items[0] = psc2
                # convert dates to subject age in days
                for i in convert:
                    if items[i] != '':
                        if psc1 in DOB_FROM_PSC1:
                            startdate = datetime.strptime(items[i],
                                                          '%d.%m.%y').date()
                            birthdate = DOB_FROM_PSC1[psc1]
                            age = startdate - birthdate
                            logging.info('age of subject %s: %d',
                                         psc1, age.days)
                            items[i] = str(age.days)
                        else:
                            items[i] = ''
                items = [item for i, item in enumerate(items)
                         if i not in skip]
                psc2_file.write('\t'.join(items))
                if not items[-1].endswith('\n'):
                    psc2_file.write('\n')


def create_psc2_files(master_dir, psc2_dir):
    """Anonymize and re-encode all DAWBA questionnaires within a directory.

    DAWBA-encoded files are read from `master_dir`, anoymized and converted
    from DAWBA codes to PSC2, and the result is written in `psc2_dir`.

    Parameters
    ----------
    master_dir: str
        Input directory with DAWBA-encoded questionnaires.
    psc2_dir: str
        Output directory with PSC2-encoded and anonymized questionnaires.

    """
    for master_file in os.listdir(master_dir):
        master_path = os.path.join(master_dir, master_file)
        psc2_path = os.path.join(psc2_dir, master_file)
        _create_psc2_file(master_path, psc2_path)


def main():
    create_psc2_files(DAWBA_BL_MASTER_DIR, DAWBA_BL_PSC2_DIR)
    create_psc2_files(DAWBA_FU1_MASTER_DIR, DAWBA_FU1_PSC2_DIR)
    create_psc2_files(DAWBA_FU2_MASTER_DIR, DAWBA_FU2_PSC2_DIR)
    create_psc2_files(DAWBA_FU3_MASTER_DIR, DAWBA_FU3_PSC2_DIR)
    create_psc2_files(DAWBA_SB_MASTER_DIR, DAWBA_SB_PSC2_DIR)


if __name__ == "__main__":
    main()
