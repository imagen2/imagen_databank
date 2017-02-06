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
from datetime import datetime

# import ../imagen_databank
import sys
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..'))
from imagen_databank import PSC2_FROM_PSC1
from imagen_databank import DOB_FROM_PSC2


def _create_psc2_file(psc2_from_psc1, psytools_path, psc2_path):
    """Anonymize and re-encode a Psytools questionnaire from PSC1 to PSC2.

    Parameters
    ----------
    psc2_from_psc1: map
        Conversion table, from PSC1 to PSC2.
    psytools_path: str
        Input: PSC1-encoded Psytools file.
    psc2_path: str
        Output: PSC2-encoded Psytools file.

    """
    with open(psytools_path, 'r') as psytools_file:
        # identify columns to anonymize in header
        header = psytools_file.readline().strip()
        convert = [i for i, field in enumerate(header.split(','))
                   if 'Timestamp' in field]
        with open(psc2_path, 'w') as psc2_file:
            psc2_file.write(header + '\n')
            for line in psytools_file:
                line = line.strip()
                items = line.split(',')
                psc1 = items[0]
                if 'id_check' in line:
                    # Psytools files contain identifying data,
                    # specifically lines containing items:
                    # - id_check_dob
                    # - id_check_gender
                    #
                    # As the name implies, the purpose of these items is
                    # cross-checking and error detection. They should not
                    # be used for scientific purposes.
                    #
                    # These items should therefore not be exposed to Imagen
                    # users.
                    #
                    # The Scito anoymization pipeline used not to filter
                    # these items out. Since the Imagen V2 server exposes raw
                    # Psytools files to end users, we need to remove these
                    # items sooner, before importing the data into the
                    # CubicWeb database.
                    logging.debug('skipping line with "id_check" from %s', psc1)
                    continue
                # subject ID is PSC1 followed by either of:
                #   -C  Child
                #   -P  Parent
                #   -I  Institute
                if '-' in psc1:
                    psc1, suffix = psc1.rsplit('-', 1)
                else:
                    suffix = None
                psc2 = None
                if (psc1.startswith('TEST') or
                        psc1.startswith('FOLLOWUP') or
                        psc1.startswith('THOMAS_PRONK') or
                        psc1.startswith('MAREN')):
                    logging.debug('skipping test subject {0}'
                                  .format(psc1))
                    continue
                elif psc1 in psc2_from_psc1:
                    logging.debug('converting subject %s from PSC1 to PSC2',
                                  psc1)
                    psc2 = psc2_from_psc1[psc1]
                    items[0] = '-'.join((psc2, suffix))
                else:
                    logging.error('PSC1 code missing from conversion table: %s',
                                  items[0])
                    continue
                for i in convert:
                    if psc2 is None or psc2 not in DOB_FROM_PSC2:
                        items[i] = ''
                    else:
                        timestamp = datetime.strptime(items[i],
                                                      '%Y-%m-%d %H:%M:%S.%f').date()
                        birth = DOB_FROM_PSC2[psc2]
                        age = timestamp - birth
                        items[i] = str(age.days)
                psc2_file.write(','.join(items) + '\n')


def create_psc2_files(psc2_from_psc1, master_dir, psc2_dir):
    """Anonymize and re-encode all psytools questionnaires within a directory.

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
    for master_file in os.listdir(master_dir):
        master_path = os.path.join(master_dir, master_file)
        psc2_path = os.path.join(psc2_dir, master_file)
        _create_psc2_file(psc2_from_psc1, master_path, psc2_path)


def main():
    create_psc2_files(PSC2_FROM_PSC1,
                      PSYTOOLS_BL_MASTER_DIR, PSYTOOLS_BL_PSC2_DIR)
    create_psc2_files(PSC2_FROM_PSC1,
                      PSYTOOLS_FU1_MASTER_DIR, PSYTOOLS_FU1_PSC2_DIR)
    create_psc2_files(PSC2_FROM_PSC1,
                      PSYTOOLS_FU2_MASTER_DIR, PSYTOOLS_FU2_PSC2_DIR)
    create_psc2_files(PSC2_FROM_PSC1,
                      PSYTOOLS_FU3_MASTER_DIR, PSYTOOLS_FU3_PSC2_DIR)


if __name__ == "__main__":
    main()
