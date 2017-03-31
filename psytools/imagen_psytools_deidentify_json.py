#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Re-encode and anonymize Psytools JSON files (FU3).

==========
Attributes
==========

Input
-----

PSYTOOLS_FU3_MASTER_DIR : str
    Location of FU3 PSC1-encoded files.

Output
------

PSYTOOLS_FU3_PSC2_DIR : str
    Location of FU3 PSC2-encoded files.

"""

PSYTOOLS_FU3_MASTER_DIR = u'/neurospin/imagen/FU3/RAW/PSC1/psytools'
PSYTOOLS_FU3_PSC2_DIR = u'/neurospin/imagen/FU3/RAW/PSC2/psytools'

import os
import json
from datetime import datetime

import logging
logging.basicConfig(level=logging.INFO)

# import ../imagen_databank
import sys
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..'))
from imagen_databank import PSC2_FROM_PSC1
from imagen_databank import DOB_FROM_PSC2


def _anonymize(response, psc1):
    """
    """
    for k in ('id', 'token', 'ipaddr'):
        if k in response:
            del response[k]
    for k in ('startdate', 'datestamp', 'submitdate'):
        if k in response and response[k]:
            date = datetime.strptime(response[k], '%Y-%m-%d %H:%M:%S').date()
            birth = DOB_FROM_PSC2[PSC2_FROM_PSC1[psc1]]
            age = date - birth
            response[k] = age.days
    return response


def _psc2_response_from_psc1_reponse(psc1_response):
    """
    """
    psc2_response = {}
    for k, v in psc1_response.items():
        if 'TEST' in k.upper():
            continue
        if (k[-3:] == 'FU3' or k[-3:] == 'FU2'):
            # 'FU2' for outstanding FU2 Parent questionnaires
            k = k[:-3]
        if k in PSC2_FROM_PSC1:
            psc2_response[PSC2_FROM_PSC1[k]] = _anonymize(v, k)
        else:
             logging.error('Unknown subject identifier "%s"', k)
    return psc2_response


def _create_psc2_file(psytools_path, psc2_path):
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
        responses = json.load(psytools_file)

        if 'responses' in responses:
            responses['responses'] = [_psc2_response_from_psc1_reponse(r)
                                      for r in responses['responses']]
        with open(psc2_path, 'w') as psc2_file:
            json.dump(responses, psc2_file,
                      indent=4, separators=(',', ': '), sort_keys=True)


def create_psc2_files(master_dir, psc2_dir):
    """Anonymize and re-encode all Psytools JSON questionnaires in a directory.

    PSC1-encoded files are read from `master_dir`, anoymized and converted
    from PSC1 codes to PSC2, and the result is written in `psc2_dir`.

    Parameters
    ----------
    master_dir: str
        Input directory with PSC1-encoded questionnaires.
    psc2_dir: str
        Output directory with PSC2-encoded and pseudonymized questionnaires.

    """
    for master_file in os.listdir(master_dir):
        master_path = os.path.join(master_dir, master_file)
        psc2_path = os.path.join(psc2_dir, master_file)
        _create_psc2_file(master_path, psc2_path)


def main():
    create_psc2_files(PSYTOOLS_FU3_MASTER_DIR, PSYTOOLS_FU3_PSC2_DIR)


if __name__ == "__main__":
    main()
