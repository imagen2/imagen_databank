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
            if psc1 in PSC2_FROM_PSC1:
                date = datetime.strptime(response[k], '%Y-%m-%d %H:%M:%S').date()
                birth = DOB_FROM_PSC2[PSC2_FROM_PSC1[psc1]]
                age = date - birth
                response[k] = age.days
            else:
                response[k] = None
    return response


def _tmp_cleanup_FU3(items):
    return [(k.replace('FU3', ''), v) for k, v in items]


def _tmp_detect_test(k):
    if 'TEST' in k.upper() or '060000123456' in k:
        return True
    else:
        return False


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
            responses['responses'] = [{PSC2_FROM_PSC1[k]: _anonymize(v, k)
                                       for k, v in _tmp_cleanup_FU3(r.items())
                                       if not _tmp_detect_test(k)}
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
