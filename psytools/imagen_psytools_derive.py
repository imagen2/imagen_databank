#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Contatenate Psytools CSV files from LimeSurvey (FU3).

For each questionnaire, we download one file per centre from the
LimeSurvey server. Concatenate them before publishing.

==========
Attributes
==========

Input
-----

PSYTOOLS_FU3_PSC1_DIR : str
    Location of PSC1-encoded FU3 files.
PSYTOOLS_SB_PSC1_DIR : str
    Location of PSC1-encoded Stratify files.

Output
------

PSYTOOLS_FU3_DERIVED_DIR : str
    Location of concatenated FU3 files.
PSYTOOLS_SB_DERIVED_DIR : str
    Location of concatenated Stratify files.

"""

PSYTOOLS_FU3_PSC1_DIR = '/neurospin/imagen/FU3/RAW/PSC1/psytools'
PSYTOOLS_FU3_DERIVED_DIR = '/tmp/imagen/FU3/processed/psytools'
PSYTOOLS_SB_PSC1_DIR = '/neurospin/imagen/STRATIFY/RAW/PSC1/psytools'
PSYTOOLS_SB_DERIVED_DIR = '/tmp/imagen/STRATIFY/processed/psytools'


import os
import shutil
import re
import logging
logging.basicConfig(level=logging.INFO)


def process(psc1_dir, derived_dir, input_template, output):
    """Concatenate LimeSurvey questionnaires from different centres.

    Parameters
    ----------
    psc1_dir: str
        Input directory with PSC1-encoded questionnaires.
    derived_dir: str
        Output directory with concatenated questionnaires.
    input_template: str
        Regex specifies file names associated to a questionnaire.
    output: str
        File name of concatenated questionnaire.

    """
    psc1_paths = {}
    for filename in os.listdir(psc1_dir):
        match = input_template.match(filename)
        if match:
            center = match.group(1)
            psc1_paths[center] = os.path.join(psc1_dir, filename)

    CENTER_ORDER = ('London', 'Nottingham', 'Dublin', 'Berlin',
                    'Hamburg', 'Mannheim', 'Paris', 'Dresden',
                    'Southampton', 'Aachen')
    ordered_psc1_paths = []
    for center in CENTER_ORDER:
        if center in psc1_paths:
            ordered_psc1_paths.append(psc1_paths[center])
    for center in psc1_paths:
        if center not in CENTER_ORDER:
            ordered_psc1_paths.append(psc1_paths[center])

    derived_path = os.path.join(derived_dir, output)
    with open(derived_path, 'w') as derived:
        if ordered_psc1_paths:
            with open(ordered_psc1_paths[0],'r') as psc1:
                shutil.copyfileobj(psc1, derived)
            for psc1_path in ordered_psc1_paths[1:]:
                with open(psc1_path,'r') as psc1:
                    psc1.readline()  # remove 1st line
                    shutil.copyfileobj(psc1, derived)


def main():
    process(PSYTOOLS_FU3_PSC1_DIR, PSYTOOLS_FU3_DERIVED_DIR,
            re.compile('Imagen_FUIII-Core1-([^\)]*).csv'),
            'Imagen_FUIII-Core1.csv')
    process(PSYTOOLS_FU3_PSC1_DIR, PSYTOOLS_FU3_DERIVED_DIR,
            re.compile('Imagen_FUIII-Core2-([^\)]*).csv'),
            'Imagen_FUIII-Core2.csv')
    process(PSYTOOLS_FU3_PSC1_DIR, PSYTOOLS_FU3_DERIVED_DIR,
            re.compile('Imagen_FUII-Parent-([^\)]*).csv'),
            'Imagen_FUII-Parent.csv')
    process(PSYTOOLS_SB_PSC1_DIR, PSYTOOLS_SB_DERIVED_DIR,
            re.compile('STRATIFY_Core1_\(([^\)]*)\).csv'),
            'STRATIFY_Core1.csv')
    process(PSYTOOLS_SB_PSC1_DIR, PSYTOOLS_SB_DERIVED_DIR,
            re.compile('STRATIFY_Core2_\(([^\)]*)\).csv'),
            'STRATIFY_Core2.csv')


if __name__ == "__main__":
    main()
