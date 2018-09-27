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

PSYTOOLS_FU3_PSC2_DIR : str
    Location of PSC2-encoded FU3 files.
PSYTOOLS_SB_PSC2_DIR : str
    Location of PSC2-encoded Stratify files.

Output
------

PSYTOOLS_FU3_PROCESSED_DIR : str
    Location of concatenated FU3 files.
PSYTOOLS_SB_PROCESSED_DIR : str
    Location of concatenated Stratify files.

"""

PSYTOOLS_FU3_PSC2_DIR = '/neurospin/imagen/FU3/RAW/PSC2/psytools'
PSYTOOLS_FU3_PROCESSED_DIR = '/neurospin/imagen/FU3/processed/psytools'
PSYTOOLS_SB_PSC2_DIR = '/neurospin/imagen/SB/RAW/PSC2/psytools'
PSYTOOLS_SB_PROCESSED_DIR = '/neurospin/imagen/SB/processed/psytools'


import os
import shutil
import re
import logging
logging.basicConfig(level=logging.INFO)


def process(psc2_dir, processed_dir, input_template, output):
    """Concatenate LimeSurvey questionnaires from different centres.

    Parameters
    ----------
    psc2_dir: str
        Input directory with PSC2-encoded questionnaires.
    processed_dir: str
        Output directory with concatenated questionnaires.
    input_template: str
        Regex specifies file names associated to a questionnaire.
    output: str
        File name of concatenated questionnaire.

    """
    psc2_paths = {}
    for filename in os.listdir(psc2_dir):
        match = input_template.match(filename)
        if match:
            center = match.group(1)
            psc2_paths[center] = os.path.join(psc2_dir, filename)

    CENTER_ORDER = ('London', 'Nottingham', 'Dublin', 'Berlin',
                    'Hamburg', 'Mannheim', 'Paris', 'Dresden',
                    'Southampton', 'Aachen')
    ordered_psc2_paths = []
    for center in CENTER_ORDER:
        if center in psc2_paths:
            ordered_psc2_paths.append(psc2_paths[center])
    for center in psc2_paths:
        if center not in CENTER_ORDER:
            ordered_psc2_paths.append(psc2_paths[center])

    processed_path = os.path.join(processed_dir, output)
    with open(processed_path, 'w') as processed:
        if ordered_psc2_paths:
            with open(ordered_psc2_paths[0],'r') as psc2:
                shutil.copyfileobj(psc2, processed)
            for psc2_path in ordered_psc2_paths[1:]:
                with open(psc2_path,'r') as psc2:
                    psc2.readline()  # remove 1st line
                    shutil.copyfileobj(psc2, processed)


def main():
    process(PSYTOOLS_FU3_PSC2_DIR, PSYTOOLS_FU3_PROCESSED_DIR,
            re.compile('Imagen_FUIII-Core1-([^\)]*).csv'),
            'Imagen_FUIII-Core1.csv')
    process(PSYTOOLS_FU3_PSC2_DIR, PSYTOOLS_FU3_PROCESSED_DIR,
            re.compile('Imagen_FUIII-Core2-([^\)]*).csv'),
            'Imagen_FUIII-Core2.csv')
    process(PSYTOOLS_FU3_PSC2_DIR, PSYTOOLS_FU3_PROCESSED_DIR,
            re.compile('Imagen_FUII-Parent-([^\)]*).csv'),
            'Imagen_FUII-Parent.csv')
    process(PSYTOOLS_SB_PSC2_DIR, PSYTOOLS_SB_PROCESSED_DIR,
            re.compile('STRATIFY_Core1_\(([^\)]*)\).csv'),
            'STRATIFY_Core1.csv')
    process(PSYTOOLS_SB_PSC2_DIR, PSYTOOLS_SB_PROCESSED_DIR,
            re.compile('STRATIFY_Core2_\(([^\)]*)\).csv'),
            'STRATIFY_Core2.csv')


if __name__ == "__main__":
    main()
