#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Download Psytools CSV files (BL, FU1, FU2 and FU3).

This script replaces the one inherited from the initial Imagen team.
It uses the Python libraries instead of calling external programs.

==========
Attributes
==========

Output
------

PSYTOOLS_BL_MASTER_DIR : str
    Location of BL PSC1-encoded files.
PSYTOOLS_FU_MASTER_DIR : str
    Location of FU1 PSC1-encoded files.
PSYTOOLS_FU2_MASTER_DIR : str
    Location of FU2 PSC1-encoded files.
PSYTOOLS_FU3_MASTER_DIR : str
    Location of FU3 PSC1-encoded files.

"""

import logging
logging.basicConfig(level=logging.INFO)

import requests
from io import BytesIO, TextIOWrapper
import os
import gzip
import re


PSYTOOLS_BL_MASTER_DIR = '/neurospin/imagen/BL/RAW/PSC1/psytools'
PSYTOOLS_FU1_MASTER_DIR = '/neurospin/imagen/FU1/RAW/PSC1/psytools'
PSYTOOLS_FU2_MASTER_DIR = '/neurospin/imagen/FU2/RAW/PSC1/psytools'
PSYTOOLS_FU3_MASTER_DIR = '/neurospin/imagen/FU3/RAW/PSC1/psytools'

CSV_BASE_URL = 'https://www.delosis.com/psytools-server/dataservice/dataset/'

BASIC_DIGEST = 'Basic digest'
IMAGEN_DIGEST = 'Imagen digest'
IMAGEN_SURVEY_DIGEST = 'Imagen survey digest'

CSV_DATASETS = (
    ('IMGN_ESPAD_PARENT_RC5', BASIC_DIGEST),  # (Drug Use Questionnaire)
    ('IMGN_GEN_RC5', BASIC_DIGEST),  # (Family history)
    ('IMGN_IDENT_RC5', IMAGEN_DIGEST),  # (Faces Task)
    ('IMGN_KIRBY_RC5', IMAGEN_DIGEST),  # (Now or later?)
    ('IMGN_MAST_PARENT_RC5', BASIC_DIGEST),  # (Alcohol questionnaire II)
    ('IMGN_NEO_FFI_PARENT_RC5', BASIC_DIGEST),  # (Personality I)
    ('IMGN_NI_DATA_RC5', BASIC_DIGEST),  # (NI data entry)
    ('IMGN_PALP_3_1_RC5', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMGN_AUDIT_CHILD_RC5', IMAGEN_DIGEST),  # (Alcohol Questionnaire)
    ('IMGN_AUDIT_PARENT_RC5', BASIC_DIGEST),  # (Alcohol Questionnaire I)
    ('IMGN_CTS_PARENT_RC5', BASIC_DIGEST),  # (Relationship questionnaire)
    ('IMGN_DOT_PROBE_RC5', IMAGEN_DIGEST),  # (Dot Identification Task)
    ('IMGN_ESPAD_CHILD_RC5', IMAGEN_DIGEST),  # (Drug Use Questionnaire)
    ('IMGN_PALP_1_1_RC5', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMGN_PALP_1_2_RC5', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMGN_PALP_1_3_RC5', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMGN_PALP_1_4_RC5', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMGN_PALP_2_1_RC5', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMGN_PALP_2_2_RC5', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMGN_PALP_2_3_RC5', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMGN_PALP_2_4_RC5', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMGN_PALP_3_2_RC5', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMGN_PALP_3_3_RC5', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMGN_PALP_3_4_RC5', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMGN_PDS_RC5', IMAGEN_DIGEST),  # (Physical Development)
    ('IMGN_SURPS_PARENT_RC5', BASIC_DIGEST),  # (Personality III)
    ('IMGN_TCI_CHILD_RC5', IMAGEN_DIGEST),  # (Personality II)
    ('IMGN_TCI_PARENT_RC5', BASIC_DIGEST),  # (Personality II)
    ('IMGN_TLFB_RC5', BASIC_DIGEST),  # (TLFB)
    ('IMGN_NEO_FFI_CHILD_RC5', IMAGEN_SURVEY_DIGEST),  # (Personality I)
    ('IMGN_SURPS_RC5', IMAGEN_SURVEY_DIGEST),  # (Personality III)
    ('IMGN_LEQ_RC5', BASIC_DIGEST),  # (Life Events)
    ('IMGN_AUDIT_CHILD_FU_RC5', IMAGEN_DIGEST),  # (Alcohol Questionnaire)
    ('IMGN_AUDIT_PARENT_FU_RC5', BASIC_DIGEST),  # (Alcohol Questionnaire I)
    ('IMGN_CONSENT_FU_RC1', IMAGEN_DIGEST),  # (Further information about the study)
    ('IMGN_ESPAD_PARENT_FU_RC5', BASIC_DIGEST),  # (Drug Use Questionnaire)
    ('IMGN_KIRBY_FU_RC5', IMAGEN_DIGEST),  # (Now or later?)
    ('IMGN_MAST_PARENT_FU_RC5', BASIC_DIGEST),  # (Alcohol questionnaire II)
    ('IMGN_NEO_FFI_CHILD_FU_RC5', IMAGEN_SURVEY_DIGEST),  # (Personality I)
    ('IMGN_NEO_FFI_PARENT_FU_RC5', BASIC_DIGEST),  # (Personality I)
    ('IMGN_PDS_FU_RC5', IMAGEN_DIGEST),  # (Physical Development)
    ('IMGN_SURPS_FU_RC5', IMAGEN_SURVEY_DIGEST),  # (Personality III)
    ('IMGN_SURPS_PARENT_FU_RC5', BASIC_DIGEST),  # (Personality III)
    ('IMGN_TCI_CHILD_FU_RC5', IMAGEN_DIGEST),  # (Personality II)
    ('IMGN_TCI_PARENT_FU_RC5', BASIC_DIGEST),  # (Personality II)
    ('IMGN_FU_RELIABILITY', BASIC_DIGEST),  # (Reliability)
    ('IMGN_TLFB_FU_RC5', BASIC_DIGEST),  # (TLFB)
    ('IMGN_PBQ_RC1', BASIC_DIGEST),  # (Pregnancy and Birth)
    ('IMGN_PBQ_FU_RC1', BASIC_DIGEST),   # (Pregnancy and Birth)
    ('IMGN_ESPAD_CHILD_FU_RC5', IMAGEN_DIGEST),  # (Drug Use Questionnaire)
    ('IMGN_ESPAD_INTERVIEW_FU', IMAGEN_DIGEST),  # (Drug Use Questionnaire)
    ('IMGN_AUDIT_INTERVIEW_FU', IMAGEN_DIGEST),  # (Alcohol Questionnaire)
    ('IMGN_LEQ_FU_RC5', IMAGEN_DIGEST),  # (Life Events)
    ('IMGN_SRC', IMAGEN_DIGEST),  # (SRC)
    ('IMGN_CSI_CHILD_FU', IMAGEN_DIGEST),  # (Part 2 - Physical Symptoms Questionnaire)
    ('IMGN_ADSR_CHILD_FU', IMAGEN_DIGEST),  # (Your mood)
    ('IMGN_SRS_PARENT_FU', IMAGEN_DIGEST),  # (Social Responsiveness Scale)
    ('IMGN_STUTT_PARENT_FU', IMAGEN_DIGEST),  # (Part 2 - Stuttering questionnaire)
    ('IMGN_FBBHKS_PARENT_FU', IMAGEN_DIGEST),  # (Part 2 - Rating scale for parents)
    ('IMGN_GATEWAY_FU_CHILD', BASIC_DIGEST),  # (Part 2 - Optional)
    ('IMGN_GATEWAY_FU_PARENT', BASIC_DIGEST),  # (Part 2 - Optional)
    ('IMGN_FU_RELIABILITY_ADDITIONAL', BASIC_DIGEST),  # (Reliability Additional)
    ('IMGN_AUDIT_CHILD_FU2', IMAGEN_DIGEST),  # (ALCOHOL Questionnaire)
    ('IMGN_NEO_FFI_FU2', IMAGEN_SURVEY_DIGEST),  # (Personality I)
    ('IMGN_ESPAD_CHILD_FU2', IMAGEN_DIGEST),  # (Drug Use Questionnaire)
    ('IMGN_TCI_CHILD_FU2', IMAGEN_DIGEST),  # (Personality II)
    ('IMGN_SURPS_FU2', IMAGEN_SURVEY_DIGEST),  # (Personality III)
    ('IMGN_KIRBY_FU2', IMAGEN_DIGEST),  # (Now or later?)
    ('IMGN_LEQ_FU2', IMAGEN_DIGEST),  # (Life Events)
    ('IMGN_PAAQ_CHILD_FU2', IMAGEN_DIGEST),  # (Childhood Relationships Questionnaire )
    ('IMGN_SRC_FU2', IMAGEN_DIGEST),  # (SRC)
    ('IMGN_IDENT_FU2', IMAGEN_DIGEST),  # (Faces Task)
    ('IMGN_PALP_1_1_FU2', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMGN_PALP_1_2_FU2', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMGN_PALP_1_3_FU2', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMGN_PALP_1_4_FU2', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMGN_PALP_2_1_FU2', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMGN_PALP_2_2_FU2', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMGN_PALP_2_3_FU2', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMGN_PALP_2_4_FU2', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMGN_PALP_3_3_FU2', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMGN_PALP_3_4_FU2', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMGN_GATEWAY_FU2_2', BASIC_DIGEST),  # (Part 2 - Optional)
    ('IMGN_CSI_CHILD_FU2', IMAGEN_DIGEST),  # (Part 2 - Physical Symptoms Questionnaire)
    ('IMGN_ADSR_CHILD_FU2', IMAGEN_DIGEST),  # (Part 2 - Your mood)
    ('IMGN_TFEQ_CHILD_FU2', IMAGEN_DIGEST),  # (Eating Habits II)
    ('IMGN_RRS_CHILD_FU2', IMAGEN_DIGEST),  # (Part 2 - Feelings and Thoughts)
    ('IMGN_EDEQ_CHILD_FU2', IMAGEN_DIGEST),  # (Eating Habits II)
    ('IMGN_TCI3_CHILD_FU2', IMAGEN_DIGEST),  # (Personality V)
    ('IMGN_VIDGAME_CHILD_FU2', IMAGEN_DIGEST),  # (Video-Gaming )
    ('IMGN_GATEWAY_FU2_3', BASIC_DIGEST),  # (Part 3 - Optional)
    ('IMGN_CAPE_CHILD_FU2', IMAGEN_DIGEST),  # (CAPE-state)
    ('IMGN_CONSENT_FU2', IMAGEN_DIGEST),  # (Further information about the study)
    ('IMGN_CIDS_PARENT_FU2', IMAGEN_DIGEST),  # (CID-S)
    ('IMGN_HRQOL_PARENT_FU2', IMAGEN_DIGEST),  # (Quality of Life Questionnaire )
    ('IMGN_K6PLUS_PARENT_FU2', IMAGEN_DIGEST),  # (Your Mood)
    ('IMGN_PHQ_PARENT_FU2', IMAGEN_DIGEST),  # (PHQ)
    ('IMGN_CTQ_CHILD_FU2', IMAGEN_DIGEST),  # (CTQ)
    ('IMGN_AUDIT_INTERVIEW_FU2', IMAGEN_DIGEST),  # (ALCOHOL Questionnaire)
    ('IMGN_ESPAD_INTERVIEW_FU2', IMAGEN_DIGEST),  # (Drug Use Questionnaire)
    ('IMGN_TLFB_FU2', IMAGEN_DIGEST),  # (TLFB)
    ('IMGN_NI_DATA_FU2', IMAGEN_DIGEST),  # (NI data entry)
    ('IMGN_IRI_CHILD_FU', IMAGEN_DIGEST),  # (Part 2 - Personality IV: Relating to other people)
    ('IMGN_IRI_CHILD_FU2', IMAGEN_DIGEST),  # (Part 2 - Personality IV: Relating to other people)
    ('IMGN_BIS_CHILD_FU2', IMAGEN_DIGEST),  # (BIS)
    ('IMGN_ANXDX_CHILD_FU2', IMAGEN_DIGEST),  # (Your Feelings)
    ('IMGN_URBANICITY_FU2', IMAGEN_DIGEST),  # (URBANICITY)
    ('IMGN_MAST_CHILD_FU2', IMAGEN_DIGEST),  # (ALCOHOL questionnaire II)
    ('IMGN_RELIABILITY_CORE_CHILD_FU2', BASIC_DIGEST),  # (FU2 Reliability Child Core)
    ('IMGN_RELIABILITY_OPT_FU2', BASIC_DIGEST),  # (FU2 Reliability Optional)
    ('IMGN_RELIABILITY_PI_FU2', BASIC_DIGEST),  # (FU2 Reliability Parent Institute )
    ('IMGN_PDS_FU2', IMAGEN_DIGEST),  # (Physical Development)
    ('IMGN_PALP_3_1_FU2', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMGN_PALP_3_2_FU2', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
)

QUOTED_PATTERN = re.compile(r'".*?"', re.DOTALL)


def download_csv(base_url, datasets):
    for task, digest in datasets:
        digest = digest.upper().replace(' ', '_')
        dataset = 'IMAGEN-{task}-{digest}.csv'.format(task=task, digest=digest)
        logging.info('downloading: %s', dataset)
        url = base_url + dataset + '.gz'
        # let Requests use ~/.netrc instead of passing an auth parameter
        #     auth = requests.auth.HTTPBasicAuth('...', '...')
        r = requests.get(url)
        compressed_data = BytesIO(r.content)
        with gzip.GzipFile(fileobj=compressed_data) as uncompressed_data:
            # unfold quoted text spanning multiple lines
            uncompressed_data = TextIOWrapper(uncompressed_data)
            data = QUOTED_PATTERN.sub(lambda x: x.group().replace('\n', '/'),
                                      uncompressed_data.read())
            # break down into different directories, one for each timepoint
            if '_FU3' in task:
                psytools_dir = PSYTOOLS_FU3_MASTER_DIR
            elif '_FU2' in task:
                psytools_dir = PSYTOOLS_FU2_MASTER_DIR
            elif '_FU' in task:
                psytools_dir = PSYTOOLS_FU1_MASTER_DIR
            else:
                psytools_dir = PSYTOOLS_BL_MASTER_DIR
            psytools_path = os.path.join(psytools_dir, dataset)
            # skip files that have not changed since last update
            if os.path.isfile(psytools_path):
                with open(psytools_path, 'r') as uncompressed_file:
                    if uncompressed_file.read() == data:
                        logging.info('skip unchanged file: %s', psytools_path)
                        continue
            # write downloaded data into file
            with open(psytools_path, 'w') as uncompressed_file:
                logging.info('write file: %s', psytools_path)
                uncompressed_file.write(data)


def main():
    download_csv(CSV_BASE_URL, CSV_DATASETS)


if __name__ == "__main__":
    main()
