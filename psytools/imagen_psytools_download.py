#!/usr/bin/env python3
"""Download CSV Psytools files from Delosis server.

This script replaces the one inherited from the initial Imagen team.
It uses Python libraries instead of calling external programs and was
expanded to the new LimeSurvey 2 service (Imagen FU3 and Stratify B)
in 2016.

==========
Attributes
==========

Output
------

PSYTOOLS_BL_MASTER_DIR : str
    Location of Imagen BL PSC1-encoded files.
PSYTOOLS_FU_MASTER_DIR : str
    Location of Imagen FU1 PSC1-encoded files.
PSYTOOLS_FU2_MASTER_DIR : str
    Location of Imagen FU2 PSC1-encoded files.
PSYTOOLS_FU3_MASTER_DIR : str
    Location of Imagen FU3 PSC1-encoded files.
PSYTOOLS_STRATIFY_MASTER_DIR : str
    Location of Stratify B PSC1-encoded files.

"""

import os
import gzip
import io
import re
import requests
import json
import base64
import csv
from urllib.parse import urlparse
import logging
logging.basicConfig(level=logging.INFO)

PSYTOOLS_IMAGEN_BL_MASTER_DIR = '/neurospin/imagen/BL/RAW/PSC1/psytools'
PSYTOOLS_IMAGEN_FU1_MASTER_DIR = '/neurospin/imagen/FU1/RAW/PSC1/psytools'
PSYTOOLS_IMAGEN_FU2_MASTER_DIR = '/neurospin/imagen/FU2/RAW/PSC1/psytools'
PSYTOOLS_IMAGEN_FU3_MASTER_DIR = '/neurospin/imagen/FU3/RAW/PSC1/psytools'
PSYTOOLS_STRATIFY_MASTER_DIR = '/neurospin/imagen/STRATIFY/RAW/PSC1/psytools'
PSYTOOLS_IMACOV19_BL_MASTER_DIR = '/neurospin/imagen/IMACOV19_BL/RAW/PSC1/psytools'
PSYTOOLS_IMACOV19_FU_MASTER_DIR = '/neurospin/imagen/IMACOV19_FU/RAW/PSC1/psytools'
PSYTOOLS_IMACOV19_FU2_MASTER_DIR = '/neurospin/imagen/IMACOV19_FU2/RAW/PSC1/psytools'
PSYTOOLS_IMACOV19_FU3_MASTER_DIR = '/neurospin/imagen/IMACOV19_FU3/RAW/PSC1/psytools'
PSYTOOLS_IMACOV19_FU4_MASTER_DIR = '/neurospin/imagen/IMACOV19_FU4/RAW/PSC1/psytools'
PSYTOOLS_STRATICO19_BL_MASTER_DIR = '/neurospin/imagen/STRATICO19_BL/RAW/PSC1/psytools'
PSYTOOLS_STRATICO19_FU_MASTER_DIR = '/neurospin/imagen/STRATICO19_FU/RAW/PSC1/psytools'
PSYTOOLS_STRATICO19_FU2_MASTER_DIR = '/neurospin/imagen/STRATICO19_FU2/RAW/PSC1/psytools'
PSYTOOLS_STRATICO19_FU3_MASTER_DIR = '/neurospin/imagen/STRATICO19_FU3/RAW/PSC1/psytools'
PSYTOOLS_STRATICO19_FU4_MASTER_DIR = '/neurospin/imagen/STRATICO19_FU4/RAW/PSC1/psytools'

LEGACY_BASE_URL = 'https://www.delosis.com/psytools-server/dataservice/dataset/'
LSRC2_BASE_URL = 'https://www.delosis.com/qs/index.php/admin/remotecontrol'
# Since the server is the identical but credentials are different and
# ~/.netrc allows only a single set of credentials per server, store
# credentials in separate file.
IMAGEN_NETRC_FILE = '~/.netrc.imagen'
STRATIFY_NETRC_FILE = '~/.netrc.stratify'
LSRC2_NETRC_FILE = '~/.lsrc2'

# The legacy service offers different digest formats for exporting data.
BASIC_DIGEST = 'Basic digest'
IMAGEN_DIGEST = 'Imagen digest'
IMAGEN_SURVEY_DIGEST = 'Imagen survey digest'
IMAGEN_KIRBY_DIGEST = 'Imagen Kirby digest'

# Unlike LSRC2, the legacy service does not advertise available surveys,
# so we copy the list of surveys from the graphical interface.
IMAGEN_LEGACY_CSV_DATASETS = (
    ('IMAGEN-IMGN_ESPAD_PARENT_RC5', BASIC_DIGEST),  # (Drug Use Questionnaire)
    ('IMAGEN-IMGN_GEN_RC5', BASIC_DIGEST),  # (Family history)
    ('IMAGEN-IMGN_IDENT_RC5', IMAGEN_DIGEST),  # (Faces Task)
    ('IMAGEN-IMGN_MAST_PARENT_RC5', BASIC_DIGEST),  # (Alcohol questionnaire II)
    ('IMAGEN-IMGN_NEO_FFI_PARENT_RC5', BASIC_DIGEST),  # (Personality I)
    ('IMAGEN-IMGN_NI_DATA_RC5', BASIC_DIGEST),  # (NI data entry)
    ('IMAGEN-IMGN_PALP_3_1_RC5', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMAGEN-IMGN_AUDIT_CHILD_RC5', IMAGEN_DIGEST),  # (Alcohol Questionnaire)
    ('IMAGEN-IMGN_AUDIT_PARENT_RC5', BASIC_DIGEST),  # (Alcohol Questionnaire I)
    ('IMAGEN-IMGN_CTS_PARENT_RC5', BASIC_DIGEST),  # (Relationship questionnaire)
    ('IMAGEN-IMGN_DOT_PROBE_RC5', IMAGEN_DIGEST),  # (Dot Identification Task)
    ('IMAGEN-IMGN_ESPAD_CHILD_RC5', IMAGEN_DIGEST),  # (Drug Use Questionnaire)
    ('IMAGEN-IMGN_PALP_1_1_RC5', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMAGEN-IMGN_PALP_1_2_RC5', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMAGEN-IMGN_PALP_1_3_RC5', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMAGEN-IMGN_PALP_1_4_RC5', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMAGEN-IMGN_PALP_2_1_RC5', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMAGEN-IMGN_PALP_2_2_RC5', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMAGEN-IMGN_PALP_2_3_RC5', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMAGEN-IMGN_PALP_2_4_RC5', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMAGEN-IMGN_PALP_3_2_RC5', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMAGEN-IMGN_PALP_3_3_RC5', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMAGEN-IMGN_PALP_3_4_RC5', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMAGEN-IMGN_PDS_RC5', IMAGEN_DIGEST),  # (Physical Development)
    ('IMAGEN-IMGN_SURPS_PARENT_RC5', BASIC_DIGEST),  # (Personality III)
    ('IMAGEN-IMGN_TCI_CHILD_RC5', IMAGEN_DIGEST),  # (Personality II)
    ('IMAGEN-IMGN_TCI_PARENT_RC5', BASIC_DIGEST),  # (Personality II)
    ('IMAGEN-IMGN_TLFB_RC5', BASIC_DIGEST),  # (TLFB)
    ('IMAGEN-IMGN_NEO_FFI_CHILD_RC5', IMAGEN_SURVEY_DIGEST),  # (Personality I)
    ('IMAGEN-IMGN_SURPS_RC5', IMAGEN_SURVEY_DIGEST),  # (Personality III)
    ('IMAGEN-IMGN_LEQ_RC5', BASIC_DIGEST),  # (Life Events)
    ('IMAGEN-IMGN_AUDIT_CHILD_FU_RC5', IMAGEN_DIGEST),  # (Alcohol Questionnaire)
    ('IMAGEN-IMGN_AUDIT_PARENT_FU_RC5', BASIC_DIGEST),  # (Alcohol Questionnaire I)
    ('IMAGEN-IMGN_CONSENT_FU_RC1', IMAGEN_DIGEST),  # (Further information about the study)
    ('IMAGEN-IMGN_ESPAD_PARENT_FU_RC5', BASIC_DIGEST),  # (Drug Use Questionnaire)
    ('IMAGEN-IMGN_MAST_PARENT_FU_RC5', BASIC_DIGEST),  # (Alcohol questionnaire II)
    ('IMAGEN-IMGN_NEO_FFI_CHILD_FU_RC5', IMAGEN_SURVEY_DIGEST),  # (Personality I)
    ('IMAGEN-IMGN_NEO_FFI_PARENT_FU_RC5', BASIC_DIGEST),  # (Personality I)
    ('IMAGEN-IMGN_PDS_FU_RC5', IMAGEN_DIGEST),  # (Physical Development)
    ('IMAGEN-IMGN_SURPS_FU_RC5', IMAGEN_SURVEY_DIGEST),  # (Personality III)
    ('IMAGEN-IMGN_SURPS_PARENT_FU_RC5', BASIC_DIGEST),  # (Personality III)
    ('IMAGEN-IMGN_TCI_CHILD_FU_RC5', IMAGEN_DIGEST),  # (Personality II)
    ('IMAGEN-IMGN_TCI_PARENT_FU_RC5', BASIC_DIGEST),  # (Personality II)
    ('IMAGEN-IMGN_FU_RELIABILITY', BASIC_DIGEST),  # (Reliability)
    ('IMAGEN-IMGN_TLFB_FU_RC5', BASIC_DIGEST),  # (TLFB)
    ('IMAGEN-IMGN_PBQ_RC1', BASIC_DIGEST),  # (Pregnancy and Birth)
    ('IMAGEN-IMGN_PBQ_FU_RC1', BASIC_DIGEST),   # (Pregnancy and Birth)
    ('IMAGEN-IMGN_ESPAD_CHILD_FU_RC5', IMAGEN_DIGEST),  # (Drug Use Questionnaire)
    ('IMAGEN-IMGN_ESPAD_INTERVIEW_FU', IMAGEN_DIGEST),  # (Drug Use Questionnaire)
    ('IMAGEN-IMGN_AUDIT_INTERVIEW_FU', IMAGEN_DIGEST),  # (Alcohol Questionnaire)
    ('IMAGEN-IMGN_LEQ_FU_RC5', IMAGEN_DIGEST),  # (Life Events)
    ('IMAGEN-IMGN_SRC', IMAGEN_DIGEST),  # (SRC)
    ('IMAGEN-IMGN_CSI_CHILD_FU', IMAGEN_DIGEST),  # (Part 2 - Physical Symptoms Questionnaire)
    ('IMAGEN-IMGN_ADSR_CHILD_FU', IMAGEN_DIGEST),  # (Your mood)
    ('IMAGEN-IMGN_SRS_PARENT_FU', IMAGEN_DIGEST),  # (Social Responsiveness Scale)
    ('IMAGEN-IMGN_STUTT_PARENT_FU', IMAGEN_DIGEST),  # (Part 2 - Stuttering questionnaire)
    ('IMAGEN-IMGN_FBBHKS_PARENT_FU', IMAGEN_DIGEST),  # (Part 2 - Rating scale for parents)
    ('IMAGEN-IMGN_GATEWAY_FU_CHILD', BASIC_DIGEST),  # (Part 2 - Optional)
    ('IMAGEN-IMGN_GATEWAY_FU_PARENT', BASIC_DIGEST),  # (Part 2 - Optional)
    ('IMAGEN-IMGN_FU_RELIABILITY_ADDITIONAL', BASIC_DIGEST),  # (Reliability Additional)
    ('IMAGEN-IMGN_AUDIT_CHILD_FU2', IMAGEN_DIGEST),  # (ALCOHOL Questionnaire)
    ('IMAGEN-IMGN_NEO_FFI_FU2', IMAGEN_SURVEY_DIGEST),  # (Personality I)
    ('IMAGEN-IMGN_ESPAD_CHILD_FU2', IMAGEN_DIGEST),  # (Drug Use Questionnaire)
    ('IMAGEN-IMGN_TCI_CHILD_FU2', IMAGEN_DIGEST),  # (Personality II)
    ('IMAGEN-IMGN_SURPS_FU2', IMAGEN_SURVEY_DIGEST),  # (Personality III)
    ('IMAGEN-IMGN_LEQ_FU2', IMAGEN_DIGEST),  # (Life Events)
    ('IMAGEN-IMGN_PAAQ_CHILD_FU2', IMAGEN_DIGEST),  # (Childhood Relationships Questionnaire )
    ('IMAGEN-IMGN_SRC_FU2', IMAGEN_DIGEST),  # (SRC)
    ('IMAGEN-IMGN_IDENT_FU2', IMAGEN_DIGEST),  # (Faces Task)
    ('IMAGEN-IMGN_PALP_1_1_FU2', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMAGEN-IMGN_PALP_1_2_FU2', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMAGEN-IMGN_PALP_1_3_FU2', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMAGEN-IMGN_PALP_1_4_FU2', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMAGEN-IMGN_PALP_2_1_FU2', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMAGEN-IMGN_PALP_2_2_FU2', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMAGEN-IMGN_PALP_2_3_FU2', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMAGEN-IMGN_PALP_2_4_FU2', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMAGEN-IMGN_PALP_3_3_FU2', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMAGEN-IMGN_PALP_3_4_FU2', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMAGEN-IMGN_GATEWAY_FU2_2', BASIC_DIGEST),  # (Part 2 - Optional)
    ('IMAGEN-IMGN_CSI_CHILD_FU2', IMAGEN_DIGEST),  # (Part 2 - Physical Symptoms Questionnaire)
    ('IMAGEN-IMGN_ADSR_CHILD_FU2', IMAGEN_DIGEST),  # (Part 2 - Your mood)
    ('IMAGEN-IMGN_TFEQ_CHILD_FU2', IMAGEN_DIGEST),  # (Eating Habits II)
    ('IMAGEN-IMGN_RRS_CHILD_FU2', IMAGEN_DIGEST),  # (Part 2 - Feelings and Thoughts)
    ('IMAGEN-IMGN_EDEQ_CHILD_FU2', IMAGEN_DIGEST),  # (Eating Habits II)
    ('IMAGEN-IMGN_TCI3_CHILD_FU2', IMAGEN_DIGEST),  # (Personality V)
    ('IMAGEN-IMGN_VIDGAME_CHILD_FU2', IMAGEN_DIGEST),  # (Video-Gaming )
    ('IMAGEN-IMGN_GATEWAY_FU2_3', BASIC_DIGEST),  # (Part 3 - Optional)
    ('IMAGEN-IMGN_CAPE_CHILD_FU2', IMAGEN_DIGEST),  # (CAPE-state)
    ('IMAGEN-IMGN_CONSENT_FU2', IMAGEN_DIGEST),  # (Further information about the study)
    ('IMAGEN-IMGN_CIDS_PARENT_FU2', IMAGEN_DIGEST),  # (CID-S)
    ('IMAGEN-IMGN_HRQOL_PARENT_FU2', IMAGEN_DIGEST),  # (Quality of Life Questionnaire )
    ('IMAGEN-IMGN_K6PLUS_PARENT_FU2', IMAGEN_DIGEST),  # (Your Mood)
    ('IMAGEN-IMGN_PHQ_PARENT_FU2', IMAGEN_DIGEST),  # (PHQ)
    ('IMAGEN-IMGN_CTQ_CHILD_FU2', IMAGEN_DIGEST),  # (CTQ)
    ('IMAGEN-IMGN_AUDIT_INTERVIEW_FU2', IMAGEN_DIGEST),  # (ALCOHOL Questionnaire)
    ('IMAGEN-IMGN_ESPAD_INTERVIEW_FU2', IMAGEN_DIGEST),  # (Drug Use Questionnaire)
    ('IMAGEN-IMGN_TLFB_FU2', IMAGEN_DIGEST),  # (TLFB)
    ('IMAGEN-IMGN_NI_DATA_FU2', IMAGEN_DIGEST),  # (NI data entry)
    ('IMAGEN-IMGN_IRI_CHILD_FU', IMAGEN_DIGEST),  # (Part 2 - Personality IV: Relating to other people)
    ('IMAGEN-IMGN_IRI_CHILD_FU2', IMAGEN_DIGEST),  # (Part 2 - Personality IV: Relating to other people)
    ('IMAGEN-IMGN_BIS_CHILD_FU2', IMAGEN_DIGEST),  # (BIS)
    ('IMAGEN-IMGN_ANXDX_CHILD_FU2', IMAGEN_DIGEST),  # (Your Feelings)
    ('IMAGEN-IMGN_URBANICITY_FU2', IMAGEN_DIGEST),  # (URBANICITY)
    ('IMAGEN-IMGN_MAST_CHILD_FU2', IMAGEN_DIGEST),  # (ALCOHOL questionnaire II)
    ('IMAGEN-IMGN_RELIABILITY_CORE_CHILD_FU2', BASIC_DIGEST),  # (FU2 Reliability Child Core)
    ('IMAGEN-IMGN_RELIABILITY_OPT_FU2', BASIC_DIGEST),  # (FU2 Reliability Optional)
    ('IMAGEN-IMGN_RELIABILITY_PI_FU2', BASIC_DIGEST),  # (FU2 Reliability Parent Institute )
    ('IMAGEN-IMGN_PDS_FU2', IMAGEN_DIGEST),  # (Physical Development)
    ('IMAGEN-IMGN_PALP_3_1_FU2', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMAGEN-IMGN_PALP_3_2_FU2', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMAGEN-IMGN_JVQ_CHILD_FU2', IMAGEN_DIGEST),  # (JVQ)
    ('IMAGEN-IMGN_RELIABILITY_FU3', BASIC_DIGEST),  # (Reliability)
    ('IMAGEN-IMGN_KIRBY_RC5', IMAGEN_KIRBY_DIGEST),  # (Now or later?)
    ('IMAGEN-IMGN_KIRBY_FU_RC5', IMAGEN_KIRBY_DIGEST),  # (Now or later?)
    ('IMAGEN-IMGN_KIRBY_FU2', IMAGEN_KIRBY_DIGEST),  # (Now or later?)
    ('IMAGEN-IMGN_PALP_1_1_FU3', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMAGEN-IMGN_PALP_1_2_FU3', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMAGEN-IMGN_PALP_1_3_FU3', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMAGEN-IMGN_PALP_1_4_FU3', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMAGEN-IMGN_PALP_2_1_FU3', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMAGEN-IMGN_PALP_2_2_FU3', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMAGEN-IMGN_PALP_2_3_FU3', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMAGEN-IMGN_PALP_2_4_FU3', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMAGEN-IMGN_PALP_3_3_FU3', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMAGEN-IMGN_PALP_3_4_FU3', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('IMAGEN-IMGN_TLFB_FU3', IMAGEN_DIGEST),  # (TLFB)
    ('IMAGEN-IMGN_SRC_FU3', IMAGEN_DIGEST),  # (SRC)
    ('IMAGEN-IMGN_NI_DATA_FU3', IMAGEN_DIGEST),  # (NI data entry)
    ('IMAGEN-cVEDA_MINI5', BASIC_DIGEST),  # (M.I.N.I)
)
IMAGEN_BL_LEGACY_CSV_DATASETS = []
IMAGEN_FU1_LEGACY_CSV_DATASETS = []
IMAGEN_FU2_LEGACY_CSV_DATASETS = []
IMAGEN_FU3_LEGACY_CSV_DATASETS = []
for task, digest in IMAGEN_LEGACY_CSV_DATASETS:
    if '_FU3' in task or 'cVEDA_' in task:
        IMAGEN_FU3_LEGACY_CSV_DATASETS.append((task, digest))
    elif '_FU2' in task:
        IMAGEN_FU2_LEGACY_CSV_DATASETS.append((task, digest))
    elif '_FU' in task:
        IMAGEN_FU1_LEGACY_CSV_DATASETS.append((task, digest))
    else:
        IMAGEN_BL_LEGACY_CSV_DATASETS.append((task, digest))
STRATIFY_LEGACY_CSV_DATASETS = (
    ('STRATIFY-IMGN_NI_DATA_FU3', IMAGEN_DIGEST),  # (NI data entry)
    ('STRATIFY-IMGN_PALP_1_1_FU3', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('STRATIFY-IMGN_PALP_1_2_FU3', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('STRATIFY-IMGN_PALP_1_3_FU3', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('STRATIFY-IMGN_PALP_1_4_FU3', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('STRATIFY-IMGN_PALP_2_1_FU3', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('STRATIFY-IMGN_PALP_2_2_FU3', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('STRATIFY-IMGN_PALP_2_3_FU3', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('STRATIFY-IMGN_PALP_2_4_FU3', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('STRATIFY-IMGN_PALP_3_1_FU3', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('STRATIFY-IMGN_PALP_3_2_FU3', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('STRATIFY-IMGN_PALP_3_3_FU3', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('STRATIFY-IMGN_PALP_3_4_FU3', IMAGEN_DIGEST),  # (Numbers Task (3 Parts))
    ('STRATIFY-IMGN_RELIABILITY_FU3', BASIC_DIGEST),  # (Reliability)
    ('STRATIFY-IMGN_SRC_FU3', IMAGEN_DIGEST),  # (SRC)
    ('STRATIFY-IMGN_TLFB_FU3', IMAGEN_DIGEST),  # (TLFB)
    ('STRATIFY-cVEDA_MINI5', BASIC_DIGEST),  # (M.I.N.I)
    ('STRATIFY-STRATIFY_ADHD', BASIC_DIGEST),  # (ADHD Data entry)
    ('STRATIFY-STRATIFY_ASCS', BASIC_DIGEST),  # (ASCS Data entry)
    ('STRATIFY-STRATIFY_CTQ', BASIC_DIGEST),  # (CTQ Data entry)
    ('STRATIFY-STRATIFY_EDDS', BASIC_DIGEST),  # (EDDS Data entry)
    ('STRATIFY-STRATIFY_PANSS', BASIC_DIGEST),  # (PANSS Data entry)
    ('STRATIFY-STRATIFY_PHQ', BASIC_DIGEST),  # (PHQ Data entry)
    ## ('STRATIFY-STRATIFY_SRS', BASIC_DIGEST),  # (SRS Data entry)
)

QUOTED_PATTERN = re.compile(r'".*?"', re.DOTALL)


def _get_netrc_auth(url, netrc_file):
    try:
        netrc_path = os.path.expanduser(netrc_file)
    except KeyError:
        import warnings
        warnings.warn('Unable to find home directory')
        return
    if not os.path.exists(netrc_path):
        return

    netloc = urlparse(url).netloc

    try:
        from netrc import netrc, NetrcParseError
        try:
            authenticators = netrc(netrc_path).authenticators(netloc)
        except (NetrcParseError, OSError):
            return
        if authenticators:
            return (authenticators[0], authenticators[2])
    except (ImportError):
        return


def download_legacy(base_url, netrc_file, datasets, psytools_dir):
    username, password = _get_netrc_auth(base_url, netrc_file)

    for task, digest in datasets:
        digest = digest.upper().replace(' ', '_')
        dataset = '{task}-{digest}.csv'.format(task=task, digest=digest)
        logging.info('downloading: %s', dataset)
        url = base_url + dataset + '.gz'
        r = requests.get(url, auth=(username, password))
        compressed_data = io.BytesIO(r.content)
        with gzip.GzipFile(fileobj=compressed_data) as uncompressed_data:
            # unfold quoted text spanning multiple lines
            data = io.TextIOWrapper(uncompressed_data).read()
            # skip files that have not changed since last update
            psytools_path = os.path.join(psytools_dir, dataset)
            if os.path.isfile(psytools_path):
                with open(psytools_path, 'r') as uncompressed_file:
                    if uncompressed_file.read() == data:
                        logging.info('skip unchanged file: %s', psytools_path)
                        continue
            # write downloaded data into file
            with open(psytools_path, 'w') as uncompressed_file:
                logging.info('write file: %s', psytools_path)
                uncompressed_file.write(data)


class LimeSurveyError(Exception):
    def __init__(self, message, code):
        super(LimeSurveyError, self).__init__(message)
        self.code = code


def error2exception(func):
    def wrapper(*args, **kwargs):
        response, error = func(*args, **kwargs)
        if error:
            try:
                code = error['code']
                message = error['message']
            except (TypeError, KeyError):
                code = -32603  # internal JSON-RPC error
                message = 'Unexpected JSON-RPC error type'
            raise LimeSurveyError(message, code)
        return response
    return wrapper


class LimeSurveySession:
    """LimeSurvey JSON-RPC LSRC2 session

    Documented here:
    https://www.delosis.com/qs/index.php/admin/remotecontrol
    https://manual.limesurvey.org/RemoteControl_2_API

    """
    __request_id = 0

    def __init__(self, url, username, password):
        self.url = url
        # start a Requests session
        self.session = requests.Session()
        # Keep-alive is 100% automatic in Requests, thanks to urllib3
        self.session.headers.update({'content-type': 'application/json'})
        # start a LimeSurvey RemoteControl 2 session
        self.key = self._get_session_key(username, password)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()
        return False  # re-raises the exception

    def close(self):
        """Release LimeSurvey session key, then close Requests session"""
        self._release_session_key(self.key)
        self.key = None
        self.session.close()

    @staticmethod
    def _generate_request_id():
        LimeSurveySession.__request_id += 1
        return LimeSurveySession.__request_id

    @staticmethod
    def _request(method, params):
        return {
            'jsonrpc': '2.0',
            'id': LimeSurveySession._generate_request_id(),
            'method': method,
            'params': params,
        }

    def _post(self, request):
        logging.debug('JSON-RPC request: %s', request)
        assert 'method' in request and 'params' in request and 'id' in request
        response = self.session.post(self.url, data=json.dumps(request))
        response = response.json()
        logging.debug('JSON-RPC response: %s', response)
        assert response['id'] == request['id']
        result = response['result']
        error = response['error']
        if error:
            logging.error('JSON-RPC error: %s', error)
        return result, error

    def _get_session_key(self, username, password):
        request = self._request('get_session_key', [username, password])
        response, error = self._post(request)

        # fix non-sensical LSRC2 error handling
        # completely at odds with JSON-RPC error handling
        try:
            status = response['status']
        except (TypeError, KeyError):
            if error is not None:
                logging.error('LSRC2 failed to create a session key')
                response = None
            else:
                logging.info('LSRC2 new session key: %s', response)
        else:
            logging.error(status)
            error = {
                'code': -32099,  # implementation-defined error in JSON-RPC
                'message': status,
            }
            response = None

        return response

    def _release_session_key(self, key):
        request = self._request('release_session_key', [key])
        logging.info('LSRC2 release session key: %s', key)
        dummy_response, dummy_error = self._post(request)  # returns ('OK', None) even if bogus key

    @error2exception
    def surveys(self):
        request = self._request('list_surveys', [self.key])
        return self._post(request)

    @error2exception
    def participants(self, survey, attributes=False):
        request = self._request('list_participants',
                                [self.key, survey, 0, 5000, False, attributes])
        responses, error = self._post(request)

        # fix non-sensical LSRC2 error handling
        # completely at odds with JSON-RPC error handling
        try:
            status = responses['status']
        except (TypeError, KeyError):
            pass
        else:
            # LSRC2 returns errors as a dict with a 'status' attribute
            if status == 'No Tokens found':
                # When a survey is empty, LSRC2 also returns a dict:
                # {"status": "No Tokens found"}
                if error is not None:
                    logging.error('JSON-RPC error report does not match "status"')
                    error = None
            else:
                error = {
                    'code': -32099,  # implementation-defined error in JSON-RPC
                    'message': status,
                }
            responses = []

        return responses, error

    @error2exception
    def participant_properties(self, survey, participant, attributes):
        request = self._request('get_participant_properties',
                                [self.key, survey, participant, attributes])
        return self._post(request)

    @error2exception
    def responses(self, survey, status='all'):
        request = self._request('export_responses',
                                [self.key, survey, 'csv', None, status])
        responses, error = self._post(request)

        try:
            responses = base64.b64decode(responses).decode('utf_8').split('\n')
        except TypeError:
            # fix non-sensical LSRC2 error handling
            # completely at odds with JSON-RPC error handling
            try:
                status = responses['status']
            except (TypeError, KeyError):
                message = 'JSON-RPC function "export_responses" expected a Base64-encoded string'
                logging.error(message)
                error = {
                    'code': -32099,  # implementation-defined error in JSON-RPC
                    'message': message,
                }
            else:
                # LSRC2 returns errors as a dict with a 'status' attribute
                if status == 'No Data, could not get max id.':
                    # When a survey is empty, LSRC2 also returns a dict:
                    # {"status": "No Data, could not get max id."}
                    if error is not None:
                        logging.error('JSON-RPC error report does not match "status"')
                        error = None
                else:
                    error = {
                        'code': -32099,  # implementation-defined error in JSON-RPC
                        'message': status,
                    }
            responses = []

        return responses, error


def download_lsrc2(base_url, netrc_file, dispatch):
    """JSON RPC calls to LSRC2 service to retrieve new questionnaires.

    """
    username, password = _get_netrc_auth(base_url, netrc_file)
    with LimeSurveySession(base_url, username, password) as session:
        surveys = session.surveys()
        for survey in surveys:
            title = survey['surveyls_title']
            sid = survey['sid']
            active = survey['active']
            expires = survey['expires']

            if active == 'N':
                logging.info('skip inactive survey: %s', title)
                continue
            logging.info('read survey: %s', title)

            # subjects in surveys are identified by "sid" and "token"
            # retrieve correlation between "token" and PSC1 code
            psc1_from_token = {}
            try:
                participants = session.participants(sid, ['attribute_1'])
            except LimeSurveyError as e:
                # skip surveys missing token tables (should not happen!)
                logging.error('skip survey without token table ("%s"): %s"',
                              str(e), title)
                continue

            for participant in participants:
                token = participant['token']
                psc1_from_token[token] = participant['attribute_1']

            # retrieve survey
            responses = session.responses(sid, 'all')
            if not responses:  # some 'FUII Parent' surveys are still empty
                logging.warning('skip survey without responses: %s"',
                                title)
                continue

            # process CSV data:
            # * change "tid" into PSC1 code
            # * keep "token"
            # * use minimal quoting as in FU2
            reader = csv.DictReader(responses, delimiter=',')
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=reader.fieldnames,
                                    delimiter=',', quoting=csv.QUOTE_MINIMAL,
                                    lineterminator='\n')
            writer.writeheader()
            for row in reader:
                token = row['token']
                if token in psc1_from_token:
                    row['id'] = psc1_from_token[row['token']]
                    writer.writerow(row)
                else:
                    logging.warning('Orphan token "%s" in response "%s"',
                                    token, row['id'])
            data = output.getvalue()

            # save survey to this file name
            psytools_path = title
            psytools_path = psytools_path.replace(' - ', '-')
            psytools_path = psytools_path.replace(' | 2', '_2')  # German Covid-19 questionnaires
            psytools_path = psytools_path.replace(' | ', '-')    # German Covid-19 questionnaires
            psytools_path = psytools_path.replace(' ', '_')
            psytools_path += '.csv'

            # break down into different directories, one for each timepoint
            for match, psytools_dir in dispatch.items():
                if match in title:
                    psytools_path = os.path.join(psytools_dir, psytools_path)
                    break
            else:
                logging.error('unidentifiable Psytools data: %s', title)
                continue

            # skip files that have not changed since last update
            if os.path.isfile(psytools_path):
                with open(psytools_path, 'r') as psytools:
                    if psytools.read() == data:
                        logging.info('skip unchanged file: %s', psytools_path)
                        continue

            # write survey into CSV file
            with open(psytools_path, 'w') as psytools:
                logging.info('write file: %s', psytools_path)
                psytools.write(data)


def main():
    download_legacy(LEGACY_BASE_URL, IMAGEN_NETRC_FILE,
                    IMAGEN_BL_LEGACY_CSV_DATASETS,
                    PSYTOOLS_IMAGEN_BL_MASTER_DIR)
    download_legacy(LEGACY_BASE_URL, IMAGEN_NETRC_FILE,
                    IMAGEN_FU1_LEGACY_CSV_DATASETS,
                    PSYTOOLS_IMAGEN_FU1_MASTER_DIR)
    download_legacy(LEGACY_BASE_URL, IMAGEN_NETRC_FILE,
                    IMAGEN_FU2_LEGACY_CSV_DATASETS,
                    PSYTOOLS_IMAGEN_FU2_MASTER_DIR)
    download_legacy(LEGACY_BASE_URL, IMAGEN_NETRC_FILE,
                    IMAGEN_FU3_LEGACY_CSV_DATASETS,
                    PSYTOOLS_IMAGEN_FU3_MASTER_DIR)
    download_legacy(LEGACY_BASE_URL, STRATIFY_NETRC_FILE,
                    STRATIFY_LEGACY_CSV_DATASETS,
                    PSYTOOLS_STRATIFY_MASTER_DIR)

    dispatch = {
        'Imagen FUII -': PSYTOOLS_IMAGEN_FU2_MASTER_DIR,
        'Imagen FUIII -': PSYTOOLS_IMAGEN_FU3_MASTER_DIR,
        'STRATIFY ': PSYTOOLS_STRATIFY_MASTER_DIR,
        'IMACOV19 Baseline - ': PSYTOOLS_IMACOV19_BL_MASTER_DIR,
        'IMACOV19 2-weekly follow-up - ': PSYTOOLS_IMACOV19_FU_MASTER_DIR,
        'IMACOV19 | 2-wöchentliche Nachfolge Befragung ': PSYTOOLS_IMACOV19_FU_MASTER_DIR,
        'IMACOV19 follow-up -': PSYTOOLS_IMACOV19_FU2_MASTER_DIR,
        'IMACOV19 follow-up 2021 - ': PSYTOOLS_IMACOV19_FU3_MASTER_DIR,
        'IMACOV19 follow-up 2022 - ': PSYTOOLS_IMACOV19_FU4_MASTER_DIR,
        'STRATICO19 Baseline - ': PSYTOOLS_STRATICO19_BL_MASTER_DIR,
        'STRATICO19 2-weekly follow-up - ': PSYTOOLS_STRATICO19_FU_MASTER_DIR,
        'STRATICO19 | 2-wöchentliche Nachfolge Befragung ': PSYTOOLS_STRATICO19_FU_MASTER_DIR,
        'STRATICO19 follow-up - ': PSYTOOLS_STRATICO19_FU2_MASTER_DIR,
        'STRATICO19 follow-up 2021 - ': PSYTOOLS_STRATICO19_FU3_MASTER_DIR,
    }
    download_lsrc2(LSRC2_BASE_URL, LSRC2_NETRC_FILE, dispatch)


if __name__ == "__main__":
    main()
