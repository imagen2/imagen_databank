#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Download Dawba codes for Imagen FU3 and Stratify and update conversion table.

==========
Attributes
==========

Output
------

PSC2PSC : str
    Table of conversion between participant codes (PSC1, Dawba, PSC2).

"""

import os
import requests
import json
import base64
from urllib.parse import urlparse
import logging
logging.basicConfig(level=logging.INFO)

# The LSRC2 service at Delosis.
LSRC2_BASE_URL = 'https://www.delosis.com/qs/index.php/admin/remotecontrol'
# Since credentials are different between the legacy and the LSRC2 service,
# and ~/.netrc allows only a single set of credentials per server, store
# LSRC2 credentials in an alternate file.
LSRC2_NETRC_FILE = '~/.lsrc2'
# The PSC1, Dawba, PSC2 conversion table
PSC2PSC = '/neurospin/imagen/src/scripts/psc_tools/psc2psc.csv'


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


class LimeSurveySession(object):
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


def _get_netrc_auth(url):
    try:
        netrc_path = os.path.expanduser(LSRC2_NETRC_FILE)
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
        except (NetrcParseError, IOError):
            return
        if authenticators:
            return (authenticators[0], authenticators[2])
    except (ImportError):
        return


def download_lsrc2_tokens(base_url):
    """JSON RPC calls to LSRC2 service to retrieve tokens.

    """
    username, password = _get_netrc_auth(base_url)
    with LimeSurveySession(base_url, username, password) as session:
        dawba_from_psc1 = {}

        surveys = session.surveys()
        for survey in surveys:
            title = survey['surveyls_title']
            sid = survey['sid']
            active = survey['active']

            if active == 'N':
                logging.info('skip inactive survey: %s', title)
                continue
            if title.startswith('Imagen FUII - '):
                logging.info('skip FU2 survey: %s', title)
                continue
            logging.info('read survey: %s', title)

            # subjects in surveys are identified by "sid" and "token"
            # retrieve correlation between "token" and PSC1 and Dawba codes
            psc1_from_token = {}
            dawba_from_token = {}
            participants = session.participants(sid, ['completed', 'reminded', 'attribute_1', 'attribute_2'])
            for participant in participants:
                token = participant['token']
                #~ if ('reminded' in participant and participant['reminded'] == 'Duplicate' or
                    #~ 'completed' in participant and participant['completed'] == 'N'):
                    #~ continue
                # PSC1
                if 'attribute_1' in participant:
                    psc1 = participant['attribute_1']
                    if psc1.endswith('SB'):
                        psc1 = psc1[:-2]
                    if psc1.endswith('FU3'):
                        psc1 = psc1[:-3]
                    if token in psc1_from_token:
                        if psc1 != psc1_from_token[token]:
                            logging.error('survey: %s: participant %s has inconsistent PSC1 codes',
                                          title, psc1_from_token[token])
                    else:
                        psc1_from_token[token] = psc1
                else:
                    logging.error('survey: %s: participant %s lacks a PSC1 code',
                                  title, psc1_from_token[token])
                # Dawba
                if 'attribute_2' in participant:
                    dawba = participant['attribute_2']
                    if token in dawba_from_token:
                        if dawba != dawba_from_token[token]:
                            logging.error('survey: %s: participant %s has inconsistent Dawba codes',
                                          title, dawba_from_token[token])
                    else:
                        dawba_from_token[token] = dawba
                else:
                    logging.error('survey: %s: participant %s lacks a Dawba code',
                                  title, psc1_from_token[token])

            for token in psc1_from_token.keys() & dawba_from_token.keys():
                psc1 = psc1_from_token[token]
                dawba = dawba_from_token[token]
                dawba_from_psc1.setdefault(psc1, {}).setdefault(dawba, set())
                dawba_from_psc1[psc1][dawba].add(title)

        for psc1, codes in dawba_from_psc1.items():
            if len(codes) > 1:
                message = '%s: multiple Dawba codes:\n'
                for dawba, titles in codes.items():
                    message += '\t%s:\n\t\t%s'.format(psc1, '\n\t\t'.join(title for title in titles))
                dawba_from_psc1[psc1] = None
            else:
                dawba_from_psc1[psc1] = next(iter(dawba_from_psc1[psc1].keys()))
        dawba_from_psc1 =  { psc1: dawba for psc1, dawba in dawba_from_psc1.items()
                             if dawba }

        return dawba_from_psc1


import sys
def main():
    dawba_from_psc1 = download_lsrc2_tokens(LSRC2_BASE_URL)
    with open(PSC2PSC, 'rU') as f:
        for line in f:
            psc1, dawba, psc2 = line.strip('\n').split('=')
            if psc1 in dawba_from_psc1:
                if dawba == '000000':
                    dawba = dawba_from_psc1[psc1]
                    line = '='.join((psc1, dawba, psc2)) + '\n'
                elif int(dawba) > 200000 and dawba != dawba_from_psc1[psc1]:
                    logging.error('%s: Dawba code changed from %s to %s',
                                  psc1, dawba, dawba_from_psc1[psc1])
            sys.stdout.write(line)


if __name__ == "__main__":
    main()
