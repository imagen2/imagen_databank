# -*- coding: utf-8 -*-

import logging
logger = logging.getLogger(__name__)

import re

from . core import detect_psc1


_SUBJECT_ID_REGEX = re.compile('\d{2}[/\.]\d{2}[/\.]\d{4} \d{2}:\d{2}:\d{2}\tSubject ID: (\w+)')


def read_scanning(path):
    """Return "Subject ID" values found in a Scanning/*.csv file.

    Parameters
    ----------
    path : unicode
        Path to the Scanning/*.csv to read from.

    Returns
    -------
    str
        "Subject ID" value found in the file.

    """

    with open(path) as scanning:
        subject_ids = set()
        for line in scanning:
            match = _SUBJECT_ID_REGEX.match(line)
            if match:
                subject_id = detect_psc1(match.group(1))
                if subject_id is None:
                    subject_id = match.group(1)
                subject_ids.add(subject_id)
        return subject_ids
