# -*- coding: utf-8 -*-

# Copyright (c) 2014-2017 CEA
#
# This software is governed by the CeCILL license under French law and
# abiding by the rules of distribution of free software. You can use,
# modify and/ or redistribute the software under the terms of the CeCILL
# license as circulated by CEA, CNRS and INRIA at the following URL
# "http://www.cecill.info".
#
# As a counterpart to the access to the source code and rights to copy,
# modify and redistribute granted by the license, users are provided only
# with a limited warranty and the software's author, the holder of the
# economic rights, and the successive licensors have only limited
# liability.
#
# In this respect, the user's attention is drawn to the risks associated
# with loading, using, modifying and/or developing or reproducing the
# software by the user in light of its specific status of free software,
# that may mean that it is complicated to manipulate, and that also
# therefore means that it is reserved for developers and experienced
# professionals having in-depth computer knowledge. Users are therefore
# encouraged to load and test the software's suitability as regards their
# requirements in conditions enabling the security of their systems and/or
# data to be ensured and, more generally, to use and operate it in the
# same conditions as regards security.
#
# The fact that you are presently reading this means that you have had
# knowledge of the CeCILL license and that you accept its terms.

import re

from . core import detect_psc1

import logging
logger = logging.getLogger(__name__)


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
