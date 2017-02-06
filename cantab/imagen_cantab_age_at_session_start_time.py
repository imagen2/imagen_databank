#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""...

==========
Attributes
==========

Input
-----

FU2_MASTER_DIR : str
    Location of FU2 PSC1-encoded data.

Output
------

???

"""

FU2_MASTER_DIR = u'/neurospin/imagen/FU2/RAW/PSC1'

import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

import os
import glob
from datetime import date

# import ../imagen_databank
import sys
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..'))
from imagen_databank import PSC2_FROM_PSC1
from imagen_databank import DOB_FROM_PSC2
from imagen_databank import read_datasheet


def main():
    # find datasheet_*.csv files
    logger.info('start globing datasheet_*.csv files')
    datasheets = glob.glob(os.path.join(FU2_MASTER_DIR,
                           '*/*/AdditionalData/datasheet_*.csv'))
    logger.info('finished globing datasheet_*.csv files')

    for datasheet in datasheets:
        subject_ids, row, cols, session_start_times, titles = read_datasheet(datasheet)
        if len(subject_ids) != 1:
            logger.warning('Proper "Subject ID" not found: %s', datasheet)
            continue
        psc1 = subject_ids.pop()
        if psc1 not in PSC2_FROM_PSC1:
            logger.error('unknown PSC1 code %s: %s', psc1, datasheet)
            continue
        psc2 = PSC2_FROM_PSC1[psc1]
        if psc2 not in DOB_FROM_PSC2:
            logger.error('unknown age for PSC2 code %s: %s', psc2, datasheet)
            continue
        dob = DOB_FROM_PSC2[psc2]
        session_start_times = set([sst.date() for sst in session_start_times])
        if len(session_start_times) != 1:
            logger.warning('Proper "Session start time" not found: %s',
                           datasheet)
            continue
        session_start_time = session_start_times.pop()
        if session_start_time < date(2007, 1, 1):
            logger.error('Bogus "Session start time" %s: %s',
                         session_start_time, datasheet)
            continue
        age = (session_start_time - dob).days
        print('{0},{1}'.format(psc2, age))


if __name__ == "__main__":
    main()
