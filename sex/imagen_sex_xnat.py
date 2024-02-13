#!/usr/bin/env python3

import os
from multiprocessing import Pool
from xml.etree import ElementTree
from imagen_databank import PSC1_FROM_PSC2
import csv
import logging

logging.basicConfig(level=logging.INFO)

BL_XNAT = '/neurospin/imagen/export/xml'

WORKER_PROCESSES = 16


FEMALE = 'F'
MALE = 'M'

_XNAT_GENDER_MAPPING = {
    'female': FEMALE,
    'male': MALE,
}

_XNAT_EXPERIMENT_GENDER_MAPPING = {
    'f': FEMALE,
    'F': FEMALE,
    'm': MALE,
    'M': MALE,
    'w': FEMALE,
    'female': FEMALE, # single occurrence!
}

_XNAT_EXPERIMENT_GENDER_VOID = {
    '0',
    'Test',
    'not known',
}


def list_xnat_BL(path):
    """List XML files exported from XNAT.

    Yields only files with standard names:
        IMAGEN_<PSC2>.xml

    Parameters
    ----------
    path : str
        Directory to read XML files from.

    Yields
    ------
    tuple of str
        Yields a pair (psc2, path).

    """
    for f in os.listdir(path):
        root, ext = os.path.splitext(f)
        if ext == '.xml':
            PREFIX = 'IMAGEN_'
            if root.startswith(PREFIX):
                psc2 = root[len(PREFIX):]
                logging.debug('%s: found XML file: %s', psc2, f)
                assert(psc2.isdigit() and len(psc2) == 12)
                yield (psc2, os.path.join(path, f))
            else:
                logging.error('unexpected XML file: %s', f)
        else:
            logging.debug('skipping non-XML file: %s', f)


def process_xnat_BL(arguments):
    """Read subject sex from XML file exported from XNAT.

    Looks for this information in two distinct places.

    Parameters
    ----------
    arguments : tuple of str
        Expects a pair (psc2, path)

    Returns
    -------
    tuple of str
        Yields a pair (xnat_sex, xnat_experiment_sex).

    """
    (psc2, path) = arguments  # unpack multiple arguments

    tree = ElementTree.parse(path)
    root = tree.getroot()

    xnat_sex = None
    xnat_gender = root.find('.//{http://nrg.wustl.edu/xnat}gender')
    if xnat_gender is None:
        logging.warn("%s: missing 'gender' in XML file", psc2)
    else:
        xnat_gender = xnat_gender.text
        if xnat_gender in _XNAT_GENDER_MAPPING:
            xnat_sex = _XNAT_GENDER_MAPPING[xnat_gender]
        else:
            logging.error("%s: incorrect 'gender' (%s) in XML file",
                          psc2, xnat_gender)

    xnat_experiment_sex = None
    xnat_experiment_gender = root.find('.//{http://nrg.wustl.edu/xnat}experiment[@gender]')
    if xnat_experiment_gender is None:
        logging.warn("%s: missing 'experiment[@gender]' in XML file", psc2)
    else:
        xnat_experiment_gender = xnat_experiment_gender.attrib['gender']
        xnat_experiment_gender = xnat_experiment_gender.strip()
        if xnat_experiment_gender in _XNAT_EXPERIMENT_GENDER_MAPPING:
            xnat_experiment_sex = _XNAT_EXPERIMENT_GENDER_MAPPING[xnat_experiment_gender]
        elif xnat_experiment_gender not in _XNAT_EXPERIMENT_GENDER_VOID:
            logging.error("%s: incorrect 'experiment[@gender]' (%s) in XML file",
                          psc2, xnat_experiment_gender)

    return xnat_sex, xnat_experiment_sex


def xnat_BL(path):
    """Process XML files exported from XNAT.

    First list the files to process, then read these files in parallel.

    Parameters
    ----------
    path : str
        Directory to read XML files from.

    Returns
    -------
    dict
        Key is PSC2 and value a pair (xnat_sex, xnat_experiment_sex).

    """
    todo_list = list(list_xnat_BL(BL_XNAT))

    pool = Pool(WORKER_PROCESSES)
    results = pool.map(process_xnat_BL, todo_list)
    pool.close()
    pool.join()

    psc1, path = zip(*todo_list)
    return dict(zip(psc1, results))


def main():
    xnat = xnat_BL(BL_XNAT)

    xnat = {PSC1_FROM_PSC2[psc2]: v for psc2, v in xnat.items()}

    with open('imagen_sex_xnat.csv', 'w', newline='') as csvfile:
        sex = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
        sex.writerow(['PSC1',
                      'XNAT gender'])
        for psc1 in sorted(xnat):
            row = [psc1]
            if xnat[psc1][0] and xnat[psc1][1]:
                if xnat[psc1][0] != xnat[psc1][1]:
                    logging.error("%s: inconsistent 'gender' (%s) / 'experiment@gender' (%s)",
                                  psc1, xnat[psc1][0], xnat[psc1][1])
                    row.append('?')
                else:
                    row.append(xnat[psc1][0])
            elif xnat[psc1][0]:
                row.append(xnat[psc1][0])
            elif xnat[psc1][1]:
                row.append(xnat[psc1][1])
            else:
                row.append(None)
            sex.writerow(row)


if __name__ == "__main__":
    main()
