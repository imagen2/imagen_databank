#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import csv
import logging
from imagen_databank import PSC2_FROM_PSC1

logging.basicConfig(level=logging.INFO)

WORKER_PROCESSES = 8

FU3_VALIDATION = '/neurospin/imagen/FU3/RAW/PSC1/meta_data/sex_validation_2018.csv'

FEMALE = 'F'
MALE = 'M'


def validation_FU3(path):
    result = {}

    with open(path, newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        next(reader)  # skip header
        for row in reader:
            psc1 = row[0]
            sex = row[1]
            result[psc1] = sex

    return result


def main():
    # read different sources
    with open('imagen_sex_recruitment.csv', 'r') as f:
        reader = csv.DictReader(f, dialect='excel')
        recruitment = {row['PSC1']: row['Recruitment']
                       for row in reader}

    with open('imagen_sex_dataset.csv', 'r') as f:
        reader = csv.DictReader(f, dialect='excel')
        dataset = {row['PSC1']:
                   (row['QualityReport.txt'] if 'QualityReport.txt' in row else None,
                    row['BL MRI'] if 'BL MRI' in row else None,
                    row['BL Cantab'] if 'BL Cantab' in row else None,
                    row['FU2 MRI'] if 'FU2 MRI' in row else None,
                    row['FU2 Cantab'] if 'FU2 Cantab' in row else None,
                    row['FU3 MRI'] if 'FU3 MRI' in row else None,
                    row['FU3 Cantab'] if 'FU3 Cantab' in row else None)
                   for row in reader}

    with open('imagen_sex_psytools.csv', 'r') as f:
        reader = csv.DictReader(f, dialect='excel')
        psytools = {row['PSC1']:
                    (row['Psytools BL'] if 'Psytools BL' in row else None,
                     row['Psytools FU1'] if 'Psytools FU1' in row else None,
                     row['Psytools FU2'] if 'Psytools FU2' in row else None,
                     row['Psytools FU3'] if 'Psytools FU3' in row else None)
                    for row in reader}

    with open('imagen_sex_xnat.csv', 'r') as f:
        reader = csv.DictReader(f, dialect='excel')
        xnat = {row['PSC1']: row['XNAT gender'] if 'XNAT gender' in row else None
                for row in reader}

    with open('imagen_sex_methylation.csv', 'r') as f:
        reader = csv.DictReader(f, dialect='excel')
        methylation = {row['PSC1']:
                       (row['Methylation BL'] if 'Methylation BL' in row else None,
                        row['Methylation FU'] if 'Methylation FU' in row else None)
                       for row in reader}

    validation = validation_FU3(FU3_VALIDATION)

    # merge sources
    psc1s = set()
    for source in (recruitment, psytools, xnat, validation, methylation):
        psc1s = psc1s.union(set(source.keys()))
    psc1s = psc1s.intersection(set(PSC2_FROM_PSC1.keys()))  # LONDON recruitment file

    with open('imagen_sex.csv', 'w', newline='') as csvfile:
        fieldnames = ['PSC1',
                      'Recruitment',
                      'QualityReport.txt', 'MRI BL', 'Cantab BL', 'MRI FU2', 'Cantab FU2', 'MRI FU3', 'Cantab FU3',
                      'Psytools BL', 'Psytools FU1', 'Psytools FU2', 'Psytools FU3',
                      'XNAT gender',
                      '2018 validation',
                      'Reference',
                      'Methylation BL', 'Methylation FU']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for psc1 in sorted(psc1s):
            row = {}
            if psc1 in recruitment:
                row['Recruitment'] = recruitment[psc1]
            if psc1 in dataset:
                if dataset[psc1][0]:
                    row['QualityReport.txt'] = dataset[psc1][0]
                if dataset[psc1][1]:
                    row['MRI BL'] = dataset[psc1][1]
                if dataset[psc1][2]:
                    row['Cantab BL'] = dataset[psc1][2]
                if dataset[psc1][3]:
                    row['MRI FU2'] = dataset[psc1][3]
                if dataset[psc1][4]:
                    row['Cantab FU2'] = dataset[psc1][4]
                if dataset[psc1][5]:
                    row['MRI FU3'] = dataset[psc1][5]
                if dataset[psc1][6]:
                    row['Cantab FU3'] = dataset[psc1][6]
            if psc1 in psytools:
                if psytools[psc1][0]:
                    row['Psytools BL'] = psytools[psc1][0]
                if psytools[psc1][1]:
                    row['Psytools FU1'] = psytools[psc1][1]
                if psytools[psc1][2]:
                    row['Psytools FU2'] = psytools[psc1][2]
                if psytools[psc1][3]:
                    row['Psytools FU3'] = psytools[psc1][3]
            if psc1 in xnat:
                row['XNAT gender'] = xnat[psc1]
            if psc1 in validation:
                row['2018 validation'] = validation[psc1]

            if psc1 in xnat and psc1 in validation:
                if xnat[psc1] != validation[psc1]:
                    logging.warning('%s: changed XNAT %s into %s',
                                    psc1, xnat[psc1], validation[psc1])

            values = set(row.values())
            if len(values) > 1:
                if psc1 in validation:
                    row['Reference'] = validation[psc1]
            else:
                row['Reference'] = next(iter(values))

            if psc1 in methylation:
                if methylation[psc1][0]:
                    row['Methylation BL'] = methylation[psc1][0]
                if methylation[psc1][1]:
                    row['Methylation FU'] = methylation[psc1][1]

            row['PSC1'] = psc1
            writer.writerow(row)


if __name__ == "__main__":
    main()
