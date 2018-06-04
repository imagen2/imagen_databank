#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import csv
import logging

logging.basicConfig(level=logging.INFO)

WORKER_PROCESSES = 8

METHYLATION = '/neurospin/imagen/TODO/predicted_gender.csv'
PSC1_FROM_CHIP = '/neurospin/imagen/TODO/PSC1/Associated PSC1 codes.csv'

FEMALE = 'F'
MALE = 'M'


def psc1_from_chip(path):
    result = {}

    with open(path, newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        next(reader)  # skip header
        for row in reader:
            chip = row[0]
            psc1 = row[1]
            if psc1.endswith('FU'):
                psc1 = psc1[:-len('FU')]
                timepoint = 'FU2'
            else:
                timepoint = 'BL'
            result[chip] = (psc1, timepoint)

    return result


def methylation_process(path, psc1_from_chip):
    result_BL = {}
    result_FU2 = {}

    with open(path, newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        next(reader)  # skip header
        for row in reader:
            chip = row[0]
            sex = row[1]
            if sex == '1':
                sex = 'F'
            elif sex == '2':
                sex = 'M'
            else:
                logging.error('%s: incorrect sex (%s) in prediction CSV file: %s',
                              chip, sex, f)
                continue
            if chip in psc1_from_chip:
                psc1, timepoint = psc1_from_chip[chip]
                if timepoint == 'FU2':
                    result = result_FU2
                elif timepoint == 'BL':
                    result = result_BL
                else:
                    logging.error('%s: incorrect connversion table', chip)
                    continue
                if psc1 in result:
                    if result[psc1] != sex:
                        logging.error('%s: inconsistent sex from methylation', psc1)
                        result[psc1] = '?'
                else:
                    result[psc1] = sex

    return result_BL, result_FU2


def main():
    psc1_from_chip_table = psc1_from_chip(PSC1_FROM_CHIP)
    methylation_BL, methylation_FU2 = methylation_process(METHYLATION, psc1_from_chip_table)
    methylation = (methylation_BL, methylation_FU2)

    with open('imagen_sex_methylation.csv', 'w', newline='') as csvfile:
        sex = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
        sex.writerow(['PSC1',
                      'Methylation BL', 'Methylation FU'])
        psc1s = set()
        for timepoint in methylation:
            psc1s = psc1s.union(set(timepoint.keys()))
        for psc1 in sorted(psc1s):
            row = [psc1]
            for timepoint in methylation:
                if psc1 in timepoint:
                    row.append(timepoint[psc1])
                else:
                    row.append(None)
            sex.writerow(row)


if __name__ == "__main__":
    main()
