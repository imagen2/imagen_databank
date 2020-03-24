#!/bin/sh

SOURCE='/neurospin/imagen/STRATIFY/RAW/PSC2/onsets'
TARGET='/neurospin/imagen/STRATIFY/processed/nifti'

for f in "${SOURCE}/"*.csv
do
    basename=`basename "$f" '.csv'`
    psc2=`echo "$basename" | sed -e 's/^.*_//; s/SB$//'`
    if [ -d "${TARGET}/${psc2}" ]
    then
        mkdir -p "${TARGET}/${psc2}/BehaviouralData"
        cp -p "${SOURCE}/${basename}.csv" "${TARGET}/${psc2}/BehaviouralData/"
    else
        >&2 echo "ERROR: $psc2: missing folder!"
    fi
done
