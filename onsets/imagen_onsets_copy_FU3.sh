#!/bin/sh

SOURCE='/neurospin/imagen/FU3/RAW/PSC2/onsets'
TARGET='/neurospin/imagen/FU3/processed/nifti'

for f in /neurospin/imagen/FU3/RAW/PSC2/onsets/*.csv
do
    basename=`basename "$f" '.csv'`
    psc2=`echo "$basename" | sed -e 's/^.*_//; s/FU3$//'`
    if [ -d "$TARGET/${psc2}" ]
    then
        mkdir -p "${TARGET}/${psc2}/BehaviouralData"
        cp -p "${SOURCE}/${basename}.csv" "${TARGET}/${psc2}/BehaviouralData/"
    else
        >&2 echo "ERROR: $psc2: missing folder!"
    fi
done
