#/bin/sh

#
# process geolocation at each time point
#
for timepoint in BL FU1 FU2 FU3
do
    DIR_PSC1="/neurospin/imagen/${timepoint}/RAW/PSC1/geolocation"
    FILE_PSC2="/neurospin/imagen/${timepoint}/processed/geolocation/IMAGEN_geolocation_${timepoint}.csv"

    # print output file header line
    echo "PSC2,latitude,longitude,notes" > "$FILE_PSC2"
    # process each input file
    for file in "${DIR_PSC1}/IMAGEN_geolocation_"*"_${timepoint}.csv"
    do
        # some commands cannot process DOS line endings
        tmpfile=`mktemp -t tmp.geolocation.XXXXXXXXXX`
        dos2unix -n "$file" "$tmpfile" 2>/dev/null
        # some sites lack a "Notes" column
        if head -1 "$tmpfile" | grep -q "Notes"
        then
            ADD_NOTES=0
        else
            ADD_NOTES=1
        fi
        # skip input file header line
        tail -n +2 "$tmpfile" |
        # some sites lack a "Notes" column
        if [ "$ADD_NOTES" ]
        then
            sed 's/$/,/'
        fi
        # clean up
        rm -f "$tmpfile"
    done | psc2psc.py 2>/dev/null | sort >> "$FILE_PSC2"
    unix2dos -o "$FILE_PSC2" 2>/dev/null
done


#
# process geolocation backdated from BL
#
BACKDATED_PSC1="/neurospin/imagen/FU3/RAW/PSC1/geolocation/IMAGEN_geolocation_ALL_SITES_backdated_Dublin_updated.csv"
BACKDATED_PSC2="/neurospin/imagen/FU3/processed/geolocation/IMAGEN_geolocation_backdated.csv"

# print output file header line
echo "PSC2,year,latitude,longitude" > "$BACKDATED_PSC2"
# skip input file header line
tail -n +2 "$BACKDATED_PSC1" | psc2psc.py 2>/dev/null | sort >> "$BACKDATED_PSC2"
unix2dos -o "$BACKDATED_PSC2" 2>/dev/null
