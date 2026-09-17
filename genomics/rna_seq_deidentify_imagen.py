import csv
from imagen_databank import PSC2_FROM_PSC1

file_labID_PSC1_conv='/imagen/FU3/RAW/PSC1/genomics/rna/env_IMAGEN_align60_no.dups_metadata.tsv'

#use either first or seconf bloc, for gene_counts or gene_tmp

input_dir_imagen_PSC1='/imagen/FU3/RAW/PSC1/genomics/rna/env_IMAGEN_align60_no.dups_no.sex.mismatch_salmon.merged.gene_counts.tsv'
output_dir_imagen_BL_PSC2='/imagen/BL/processed/genetics/rna/env_IMAGEN_align60_no.dups_no.sex.mismatch_salmon.merged.gene_counts_PSC2_BL.tsv'
output_dir_imagen_FU2_PSC2='/imagen/FU2/processed/genetics/rna/env_IMAGEN_align60_no.dups_no.sex.mismatch_salmon.merged.gene_counts_PSC2_FU2.tsv'
output_dir_imagen_FU3_PSC2='/imagen/FU3/processed/genetics/rna/env_IMAGEN_align60_no.dups_no.sex.mismatch_salmon.merged.gene_counts_PSC2_FU3.tsv'
"""
input_dir_imagen_PSC1="/imagen/FU3/RAW/PSC1/genomics/rna/env_IMAGEN_align60_no.dups_no.sex.mismatch_salmon.merged.gene_tpm.tsv"
output_dir_imagen_BL_PSC2='/imagen/BL/processed/genetics/rna/env_IMAGEN_align60_no.dups_no.sex.mismatch_salmon.merged.gene_tpm_PSC2_BL.tsv'
output_dir_imagen_FU2_PSC2='/imagen/FU2/processed/genetics/rna/env_IMAGEN_align60_no.dups_no.sex.mismatch_salmon.merged.gene_tpm_PSC2_FU2.tsv'
output_dir_imagen_FU3_PSC2='/imagen/FU3/processed/genetics/rna/env_IMAGEN_align60_no.dups_no.sex.mismatch_salmon.merged.gene_tpm_PSC2_FU3.tsv'
"""



def convert_labID_to_PSC2_with_timepoint(labID):
    labID_index = headers.index("Lab_Code")
    psc1_index = headers.index("PSC1")
    timepoint_index=headers.index("TimePoint")
    for line in tab_conv_labID_psc1:
        if line[labID_index]==labID:
            try:
                if len(line[psc1_index])<12:
                    psc1="0"+line[psc1_index]
                elif len(line[psc1_index])<12:
                    psc1=line[psc1_index]
                psc2 = PSC2_FROM_PSC1[psc1]
                return (psc2, line[timepoint_index])
            except Exception:
                print("invalid PSC1 code:", line[psc1_index])
                #return ("###", line[timepoint_index])

    print("PSC1 not found for labID: ", labID)

    """
    for line in file_labID_PSC1:
        columns = line.strip().split(",")
        #print("check:",columns[labID_index],labID==columns[labID_index])
        if columns[labID_index] == labID:
            #print("deidentified: ",columns[psc1_index], "****", columns[timepoint_index])
            psc2 = PSC2_FROM_PSC1["0"+columns[psc1_index]]
            return(psc2,columns[timepoint_index])
    print("PSC1 not found for labID: ", labID)
    """

if __name__ == "__main__":
    with open(file_labID_PSC1_conv, 'r', errors='ignore') as file_labID_PSC1:
        reader = csv.reader(file_labID_PSC1, delimiter=',')
        tab_conv_labID_psc1 = [row for row in reader]
        headers=tab_conv_labID_psc1[0]
        #headers = list(next(reader))
        print(headers)
        print(convert_labID_to_PSC2_with_timepoint("GB97ENVKCLR301518"))

        with open(input_dir_imagen_PSC1, 'r', newline='',errors='ignore') as labID_infile:
            reader_input = csv.reader(labID_infile, delimiter='\t')

            data = [row for row in reader_input]

            #print(data[0])
            #intialize list of lists that will be written in the output file
            data_psc2_BL=[[] for i in range(len(data))]
            data_psc2_FU2 = [[] for i in range(len(data))]
            data_psc2_FU3 = [[] for i in range(len(data))]
            #intialize the two first columns of the three timepoints
            for i in range(len(data)):
                #print(data[i][0]," ***** ", data[i][1], " ***** ", data[i][2])
                #print(data_psc2_BL[i])
                data_psc2_BL[i].append(data[i][0])
                data_psc2_BL[i].append(data[i][1])

                data_psc2_FU2[i].append(data[i][0])
                data_psc2_FU2[i].append(data[i][1])

                data_psc2_FU3[i].append(data[i][0])
                data_psc2_FU3[i].append(data[i][1])


            count_BL=0
            count_FU2=0
            count_FU3 = 0
            #copy the resting column to the respective matrix depending on the timepoint
            for col_index in range(2,len(data[0])):
                #print(col_index)
                lab_id=data[0][col_index]
                lab_id.strip()
                #print(convert_labID_to_PSC2_with_timepoint(lab_id))
                try:
                    (psc2, timepoint)= convert_labID_to_PSC2_with_timepoint(lab_id)
                    if timepoint == "BL":

                        count_BL=count_BL+1
                        data_psc2_BL[0].append(psc2)
                        for i in range(1,len(data)):
                            data_psc2_BL[i].append(data[i][col_index])
                    elif timepoint == "FU2":

                        count_FU2=count_FU2+1
                        data_psc2_FU2[0].append(psc2)
                        for i in range(1,len(data)):
                            data_psc2_FU2[i].append(data[i][col_index])
                    elif timepoint == "FU3":

                        count_FU3=count_FU3+1
                        data_psc2_FU3[0].append(psc2)
                        for i in range(1,len(data)):
                            data_psc2_FU3[i].append(data[i][col_index])
                    else:
                        print("invalid timepoint:",timepoint)
                except Exception:
                    continue
            print("BL", count_BL)
            print("FU2", count_FU2)
            print("FU3", count_FU3)

        #write the output to the files
        print("writing ...")
        with open(output_dir_imagen_BL_PSC2, 'w', newline='') as PSC2_BL_outfile:
            writer_BL = csv.writer(PSC2_BL_outfile, delimiter='\t')
            writer_BL.writerows(data_psc2_BL)

        with open(output_dir_imagen_FU2_PSC2, 'w', newline='') as PSC2_FU2_outfile:
            writer_FU2 = csv.writer(PSC2_FU2_outfile, delimiter='\t')
            writer_FU2.writerows(data_psc2_FU2)

        with open(output_dir_imagen_FU3_PSC2, 'w', newline='') as PSC2_FU3_outfile:
            writer_FU3 = csv.writer(PSC2_FU3_outfile, delimiter='\t')
            writer_FU3.writerows(data_psc2_FU3)
