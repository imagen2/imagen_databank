import csv
from imagen_databank import PSC2_FROM_PSC1

file_labID_PSC1_conv_stratify='/imagen/STRATIFY/RAW/PSC1/genomics/rna/env_STRATIFY_align60_no.dups_metadata.tsv'
file_labID_PSC1_conv_estra='/imagen/STRATIFY/RAW/PSC1/genomics/rna/env_ESTRA_align60_no.dups_metadata.tsv'

#use either first or seconf bloc, for gene_counts or gene_tmp

input_dir_STRATIFY_PSC1_counts='/imagen/STRATIFY/RAW/PSC1/genomics/rna/env_STRATIFY_align60_no.dups_no.sex.mismatch_salmon.merged.gene_counts.tsv'
input_dir_ESTRA_PSC1_counts='/imagen/STRATIFY/RAW/PSC1/genomics/rna/env_ESTRA_align60_no.dups_no.sex.mismatch_salmon.merged.gene_counts.tsv'
output_dir_STRATIFY_PSC2_counts='/imagen/STRATIFY/processed/genetics/rna/env_STRATIFY_align60_no.dups_no.sex.mismatch_salmon.merged.gene_counts_PSC2.tsv'
output_dir_ESTRA_PSC2_counts='/imagen/STRATIFY/processed/genetics/rna/env_ESTRA_align60_no.dups_no.sex.mismatch_salmon.merged.gene_counts_PSC2.tsv'


input_dir_STRATIFY_PSC1_tpm='/imagen/STRATIFY/RAW/PSC1/genomics/rna/env_STRATIFY_align60_no.dups_no.sex.mismatch_salmon.merged.gene_tpm.tsv'
input_dir_ESTRA_PSC1_tpm='/imagen/STRATIFY/RAW/PSC1/genomics/rna/env_ESTRA_align60_no.dups_no.sex.mismatch_salmon.merged.gene_tpm.tsv'
output_dir_STRATIFY_PSC2_tpm='/imagen/STRATIFY/processed/genetics/rna/env_STRATIFY_align60_no.dups_no.sex.mismatch_salmon.merged.gene_tpm_PSC2.tsv'
output_dir_ESTRA_PSC2_tpm='/imagen/STRATIFY/processed/genetics/rna/env_ESTRA_align60_no.dups_no.sex.mismatch_salmon.merged.gene_tpm_PSC2.tsv'




def convert_labID_to_PSC2_with_timepoint(labID,tab_conv_labID_psc1):
    headers = tab_conv_labID_psc1[0]
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


def convert_file_to_PSC2(file_labID_PSC1_conv, input_dir_PSC1, output_dir_PSC2, delimiter_metadata):
    print("converting ", input_dir_PSC1, " to PSC2...")
    with open(file_labID_PSC1_conv, 'r', errors='ignore') as file_labID_PSC1:
        reader = csv.reader(file_labID_PSC1, delimiter=delimiter_metadata)
        tab_conv_labID_psc1 = list(reader)
        headers = tab_conv_labID_psc1[0]
        # headers = list(next(reader))
        print(headers)
        # print(convert_labID_to_PSC2_with_timepoint("GB97ENVKCLR301518"))

        with open(input_dir_PSC1, 'r', newline='',errors='ignore') as labID_infile:
            reader_input = csv.reader(labID_infile, delimiter='\t')

            data = list(reader_input)

            #print(data[0])
            #intialize list of lists that will be written in the output file
            data_psc2=[[] for i in range(len(data))]

            #intialize the two first columns of the three timepoints
            for i in range(len(data)):
                #print(data[i][0]," ***** ", data[i][1], " ***** ", data[i][2])
                #print(data_psc2_BL[i])
                data_psc2[i].append(data[i][0])
                data_psc2[i].append(data[i][1])

            count=0

            #copy the resting column to the respective matrix depending on the timepoint
            for col_index in range(2,len(data[0])):
                #print(col_index)
                lab_id=data[0][col_index]
                lab_id.strip()
                #print(convert_labID_to_PSC2_with_timepoint(lab_id))
                try:
                    (psc2, timepoint)= convert_labID_to_PSC2_with_timepoint(lab_id,tab_conv_labID_psc1)
                    count=count+1
                    data_psc2[0].append(psc2)
                    for i in range(1,len(data)):
                        data_psc2[i].append(data[i][col_index])

                except Exception:
                    continue
            print("number of lines in file: " ,count)


        #write the output to the files
        print("writing ...")
        with open(output_dir_PSC2, 'w', newline='') as PSC2_outfile:
            writer = csv.writer(PSC2_outfile, delimiter='\t')
            writer.writerows(data_psc2)



if __name__ == "__main__":
    convert_file_to_PSC2(file_labID_PSC1_conv_stratify, input_dir_STRATIFY_PSC1_counts, output_dir_STRATIFY_PSC2_counts,",")

    convert_file_to_PSC2(file_labID_PSC1_conv_stratify, input_dir_STRATIFY_PSC1_tpm, output_dir_STRATIFY_PSC2_tpm, ",")

    convert_file_to_PSC2(file_labID_PSC1_conv_estra, input_dir_ESTRA_PSC1_counts, output_dir_ESTRA_PSC2_counts, "\t")

    convert_file_to_PSC2(file_labID_PSC1_conv_estra, input_dir_ESTRA_PSC1_tpm, output_dir_ESTRA_PSC2_tpm, "\t")







