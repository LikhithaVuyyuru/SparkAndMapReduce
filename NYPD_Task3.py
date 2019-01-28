#importing packages and libraries
import csv
import os

#Location of outputs
res="FinalResults"
files3 = os.listdir(res)
#Sorting the outputs according to the creation order of output files
files3.sort()
#Storing the outputs in a specified location
output_f1 = filter(lambda x:x[-4:] == '.csv',files3)
if(len(output_f1)>0):
    for i in output_f1:
        with open(res+"/"+i) as f1:
            read_csv = csv.reader(f1, delimiter=',')
            for j in read_csv:
                print  repr(j)
