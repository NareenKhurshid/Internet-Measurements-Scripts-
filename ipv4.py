import bz2
import json
import os
from pathlib import Path
import tarfile
import sys
import avro
import avro.schema
from avro.datafile import DataFileWriter, DataFileReader
from avro.io import DatumWriter, DatumReader
import pandas as pd
import datetime
import filecmp
from datetime import datetime
from zipfile import ZipFile
import numpy as np
from functools import reduce

# The purpose of this script is to check the parsed files prepared by other scripts
# It will check to see if any elements from one file is found in the other file
# we will then use this information to analyze the overlap between them 

def main():
    # path to folder containing all my text files
    directory = 'C:\\Users\\Naree\\Desktop\\DataSets_to_Compare_Nov_1_and_Dec_1_for_all_4_tools\\'

"""
# first we compare the four query name files
alexanovndec_query_name.txt
ciscoumbnovanddec_query_name.txt
Cloudflare _data_query_name.txt
tranco_query_name.txt
"""
f1 = 'C:\\Users\\Naree\\Desktop\\DataSets_to_Compare_Nov_1_and_Dec_1_for_all_4_tools\\alexanovndec_ip4_address.txt'
f2 = 'C:\\Users\\Naree\\Desktop\\DataSets_to_Compare_Nov_1_and_Dec_1_for_all_4_tools\\ciscoumbnovanddec_ip4_address.txt'
f3 = 'C:\\Users\\Naree\\Desktop\\DataSets_to_Compare_Nov_1_and_Dec_1_for_all_4_tools\\Cloudflare _data_ip4_address.txt'
f4 = 'C:\\Users\\Naree\\Desktop\\DataSets_to_Compare_Nov_1_and_Dec_1_for_all_4_tools\\tranco_ip4_address.txt'
names_in_1_and_2 = 0
names_in_1_and_3 = 0
names_in_1_and_4 = 0
names_in_2_and_3 = 0
names_in_2_and_4 = 0
names_in_3_and_4 = 0

with open('C:\\Users\\Naree\\Desktop\\DataSets_to_Compare_Nov_1_and_Dec_1_for_all_4_tools\\similaritiesf1f2.txt', 'w') as outfile:
    
    with open(f1) as file1:
        with open(f2) as file2:
            same = set(file1).intersection(file2)    # determine which domain names are found in both files
    for line in same:
        names_in_1_and_2 +=1                         # counter for total number of overlapping domain names
        print(line, end='', file=outfile)

# empty 
with open('C:\\Users\\Naree\\Desktop\\DataSets_to_Compare_Nov_1_and_Dec_1_for_all_4_tools\\similaritiesf1f3.txt', 'w') as outfile:
    with open(f1) as file1:
        with open(f3) as file2:
            same = set(file1).intersection(file2)
    for line in same:
        names_in_1_and_3 +=1
        print(line, end='', file=outfile)

with open('C:\\Users\\Naree\\Desktop\\DataSets_to_Compare_Nov_1_and_Dec_1_for_all_4_tools\\similaritiesf1f4.txt', 'w') as outfile:
    with open(f1) as file1:
        with open(f4) as file2:
            same = set(file1).intersection(file2)
    for line in same:
        names_in_1_and_4 +=1
        print(line, end='', file=outfile)
with open('C:\\Users\\Naree\\Desktop\\DataSets_to_Compare_Nov_1_and_Dec_1_for_all_4_tools\\similaritiesf2f3.txt', 'w') as outfile:
    with open(f2) as file1:
        with open(f3) as file2:
            same = set(file1).intersection(file2)
    for line in same:
        names_in_2_and_3 +=1
        print(line, end='', file=outfile)
with open('C:\\Users\\Naree\\Desktop\\DataSets_to_Compare_Nov_1_and_Dec_1_for_all_4_tools\\similaritiesf2f4.txt', 'w') as outfile:
    with open(f2) as file1:
        with open(f4) as file2:
            same = set(file1).intersection(file2)
    for line in same:
        names_in_2_and_4 +=1
        print(line, end='', file=outfile)
with open('C:\\Users\\Naree\\Desktop\\DataSets_to_Compare_Nov_1_and_Dec_1_for_all_4_tools\\similaritiesf3f4.txt', 'w') as outfile:
    with open(f3) as file1:
        with open(f4) as file2:
            same = set(file1).intersection(file2)
    for line in same:
        names_in_3_and_4 += 1
        print(line, end='', file=outfile)  

with open('C:\\Users\\Naree\\Desktop\\similaritiesALEXAANDCISCONAMES.txt', 'w') as outfile:    
    with open('C:\\Users\\Naree\\Desktop\\first dataset\\query_name.txt') as file1:
        with open('C:\\Users\\Naree\\Desktop\\first dataset\\cisco_query_name.txt') as file2:
            same = set(file1).intersection(file2)
    for line in same:
        print(line, end='', file=outfile)

all = 0

with open('C:\\Users\\Naree\\Desktop\\DataSets_to_Compare_Nov_1_and_Dec_1_for_all_4_tools\\similaritiesAll.txt', 'w') as outfile:
    with open(f1) as file1:
        with open(f2) as file2:
            with open(f3) as file3:
                with open(f4) as file4:
                    
                    x = file1.readlines()
                    xx = file2.readlines()
                    y = file3.readlines()
                    yy = file4.readlines()
                    same = set(x) & set(xx) & set(y) & set(yy)
                    #same = set(file1) & set(file2) & set(file3) & set(file4)
                    for line in same:
                        all += 1
                        print(line, end='', file=outfile) 


print("Same IPv4 Address in  Alexa and Cisco: ", names_in_1_and_2)
print("Same IPv4 Address in  Alexa and CloudFlare: ",names_in_1_and_3)
print("Same IPv4 Address in  Alexa and Tranco: ",names_in_1_and_4)
print("Same IPv4 Address in  Cisco and CloudFlare: ",names_in_2_and_3)
print("Same IPv4 Address in  Cisco and Tranco: ",names_in_2_and_4)
print("Same IPv4 Address in  CloudFlare and Tranco: ",names_in_3_and_4)
print("Same IPv4 Address in  all four: ", all )
#print("country codes unique: ", )




"""
    # all files are in the one folder, but if we had them sorted into different sub folders we would use this for loop to traverse through each subfolder
    for root, subdirectories, files in os.walk(directory):
        for file in files:
            aSingleFile = os.path.join(root, file)

            if aSingleFile.lower().endswith('.avro'):
                
                reader = DataFileReader(open(aSingleFile, "rb"), DatumReader())
                for instance in reader:
                    print(instance['query_type'], file=open("C:\\Users\\Naree\\Desktop\\ciscoumbnovanddec\\ciscoumbnovanddec_query_type.txt", "a"))
                    print(instance['query_name'], file=open("C:\\Users\\Naree\\Desktop\\ciscoumbnovanddec\\ciscoumbnovanddec_query_name.txt", "a"))
                    print(instance['country'], file=open("C:\\Users\\Naree\\Desktop\\ciscoumbnovanddec\\ciscoumbnovanddec_country.txt", "a"))
                    print(instance['ip4_address'], file=open("C:\\Users\\Naree\\Desktop\\ciscoumbnovanddec\\ciscoumbnovanddec_ip4_address.txt", "a"))
                    print(instance['ip6_address'], file=open("C:\\Users\\Naree\\Desktop\\ciscoumbnovanddec\\ciscoumbnovanddec_ip6_address.txt", "a"))
"""  
main()

