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
from datetime import datetime
from zipfile import ZipFile

def main():
    directory = 'C:\\Users\\Naree\\Desktop\\alexa_data'
    #outputPath = 'C:\\Users\\Naree\\Desktop\\results.txt'
    #sys.stdout = open(outputPath, "w", encoding="utf-8")
    #tar = tarfile.open(...)
    singles = []
    for root, subdirectories, files in os.walk(directory):
        for file in files:
            aSingleFile = os.path.join(root, file)

            #iterate over files, if tar extract the files within
            singles.append(aSingleFile)
            if aSingleFile.lower().endswith('.tar'):
                tar = tarfile.open(os.path.join(directory, file))
                tar.extractall(path=directory)
                tar.close()

            elif aSingleFile.lower().endswith('.avro'):
                print(aSingleFile)
                #show avro as python dictionary 
                #reader: DataFileReader = DataFileReader(open(aSingleFile, 'rb'), DatumReader())
                #schema: dict = json.loads(reader.meta.get('avro.schema').decode('utf-8'))
                #print (schema)

                reader = DataFileReader(open(aSingleFile, "rb"), DatumReader())
                for instance in reader:
                    print(instance['query_type'], file=open("alexa_query_type.txt", "a"))
                    print(instance['query_name'], file=open("query_name.txt", "a"))
                    print(instance['country'], file=open("country.txt", "a"))
                    print(instance['ip4_address'], file=open("ip4_address.txt", "a"))
                    print(instance['ip6_address'], file=open("ip6_address.txt", "a"))



                #reader.close()
main()

#intrested in these:
#query_type
#query_name
#response_type
#timestamp
#country
#ip4_address
#ip6_address

