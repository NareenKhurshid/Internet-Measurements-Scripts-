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

f1 = 'C:\\Users\\Naree\\Desktop\\domain names\\similaritiesf1f2.txt'

def main():
    directory = 'C:\\Users\\Naree\\Desktop\\alexa_data'

    for root, subdirectories, files in os.walk(directory):
        for file in files:
            aSingleFile = os.path.join(root, file)

            if aSingleFile.lower().endswith('.avro'):
                reader = DataFileReader(open(aSingleFile, "rb"), DatumReader())
                for instance in reader:

                    with open(f1) as file1:
                        for line in file1:
                            if (instance['query_name'] == 'line'):
                                print(instance['country'])
                    

main()

