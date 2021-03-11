#!/bin/bash

# This bash script tests xmi.py against the sample XMI/AWS/HET files contained
# in this folder.

echo "Testing XMI files"
for i in *.xmi; do echo "python3 ../xmi.py $i"; python3 ../xmi.py $i; done

echo "Testing AWS files"
for i in *.aws; do echo "python3 ../xmi.py $i";python3 ../xmi.py $i; done

echo "Testing HET files"
for i in *.het; do echo "python3 ../xmi.py $i";python3 ../xmi.py $i; done
