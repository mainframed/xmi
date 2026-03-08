#!/bin/bash

# This bash script tests xmi.py against the sample XMI/AWS/HET files contained
# in this folder.

cd "$(dirname "$0")"

# echo "Testing file parsing (XMI / AWS / HET) and creation"
python3 -m unittest discover -s . -p "test_*.py" -v


