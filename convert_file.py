#!/usr/bin/env python3

from xmilib import XMIT
import sys

if len(sys.argv) < 2:
    print("usage:\n\n{} ebcdic_file [record length] [ebcdic codepage]".format(sys.argv[0]))
    print("\nset record length to 0 to not add new lines every [record length]\nDefault record length is 80\nDefault codepage is cp1140")
    sys.exit()

lrecl = 80
cp = 'cp1140'
if len(sys.argv) == 3:
    lrecl = sys.argv[2]
if len(sys.argv) == 4:
    cp = sys.argv[3]

XMI=XMIT(unnum=False,encoding=cp)

with open(sys.argv[1], 'rb') as eb_file:
    ebcdic_file = eb_file.read()

print(XMI.convert_text_file(ebcdic_file, int(lrecl)))

