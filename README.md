# The Python XMIT/Virtual Tape Unload Library

This script parses and extracts the contents of XMIT/AWS/HET files dumping them in to a folder named after the file or dataset in the XMIT file.

This library uses the magic library to try to determine the mimetype of the file in the XMIT/TAPE and convert it from ebcdic to UTF-8 if needed. Appropriate file extentions are also added to identified file times. For example, if a file is a JCL file it will convert it to UTF-8, however if it is a ZIP file it will not convert it. This is configurable as described below.

To use this library:
- Create an XMI object: `XMI = XMIT(<args>)`
- The arguments are:
   - `filename`: the file to load
   - `LRECL`: manual LRECL override
   - `outputfolder`: specific output folder, default is ./
   - `encoding`: EBCDIC table to use to translate files, default is cp1140
   - `loglevel`: by default logging is set to WARNING, set to DEBUG for verbose debug output
   - `unnum`: removes the numbers in the rightmost column, default `True`
   - `quiet`: no output except to STDERR, default `False`
   - `force`: force convert all files/members to UTF-8, default `False`
   - `binary`: do not convert anyfiles, default `False`
   - `modifydate`: change the last modify date on the file system to match ISPF stats
- If the file you are loading is an XMI file (XMIT/TSO Transmission) use `XMI.parse_xmi()` this will generate a XMIT dict (`XMI.xmit`) which contains the contents of the XMI file
- Next `XMI.get_xmi_files()`/`XMI.get_tape_files()` will collect filenames and files (members) from the XMIT/Tape and populate `XMI.xmit`/`XMI.tape` with the files/members of the datasets and stores the information in `XMI.xmit`/`XMI.tape`
- Finally now you can print/dump the contents
   - `XMI.print_xmit()`/`XMI.print_tape()` prints the contents of the XMIT file directory. If the optional argument `human` is passed file sizes are converted to human readable
   - `XMI.unload()` this function will extract and translate (if needed based on the file mimetype) all the files/members from the provided XMIT/Tape. The folder and other options provided upon initialization affect the output folder and translation. By default the output folder is `./`, and the file will have the number column removed from the far right.
   - `XMI.dump_xmit_json()` takes all the arguments and file flags/information and dumps it to a json file named after the XMIT file

## Example Usage

A very simple usage of this script:

Assuming you dowloaded `CBT982.XMI` fomr http://CBTTAPE.org:

```python
XMI = XMIT(filename="CBT982.XMI")
XMI.parse_xmi()
XMI.get_xmi_files()
XMI.print_xmit(human=True)
XMI.unload()
XMI.dump_xmit_json()
```

## Included Scripts

### recv.py

Simple python script to extract and unload XMIT file:

```
usage: recv.py [options] [tape File]

TSO XMIT File Unload utility

positional arguments:
  XMIT_FILE             XMIT File

optional arguments:
  -h, --help            show this help message and exit
  -u, --unnum           Remove number column from text files (default: True)
  -j, --json            Write XMIT file information to json file (default: False)
  --jsonfile JSONFILE   Dump json file location (default: ./)
  -q, --quiet           Don't print unload output (default: False)
  -d, --debug           Print lots of debugging statements (default: 30)
  -p, --print           Print unload information only (no file creation) (default: False)
  -H, --human           Print unload information human readable (default: False)
  -f, --force           Force all files to be translated to plain text regardless of mimetype (default: False)
  -b, --binary          Store all files as binary (default: False)
  -m, --modify          Set the unloaded last modify date to match ISPF statistics if available (default: False)
  --outputdir OUTPUTDIR
                        Folder to place tape files in, default is current working directory (default:
                        /home/phil/DEV/recvxmi)
  --encoding ENCODING   EBCDIC encoding translation table (default: cp1140)
```

### tape.py

Simple python script to extract and unload a virtual tape file. Supports HET and AWS tape formats.

```
usage: tape.py [options] [tape File]

AWS/HET Tape Unload utility

positional arguments:
  tape_file             Virtual tape file

optional arguments:
  -h, --help            show this help message and exit
  -u, --unnum           Do not remove number column from text files (default: True)
  -l LRECL, --lrecl LRECL
                        Set record length (default: 80)
  -q, --quiet           Don't print unload output (default: False)
  -d, --debug           Print lots of debugging statements (default: 30)
  -p, --print           Print unload information only (no file creation) (default: False)
  -H, --human           Print unload information human readable (default: False)
  -f, --force           Force all files to be translated to plain text regardless of mimetype (default: False)
  --outputdir OUTPUTDIR
                        Folder to place tape files in, default is current working directory (default:
                        /home/phil/DEV/recvxmi)
  --encoding ENCODING   EBCDIC encoding translation table (default: cp1140)
```

### conver_file.py

This script will convert an EBCDIC file to UTF-8 using record length 80 and EBCDIC codepage 1140. To change the record length and codepage pass two arguments:

```
usage:

./convert_file.py ebcdic_file [record length] [ebcdic codepage]

set record length to 0 to not add new lines every [record length]
Default record length is 80
Default codepage is cp1140
```

## Known issues:

* PDSE support is beta it works for now
* Aliases may not dump the right file name


