#!/usr/bin/env python3

# The Python XMIT/Virtual Tape unload script
# This script will unload XMI(T)/AWS/HET files
# dumping them in to a folder named after the file or
# dataset in the XMIT file.
#
# This library will also try to determine the mimetype
# of the file in the XMIT/TAPE and convert it from ebcdic to
# ascii if needed. Appropriate file extentions are also added
# to identified file times.
#
# To use this library:
#  - Create an XMI object: XMI = XMIT(<args>)
#    - The arguments are:
#    - filename: the file to load
#    - LRECL: manual LRECL override
#    - outputfolder: specific output folder, default is ./
#    - encoding: EBCDIC table to use to translate files, default is cp1140
#    - loglevel: by default logging is set to WARNING, set to DEBUG for verbose debug output
#    - unnum: removes the numbers in the rightmost column, default true
#    - quiet: no output except to STDERR
#    - force: force convert all files/members to UTF-8
#    - binary: do not convert anyfiles.
#    - modifydate: change the last modify date on the file system to match ISPF
#  - If the file your loading is an XMI file (XMIT/TSO Transmission) use
#    `XMI.parse_xmi()` this will generate a XMIT dict (`XMI.xmit`) which contains
#    the contents of the XMI file
#  - Next `XMI.get_xmi_files()`/`XMI.get_tape_files()` will collect filenames and files (members) from the XMIT/Tape
#    and populates `XMI.xmit`/`XMI.tape` with the files/members of the dataset and stores the information in `XMI.xmit`/`XMI.tape`
#  - Finally now you can print/dump the contents
#    - XMI.print_xmit()/XMI.print_tape() prints the contents of the XMIT file. If the optional argument `human` is passed
#      file sizes are converted to human readable
#    - XMI.unload() this function will extract and translate (if needed based on the file mimetype)
#      all the files/members from the provided XMIT/Tape. The folder and other options provided
#      upon initialization affect the output folder and translation. By default the output folder is `./`,
#      the file will have the number column removed in the far right.
#    - XMI.dump_xmit_json() takes all the arguments and file flags/information and dumps it to a json file
#      named after the XMIT file



from hexdump import hexdump
from pprint import pprint
from array import array
from struct import pack
from pathlib import Path

import zlib
import bz2
import json
import logging
import argparse
import ebcdic
import os
import struct
import sys
import re
import datetime
import time
import magic
import mimetypes

text_keys = {}
text_keys[0x0001] = { 'name' : "INMDDNAM", 'type' : "character", 'desc' :'DDNAME for the file'}
text_keys[0x0002] = { 'name' : "INMDSNAM", 'type' : "character", 'desc' :'Name of the file'}
text_keys[0x0003] = { 'name' : "INMMEMBR", 'type' : "character", 'desc' :'Member name list'}
text_keys[0x000B] = { 'name' : "INMSECND", 'type' : "decimal", 'desc' :'Secondary space quantity'}
text_keys[0x000C] = { 'name' : "INMDIR"  , 'type' : "decimal", 'desc' :'Number of directory blocks'}
text_keys[0x0022] = { 'name' : "INMEXPDT", 'type' : "character", 'desc' :'Expiration date'}
text_keys[0x0028] = { 'name' : "INMTERM" , 'type' : "character", 'desc' :'Data transmitted as a message'}
text_keys[0x0030] = { 'name' : "INMBLKSZ", 'type' : "decimal", 'desc' :'Block size'}
text_keys[0x003C] = { 'name' : "INMDSORG", 'type' : "hex", 'desc' :'File organization'}
text_keys[0x0042] = { 'name' : "INMLRECL", 'type' : "decimal", 'desc' :'Logical record length'}
text_keys[0x0049] = { 'name' : "INMRECFM", 'type' : "hex", 'desc' :'Record format'}
text_keys[0x1001] = { 'name' : "INMTNODE", 'type' : "character", 'desc' :'Target node name or node number'}
text_keys[0x1002] = { 'name' : "INMTUID" , 'type' : "character", 'desc' :'Target user ID'}
text_keys[0x1011] = { 'name' : "INMFNODE", 'type' : "character", 'desc' :'Origin node name or node number'}
text_keys[0x1012] = { 'name' : "INMFUID" , 'type' : "character", 'desc' :'Origin user ID'}
text_keys[0x1020] = { 'name' : "INMLREF" , 'type' : "character", 'desc' :'Date last referenced'}
text_keys[0x1021] = { 'name' : "INMLCHG" , 'type' : "character", 'desc' :'Date last changed'}
text_keys[0x1022] = { 'name' : "INMCREAT", 'type' : "character", 'desc' :'Creation date'}
text_keys[0x1023] = { 'name' : "INMFVERS", 'type' : "character", 'desc' :'Origin version number of the data format'}
text_keys[0x1024] = { 'name' : "INMFTIME", 'type' : "character", 'desc' :'Origin timestamp'} # yyyymmddhhmmssuuuuuu
text_keys[0x1025] = { 'name' : "INMTTIME", 'type' : "character", 'desc' :'Destination timestamp'}
text_keys[0x1026] = { 'name' : "INMFACK" , 'type' : "character", 'desc' :'Originator requested notification'}
text_keys[0x1027] = { 'name' : "INMERRCD", 'type' : "character", 'desc' :'RECEIVE command error code'}
text_keys[0x1028] = { 'name' : "INMUTILN", 'type' : "character", 'desc' :'Name of utility program'}
text_keys[0x1029] = { 'name' : "INMUSERP", 'type' : "character", 'desc' :'User parameter string'}
text_keys[0x102A] = { 'name' : "INMRECCT", 'type' : "character", 'desc' :'Transmitted record count'}
text_keys[0x102C] = { 'name' : "INMSIZE" , 'type' : "decimal", 'desc' :'File size in bytes'}
text_keys[0x102F] = { 'name' : "INMNUMF" , 'type' : "decimal", 'desc' :'Number of files transmitted'}
text_keys[0x8012] = { 'name' : "INMTYPE" , 'type' : "hex", 'desc' :'Data set type'}

class XMIT:
    def __init__(self, filename=None,LRECL=80,
                 loglevel=logging.WARNING,
                 outputfolder="./",
                 encoding='cp1140',
                 unnum=True,
                 quiet=False,
                 force=False,
                 binary=False,
                 modifydate=False):
        self.filename = filename
        self.manual_recordlength = LRECL
        self.xmit_object = ''
        self.tape_object = ''
        self.outputfolder = Path(outputfolder)
        self.INMR02_count = 0
        self.INMR03_count = 0
        self.msg = False
        self.force = force
        self.binary = binary
        self.filelocation = 1
        self.ebcdic = encoding
        self.unnum = unnum
        self.quiet = quiet
        self.pdstype = False
        self.xmit = None
        self.tape = None
        self.modifydate = modifydate
        self.loglevel = loglevel

        # Create the Logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        logger_formatter = logging.Formatter('%(levelname)s :: {} :: %(funcName)s :: %(message)s'.format(self.filename))
        # Log to stderr
        ch = logging.StreamHandler()
        ch.setFormatter(logger_formatter)
        ch.setLevel(loglevel)
        self.logger.addHandler(ch)

        self.logger.debug("File: {}".format(self.filename))
        self.logger.debug("LRECL: {}".format(LRECL))
        self.logger.debug("Output Folder: {}".format(outputfolder))
        self.logger.debug("Encoding: {}".format(encoding))
        self.logger.debug("Unnum: {}".format(unnum))
        self.logger.debug("quiet: {}".format(quiet))
        self.logger.debug("force: {}".format(force))
        self.logger.debug("binary: {}".format(binary))


    def set_xmit_file(self, filename):
        self.logger.debug("Setting XMIT filename to: {}".format(filename))
        self.filename = filename

    def set_tape_file(self, filename):
        self.logger.debug("Setting TAPE filename to: {}".format(filename))
        self.filename = filename

    def set_output_folder(self, outputfolder):
        # by default this function will create the output folder if it doesnt exist
        self.logger.debug("Setting output folder to: {}".format(outputfolder))
        self.outputfolder = Path(outputfolder)

    def read_xmit_file(self):
        self.logger.debug("Reading file: {}".format(self.filename))
        with open(self.filename, 'rb') as xmifile:
            self.xmit_object = xmifile.read()
        self.logger.debug("Total bytes: {}".format(len(self.xmit_object)))

    def read_tape_file(self):
        self.logger.debug("Reading file: {}".format(self.filename))
        with open(self.filename, 'rb') as tapefile:
            self.tape_object = tapefile.read()
        self.logger.debug("Total bytes: {}".format(len(self.tape_object)))

    def set_force(self):
        self.logger.debug("Setting force file conversion")
        self.force = True

    def print_xmit(self, human=True):

        if 'INMR02' not in self.xmit:
            raise('No INMR02 found in {}'.format(self.filename))

        IEBCOPY = False
        for i in self.xmit['INMR02']:

            if IEBCOPY:
                IEBCOPY = False
                continue

            if 'INMDSNAM' not in self.xmit['INMR02'][i]:
                print("There is 1 message in this XMIT\n")
                continue
            DSN = self.xmit['INMR02'][i]['INMDSNAM']

            if self.xmit['INMR02'][i]['INMUTILN'] == 'IEBCOPY':

                members = self.xmit['file'][DSN]['members']
                IEBCOPY = True

                directory_listing = "{:<"+str(len(DSN))+"}\t{:<8}   {:<7}  {:<12}  {:<17}  {:<8}  {:<10}  {} {}"
                print(directory_listing.format('Dataset','Member', 'Version', 'Create Date', 'Modify Date/Time', 'Username', 'Size', 'Mimetype',''))
                for member in members:
                    #pprint(members[member])
                    if not members[member]['alias']:
                        if 'ispf' in members[member] and members[member]['ispf']:
                            createdate = datetime.datetime.fromisoformat(members[member]['ispf']['createdate'])
                            modifydate = datetime.datetime.fromisoformat(members[member]['ispf']['modifydate'])
                            print(directory_listing.format(
                                DSN,
                                member,
                                members[member]['ispf']['version'],
                                createdate.strftime("%x"),
                                modifydate.strftime("%x %X"),
                                members[member]['ispf']['user'],
                                self.sizeof_fmt(len(members[member]['data'])) if human else len(members[member]['data']),
                                members[member]['mimetype'],
                                members[member]['datatype']
                            ))
                        else:
                            print(directory_listing.format(
                                DSN,
                                member,
                                '','','','',
                                self.sizeof_fmt(len(members[member]['data'])) if human else len(members[member]['data']),
                                members[member]['mimetype'],
                                members[member]['datatype']))
                    else:
                        alias_ttr = members[member]['ttr']
                        for ttr in members:
                            if 'ttr' in members[ttr] and members[ttr]['ttr'] == alias_ttr and ttr not in member:
                                print(directory_listing.format(DSN, member,'  ->', ttr, '', '' ,'alias','',''))
            else:
                sequential_listing = "{:<"+str(len(DSN))+"}\t{:<20}  {:<8}  {:<10}  {}"
                date = self.xmit['INMR01']['INMFTIME']
                d = datetime.datetime.strptime(date,'%Y%m%d%H%M%S%f')

                print(sequential_listing.format("Dataset", "Create Date", "Username", "Size", "Mimetype"))
                print(sequential_listing.format(
                    DSN,
                    d.strftime('%x %X'),
                    self.xmit['INMR01']['INMFUID'],
                    self.sizeof_fmt(len(self.xmit['file'][DSN]['text'])) if human else len(self.xmit['file'][DSN]['text']),
                    "text/plain ebcdic"
                     ))

    def print_tape(self, human=True):
        if not self.tape:
            raise Exception('No tape loaded')
        filename_size = (len(self.filename) + 6) if len(self.filename) > 17 else 17

        print_tape_simple = "{:<"+str(filename_size)+"}  {:<12}  {:<20}  {:<10}  {:<13}  {:<10}  {} {}"
        print(print_tape_simple.format("Filename", "Create Date", "Modified Date", "Owner", "System Code", "Size", "Mimetype", '' ))
        for tape_file in self.tape['files']:
            if 'members' in tape_file and 'dsn' in tape_file:

                tape_file['datatype'] = "IEBCOPY"
                print(print_tape_simple.format(
                    tape_file['dsn'],
                    tape_file['createdate'],
                    '',
                    tape_file['owner'],
                    tape_file['system_code'],
                    self.sizeof_fmt(len(tape_file['data'])) if human else len(tape_file['data']),
                    tape_file['filetype'],
                    tape_file['datatype']
                    ))

                for member in tape_file['members']:
                    if not tape_file['members'][member]['alias'] and 'data' in tape_file['members'][member] :
                        if 'ispf' in tape_file['members'][member] and tape_file['members'][member]['ispf']:

                            createdate = datetime.datetime.fromisoformat(tape_file['members'][member]['ispf']['createdate'])
                            modifydate = datetime.datetime.fromisoformat(tape_file['members'][member]['ispf']['modifydate'])
                            print(print_tape_simple.format(
                                "{}({})".format(tape_file['dsn'],member),
                                createdate.strftime("%x"),
                                modifydate.strftime("%x %X"),
                                tape_file['members'][member]['ispf']['user'],
                                '',
                                self.sizeof_fmt(len(tape_file['members'][member]['data'])) if human else len(tape_file['members'][member]['data']),
                                tape_file['members'][member]['mimetype'],
                                tape_file['members'][member]['datatype']
                            ))
                        else:
                            print(print_tape_simple.format(
                                "{}({})".format(tape_file['dsn'],member),
                                '',
                                '',
                                '',
                                '',
                                self.sizeof_fmt(len(tape_file['members'][member]['data'])) if human else len(tape_file['members'][member]['data']),
                                tape_file['members'][member]['mimetype'],
                                tape_file['members'][member]['datatype']
                            ))
                    else:
                        if tape_file['members'][member]['alias']:
                            alias_ttr = tape_file['members'][member]['ttr']
                            for ttr in tape_file['members']:
                                if 'ttr' in tape_file['members'][ttr] and tape_file['members'][ttr]['ttr'] == alias_ttr and ttr not in member:
                                    print(print_tape_simple.format("{}({})".format(tape_file['dsn'],member),
                                    '  ->', ttr, '', '' ,'','alias',''))
                        else:
                            print(print_tape_simple.format("{}({})".format(tape_file['dsn'],member),
                                    '', '', '', '' ,'','empty',''))
            elif 'members' in tape_file and 'dsn' not in tape_file:

                print(print_tape_simple.format(
                    "{}.{:#02}{}".format(Path(self.filename).stem, tape_file['filenum'], tape_file['extention']),
                    '', '', '','',
                    self.sizeof_fmt(len(tape_file['data'])) if human else len(tape_file['data']),
                    tape_file['filetype'],
                    tape_file['datatype']
                    ))
                for member in tape_file['members']:
                    if not tape_file['members'][member]['alias'] and 'data' in tape_file['members'][member] :
                        if 'ispf' in tape_file['members'][member] and tape_file['members'][member]['ispf']:

                            createdate = datetime.datetime.fromisoformat(tape_file['members'][member]['ispf']['createdate'])
                            modifydate = datetime.datetime.fromisoformat(tape_file['members'][member]['ispf']['modifydate'])
                            print(print_tape_simple.format(
                                "{}.{:#02} {}".format(Path(self.filename).stem, tape_file['filenum'], member),
                                createdate.strftime("%x"),
                                modifydate.strftime("%x %X"),

                                tape_file['members'][member]['ispf']['user'],
                                '',
                                self.sizeof_fmt(len(tape_file['members'][member]['data'])) if human else len(tape_file['members'][member]['data']),
                                tape_file['members'][member]['mimetype'],
                                tape_file['members'][member]['datatype']
                            ))
                        else:
                            print(print_tape_simple.format(
                                "{}.{:#02} {}".format(Path(self.filename).stem, tape_file['filenum'], member),
                                '',
                                '',
                                '',
                                '',
                                self.sizeof_fmt(len(tape_file['members'][member]['data'])) if human else len(tape_file['members'][member]['data']),
                                tape_file['members'][member]['mimetype'],
                                tape_file['members'][member]['datatype']
                            ))
                    else:
                        if tape_file['members'][member]['alias']:
                            alias_ttr = tape_file['members'][member]['ttr']
                            for ttr in tape_file['members']:
                                if 'ttr' in tape_file['members'][ttr] and tape_file['members'][ttr]['ttr'] == alias_ttr and ttr not in member:
                                    print(print_tape_simple.format("{}({})".format(self.filename,member),
                                    '  ->', ttr, '', '' ,'','alias',''))
                        else:
                            print(print_tape_simple.format("{}({})".format(self.filename,member),
                                    '', '', '', '' ,'','empty',''))

            elif 'members' not in tape_file and 'dsn' in tape_file:
                print(print_tape_simple.format(
                    "{}{}".format(tape_file['dsn'], tape_file['extention']),
                    '', '', '','',
                    self.sizeof_fmt(len(tape_file['data'])) if human else len(tape_file['data']),
                    tape_file['filetype'],
                    tape_file['datatype']
                    ))

            else:

                print(print_tape_simple.format(
                    "{}.{:#02}{}".format(Path(self.filename).stem, tape_file['filenum'], tape_file['extention']),
                    '', '', '','',
                    self.sizeof_fmt(len(tape_file['data'])) if human else len(tape_file['data']),
                    tape_file['filetype'],
                    tape_file['datatype']
                    ))



                #self.hexdump(tape_file['data'])


    def unload(self):

        if not self.tape and not self.xmit:
            raise Exception("No tape or xmit file loaded")

        if self.xmit:
            self.unload_xmit()
        else:
            self.unload_tape()

    def unload_xmit(self):
        self.logger.debug("Unloading XMIT")
        IEBCOPY = False
        if not self.outputfolder.exists():
            self.logger.debug("Output folder '{}' does not exist, creating".format(self.outputfolder.absolute()))
            os.makedirs(self.outputfolder)

        if 'INMR02' not in self.xmit:
            raise Exception('No dataset loaded')

        dialog = "{dsn}({member})\t->\t{path}"

        for i in self.xmit['INMR02']:

            if IEBCOPY:
                # skip the second INMCOPY that follows IEBCOPY
                IEBCOPY = False
                continue

            if 'INMDSNAM' not in self.xmit['INMR02'][i]:
                self.logger.debug('Printing Message')
                self.print_message()
                continue

            DSN = self.xmit['INMR02'][i]['INMDSNAM']
            dataset_folder = self.outputfolder / DSN
            self.logger.debug("Unloading {} File to {}".format(self.filename, dataset_folder.absolute()))

            if not dataset_folder.exists():
                self.logger.debug("Creating {} folder: {}".format(DSN, dataset_folder.absolute()))
            dataset_folder.mkdir(parents=True, exist_ok=True)

            if self.xmit['INMR02'][i]['INMUTILN'] == 'IEBCOPY':
                IEBCOPY = True
                members = self.xmit['file'][DSN]['members']
                for member in members:
                    if members[member]['alias']:
                        self.logger.debug("File alias skipping")
                        continue
                    if 'data' not in members[member]:
                        self.logger.debug("Member {} has no data".format(member)) # This is a known bug with certain files formatted U
                        continue
                    member_path = dataset_folder / "{}{}".format(member,members[member]['extension'])
                    if 'text' in members[member] and not self.binary:
                        self.logger.debug("Unloading plaintext file to {}".format(member_path.absolute()))
                        if not self.quiet:
                            print(dialog.format(dsn=DSN, member=member, path=member_path.absolute()))
                        with member_path.open("w", encoding ="utf-8") as f:
                            f.write(members[member]['text'])
                            f.close()
                        if ('ispf' in members[member] and
                            members[member]['ispf'] and
                            'createdate' in members[member]['ispf']
                            and self.modifydate):

                            self.logger.debug("Changing last modify date to match ISPF records: {}".format(members[member]['ispf']['createdate']))
                            d = datetime.datetime.fromisoformat(members[member]['ispf']['modifydate'])
                            modTime = time.mktime(d.timetuple())
                            os.utime(member_path.absolute(), (modTime, modTime))



                    else:
                        self.logger.debug("Unloading binary file to {}".format(member_path.absolute()))
                        if not self.quiet:
                            print(dialog.format(dsn=DSN, member=member, path=member_path.absolute()))
                        with member_path.open("wb") as f:
                            f.write(members[member]['data'])
                            f.close()
                        if ('ispf' in members[member] and
                            members[member]['ispf'] and
                            'createdate' in members[member]['ispf']
                            and self.modifydate):
                            self.logger.debug("Changing last modify date to match ISPF records: {}".format(members[member]['ispf']['createdate']))
                            d = datetime.datetime.fromisoformat(members[member]['ispf']['modifydate'])
                            modTime = time.mktime(d.timetuple())
                            os.utime(member_path.absolute(), (modTime, modTime))
            else:
                #sequential dataset
                if not self.binary:
                    seq_path = dataset_folder / "{}{}".format(DSN, ".txt")
                    print("{}\t->\t{}".format(DSN, seq_path.absolute()))
                    with seq_path.open("w") as f:
                        f.write(self.xmit['file'][DSN]['text'])
                        f.close()
                else:

                    seq_path = dataset_folder / "{}{}".format(DSN, ".bin")
                    print("{}\t->\t{}".format(DSN, seq_path.absolute()))
                    with seq_path.open("wb") as f:
                        f.write(b''.join(self.xmit['file'][DSN]['data']))
                        f.close()

    def unload_tape(self):
        # PY: This is messy and redundant but its 3am
        self.logger.debug("Unloading tape")

        if not self.outputfolder.exists():
            self.logger.debug("Output folder '{}' does not exist, creating".format(self.outputfolder.absolute()))
            os.makedirs(self.outputfolder)
        tape_folder = self.outputfolder / Path(self.filename).stem
        self.logger.debug("Unloading tape to {}".format(tape_folder.absolute()))

        dialog="{} {} -> {}"
        dialog_dsn_members = "{} {} -> {}"

        if not tape_folder.exists():
            self.logger.debug("Creating folder: {}".format(tape_folder.absolute()))
        tape_folder.mkdir(parents=True, exist_ok=True)

        for tape_file in self.tape['files']:

            if 'members' in tape_file and 'dsn' in tape_file:
                dsn_folder = tape_folder / tape_file['dsn']

                if not dsn_folder.exists():
                    self.logger.debug("Creating folder: {}".format(dsn_folder.absolute()))

                dsn_folder.mkdir(parents=True, exist_ok=True)

                for member in tape_file['members']:
                    if tape_file['members'][member]['alias'] or 'data' not in tape_file['members'][member]:
                        continue
                    member_path = dsn_folder / "{}{}".format(member, tape_file['members'][member]['extension'])
                    if 'text' in tape_file['members'][member]:
                        self.logger.debug("Unloading plaintext file to {}".format(member_path.absolute()))
                        if not self.quiet:
                            print(dialog.format(tape_file['dsn'], member, member_path.absolute()))
                        with member_path.open("w", encoding ="utf-8") as f:
                            f.write(tape_file['members'][member]['text'])
                            f.close()
                    else:
                        self.logger.debug("Unloading binary file to {}".format(member_path.absolute()))
                        if not self.quiet:
                            print(dialog.format(tape_file['dsn'], member, member_path.absolute()))
                        with member_path.open("wb") as f:
                            f.write(tape_file['members'][member]['data'])
                            f.close()

            elif 'members' in tape_file and 'dsn' not in tape_file:

                dsn_folder = tape_folder / "{}.{:#02}".format(tape_folder.stem, tape_file['filenum'])
                if not dsn_folder.exists():
                    self.logger.debug("Creating folder: {}".format(dsn_folder.absolute()))
                dsn_folder.mkdir(parents=True, exist_ok=True)
                for member in tape_file['members']:
                    if tape_file['members'][member]['alias'] or 'data' not in tape_file['members'][member]:
                        continue
                    member_path = dsn_folder / "{}{}".format(member, tape_file['members'][member]['extension'])
                    if 'text' in tape_file['members'][member]:
                        self.logger.debug("Unloading plaintext file to {}".format(member_path.absolute()))
                        if not self.quiet:
                            print(dialog.format("{}.{:#02}".format(tape_folder.stem, tape_file['filenum']), member, member_path.absolute()))
                        with member_path.open("w", encoding ="utf-8") as f:
                            f.write(tape_file['members'][member]['text'])
                            f.close()
                    else:
                        self.logger.debug("Unloading binary file to {}".format(member_path.absolute()))
                        if not self.quiet:
                            print(dialog.format("{}.{:#02}".format(tape_folder.stem, tape_file['filenum']), member, member_path.absolute()))
                        with member_path.open("wb") as f:
                            f.write(tape_file['members'][member]['data'])
                            f.close()

            elif 'dsn' in tape_file and 'members' not in tape_file:

                tape_file_path = tape_folder / "{}{}".format(tape_file['dsn'], tape_file['extention'])
                if tape_file['text']:
                    self.logger.debug("Unloading plaintext file to {}".format(tape_file_path.absolute()))
                    if not self.quiet:
                        print(dialog.format(self.filename, tape_file['dsn'], tape_file_path.absolute()))
                    with tape_file_path.open("w", encoding ="utf-8") as f:
                        f.write(tape_file['text'])
                        f.close()
                else:
                    self.logger.debug("Unloading binary file to {}".format(tape_file_path.absolute()))
                    if not self.quiet:
                        print(dialog.format(self.filename, tape_file['dsn'], tape_file_path.absolute()))
                    with tape_file_path.open("wb") as f:
                        f.write(tape_file['data'])
                        f.close()


            else:
                tape_file_path = tape_folder / "{}.{}{}".format(tape_folder.stem, tape_file['filenum'], tape_file['extention'])
                if tape_file['text']:
                    self.logger.debug("Unloading plaintext file to {}".format(tape_file_path.absolute()))
                    if not self.quiet:
                        print(dialog.format(self.filename, tape_file['filenum'], tape_file_path.absolute()))
                    with tape_file_path.open("w", encoding ="utf-8") as f:
                        f.write(tape_file['text'])
                        f.close()
                else:
                    self.logger.debug("Unloading binary file to {}".format(tape_file_path.absolute()))
                    if not self.quiet:
                        print(dialog.format(self.filename, tape_file['filenum'], tape_file_path.absolute()))
                    with tape_file_path.open("wb") as f:
                        f.write(tape_file['data'])
                        f.close()

    def dump_xmit_json(self, json_file_target=None):

        for i in self.xmit['INMR02']:

            if 'INMDSNAM' not in self.xmit['INMR02'][i]:
                continue
            DSN = self.xmit['INMR02'][i]['INMDSNAM']

        if not json_file_target:
            json_file_target = self.outputfolder / "{}.json".format(Path(self.filename).stem)

        self.logger.debug("Dumping {} JSON file to {}".format(DSN, json_file_target.absolute()))

        output_dict = self.xmit
        # if 'message' in self.xmit:
        #     self.xmit['message'].pop('file', None)
        for f in output_dict['file']:
            output_dict['file'][f].pop('data', None)

            if 'members' in output_dict['file'][f]:
                for m in output_dict['file'][f]['members']:
                    output_dict['file'][f]['members'][m].pop('data', None)
                    #output_dict['file'][f]['members'][m].pop('parms', None)
        output_dict['SCRIPTOPTIONS'] = {
            'filename' : self.filename,
            'LRECL' : self.manual_recordlength,
            'loglevel' : self.loglevel,
            'outputfolder' : self.outputfolder,
            'encoding' : self.ebcdic,
            'unnum' : self.unnum,
            'quiet' : self.quiet,
            'force' : self.force,
            'binary' : self.binary,
            'modifydate' : self.modifydate
        }

        with json_file_target.open('w') as outfile:
            json.dump(self.xmit, outfile, default=str)
        self.logger.debug("Done")

    def hexdump(self,data):
        print("="* 5, "hex", "ebcdic")
        hexdump(data)
        print("="* 5, "hex", "ascii" )
        hexdump(data.decode(self.ebcdic).encode('ascii', 'replace'))
        print("="* 5, "hex end")

    def sizeof_fmt(self, num):
        for unit in ['','K','M','G','T','P','E','Z']:
            if abs(num) < 1024.0:
                return "{:3.1f}{}".format(num, unit).rstrip('0').rstrip('.')
            num /= 1024.0
        return "{:.1f}{}".format(num, 'Y')

    def parse_xmi(self):
        self.logger.debug("Parsing XMIT file")
        if self.xmit_object == '':
            self.read_xmit_file()
        self.xmit = {}

        # Get XMI header

        segment_name = self.xmit_object[2:8].decode(self.ebcdic)

        #self.hexdump(self.xmit_object[0:10])

        if segment_name != 'INMR01':
            raise Exception('No INMR01 record found in {}.'.format(self.filename))

        record_data = b''
        raw_data = b''
        loc = 0
        while loc < len(self.xmit_object):
            section_length = self.get_int(self.xmit_object[loc:loc+1])
            flag = self.get_int(self.xmit_object[loc+1:loc+2])


            #self.hexdump(self.xmit_object[loc:loc+section_length])

            if 0x20 != (0x20 & flag): # If we're not a control record


                if 'INMDSNAM' not in self.xmit['INMR02'][1] and self.msg and len(self.xmit['INMR03']) < 2:

                    if "message" not in self.xmit:
                        self.logger.debug("Message record found")
                        self.xmit['message'] = {}
                        self.xmit['message']['file'] = b''
                        self.xmit['message']['lrecl'] = self.xmit['INMR03'][1]['INMLRECL']
                    self.xmit['message']['file'] += self.xmit_object[loc+2:loc+section_length]
                    self.filelocation = 2

                else:
                    dsn = self.xmit['INMR02'][self.filelocation]['INMDSNAM'] # filename
                    if 'file' not in self.xmit:
                        self.xmit['file'] = {}
                    if dsn not in self.xmit['file']:
                        self.logger.debug("{} not recorded creating".format(dsn))
                        self.xmit['file'][dsn] = {}
                        self.xmit['file'][dsn]['data'] = []
                    record_data += self.xmit_object[loc+2:loc+section_length] # get the various segments
                    eighty = False
                    forty = False
                    l = len(self.xmit_object[loc+2:loc+section_length])
                    if 0x80 == (0x80 & flag):
                       eighty = True
                    if 0x40 == (0x40 & flag):
                        forty = True
                        self.xmit['file'][dsn]['data'].append(record_data)
                        record_data = b''
                    self.logger.debug("Location: {:8} Writting {:<3} bytes Flag: 0x80 {:<1} 0x40 {:<1} (Section length: {})".format(loc, l, eighty, forty, section_length))

            if 0x20 == (0x20 & flag):
                self.logger.debug("[flag 0x20] This is (part of) a control record.")
                record_type = self.xmit_object[loc+2:loc+8].decode(self.ebcdic)
                self.logger.debug("Record Type: {}".format(record_type))
                if record_type == "INMR01":
                    self.parse_INMR01(self.xmit_object[loc+8:loc+section_length])
                elif record_type == "INMR02":
                    self.parse_INMR02(self.xmit_object[loc+8:loc+section_length])
                elif record_type == "INMR03":
                    self.parse_INMR03(self.xmit_object[loc+8:loc+section_length])
                elif record_type == "INMR06":
                    self.logger.debug("[INMR06] Processing last record")
                    return


            if 0x0F == (0x0F & flag):
                self.logger.debug("[flag 0x0f] Reserved")
            loc += section_length

        # Convert messages if there are any
        self.convert_message()

    def parse_INMR01(self, inmr01_record):
        # INMR01 records are the XMIT header and contains information
        # about the XMIT file
        self.xmit['INMR01'] = self.text_units(inmr01_record)
        if 'INMFTIME' in self.xmit['INMR01']:
            # Changing date format to '%Y%m%d%H%M%S%f'
            self.xmit['INMR01']['INMFTIME'] = self.xmit['INMR01']['INMFTIME'] + "0" * (20 - len(self.xmit['INMR01']['INMFTIME']))

    def parse_INMR02(self, inmr02_record):
        self.INMR02_count += 1
        numfiles = struct.unpack('>L', inmr02_record[0:4])[0]
        if 'INMR02' not in self.xmit:
            self.xmit['INMR02'] = {}
        self.xmit['INMR02'][self.INMR02_count] = self.text_units(inmr02_record[4:])
        self.xmit['INMR02'][self.INMR02_count]['INMDSORG'] = self.get_dsorg(self.xmit['INMR02'][self.INMR02_count]['INMDSORG'])
        self.xmit['INMR02'][self.INMR02_count]['INMRECFM'] = self.get_recfm(self.xmit['INMR02'][self.INMR02_count]['INMRECFM'])
        self.xmit['INMR02'][self.INMR02_count]['numfile'] = numfiles


    def parse_INMR03(self, inmr03_record):
        self.INMR03_count += 1
        if 'INMR03' not in self.xmit:
            self.xmit['INMR03'] = {}
        self.xmit['INMR03'][self.INMR03_count] = self.text_units(inmr03_record)
        self.xmit['INMR03'][self.INMR03_count]['INMDSORG'] = self.get_dsorg(self.xmit['INMR03'][self.INMR03_count]['INMDSORG'])
        self.xmit['INMR03'][self.INMR03_count]['INMRECFM'] = self.get_recfm(self.xmit['INMR03'][self.INMR03_count]['INMRECFM'])
        #self.logger.debug("dsorg: {} recfm: {}".format(hex(dsorg), hex(recfm)))

    def get_dsorg(self, dsorg):
        try:
            file_dsorg = self.get_int(dsorg)
        except TypeError:
            file_dsorg = dsorg

        org = ''
        if 0x8000 == (0x8000 & file_dsorg):
            org = 'ISAM'
        if 0x4000 == (0x4000 & file_dsorg):
            org = 'PS'
        if 0x2000 == (0x2000 & file_dsorg):
            org = 'DA'
        if 0x1000 == (0x1000 & file_dsorg):
            org = 'BTAM'
        if 0x0200 == (0x0200 & file_dsorg):
            org = 'PO'
        if not org:
            org = '?'
        if 0x0001 == (0x0001 & file_dsorg):
            org += 'U'
        return org

    def get_recfm(self, recfm):
        # https://www.ibm.com/support/knowledgecenter/SSLTBW_2.3.0/com.ibm.zos.v2r3.idas300/s3013.htm
        rfm = '?'

        flag = recfm[0]
        if (flag & 0xC0) == 0x40:
            rfm = 'V'
        elif (flag & 0xC0) == 0x80:
            rfm = 'F'
        elif (flag & 0xC0) == 0xC0:
            rfm = 'U'

        if 0x10 == (0x10 & flag):
            rfm += 'B'

        if 0x04 == (0x04 & flag):
            rfm += 'A'

        if 0x02 == (0x02 & flag):
            rfm += 'M'

        if 0x08 == (0x08 & flag):
            rfm += 'S'

        self.logger.debug("Record Format (recfm): {} ({:#06x})".format(rfm, self.get_int(recfm)))

        return rfm

    def text_units(self, text_records):
        # Text units in INMR## records are broken down like this:
        # First two bytes is the 'key'/type
        # Second two bytes are how many text unit records there are
        # Then records are broken down in size (two bytes) and the data
        # data can be character, decimal or hex
        # returns a dictionary of text units 'name' : 'value'

        loc = 0
        tu = {}
        INMDSNAM = ''
        debug = ("Key: {k:#06x}, Mnemonic: '{n}', Type: '{t}', Description: '{d}'," +
                " Text Unit number: {tun}, length: {l}, Value: '{v}'")
        self.logger.debug("Total record Length: {}".format(len(text_records)))
        #self.hexdump(text_records)
        while loc < len(text_records):

            key = struct.unpack('>H', text_records[loc:loc+2])[0]
            num = struct.unpack('>H', text_records[loc+2:loc+4])[0]
            #print(loc, hex(key), num)

            if key == 0x1026 and num == 0:
                # this record can be empty so we skip it
                loc = loc + 4

            if key == 0x0028 and num == 0:
                # this record can be empty so we skip it
                self.logger.debug('This is a message')
                self.msg = True
                loc += 4


            for i in range(0,num):
                if i == 0:
                    tlen = self.get_int(text_records[loc+4:loc+6])
                    item = text_records[loc+6:loc+6+tlen]
                else:
                    tlen = self.get_int(text_records[loc:loc+2])
                    item = text_records[loc+2:loc+2+tlen]

                if key in text_keys:
                    if text_keys[key]['type'] == 'character':
                        #self.logger.debug("Text Unit value: {}".format(item.decode(self.ebcdic)))
                        value = item.decode(self.ebcdic)
                        if text_keys[key]['name'] == 'INMDSNAM':
                            INMDSNAM += item.decode(self.ebcdic) + "."
                    elif text_keys[key]['type'] == 'decimal':
                        value = self.get_int(item)
                        #self.logger.debug("Decimal Unit value: {}".format(value))
                    else:
                        #self.logger.debug("Hex value: {}".format(hex(self.get_int(item))))
                        value = item

                        if text_keys[key]['name'] == 'INMTYPE':
                            value = self.get_int(value)
                            # 80 Data Library
                            # 40 program library
                            # 04 extended ps
                            # 01 large format ps
                            if   value == 0x80:
                                value = "Data Library"
                            elif value == 0x40:
                                value = "Program Library"
                            elif value == 0x80:
                                value = "Extended PS"
                            elif value == 0x80:
                                value = "Large Format PS"
                            else:
                                value = "None"



                    if INMDSNAM:
                        value = INMDSNAM[:-1]

                    tu[text_keys[key]['name']] = value

                    self.logger.debug(debug.format(
                        k = key,
                        n = text_keys[key]['name'],
                        t = text_keys[key]['type'],
                        d = text_keys[key]['desc'],
                        tun = num,
                        l = tlen,
                        v = value))


                if i == 0:
                    loc += 6 + tlen
                else:
                    loc += 2 + tlen
        self.logger.debug("Final Loc: {}".format(loc))
        return tu

    def get_int(self, bytes, endian='big'):
        return int.from_bytes(bytes, endian)

    def convert_text_file(self, ebcdic_text, recl):
        self.logger.debug("Converting EBCDIC file to UTF-8. Using EBCDIC Table: '{}' LRECL: {} UnNum: {} Force: {}".format(self.ebcdic, recl, self.unnum, self.force))
        asciifile = ebcdic_text.decode(self.ebcdic)
        seq_file = []
        if recl < 1:
            return asciifile + '\n'
        for i in range(0, len(asciifile), recl):
            if asciifile[i+recl-8:i+recl].isnumeric() and self.unnum:
                seq_file.append(asciifile[i:i+recl-8].rstrip())
            else:
                seq_file.append(asciifile[i:i+recl].rstrip())
        return '\n'.join(seq_file) + '\n'

    def convert_message(self):
        if not self.msg:
            self.logger.debug("No message file included in XMIT")
            return

        message = self.xmit['message']['file']
        recl = self.xmit['message']['lrecl']
        self.xmit['message']['text'] = self.convert_text_file(message, recl)

    def get_xmi_files(self):
        # Populates self.xmit with the members of the dataset stores the information in:
        # - self.xmit['file'][filename]['members'] -> a structure with member information
        # - self.xmit['file'][filename]['COPYR1'] -> information about the dataset and header records
        # - self.xmit['file'][filename]['COPYR2'] -> Dataset extent blocks

        inrm02num = 1
        if self.msg:
            inrm02num = 2
        filename = self.xmit['INMR02'][inrm02num]['INMDSNAM']
        dsnfile = self.xmit['file'][filename]['data']
        recl = self.xmit['INMR03'][inrm02num]['INMLRECL']
        # blocksize = self.xmit['INMR02'][inrm02num]['INMBLKSZ']
        # dsorg = self.xmit['INMR02'][inrm02num]['INMDSORG']
        # utility = self.xmit['INMR02'][inrm02num]['INMUTILN']
        # recfm = self.xmit['INMR02'][inrm02num]['INMRECFM']


        if self.xmit['INMR02'][inrm02num]['INMUTILN'] == 'INMCOPY':
            self.xmit['file'][filename]['text'] = self.convert_text_file( b''.join(dsnfile), recl)
        else:
            self.xmit['file'][filename]['COPYR1'] = self.iebcopy_record_1(dsnfile[0])
            self.xmit['file'][filename]['COPYR2'] = self.iebcopy_record_2(dsnfile[1])

            # Directory Info https://www.ibm.com/support/knowledgecenter/SSLTBW_2.3.0/com.ibm.zos.v2r3.idad400/pdsd.htm
            last_member = False
            dir_block_location = 2

            member_dir = b''
            count_dir_blocks = 2

            for blocks in dsnfile[count_dir_blocks:]:
                # End of PDS directory is 12 0x00
                # loop until there and store it
                member_dir += blocks
                count_dir_blocks += 1
                if self.all_members(member_dir):
                    break

            self.xmit['file'][filename]['members'] =  self.get_members_info(member_dir)

            # Now we have PDS directory information
            # Process the member data (which is everything until the end of the file)
            raw_data = b''.join(dsnfile[count_dir_blocks:])
            self.xmit['file'][filename] = self.process_blocks(self.xmit['file'][filename], raw_data)

    def get_tape_files(self):
        # populates self.tape with pds/seq dataset information

        for i in range(0, len(self.tape['files']) - 1):
            if 'dsn' in self.tape['files'][i]:
                filename = self.tape['files'][i]['dsn']
            else:
                filename = self.filename
            self.logger.debug('Processing Dataset: {}'.format(filename))

            dataset = self.tape['files'][i]['data']
            copyr1_size = self.get_int(dataset[:2])
            self.logger.debug("Size of COPYR1 Field: {}".format(copyr1_size))
            try:
                self.tape['files'][i]['COPYR1'] = self.iebcopy_record_1(dataset[:copyr1_size])
            except:
                self.logger.debug("{} is not a PDS leaving".format(filename))
                if 'lrecl' in self.tape['files'][i]:
                    self.tape['files'][i]['text'] = self.convert_text_file( self.tape['files'][i]['data'], self.tape['files'][i]['lrecl'])
                else:
                    self.tape['files'][i]['text'] = self.convert_text_file( self.tape['files'][i]['data'], self.manual_recordlength)
                continue
            copyr2_size = self.get_int(dataset[copyr1_size:copyr1_size+2])
            self.logger.debug("Size of COPYR2 Field: {}".format(copyr2_size))

            self.tape['files'][i]['COPYR2'] = self.iebcopy_record_2(dataset[copyr1_size+8:copyr1_size+copyr2_size])

            loc = 0
            dataset = dataset[copyr1_size+copyr2_size:]
            member_dir = b''
            #self.hexdump(dataset)
            while loc < len(dataset):
                block_size = self.get_int(dataset[loc:loc+2])
                seg_size = self.get_int(dataset[loc+4:loc+6])
                self.logger.debug("BDW Size: {} SDW Size: {}".format(block_size, seg_size))
                member_dir += dataset[loc+8:loc+block_size] # skip BDW and SDW
                #self.hexdump(dataset[loc:loc+size])
                loc += block_size
                if self.all_members(member_dir):
                    break
                #self.hexdump(member_dir[-12:])
            #self.hexdump(member_dir)
            self.tape['files'][i]['members'] = self.get_members_info(member_dir)
            # Now getting member blocks
            dataset = dataset[loc:]
            loc = 0
            member_files = b''

            while loc < len(dataset):
                block_size = self.get_int(dataset[loc:loc+2])
                seg_size = self.get_int(dataset[loc+4:loc+6])
                self.logger.debug("BDW Size: {} SDW Size: {}".format(block_size, seg_size))
                member_files += dataset[loc+8:loc+block_size] # skip BDW and SDW
                #self.hexdump(dataset[loc:loc+block_size])
                loc += block_size
                if member_files[-12:] == b'\x00' * 12:
                    break
            self.logger.debug('Processing PDS: {}'.format(filename))
            self.tape['files'][i] = self.process_blocks(self.tape['files'][i], member_files)


    def iebcopy_record_1(self, first_record):
        self.logger.debug("IEBCOPY First Record Atributes (COPYR1)")
        # https://www.ibm.com/support/knowledgecenter/SSLTBW_2.2.0/com.ibm.zos.v2r2.idau100/u1322.htm
        # PDS i.e. IEBCOPY
        if self.get_int(first_record[1:4]) != 0xCA6D0F and self.get_int(first_record[9:12]) != 0xCA6D0F:
            self.logger.debug("COPYR1 header eyecatcher 0xCA6D0F not found")
            #self.hexdump(first_record)
            raise Exception("COPYR1 header eyecatcher 0xCA6D0F not found")
        if len(first_record) > 64:
            self.logger.debug("COPYR1 Length {} longer than 64 records".format(len(first_record)))
            #self.hexdump(first_record)
            raise Exception("COPYR1 Length {} longer than 64 records".format(len(first_record)))

        COPYR1 = {}
        COPYR1['type'] = 'PDS'

        if self.get_int(first_record[1:4]) != 0xCA6D0F: #XMIT files omit the first 8 bytes?
            COPYR1['block_length'] = self.get_int(first_record[0:2])
            COPYR1['seg_length'] = self.get_int(first_record[4:6])
            first_record = first_record[8:]

        if first_record[0] & 0x01:
            COPYR1['type'] = 'PDSE'
            self.logger.warning("Warning: Beta PDSE support.")

        # Record 1
        # https://www.ibm.com/support/knowledgecenter/SSLTBW_2.2.0/com.ibm.zos.v2r2.idau100/u1322.htm#u1322__nt2

        COPYR1['DS1DSORG'] = self.get_int(first_record[4:6])
        COPYR1['DS1BLKL'] = self.get_int(first_record[6:8])
        COPYR1['DS1LRECL'] = self.get_int(first_record[8:10])
        COPYR1['DS1RECFM'] = self.get_recfm(first_record[10:12])
        COPYR1['DS1KEYL'] = first_record[11]
        COPYR1['DS1OPTCD'] = first_record[12]
        COPYR1['DS1SMSFG'] = first_record[13]
        COPYR1['file_tape_blocksize'] = self.get_int(first_record[14:16])
        # Device type mapped from IHADVA macro
        # https://www.ibm.com/support/knowledgecenter/SSLTBW_2.2.0/com.ibm.zos.v2r2.idas300/ihadva.htm
        #  0  (0)  CHARACTER    4    DVAUCBTY       UCB TYPE FIELD
        #  0  (0)  BITSTRING    2     DVAOPTS       UCB OPTIONS
        #  2  (2)  BITSTRING    1     DVACLASS      DEVICE CLASS
        #  3  (3)  BITSTRING    1     DVAUNIT       UNIT TYPE
        #  4  (4)  SIGNED       4    DVAMAXRC       MAXIMUM RECORD SIZE
        #  8  (8)  CHARACTER   12    DVATAB         SECTION INCLUDED BY DEVTAB
        #  8  (8)  UNSIGNED     2    DVACYL         PHYS NUMBER CYL PER VOLUME
        # 10  (A)  SIGNED       2    DVATRK         NR OF TRACKS PER CYL
        # 12  (C)  SIGNED       2    DVATRKLN       TRACK LENGTH ( BYTES)
        # 14  (E)  SIGNED       2    DVAOVHD        BLOCK OVERHEAD IF DVA2BOV IS
        #                                           ON
        COPYR1['DVAOPTS'] = self.get_int(first_record[16:18])
        COPYR1['DVACLASS'] = first_record[18]
        COPYR1['DVAUNIT'] = first_record[19]
        COPYR1['DVAMAXRC'] = self.get_int(first_record[20:24])
        COPYR1['DVACYL'] = self.get_int(first_record[24:26])
        COPYR1['DVATRK'] = self.get_int(first_record[26:28])
        COPYR1['DVATRKLN'] = self.get_int(first_record[28:30])
        COPYR1['DVAOVHD'] = self.get_int(first_record[30:32])
        COPYR1['num_header_records'] = self.get_int(first_record[36:38])

        if first_record[38:] != (b'\x00'*18):
            reserved = first_record[38]
            COPYR1['DS1REFD'] = first_record[39:42]
            COPYR1['DS1SCEXT'] = first_record[42:45]
            COPYR1['DS1SCALO'] = first_record[45:49]
            COPYR1['DS1LSTAR'] = first_record[49:52]
            COPYR1['DS1TRBAL'] = first_record[52:54]
            reserved = first_record[54:]
            COPYR1['DS1REFD'] = "{:02d}{:04d}".format(
                COPYR1['DS1REFD'][0] % 100, self.get_int(COPYR1['DS1REFD'][1:]))

        self.logger.debug("Record Size: {}".format(len(first_record)))
        for i in COPYR1:
            self.logger.debug("{:<19} : {}".format(i, COPYR1[i]))
        return COPYR1

    def all_members(self, members):
        self.logger.debug('Checking for last member found')
        block_loc = 0
        while block_loc < len(members):
            directory_len = self.get_int(members[block_loc+20:block_loc+22]) - 2 # Length includes this halfword
            directory_members_info = members[block_loc+22:block_loc+22+directory_len]
            loc = 0
            while loc < directory_len:
                if directory_members_info[loc:loc+8] == b'\xff' * 8:
                    #self.hexdump(members)
                    return True
                loc = loc + 8 + 3 + 1 + (directory_members_info[loc+11] & 0x1F) * 2
            block_loc += 276
        return False

    def iebcopy_record_2(self, second_record):
        self.logger.debug("IEBCOPY Second Record Atributes (COPYR2)")
        if len(second_record) > 276:
            self.logger.debug("COPYR2 Length {} longer than 276 records".format(len(first_record)))
            #self.hexdump(first_record)
            raise Exception("COPYR2 Length {} longer than 276 records".format(len(first_record)))

        deb = second_record[0:16] # Last 16 bytes of basic section of the Data Extent Block (DEB) for the original data set.
        deb_extents = []
        for i in range(0, 256, 16):
            deb_extents.append(second_record[i:i+16])
        reserved = second_record[272:276] # Must be zero
        return {'deb': deb, 'extents' : deb_extents}

        # https://www.ibm.com/support/knowledgecenter/en/SSLTBW_2.1.0/com.ibm.zos.v2r1.idas300/debfiel.htm#debfiel
        self.logger.debug("DEB: {:#040x}".format( self.get_int(deb)))
        deb_mask = deb[0]       # DEBDVMOD
        deb_ucb = self.get_int(deb[1:4])      # DEBUCBA
        DEBDVMOD31 = deb[4]      # DEBDVMOD31
        DEBNMTRKHI = deb[5]
        deb_cylinder_start = self.get_int(deb[6:8]) # DEBSTRCC
        deb_tracks_start = self.get_int(deb[8:10])  # DEBSTRHH
        deb_cylinder_end = self.get_int(deb[10:12]) # DEBENDCC
        deb_tracks_end = self.get_int(deb[12:14]) # DEBENDHH
        deb_tracks_num = self.get_int(deb[14:]) #DEBNMTRK

        self.logger.debug("Mask {:#04x} UCB: {:#06x} Start CC: {:#06x} Start Tracks: {:#06x} End CC: {:#06x} End Tracks: {:#06x} Num tracks: {:#06x} ".format(deb_mask, deb_ucb, deb_cylinder_start, deb_tracks_start, deb_cylinder_end, deb_tracks_end, deb_tracks_num))
        x = 1
        for i in deb_extents:
            self.logger.debug("DEB Extent {}: {:#040x}".format(x, self.get_int(i)))
            x +=1

    def get_members_info(self, directory):
        self.logger.debug("Getting PDS Member information. Directory length: {}".format(len(directory)))
        members = {}

        block_loc = 0
        while block_loc < len(directory):

            directory_zeroes = directory[block_loc:block_loc+8] # PDSe this may be 08 00 00 00 00 00 00 00
            directory_key_len = directory[block_loc+8:block_loc+10] # 0x0008
            directory_data_len =  self.get_int(directory[block_loc+10:block_loc+12]) # 0x0100
            directory_F_in_chat = directory[block_loc+12:block_loc+20] # last referenced member
            directory_len = self.get_int(directory[block_loc+20:block_loc+22]) - 2 # Length includes this halfword
            directory_members_info = directory[block_loc+22:block_loc+22+directory_len]
            #self.logger.debug("Directory Length: {}".format(directory_len))
            loc = 0
            while loc < directory_len:
                member_name = directory_members_info[loc:loc+8].decode(self.ebcdic).rstrip()
                if directory_members_info[loc:loc+8] == b'\xff' * 8:
                    self.logger.debug("End of Directory Blocks. Total members: {}".format(len(members)))
                    last_member = True
                    loc = len(directory)
                    break
                else:
                    members[member_name] = {
                        'ttr' : self.get_int(directory_members_info[loc+8:loc+11]),
                        'alias' : True if 0x80 == (directory_members_info[loc+11] & 0x80) else False,
                        'halfwords' : (directory_members_info[loc+11] & 0x1F) * 2,
                        'notes' : (directory_members_info[loc+11] & 0x60) >> 5
                    }
                    members[member_name]['parms'] = directory_members_info[loc+12:loc+12+members[member_name]['halfwords']]

                    if len( members[member_name]['parms']) >= 30 and members[member_name]['notes'] == 0: # ISPF Stats
                        # https://www.ibm.com/support/knowledgecenter/en/SSLTBW_2.1.0/com.ibm.zos.v2r1.f54mc00/ispmc28.htm
                        # ISPF statistics entry in a PDS directory
                        member_parms = members[member_name]['parms']
                        members[member_name]['ispf'] = {
                            'version' : "{:02}.{:02}".format(member_parms[0], member_parms[1]),
                            'flags' : member_parms[2],
                            'createdate' : self.ispf_date(member_parms[4:8]),
                            'modifydate' : self.ispf_date(member_parms[8:14], seconds = member_parms[3]),
                            'lines' : self.get_int(member_parms[14:16]),
                            'newlines' : self.get_int(member_parms[16:18]),
                            'modlines' : self.get_int(member_parms[18:20]),
                            'user' : member_parms[20:28].decode(self.ebcdic).rstrip()
                        }
                        if 0x10 == (members[member_name]['ispf']['flags'] & 0x10):
                            members[member_name]['ispf']['lines'] = self.get_int(member_parms[28:32])
                            members[member_name]['ispf']['newlines'] = self.get_int(member_parms[32:36])
                            members[member_name]['ispf']['modlines'] = self.get_int(member_parms[36:40])

                    else:
                        members[member_name]['ispf'] = False

                    loc = loc + 8 + 3 + 1 + members[member_name]['halfwords']
            block_loc += loc + 24
            if (block_loc % 276) > 0: # block lengths must be 276
                block_loc = (276 * (block_loc // 276)) + 276

        member_info = ''
        #print debug information
        for member in members:
            member_info = "Member: {}".format(member)
            for item in members[member]:
                if isinstance(members[member][item], dict):
                    for i in members[member][item]:
                        member_info += " {}: {},".format(i, members[member][item][i])
                elif item not in 'parms':
                    member_info += " {}: {},".format(item, members[member][item])
            self.logger.debug(member_info[:-1])
        return members

    def process_blocks(self, dsn={}, member_blocks=b''):
        self.logger.debug("Processing PDS Blocks")
        if not dsn:
            raise Exception("File data structure empty")
        loc = 0
        ttr_location = 0
        member_data = b''
        vb_member_data = []
        deleted = False
        deleted_num = 1
        prev_ttr = 0
        record_closed = False
        magi = magic.Magic(mime_encoding=True, mime=True)
        lrecl = dsn['COPYR1']['DS1LRECL']
        recfm = dsn['COPYR1']['DS1RECFM']
        self.logger.debug("LRECL: {} RECFM: {}".format(lrecl, recfm))

        #pprint(self.xmit)

        ttrs = {}
        aliases = {}

        # Create a dictionary of TTRs to Members
        for m in dsn['members']:
            # M is a member name
            if dsn['members'][m]['alias']:
                # Skip if this is an alias
                aliases[dsn['members'][m]['ttr']] = m
            else:
                ttrs[dsn['members'][m]['ttr']] = m

        for a in aliases:
            # we need to handle the case where all the aliases point to each other
            if a not in ttrs:
                ttrs[dsn['members'][aliases[a]]['ttr']] =  aliases[a]
                dsn['members'][aliases[a]]['alias'] = False
            else:
                self.logger.debug("Member Alias {} -> {}".format(aliases[a], ttrs[a]))

        # Sort the TTRs
        sorted_ttrs = []
        for i in sorted (ttrs.keys()) :
            sorted_ttrs.append(i)

        while loc < len(member_blocks):
            # i.e.
            #F  M  BB    CC    TT    R  KL DLen
            #00 00 00 00 04 45 00 09 04 00 03 C0
            #00 00 00 00 00 3E 00 05 0E 00 00 FB
            #00 00 00 00 00 3E 00 05 12 00 1D 38
            member_data_len = self.get_int(member_blocks[loc + 10:loc + 12])
            member_ttr = self.get_int(member_blocks[loc + 6:loc + 9])

            if dsn['COPYR1']['type'] == 'PDSE' and record_closed:
                while True:
                    member_ttr = self.get_int(member_blocks[loc + 6:loc + 9])
                    member_data_len = self.get_int(member_blocks[loc + 10:loc + 12])
                    if member_ttr != prev_ttr:
                        break
                    loc += member_data_len + 12
                record_closed = False



            if member_ttr == 0 and member_data_len == 0:
                # skip empty entries
                loc += member_data_len + 12
                continue

            member_flag = member_blocks[loc]
            member_extent = member_blocks[loc + 1]
            member_bin = member_blocks[loc + 2:loc + 4]
            member_cylinder = self.get_int(member_blocks[loc + 4:loc + 6])
            member_key_len = member_blocks[loc + 9]

            if ttr_location +1 > len(sorted_ttrs):

                self.logger.warning("Encoutered more files than members: Total members: {} Current file: {} (Potentially deleted?)".format(len(ttrs), ttr_location+1))
                sorted_ttrs.append("??{}".format(deleted_num))
                ttrs["??{}".format(deleted_num)] = "DELETED{}".format(deleted_num )
                dsn['members'][ "DELETED{}".format(deleted_num )] = { 'alias' : False}
                deleted_num += 1

            ttr_num = sorted_ttrs[ttr_location]
            member_name = ttrs[ttr_num]

            self.logger.debug("DIR TTR: {} DIR Member: {} Extent: {} BB: {} CC: {:#08x} TTR: {:#08x} key: {} data: {}".format(
                ttr_num,
                member_name,
                member_extent,
                member_bin,
                member_cylinder,
                member_ttr,
                member_key_len,
                member_data_len
                ))
            #self.hexdump(member_blocks[loc + 12:loc + 12 + member_data_len])
            if 'V' in recfm:
                vb_member_data += self.handle_vb(member_blocks[loc + 12:loc + 12 + member_data_len])
                member_data = b''.join(vb_member_data)
            else:
                member_data += member_blocks[loc + 12:loc + 12 + member_data_len]


            if member_data_len == 0:
                if dsn['COPYR1']['type'] == 'PDSE':
                    record_closed = True
                #self.hexdump(member_data)
                filetype,datatype = magi.from_buffer(member_data).split('; ')
                datatype = datatype.split("=")[1]
                extention = mimetypes.guess_extension(filetype)

                if not extention:
                    extention = "." + filetype.split("/")[1]

                if self.force:
                    extention = ".txt"


                # File magic cant detec XMIT files
                if ( filetype == 'application/octet-stream' and
                   len(member_data) >= 8 and
                   member_data[2:8].decode(self.ebcdic) == 'INMR01'):
                    extention = ".xmi"
                    filetype = 'application/xmit'

                if filetype == 'text/plain' or datatype != 'binary' or self.force:

                    if 'V' in recfm:
                        vb_member_text = ''
                        for record in vb_member_data:
                            vb_member_text += self.convert_text_file(record, len(record)).rstrip() + '\n'
                        dsn['members'][member_name]['text'] = vb_member_text

                    else:
                        dsn['members'][member_name]['text'] = self.convert_text_file(member_data, lrecl)

                self.logger.debug("Member name: {} Mime Type: {} Datatype: {} File ext: {} Size: {}".format(member_name, filetype, datatype, extention, len(member_data)))
                dsn['members'][member_name]['mimetype'] = filetype
                dsn['members'][member_name]['datatype'] = datatype
                dsn['members'][member_name]['extension'] = extention
                dsn['members'][member_name]['data'] = member_data
                member_data = b''
                vb_member_data = []
                # End of member
                ttr_location += 1
                prev_ttr = member_ttr

            loc += member_data_len + 12

        if len(member_data) > 0:
            # sometimes trailing records aren't followed by a zero
            self.logger.debug('Parsing trailing record')
            filetype,datatype = magi.from_buffer(member_data).split('; ')
            datatype = datatype.split("=")[1]
            extention = mimetypes.guess_extension(filetype)

            if not extention:
                extention = "." + filetype.split("/")[1]

            if self.force:
                extention = ".txt"


            # File magic cant detec XMIT files
            if ( filetype == 'application/octet-stream' and
                len(member_data) >= 8 and
                member_data[2:8].decode(self.ebcdic) == 'INMR01'):
                extention = ".xmi"
                filetype = 'application/xmit'

            if filetype == 'text/plain' or datatype != 'binary' or self.force:

                if 'V' in recfm:
                    vb_member_text = ''
                    for record in vb_member_data:
                        vb_member_text += self.convert_text_file(record, len(record)).rstrip() + '\n'
                    dsn['members'][member_name]['text'] = vb_member_text

                else:
                    dsn['members'][member_name]['text'] = self.convert_text_file(member_data, lrecl)

            self.logger.debug("Member name: {} Mime Type: {} Datatype: {} File ext: {} Size: {}".format(member_name, filetype, datatype, extention, len(member_data)))
            dsn['members'][member_name]['mimetype'] = filetype
            dsn['members'][member_name]['datatype'] = datatype
            dsn['members'][member_name]['extension'] = extention
            dsn['members'][member_name]['data'] = member_data


        return dsn


    def handle_vb(self, vbdata):
        self.logger.debug("Processing Variable record format")
        # the first 4 bytes are bdw
        loc = 4
        data = []
        lrecl = 10
        while loc < len(vbdata) and lrecl > 0:
            lrecl = self.get_int(vbdata[loc:loc+2])
            #self.logger.debug("Location: {} LRECL: {}".format(loc, lrecl))
            data.append(vbdata[loc+4:loc+lrecl])
            loc += lrecl
        return data

    def print_message(self):
        if not self.msg:
            self.logger.debug("No message file included in XMIT")
        else:
            print(self.xmit['message']['text'])


    def ispf_date(self, ispfdate, seconds=0):
        # Packed Decimal https://www.ibm.com/support/knowledgecenter/ssw_ibm_i_74/rzasd/padecfo.htm
        century = 19 + ispfdate[0]
        year = format(ispfdate[1],'02x')
        day = format(ispfdate[2],'02x') + format(ispfdate[3],'02x')[0]
        if day == '000':
            day = '001'
        if len(ispfdate) > 4:
            hours = format(ispfdate[4],'02x')
            minutes = format(ispfdate[5],'02x')
        else:
            hours = '00'
            minutes = '00'

        if seconds != 0:
            seconds = format(seconds,'02x')
        else:
            seconds = '00'

        date = "{}{}{}{}{}{}".format(century, year, day, hours, minutes, seconds)

        try:
            d = datetime.datetime.strptime(date,'%Y%j%H%M%S')
            return(d.isoformat())
        except:
            self.logger.debug("Cannot parse ISPF date field")
            return ''

    def convert_date(self, d):
        return (d//16)*10 + (d - (d//16)*16)

    def parse_tape(self):
        self.logger.debug("Parsing virtual tape file")
        self.logger.debug("Using LRECL: {}".format(self.manual_recordlength))
        magi = magic.Magic(mime_encoding=True, mime=True)

        if not self.tape_object:
            self.read_tape_file()
        self.tape = {}
        self.tape['files'] = []
        loc = 0
        tape_file = b''
        tape_text = ''
        file_num = 1
        eof_marker = eor_marker = False
        HDR1 = HDR2 = volume_label = {}

        while loc < len(self.tape_object):
        # Get tape header

        # Header:
        # blocksize little endian
        # prev blocksize little endian
        # Flags(2 bytes)
        #   0x2000 ENDREC End of record
        #   0x4000 EOF    tape mark
        #   0x8000 NEWREC Start of new record
        #   HET File:
        #     0x02 BZIP2 compression
        #     0x01 ZLIB compression
        # Labels:
        # https://www.ibm.com/support/knowledgecenter/en/SSLTBW_2.1.0/com.ibm.zos.v2r1.idam300/formds1.htm

            cur_blocksize = self.get_int(self.tape_object[loc:loc+2], 'little')
            #self.logger.debug("Current Blocksize: {b} ({b:#06x})".format(b=cur_blocksize))
            prev_blocksize = self.get_int(self.tape_object[loc+2:loc+4], 'little')
            #self.logger.debug("Previous Blocksize: {b} ({b:#06x})".format(b=prev_blocksize))
            flags = self.get_int(self.tape_object[loc+4:loc+6])
            #self.logger.debug("Flags bytes: {b} ({b:#06x})".format(b=flags))


            if 0x4000 == (flags & 0x4000 ) :
                eof_marker = True

            if 0x2000 == (flags & 0x2000 ):
                eor_marker = True

            if 0x8000 == (flags & 0x8000 ):
                eof_marker = False
                eor_marker = False

            if 0x8000 != (flags & 0x8000 ) and 0x4000 != (flags & 0x4000 ) and 0x2000 != (flags & 0x2000 ):
                raise Exception('Header flag {:#06x} unrecognized'.format(self.get_int(self.tape_object[loc+4:loc+6])))

            if 0x0200 == (flags & 0x0200):
                # BZLIB Compression
                self.logger.debug("Record compresed with BZLIB")
                tape_file += bz2.decompress(self.tape_object[loc+6:loc+cur_blocksize+6])
            elif 0x0100 == (flags & 0x0100):
                self.logger.debug("Record compresed with zLIB")
                tape_file += zlib.decompress(self.tape_object[loc+6:loc+cur_blocksize+6])
            else:
                tape_file += self.tape_object[loc+6:loc+cur_blocksize+6]


            if not volume_label and tape_file[:4].decode(self.ebcdic) == 'VOL1':
                volume_label = {
                  #  'label_id' : tape_file[:4].decode(self.ebcdic),
                    'volser'   : tape_file[4:10].decode(self.ebcdic),
                    'owner'   : tape_file[41:51].decode(self.ebcdic)
                }

            if self.tape_object[loc+6:loc+10].decode(self.ebcdic) == 'HDR1' and cur_blocksize == 80:
                t = self.tape_object[loc+6:loc+cur_blocksize+6].decode(self.ebcdic)
                HDR1 = {
                 #   'label_num' : self.make_int(t[3]),
                    'dsn' : t[4:21].strip(),
                    'dsnser' : t[21:27],
                    'volseq' : self.make_int(t[27:31]),
                    'dsnseq' : self.make_int(t[31:35]),
                    'gennum' : self.make_int(t[35:39]),
                    'version' : self.make_int(t[39:41]),
                    'createdate' : self.get_tape_date(t[41:47]),
                    'expirationdate' : self.get_tape_date(t[47:53]),
                    'dsnsec' : False if self.make_int(t[53]) == 0 else True,
                    'block_count_low' : self.make_int(t[54:60]),
                    'system_code' : t[60:73],
                    'block_count_high' : self.make_int(t[76:80])
                }
            if self.tape_object[loc+6:loc+10].decode(self.ebcdic) == 'HDR2' and cur_blocksize == 80:
                t = self.tape_object[loc+6:loc+cur_blocksize+6].decode(self.ebcdic)
                HDR2 = {
                   # 'label_num' : self.make_int(t[3]),
                    'recfm' : t[4],
                    'block_len' : self.make_int(t[5:10]),
                    'lrecl' : self.make_int(t[10:15].strip()),
                    'density' : self.make_int(t[15]),
                    'position' : t[16],
                    'jobid' : t[17:34],
                    'technique' : t[34:36],
                    'control_char' : t[36],
                    'block_attr' : t[38],
                    'devser' : t[41:47],
                    'dsnid' : t[47],
                    'large_block_len' : t[70:80]
                }



            self.logger.debug("Location: {} Blocksize: {} Prev Blocksize: {} EoF: {} EoR: {} Flags: {:#06x} File Size: {}".format(loc, cur_blocksize, prev_blocksize, eof_marker, eor_marker, flags, len(tape_file)))

            if eof_marker:
                if tape_file[:4].decode(self.ebcdic) in  ['VOL1', 'HDR1', 'HDR2', 'EOF1', 'EOF2']:
                    self.logger.debug('Skipping VOL/HDR/EOF records type: {}'.format(tape_file[:4].decode(self.ebcdic)))
                    tape_file = b''
                    continue

                if 'V' in HDR2:
                    vb_tape_file = self.handle_vb(tape_file)
                    tape_file = b''.join(vb_tape_file)

                filetype,datatype = magi.from_buffer(tape_file).split('; ')
                datatype = datatype.split("=")[1]
                extention = mimetypes.guess_extension(filetype)
                #eof_marker = False

                if not extention:
                    extention = "." + filetype.split("/")[1]

                # File magic cant detec XMIT files
                if ( filetype == 'application/octet-stream' and
                   len(tape_file) >= 8 and
                   tape_file[2:8].decode(self.ebcdic) == 'INMR01'):
                    extention = ".xmi"
                    filetype = 'application/xmit'

                if self.force:
                    extention = ".txt"

                if filetype == 'text/plain' or datatype != 'binary' or self.force:

                    if 'lrecl' in HDR2:
                        if 'F' in HDR2['recfm']:
                            tape_text = self.convert_text_file(tape_file, HDR2['lrecl'])
                        elif 'V' in HDR2['recfm']:
                            for record in vb_tape_file:
                                tape_text += self.convert_text_file(record, len(record)).rstrip() + '\n'
                    else:
                        tape_text = self.convert_text_file(tape_file, self.manual_recordlength)
                else:
                    tape_text = ''
                self.logger.debug("Record {}: filetype: {} datatype: {} size: {}".format(file_num, filetype, datatype, len(tape_file)))

                if len(tape_file) > 0:
                    tape_details = {
                        'filenum' : file_num,
                        'data' : tape_file,
                        'text': tape_text,
                        'filetype' : filetype,
                        'datatype': datatype,
                        'extention' : extention
                        }
                    self.tape['files'].append({**volume_label, **HDR1, **HDR2, **tape_details})

                    HDR1 = {}
                    HDR2 = {}
                    file_num += 1
                else:
                    self.logger.debug('Empty Tape Entry')

                tape_file = b''
                self.logger.debug('EOF')


            loc += cur_blocksize + 6

    def make_int(self, num):
        num = num.strip()
        return int(num) if num else 0

    def get_tape_date(self, tape_date):
        self.logger.debug("changing date {}".format(tape_date))
        #   c = century (blank implies 19)
        #  yy = year (00-99)
        # ddd = day (001-366)
        if tape_date[0] == ' ':
            tape_date = '19' + tape_date[1:]
        else:
            tape_date = str(20 + int(tape_date[0])) + tape_date[1:]
            # strfmt %Y%j
        if tape_date[-1] == '0':
            tape_date = tape_date[:-1] + "1"
        d = datetime.datetime.strptime(tape_date,'%Y%j')
        return(d.strftime('%x %X'))

