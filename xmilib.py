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
#    - XMI.unload_files() this function will extract and translate (if needed based on the file mimetype)
#      all the files/members from the provided XMIT/Tape. The folder and other options provided
#      upon initialization affect the output folder and translation. By default the output folder is `./`,
#      the file will have the number column removed in the far right.
#    - XMI.dump_xmit_json() takes all the arguments and file flags/information and dumps it to a json file
#      named after the XMIT file


__version__ = '0.2'
__author__ = 'Philip Young'
__license__ = "GPL"


from hexdump import hexdump
from pprint import pprint
from array import array
from struct import pack
from pathlib import Path
from prettytable import PrettyTable

import zlib
import bz2
import json
import logging
import argparse
import copy
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
        self.file_object = None
        self.force = force
        self.binary = binary
        self.filelocation = 1
        self.ebcdic = encoding
        self.unnum = unnum
        self.quiet = quiet
        self.pdstype = False
        self.xmit = {}
        self.tape = {}
        self.modifydate = modifydate
        self.loglevel = loglevel
        self.overwrite = True

        # Create the Logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        if filename is not None:
            logger_formatter = logging.Formatter('%(levelname)s :: {} :: %(funcName)s :: %(message)s'.format(self.filename))
        else:
            logger_formatter = logging.Formatter('%(levelname)s :: %(funcName)s :: %(message)s')
        # Log to stderr
        ch = logging.StreamHandler()
        ch.setFormatter(logger_formatter)
        ch.setLevel(loglevel)
        if not self.logger.hasHandlers():
            self.logger.addHandler(ch)

        self.logger.debug("File: {}".format(self.filename))
        self.logger.debug("LRECL: {}".format(LRECL))
        self.logger.debug("Output Folder: {}".format(outputfolder))
        self.logger.debug("Encoding: {}".format(encoding))
        self.logger.debug("Unnum: {}".format(unnum))
        self.logger.debug("quiet: {}".format(quiet))
        self.logger.debug("force: {}".format(force))
        self.logger.debug("binary: {}".format(binary))

    def go(self):
        """ Function to determine if file is XMIT or Virtual Tape and parses the file

            Use either XMIT.set_filename(filename=) or XMIT.set_file_object(data=) to use
            this function.
        """
        if not self.filename and not self.file_object:
            raise Exception("No file or object to load. Use set_filename(filename=) or set_file_object(data=)")

        if self.filename and self.file_object:
            self.logger.warning("go function called with both a filename and a file object. Using file object.")

        if not self.file_object:
            self.read_file()

        # Is the file an XMI file?

        if self.filetype_is_xmi(self.file_object[0:10]):
            self.logger.debug("File is an XMIT file")
            self.set_xmit_object(self.file_object)
            self.parse_xmi()
            self.get_xmi_files()
        elif self.filetype_is_tape(self.file_object[0:4]):
            self.logger.debug("File is a Virtual Tape file")
            self.set_tape_object(self.file_object)
            self.parse_tape()
            self.get_tape_files()
        else:
            raise Exception("File not XMIT or Virtual Tape")





    def set_overwrite(self, setting=True):
        self.overwrite = setting

    def set_modify(self, setting=True):
        self.modifydate = setting

    def set_quiet(self, setting=True):
        self.quiet = setting

    def set_filename(self, filename):
        self.filename = filename

    def set_xmit_file(self, filename):
        self.logger.debug("Setting XMIT filename to: {}".format(filename))
        self.set_filename(filename)

    def set_tape_file(self, filename):
        self.logger.debug("Setting TAPE filename to: {}".format(filename))
        self.set_filename(filename)

    def set_file_object(self, data):
        # instead of loading from a file this function
        # allows you to pass an object
        self.logger.debug("Setting file object")
        self.file_object = data
        self.logger.debug("Total bytes: {}".format(len(self.file_object)))

    def set_xmit_object(self, xmit_data):
        # instead of loading from a file this function
        # allows you to pass an object
        self.logger.debug("Setting XMIT object")
        self.xmit_object = xmit_data
        self.logger.debug("Total bytes: {}".format(len(self.xmit_object)))

    def set_tape_object(self, virtual_tape_data):
        # instead of loading from a file this function
        # allows you to pass an object
        self.logger.debug("Setting Virtual Tape object")
        self.tape_object = virtual_tape_data
        self.logger.debug("Total bytes: {}".format(len(self.tape_object)))

    def set_output_folder(self, outputfolder):
        # by default this function will create the output folder if it doesnt exist
        self.logger.debug("Setting output folder to: {}".format(outputfolder))
        self.outputfolder = Path(outputfolder)

    def set_codepage(self, codepage='cp1140'):
        self.logger.debug("Changing codepage from {} to {}".format(self.ebcdic, codepage))
        self.ebcdic = codepage

    def read_file(self):
        self.logger.debug("Reading file: {}".format(self.filename))
        with open(self.filename, 'rb') as xmifile:
            self.file_object = xmifile.read()
        self.logger.debug("Total bytes: {}".format(len(self.xmit_object)))

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

    def is_xmi(self, member_name):
        #checks if a member is an XMI file
        self.check_parsed()
        self.logger.debug("Checking if member {} is an XMI file".format(member_name))
        pds = self.xmit['file']
        if member_name in self.xmit['file'][pds]:
            if self.xmit['file'][pds]['members'][member_name]['mimetype'] == 'application/xmit':
                return True
            self.logger.debug("Member {} not found in {}".format(member_name, pds))
        return False

    def has_xmi(self):
        self.check_parsed()
        if self.xmit:
            return True
        return False

    def has_tape(self):
        self.check_parsed()
        if self.tape:
            return True
        return False

    def get_file(self):
        self.check_parsed()
        return self.get_files()[0]

    def get_files(self):
        self.check_parsed()
        f = []
        if self.has_xmi():
            for pds in self.xmit['file']:
                f.append(pds)
        if self.has_tape():
            for pds in self.tape['file']:
                f.append(pds)
        return f

    def get_last_modified(self, filename):
        self.check_parsed()
        if self.has_xmi():
            return self.xmit['INMR01']['INMFTIME']
        elif 'HDR1' in self.tape['file'][filename]:
            return self.tape['file'][filename]['HDR1']['createdate']

    def get_owner(self):
        self.check_parsed()
        if self.has_xmi():
            return self.xmit['INMR01']['INMFUID']
        elif 'label' in self.tape:
                return self.tape['label']['owner']
        else:
            return ''

    def get_folder_size(self, p):
        self.check_parsed()
        total_size = 0
        if self.has_xmi():
            info = self.xmit
        elif self.has_tape():
            info = self.tape

        if 'members' in info['file'][p]:
            for m in info['file'][p]['members']:
                if 'data' in info['file'][p]['members'][m]:
                    total_size += len(info['file'][p]['members'][m]['data'])
        elif 'data' in info['file'][p]:
            total_size = len(info['file'][p]['data'])
        return total_size


    def get_total_size(self):
        self.check_parsed()
        size = 0

        if self.has_xmi():
            for f in self.xmit['file']:
                size += self.get_folder_size(f)
        if self.has_tape():
            for f in self.tape['file']:
                size += self.get_folder_size(f)
        return size

    def get_codecs(self):
        """ Returns supported codecs """
        return ebcdic.codec_names + ebcdic.ignored_codec_names()

    def get_codec(self):
        """ Returns current codec """
        return self.ebcdic

    def has_message(self):
        self.check_parsed()
        return self.msg

    def get_message(self):
        self.check_parsed()
        if self.msg:
            self.convert_message()
            return self.xmit['message']['text']

    def get_type(self):
        self.check_parsed()
        r = ''
        if self.has_xmi:
            r += "XMIT"
        else:
            r += "TAPE"
        if self.xmit and self.tape:
            r = "XMIT/TAPE"

    def get_num_files(self):
        self.check_parsed()
        total = 1
        if self.has_xmi():
            for i in self.xmit['file']:
                if 'members' in self.xmit['file'][i]:
                    for m in self.xmit['file'][i]['members']:
                        total += 1
                    total -= 1

        if self.has_tape():
            for i in self.tape['file']:
                total += 1
                if 'members' in self.tape['file'][i]:
                    for m in self.tape['file'][i]['members']:
                        total += 1
                    total -= 1
            total -= 1
        return total

    def get_members(self, pds):
        self.check_parsed()
        # returns an array of all members in the folder
        self.logger.debug("Getting members for {}".format(pds))

        members = []

        if self.has_xmi():
            if 'members' in self.xmit['file'][pds]:
                for m in self.xmit['file'][pds]['members']:
                    members.append(m)
        if self.has_tape():
            if 'members' in self.tape['file'][pds]:
                for m in self.tape['file'][pds]['members']:
                    members.append(m)
        return members

    def get_member_info(self, pds, member):
        self.check_parsed()
        # returns member information dict for the
        # member and pds provided
        # If its an alias the dict will contain the member name
        # and the name of the aliased member
        self.logger.debug("Getting info for {}({})".format(pds, member))
        info = {}

        if self.has_xmi():
            files = self.xmit['file']
        else:
            files = self.tape['file']

        if 'members' not in files[pds]:
            raise Exception("No members in {}".format(pds))

        if member not in files[pds]['members']:
            raise Exception("Member {} not found in {}".format(member, pds))

        if files[pds]['members'][member]['alias']:
            member = self.get_alias(pds, member)
            if member is None:
                raise Exception("Member Alias target not found")
            info['alias'] = member
        if 'mimetype' in files[pds]['members'][member]:
            info['mimetype'] = files[pds]['members'][member]['mimetype']
        if 'extension' in files[pds]['members'][member]:
            info['extension'] = files[pds]['members'][member]['extension']
        if files[pds]['members'][member]['ispf']:
            info['modified'] = files[pds]['members'][member]['ispf']['modifydate']
            info['owner'] = files[pds]['members'][member]['ispf']['user']
            info['version'] = files[pds]['members'][member]['ispf']['version']
            info['created'] = files[pds]['members'][member]['ispf']['createdate']

        info['RECFM'] = files[pds]['COPYR1']['DS1RECFM']
        info['LRECL'] = files[pds]['COPYR1']['DS1LRECL']

        if 'text' in files[pds]['members'][member] and not self.binary:
            info['size'] = len(files[pds]['members'][member]['text'])
        elif 'data' in files[pds]['members'][member]:
            info['size'] = len(files[pds]['members'][member]['data'])
        else:
            info['size'] = 0

        return info

    def get_member_info_simple(self, pds, member):
        self.check_parsed()
        # returns member information dict for the
        # member and pds provided
        # returns a dict with: size, mimetype, modified, owner if available
        # If its an alias the dict will contain the alias which is the name
        # of the aliased member
        self.logger.debug("Getting info for {}({})".format(pds, member))
        info = {}

        if self.has_xmi():
            files = self.xmit['file']
        else:
            files = self.tape['file']

        if files[pds]['members'][member]['alias']:
            member = self.get_alias(pds, member)
            if member is None:
                raise Exception("Member Alias target not found")
            info['alias'] = member
            info['mimetype'] = 'alias/symlink'
            info['size'] = 0
            if 'extension' in files[pds]['members'][member]:
                info['extension'] = files[pds]['members'][member]['extension']
            return info
        if 'mimetype' in files[pds]['members'][member]:
            info['mimetype'] = files[pds]['members'][member]['mimetype']
        if 'extension' in files[pds]['members'][member]:
            info['extension'] = files[pds]['members'][member]['extension']
        if files[pds]['members'][member]['ispf']:
            info['modified'] = files[pds]['members'][member]['ispf']['modifydate']
            info['owner'] = files[pds]['members'][member]['ispf']['user']

        if 'text' in files[pds]['members'][member] and not self.binary:
            info['size'] = len(files[pds]['members'][member]['text'])
        elif 'data' in files[pds]['members'][member]:
            info['size'] = len(files[pds]['members'][member]['data'])
        else:
            info['size'] = 0

        return info

    def get_seq_info(self, seq):
        self.check_parsed()

        self.logger.debug("Getting info for {}".format(seq))
        return {
            'mimetype' : "text/plain",
            'extension' : ".txt",
            'modified' : self.get_last_modified_xmi(),
            'owner' : self.get_owner()
        }

    def get_pds_info_simple(self, pds):
        # returns a dict with: size, mimetype, modified, owner and extension if available
        return self.get_file_info_simple(f)

    def get_file_info_simple(self, filename):
        # returns a dict with: size, mimetype, modified, owner and extension if available
        self.check_parsed()
        self.logger.debug("Getting info for {}".format(filename))
        info = {}
        if self.has_tape():
            info['mimetype'] = self.tape['file'][filename]['filetype']
            info['extension'] = self.tape['file'][filename]['extension']
        elif self.has_xmi():
            info['mimetype'] = self.xmit['file'][filename]['filetype']
            info['extension'] = self.xmit['file'][filename]['extension']

        info['modified'] = self.get_last_modified(filename)
        info['size'] = self.get_folder_size(filename)
        info['owner'] = self.get_owner()

        return info

    def get_file_info_detailed(self, filename):
        self.check_parsed()

        info = {}
        if self.has_tape() and filename in self.tape['file']:
            info['owner'] = self.get_owner()
            if 'HDR1' in self.tape['file'][filename]:
                info['dsnser'] = self.tape['file'][filename]['HDR1']['dsnser']
                info['created'] = self.tape['file'][filename]['HDR1']['createdate']
                info['expires'] = self.tape['file'][filename]['HDR1']['expirationdate']
                info['syscode'] = self.tape['file'][filename]['HDR1']['system_code']
            else:
                info['dsnser'] = 'N/A'
                info['created'] = 'N/A'
                info['expires'] = 'N/A'
                info['syscode'] = 'N/A'
            if 'HDR2' in self.tape['file'][filename]:
                info['jobid'] = self.tape['file'][filename]['HDR2']['jobid']
                info['RECFM'] = self.tape['file'][filename]['HDR2']['recfm']
                info['LRECL'] = self.tape['file'][filename]['HDR2']['lrecl']
            else:
                info['jobid'] = 'N/A'
                info['RECFM'] = 'N/A'
                info['LRECL'] = 'N/A'
            info['size'] = self.get_folder_size(filename)
            info['mimetype'] = self.tape['file'][filename]['filetype']
            info['extension'] = self.tape['file'][filename]['extension']
        return info

    def get_volser(self):
        if self.has_tape() and 'label' in self.tape:
            return self.tape['label']['volser']
        return ''

    def get_user_label(self):
        if self.has_tape() and 'UTL' in self.tape:
            l = ''
            for i in self.tape['UTL']:
                l = i + "\n"
            return i

        return ''

    def get_member_size(self, pds, member):
        self.check_parsed()
        return (len(self.get_member_decoded(pds, member)))

    def get_member_decoded(self, pds, member):
        self.check_parsed()
        # returns the data either translated
        # or as binary
        # RECFM 'U' are empty and dont make a 'data' item
        # So we return an empty byte

        if self.is_alias(pds, member):
            member = self.get_alias(pds, member)

        if self.has_xmi():
            rfile = self.xmit['file'][pds]['members'][member]
        else:
            rfile = self.tape['file'][pds]['members'][member]

        if 'text' in rfile:
            return rfile['text']
        elif 'data' in rfile:
            return rfile['data']
        else:
            return b''

    def get_member_binary(self, pds, member):
        self.check_parsed()
        if self.has_xmi() and pds in self.xmit['file']:
            return self.xmit['file'][pds]['members'][member]['data']
        if self.has_tape() and pds in self.tape['file']:
            return self.tape['file'][pds]['members'][member]['data']

    def get_member_text(self, pds, member):
        self.check_parsed()
        if self.force:
            return self.get_member_decoded(pds, member)

        if self.has_xmi():
            f = self.xmit['file'][pds]['members'][member]
        else:
            f = self.tape['file'][pds]['members'][member]
        if 'text' in f:
            return f['text']
        else:
            return self.xmit['file'][pds]['members'][member]['data'].decode(self.ebcdic)
        # Translates member from EBCDIC to UTF-8 regardless of mimetype

    def get_file_decoded(self, filename):
        self.check_parsed()
        if self.has_xmi():
            rfile = self.xmit['file'][filename]
        else:
            rfile = self.tape['file'][filename]

        if 'text' in rfile:
            return rfile['text']
        elif 'data' in rfile:
            return rfile['data']
        else:
            return b''

    def get_file_binary(self, filename):
        self.check_parsed()
        if self.has_xmi():
            rfile = self.xmit['file'][filename]
        else:
            rfile = self.tape['file'][filename]

        if 'data' in rfile:
            return rfile['data']
        else:
            return b''

    def get_file_text(self, filename):
        self.check_parsed()
        if self.force:
            return self.get_file_decoded(filename)

        if self.has_xmi():
            f = self.xmit['file'][pds]
        else:
            f = self.tape['file'][pds]

        if 'text' in f:
            return f['text']
        else:
            return self.xmit['file'][pds]['data'].decode(self.ebcdic)

    def get_seq_decoded(self, pds):
        return self.get_file_decoded(pds)

    def get_seq_raw(self, pds):
        return self.get_file_binary(pds)

    def is_alias(self, pds, member):
        self.check_parsed()
        if self.has_xmi():
            return self.xmit['file'][pds]['members'][member]['alias']
        else:
            return self.tape['file'][pds]['members'][member]['alias']

    def is_file(self, pds):
        self.check_parsed()
        return False

    def is_member(self, pds, member):
        self.check_parsed()

        if self.has_xmi():
            if ('file' in self.xmit and
                pds in self.xmit['file'] and
                'members' in self.xmit['file'][pds] and
                member in self.xmit['file'][pds]['members']):
                return True
        else:
            if ('file' in self.tape and
                pds in self.tape['file'] and
                'members' in self.tape['file'][pds] and
                member in self.tape['file'][pds]['members']):
                return True

        return False

    def is_sequential(self, pds):
        self.check_parsed()
        if self.has_xmi():
            if ('file' in self.xmit and
                pds in self.xmit['file'] and
                'members' not in self.xmit['file'][pds]):
                return True
        else:
            if ('file' in self.tape and
                pds in self.tape['file'] and
                'members' not in self.tape['file'][pds]):
                return True

        return False

    def is_pds(self, pds):
        self.check_parsed()
        return not self.is_sequential(pds)

    def get_alias(self, pds, member):
        self.check_parsed()
        if self.has_xmi():
            a = self.xmit['file'][pds]
        else:
            a = self.tape['file'][pds]
        alias_ttr = a['members'][member]['ttr']
        self.logger.debug("Getting Alias link for {}({}) TTR: {}".format(pds, member, alias_ttr))
        members = a['members']
        for m in members:
            if ( 'ttr' in members[m] and
                 not members[m]['alias'] and
                 members[m]['ttr'] == alias_ttr ):
                self.logger.debug("Found alias to: {}".format(m))
                return m
        return None

    def get_xmi_node_user(self):
        self.check_parsed()
        # Returns an array with from node, from user, to node, to user
        if not self.xmit:
            raise Exception("no xmi file loaded")
        if 'INMR01' not in self.xmit:
            raise Exception("No INMR01 in XMI file, has it been parsed yet?")

        return [self.xmit['INMR01']['INMFNODE'],
                self.xmit['INMR01']['INMFUID'],
                self.xmit['INMR01']['INMTNODE'],
                self.xmit['INMR01']['INMTUID']]

    def print_message(self):
        if not self.msg:
            self.logger.debug("No message file included in XMIT")
            return

        if 'text' not in self.xmit['message']:
            self.convert_message()

        print(self.xmit['message']['text'])


    def get_xmit_json(self):
        return self.get_json()

    def get_tape_json(self):
        return self.get_json()

    def get_json(self, text=False, indent=2):
        if not text:
            return json.dumps(self.get_clean_json_no_text(), default=str, indent=indent)

        return json.dumps(self.get_clean_json(), default=str, indent=indent)

    def dump_xmit_json(self, json_file_target=None):
        output_dict = self.get_clean_json()
        if not json_file_target:
            json_file_target = self.outputfolder / "{}.json".format(Path(self.filename).stem)

        self.logger.debug("Dumping JSON to {}".format(json_file_target.absolute()))
        json_file_target.write_text(self.get_json())

    def pprint(self):
        # Prints object dict
        self.check_parsed()
        if self.xmit:
            pprint(self.xmit)
        else:
            pprint(self.tape)

    def get_clean_json(self):

        if self.has_xmi():
            output_dict = copy.deepcopy(self.xmit)
        else:
            output_dict = copy.deepcopy(self.tape)

        for f in output_dict['file']:
            output_dict['file'][f].pop('data', None)
            if 'message' in output_dict:
                output_dict['message'].pop('file', None)

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

        return output_dict

    def get_clean_json_no_text(self):
        output_dict = self.get_clean_json()

        if 'message' in output_dict:
            output_dict['message'].pop('text', None)
        for f in output_dict['file']:
            if 'text' in output_dict['file'][f]:
                output_dict['file'][f].pop('text', None)
            if 'members' in output_dict['file'][f]:
                for m in output_dict['file'][f]['members']:
                    output_dict['file'][f]['members'][m].pop('text', None)
        return output_dict


    def filetype_is_xmi(self, current_file):
        # Determine if a file is an XMI
        self.logger.debug("Checking for INMR01 in bytes 2-8")
        if current_file[2:8].decode(self.ebcdic) == 'INMR01':
            return True

    def filetype_is_tape(self, current_file):
        self.logger.debug("Checking for 00 00 in bytes 2-4")
        # Determine if a file is a virtual tape file
        if self.get_int(current_file[2:4]) == 0:
            return True

    def print_xmit(self, human=True):
        self.print_details(human=human)

    def print_tape(self, human=True):
        self.print_details(human=human)

    def print_details(self, human=True):
        self.logger.debug("Printing detailed output. Human file sizes: {}".format(human))
        self.check_parsed()
        members = False
        table = PrettyTable()
        headers = []
        for f in self.get_files():
            headers += list(self.get_file_info_simple(f).keys())
            if self.is_pds(f):
                members = True
                for m in self.get_members(f):
                    headers += list(self.get_member_info_simple(f, m).keys())
        headers = sorted(set(headers))
        if members:
            headers = ['filename', 'member'] + headers
        else:
            headers = ['filename'] + headers
        table.field_names = headers

        table.align['filename'] = 'l'
        table.align['size'] = 'r'
        if members:
            table.align['member'] = 'l'
            table.align['alias'] = 'l'


        for f in self.get_files():
            info = self.get_file_info_simple(f)
            l = []
            for h in headers:
                if h == 'size' and human:
                    l.append( "{}".format(self.sizeof_fmt(info[h])))
                elif h in info:
                    l.append( "{}".format(info[h]))
                else:
                    l.append('')
            table.add_row([f] + l[1:])
            if self.is_pds(f):
                for m in self.get_members(f):
                    l = []
                    info = self.get_member_info_simple(f, m)
                    for h in headers:

                        if h == 'size' and human:
                            l.append( "{}".format(self.sizeof_fmt(info[h])))
                        elif h =='member':
                            l.append(m)
                        elif h in info:
                            l.append( "{}".format(info[h]))
                        else:
                            l.append('')
                    table.add_row([f] + l[1:])
        print(table)

    def unload_folder(self, pds):
        self.unload_pds(pds)

    def unload_xmit(self):
        self.unload_files()

    def unload_tape(self):
        self.unload_files()

    def unload_files(self):
        self.check_parsed()

        if self.has_xmi():
            self.logger.debug("Unloading XMIT")

        if self.has_tape():
            self.logger.debug("Unloading Virtual Tape")

        if not self.outputfolder.exists():
            self.logger.debug("Output folder '{}' does not exist, creating".format(self.outputfolder.absolute()))
            self.outputfolder.mkdir(parents=True, exist_ok=True)

        if self.has_message():
            msg_out = self.outputfolder / "{}.msg".format(self.get_files()[0])
            msg_out.write_text(self.get_message())

        for f in self.get_files():
                self.unload_pds(f)

    def unload_pds(self, pds):
        self.check_parsed()

        if not self.is_pds(pds):
            self.unload_file(pds)
            return

        if not self.outputfolder.exists():
            self.logger.debug("Output folder '{}' does not exist, creating".format(self.outputfolder.absolute()))
            self.outputfolder.mkdir(parents=True, exist_ok=True)

        outfolder = self.outputfolder / pds
        outfolder.mkdir(parents=True, exist_ok=True)
        for m in self.get_members(pds):

            info = self.get_member_info_simple(pds, m)
            ext = info['extension']
            outfile = outfolder / "{}{}".format(m, ext)

            if not self.overwrite and outfile.exists():
                self.logger.debug("File {} exists and overwrite disabled".format(outfile.absolute()))
                continue

            if self.is_alias(pds,m):
                alias = outfolder / "{}{}".format(info['alias'], ext)
                if outfile.is_symlink():
                    outfile.unlink()
                if not self.quiet:
                    print("Linking {} -> {}".format(outfile.absolute(), alias.absolute()))
                outfile.symlink_to(alias)
                continue

            member_data = self.get_member_decoded(pds, m)
            if self.binary:
                member_data = self.get_member_binary(pds, m)

            if not self.quiet:
                print("{dsn}({member})\t->\t{path}".format(dsn=pds, member=m, path=outfile.absolute()))
            if isinstance(member_data, str):
                outfile.write_text(member_data)
            else:
                outfile.write_bytes(member_data)

            if 'modified' in info and info['modified']:
                self.change_outfile_date(outfile, info['modified'])

    def unload_file(self, filename, member=None):
        self.check_parsed()

        if not self.outputfolder.exists():
            self.logger.debug("Output folder '{}' does not exist, creating".format(self.outputfolder.absolute()))
            self.outputfolder.mkdir(parents=True, exist_ok=True)

        if member:
            info = self.get_member_info(filename, member)
            outfile = self.outputfolder / "{}{}".format(member, info['extension'])
            file_data = self.get_member_decoded(filename, member) if not self.binary else self.get_member_binary(filename, member)
        else:
            info = self.get_file_info_simple(filename)
            outfile = self.outputfolder / "{}{}".format(filename, info['extension'])
            file_data = self.get_file_decoded(filename) if not self.binary else self.get_file_binary(filename)

        if not self.overwrite and outfile.exists():
            self.logger.debug("File {} exists and overwrite disabled".format(outfile.absolute()))
            return

        if not self.quiet:
            print("{dsn}\t->\t{path}".format(dsn=filename, path=outfile.absolute()))
        if isinstance(file_data, str):
            outfile.write_text(file_data)
        else:
            outfile.write_bytes(file_data)
        if 'modified' in info and info['modified']:
            self.change_outfile_date(outfile, info['modified'])

### Helper Functions

# hexdump: prints hexdub in ascii and ebcdic
# sizeof_fmt: human friendly file size
# convert_text_file: converts EBCDIC plain/text to UTF-8
# convert_message: converts message from ebcdic to utf-8
# get_dsorg: converts DSORG flag to text
# get_recfm: converts RECFM flag to text
# Check_parse: checks if we've parsed the XMI/Tape file
# make_int: converts tape label strings to integer
# ispf_date: converts ispf date to ISO format string
# get_int: wrapper for int.from_bytes() to save typing


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

    def convert_text_file(self, ebcdic_text, recl):
        self.logger.debug("Converting EBCDIC file to UTF-8. Using EBCDIC codepage: '{}' LRECL: {} UnNum: {} Force: {}".format(self.ebcdic, recl, self.unnum, self.force))
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

    def check_parsed(self):
        if not self.xmit and not self.tape:
            raise Exception("No XMI or Virtual Tape loaded.")
        if self.xmit and 'INMR01' not in self.xmit:
             raise Exception("No INMR01 in XMI file, has it been parsed yet?")

    def make_int(self, num):
        # Converts string to integer
        # Mostly used in tape labels
        num = num.strip()
        return int(num) if num else 0

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
            return(d.isoformat(timespec='microseconds'))
        except:
            self.logger.debug("Cannot parse ISPF date field")
            return ''

    def get_int(self, bytes, endian='big'):
        return int.from_bytes(bytes, endian)

    def change_outfile_date(self, outfile, date):
        # outfile: Path object
        # date: iso format date string
        if not self.modifydate:
            return

        self.logger.debug("Changing last modify date to match file records: {}".format(date))
        d = datetime.datetime.fromisoformat(date)
        modTime = time.mktime(d.timetuple())
        os.utime(outfile.absolute(), (modTime, modTime))

### XMIT (XMI/Transmission) Files

# parse_xmi: parses XMI object extracting headers and files in to self.xmit dict
# get_xmi_files: extracts files from XMI
# parse_INMR01/parse_INMR02/parse_INMR03: gets information contained in header records


    def parse_xmi(self):
        self.logger.debug("Parsing XMIT file")
        if self.xmit_object == '':
            self.read_xmit_file()
        self.xmit = {}

        # Get XMI header

        segment_name = self.xmit_object[2:8].decode(self.ebcdic)
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

        #self.logger.debug("dsorg: {} recfm: {}".format(hex(dsorg), hex(recfm)))

    def get_xmi_files(self):
        # Populates self.xmit with the members of the dataset stores the information in:
        # - self.xmit['file'][filename]['members'] -> a structure with member information
        # - self.xmit['file'][filename]['COPYR1'] -> information about the dataset and header records
        # - self.xmit['file'][filename]['COPYR2'] -> Dataset extent blocks

        magi = magic.Magic(mime_encoding=True, mime=True)
        inrm02num = 1
        if self.msg:
            inrm02num = 2
        filename = self.xmit['INMR02'][inrm02num]['INMDSNAM']
        dsnfile = self.xmit['file'][filename]['data']
        recl = self.xmit['INMR03'][inrm02num]['INMLRECL']
        recfm = self.xmit['INMR02'][inrm02num]['INMRECFM']
        # blocksize = self.xmit['INMR02'][inrm02num]['INMBLKSZ']
        # dsorg = self.xmit['INMR02'][inrm02num]['INMDSORG']
        # utility = self.xmit['INMR02'][inrm02num]['INMUTILN']


        filetype,datatype = magi.from_buffer( b''.join(dsnfile)).split('; ')
        datatype = datatype.split("=")[1]
        extention = mimetypes.guess_extension(filetype)
        #eof_marker = False

        if not extention:
            extention = "." + filetype.split("/")[1]

        # File magic cant detec XMIT files
        if (filetype == 'application/octet-stream' and
            len(dsnfile[0]) >= 8 and
            dsnfile[0][2:8].decode(self.ebcdic) == 'INMR01'):
            extention = ".xmi"
            filetype = 'application/xmit'

        if self.force:
            extention = ".txt"

        if filetype == 'text/plain' or datatype != 'binary':
            if 'F' in recfm:
                self.xmit['file'][filename]['text'] = self.convert_text_file( b''.join(dsnfile), recl)
            elif 'V' in recfm:
                    for record in dsnfile:
                        self.xmit['file'][filename]['text'] += self.convert_text_file(record, len(record)).rstrip() + '\n'
            else:
                self.xmit['file'][filename]['text'] = self.convert_text_file(b''.join(dsnfile), self.manual_recordlength)

        self.logger.debug("filetype: {} datatype: {} size: {}".format(filetype, datatype, len(b''.join(dsnfile))))
        self.xmit['file'][filename]['filetype'] = filetype
        self.xmit['file'][filename]['datatype'] = datatype
        self.xmit['file'][filename]['extension'] = extention

        try:
            self.xmit['file'][filename]['COPYR1'] = self.iebcopy_record_1(dsnfile[0])
        except:
            self.logger.debug("{} is not a PDS leaving".format(filename))
            return
        self.xmit['file'][filename]['filetype'] = "pds/directory"
        self.xmit['file'][filename]['extension'] = None

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


    def parse_INMR01(self, inmr01_record):
        # INMR01 records are the XMIT header and contains information
        # about the XMIT file
        self.xmit['INMR01'] = self.text_units(inmr01_record)
        if 'INMFTIME' in self.xmit['INMR01']:
            # Changing date format to '%Y%m%d%H%M%S%f'
            self.xmit['INMR01']['INMFTIME'] = self.xmit['INMR01']['INMFTIME'] + "0" * (20 - len(self.xmit['INMR01']['INMFTIME']))
            # Changing date format to isoformat
            self.xmit['INMR01']['INMFTIME'] = datetime.datetime.strptime(self.xmit['INMR01']['INMFTIME'],'%Y%m%d%H%M%S%f').isoformat(timespec='microseconds')


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



### Virtual Tape Files

# get_tape_date: If the tape has headers convert the date
# get_tape_files: get the files from the tape
# pase_tape: parse the tape file and get each segment

    def parse_tape(self):
        self.logger.debug("Parsing virtual tape file")
        self.logger.debug("Using LRECL: {}".format(self.manual_recordlength))
        magi = magic.Magic(mime_encoding=True, mime=True)

        if not self.tape_object:
            self.read_tape_file()

        self.tape = {}
        self.tape['file'] = {}
        UTL = []
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

            if self.tape_object[loc+6:loc+9].decode(self.ebcdic) == 'UTL' and cur_blocksize == 80:
                t = self.tape_object[loc+6:loc+cur_blocksize+6].decode(self.ebcdic)
                UTL.append( t[4:] )

            self.logger.debug("Location: {} Blocksize: {} Prev Blocksize: {} EoF: {} EoR: {} Flags: {:#06x} File Size: {}".format(loc, cur_blocksize, prev_blocksize, eof_marker, eor_marker, flags, len(tape_file)))

            if eof_marker:
                if tape_file[:4].decode(self.ebcdic) in  ['VOL1', 'HDR1', 'HDR2', 'EOF1', 'EOF2']:
                    self.logger.debug('Skipping VOL/HDR/EOF records type: {}'.format(tape_file[:4].decode(self.ebcdic)))
                    tape_file = b''
                    continue

                #if 'recfm' in HDR2 and 'V' in HDR2['recfm']:
                #    vb_tape_file = self.handle_vb(tape_file)
                #    tape_file = b''.join(vb_tape_file)

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
                    output = {
                        'data' : tape_file,
                        'filetype' : filetype,
                        'datatype': datatype,
                        'extension' : extention,
                        'num' : file_num
                        }

                    if tape_text:
                        output['text'] = tape_text

                    if HDR1:
                        output['HDR1'] =  HDR1
                        msg = 'HDR1:'
                        for key in HDR1:
                            msg += " {}: {}".format(key, HDR1[key])
                        self.logger.debug(msg)
                    if HDR2:
                        output['HDR2'] =  HDR2
                        msg = 'HDR2:'
                        for key in HDR2:
                            msg += " {}: {}".format(key, HDR2[key])
                        self.logger.debug(msg)
                    if UTL:
                        output['UTL'] =  UTL
                        for i in UTL:
                            self.logger.debug("User Label: {}".format(i))

                    if 'dsn' in HDR1:
                        self.tape['file'][HDR1['dsn']] = copy.deepcopy(output)
                    else:
                        self.tape['file']['FILE{:>04d}'.format(file_num)] = copy.deepcopy(output)

                    file_num += 1
                    HDR1 = {}
                    HDR2 = {}
                    output = {}
                    UTL = []
                else:
                    self.logger.debug('Empty tape entry, skipping')


                tape_file = b''
                self.logger.debug('EOF')


            loc += cur_blocksize + 6
        if volume_label:
            self.tape['label'] = volume_label
            msg = 'label:'
            for key in volume_label:
                msg += " {}: {}".format(key, volume_label[key])
            self.logger.debug(msg)


    def get_tape_files(self):
        # populates self.tape with pds/seq dataset information

        for filename in self.tape['file']:
            self.logger.debug('Processing Dataset: {}'.format(filename))

            if 'data' not in self.tape['file'][filename]:
                self.logger.debug("Skipping empty tape")
                continue

            dataset = self.tape['file'][filename]['data']
            copyr1_size = self.get_int(dataset[:2])
            try:
                self.tape['file'][filename]['COPYR1'] = self.iebcopy_record_1(dataset[:copyr1_size])
                self.logger.debug("Size of COPYR1 Field: {}".format(copyr1_size))
            except:
                self.logger.debug("{} is not a PDS leaving".format(filename))
                continue

            self.tape['file'][filename]['filetype'] = "pds/directory"
            self.tape['file'][filename]['extension'] = None
            copyr2_size = self.get_int(dataset[copyr1_size:copyr1_size+2])
            self.logger.debug("Size of COPYR2 Field: {}".format(copyr2_size))

            self.tape['file'][filename]['COPYR2'] = self.iebcopy_record_2(dataset[copyr1_size+8:copyr1_size+copyr2_size])

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
            self.tape['file'][filename]['members'] = self.get_members_info(member_dir)
            # Now getting member blocks
            dataset = dataset[loc:]
            loc = 0
            member_files = b''

            while loc < len(dataset):
                # loop until we get to the end of the PDS
                block_size = self.get_int(dataset[loc:loc+2])
                seg_size = self.get_int(dataset[loc+4:loc+6])
                self.logger.debug("BDW Size: {} SDW Size: {}".format(block_size, seg_size))
                member_files += dataset[loc+8:loc+block_size] # skip BDW and SDW
                #self.hexdump(dataset[loc:loc+block_size])
                loc += block_size
                if member_files[-12:] == b'\x00' * 12:
                    break
            self.logger.debug('Processing PDS: {}'.format(filename))
            self.tape['file'][filename] = self.process_blocks(self.tape['file'][filename], member_files)


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
        return d.isoformat(timespec='microseconds')

### Process PDS Functions

# iebcopy_record_1: parses COPYR1
# iebcopy_record_2: parses COPYR2
# all_members: Checks if all members have been processed in a directory
# get_members_info: gets member stats and info for all members in a directory
# process_blocks: process the directory blocks in a PDS
# handle_vb: Deals with variable record lengths
# text_units: Process IBM text units and return info

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

