"""
xmi Python Library
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    The NETDATA file format (CMS SENDFILE or TSO TRANSMIT/XMIT) is used
    primarily to transfer files between mainframes. The file consists of
    a dataset "unloaded" with either INMCOPY or IEBCOPY and metadata.
    Multiple control records exist, see https://en.wikipedia.org/wiki/NETDATA
    for more details. NETDATA and XMIT/XMI are used interchangably however
    NETDATA files are more commonly refered to as XMI.

    The AWSTAPE file format is used to transfer virtual tape files. Originally
    created for P/390 it is used primarily today with virtual tape offerings.
    AWS is the short name for these tape file types. Later the opensource
    project Hercules created the Hercules Emulated Tape, or HET, which builds
    on the AWS format by adding compression. Virtual tape files consist
    of one or more datasets and optional metadata stored in labels.

    This module consists of methods to extract or open XMI/AWS/HET mainframe
    files. It also contains the XMIT class which implements XMI, AWS and HET
    file management to read and extract datasets and members.
"""

# Copyright (c) 2021, Philip Young
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice,
# this list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

__version__ = '1.0.2'
__author__ = 'Philip Young, Henri Kuiper'
__license__ = "GPL"

__xmi_version__ = __version__

from pprint import pprint
from pathlib import Path
from prettytable import PrettyTable

import zlib
import bz2
import json
import logging
import copy
import ebcdic
import os
import struct
import datetime
import time
import magic
import mimetypes
import sys

# As documented here:
#     https://www.ibm.com/support/knowledgecenter/en/SSLTBW_2.1.0/com.ibm.zos.v2r1.ikjb400/txunit.htm
IBM_text_units = {}
IBM_text_units[0x0001] = { 'name' : "INMDDNAM", 'type' : "character", 'desc' : 'DDNAME for the file'}
IBM_text_units[0x0002] = { 'name' : "INMDSNAM", 'type' : "character", 'desc' : 'Name of the file'}
IBM_text_units[0x0003] = { 'name' : "INMMEMBR", 'type' : "character", 'desc' : 'Member name list'}
IBM_text_units[0x000B] = { 'name' : "INMSECND", 'type' : "decimal",   'desc' : 'Secondary space quantity'}
IBM_text_units[0x000C] = { 'name' : "INMDIR"  , 'type' : "decimal",   'desc' : 'Number of directory blocks'}
IBM_text_units[0x0022] = { 'name' : "INMEXPDT", 'type' : "character", 'desc' : 'Expiration date'}
IBM_text_units[0x0028] = { 'name' : "INMTERM" , 'type' : "character", 'desc' : 'Data transmitted as a message'}
IBM_text_units[0x0030] = { 'name' : "INMBLKSZ", 'type' : "decimal",   'desc' : 'Block size'}
IBM_text_units[0x003C] = { 'name' : "INMDSORG", 'type' : "hex",       'desc' : 'File organization'}
IBM_text_units[0x0042] = { 'name' : "INMLRECL", 'type' : "decimal",   'desc' : 'Logical record length'}
IBM_text_units[0x0049] = { 'name' : "INMRECFM", 'type' : "hex",       'desc' : 'Record format'}
IBM_text_units[0x1001] = { 'name' : "INMTNODE", 'type' : "character", 'desc' : 'Target node name or node number'}
IBM_text_units[0x1002] = { 'name' : "INMTUID" , 'type' : "character", 'desc' : 'Target user ID'}
IBM_text_units[0x1011] = { 'name' : "INMFNODE", 'type' : "character", 'desc' : 'Origin node name or node number'}
IBM_text_units[0x1012] = { 'name' : "INMFUID" , 'type' : "character", 'desc' : 'Origin user ID'}
IBM_text_units[0x1020] = { 'name' : "INMLREF" , 'type' : "character", 'desc' : 'Date last referenced'}
IBM_text_units[0x1021] = { 'name' : "INMLCHG" , 'type' : "character", 'desc' : 'Date last changed'}
IBM_text_units[0x1022] = { 'name' : "INMCREAT", 'type' : "character", 'desc' : 'Creation date'}
IBM_text_units[0x1023] = { 'name' : "INMFVERS", 'type' : "character", 'desc' : 'Origin version number of the data format'}
IBM_text_units[0x1024] = { 'name' : "INMFTIME", 'type' : "character", 'desc' : 'Origin timestamp'}  # yyyymmddhhmmssuuuuuu
IBM_text_units[0x1025] = { 'name' : "INMTTIME", 'type' : "character", 'desc' : 'Destination timestamp'}
IBM_text_units[0x1026] = { 'name' : "INMFACK" , 'type' : "character", 'desc' : 'Originator requested notification'}
IBM_text_units[0x1027] = { 'name' : "INMERRCD", 'type' : "character", 'desc' : 'RECEIVE command error code'}
IBM_text_units[0x1028] = { 'name' : "INMUTILN", 'type' : "character", 'desc' : 'Name of utility program'}
IBM_text_units[0x1029] = { 'name' : "INMUSERP", 'type' : "character", 'desc' : 'User parameter string'}
IBM_text_units[0x102A] = { 'name' : "INMRECCT", 'type' : "character", 'desc' : 'Transmitted record count'}
IBM_text_units[0x102C] = { 'name' : "INMSIZE" , 'type' : "decimal",   'desc' : 'File size in bytes'}
IBM_text_units[0x102F] = { 'name' : "INMNUMF" , 'type' : "decimal",   'desc' : 'Number of files transmitted'}
IBM_text_units[0x8012] = { 'name' : "INMTYPE" , 'type' : "hex",       'desc' : 'Data set type'}

MESSAGE_FORMATS = {
    '80x32':  (80, 32),
    '132x27': (132, 27),
}

def convert_ebcdic(ebcdic_file, lrecl=80):
    '''Converts an ebcdic mainframe file to text. Returns string.

    Args:
        ebcdic_file (byte): the file to be converted
        lrecl (int): Record length. A newline is inserted every lrecl bytes.
    '''
    return XMIT.convert_text_file(ebcdic_file, lrecl)


def extract_all(mainframe_file, output='./'):
    '''Extracts all datasets and members to output directory.

    Args:
        mainframe_file (str): path to XMI/AWS/HET file
        output (str): output folder. Defaults to current working directory
    '''
    mfile = XMIT(filename=mainframe_file, outputfolder=output)
    mfile.open()
    mfile.extract_all()


def list_all(mainframe_file):
    '''Returns a list of all datasets and members in XMI/AWS/HET file.'''
    mfile = XMIT(filename=mainframe_file)
    mfile.open()
    files = []
    for f in mfile.get_files():
        if mfile.is_pds(f):
            for m in mfile.get_members(f):
                files.append("{}({})".format(f, m))
        else:
            files.append(f)
    return files

def open_file(
        filename=None,
        LRECL=80,
        loglevel=logging.WARNING,
        infile=None,
        outputfolder="./",
        encoding='cp500',
        unnum=True,
        quiet=False,
        force_convert=False,
        binary=False,
        modifydate=False
    ):
    '''Opens a XMI/AWS/HET file and returns an XMIT object

    Args:
        filename (str): The path and filename of an XMI/AWS/HET file. Defaults
            to None.
        LRECL (int): If record length cannot be determined this value is used
        when converting from EBCDIC to UTF-8. Defaults to 80.
        loglevel (int): Level of logging, based on
            https://docs.python.org/3/library/logging.html#levels.
            Defaults to ``logging.WARNING``.
        outputfolder (str): Output file path for extracted files.
            Detaults to current working directory.
        encoding (str): EBCDIC codepage used when translating from EBCDIC to
            UTF-8. Defaults to cp1140
        infile (str): folder/file name to use for XMI output instead of the
                detaset name included in the metadata.
        unnum (bool): Some mainframe files have numbers in columns 72-80
            denoting line numbers. If True files converted from EBCDIC
            will have these columns removed. Default to True.
        quiet (bool): Do not print any output messages whle extracting files.
            Default to False.
        force_convert (bool): Converts all files utf-8 ignoring mimetype.
            Defaults to False.
        binary (bool): Extract files as binaries, ignoring mimetype.
            Defaults to False.
        modifydate (bool): If created date/last modified date information is
            available change the last modified date of the extracted file to
            match. Defaults to False.
    '''
    mfile = XMIT(
        filename=filename,
        LRECL=LRECL,
        loglevel=loglevel,
        infile=infile,
        outputfolder=outputfolder,
        encoding=encoding,
        unnum=unnum,
        quiet=quiet,
        force_convert=force_convert,
        binary=binary,
        modifydate=modifydate
    )
    mfile.open()
    return mfile


def resolve_message(message=None, message_file=None, message_format='80x32'):
    '''Resolve and validate message source; return (text, lrecl, max_lines).

    text is None when no message is provided or the content is whitespace-only.
    File path is consumed here and never passed downstream.
    '''
    if message_format not in MESSAGE_FORMATS:
        raise ValueError(
            "Unknown message_format {!r}. Choose from: {}".format(
                message_format, ', '.join(sorted(MESSAGE_FORMATS))))

    lrecl, max_lines = MESSAGE_FORMATS[message_format]

    if message_file is not None:
        text = Path(message_file).read_text(encoding='utf-8')
    elif message is not None:
        text = message.replace('\\n', '\n')
    else:
        return (None, lrecl, max_lines)

    if not text.strip():
        return (None, lrecl, max_lines)

    lines = text.splitlines()

    clipped = []
    for i, line in enumerate(lines):
        if len(line) > lrecl:
            print(
                "Warning: message line {} truncated to {} characters".format(i + 1, lrecl),
                file=sys.stderr)
            line = line[:lrecl]
        clipped.append(line)

    if len(clipped) > max_lines:
        print(
            "Warning: message truncated to {} lines".format(max_lines),
            file=sys.stderr)
        clipped = clipped[:max_lines]

    return ('\n'.join(clipped), lrecl, max_lines)


def create_xmi(
        input_path,
        output_file=None,
        dsn=None,
        lrecl=80,
        recfm='FB',
        encoding='cp500',
        from_user='PYTHON',
        from_node='LOCAL',
        to_user='PYTHON',
        to_node='LOCAL',
        message=None,
        message_file=None,
        message_format='80x32',
        loglevel=logging.WARNING
    ):
    '''Creates an XMI (NETDATA) file from a local file or folder.

    A single file produces a sequential dataset XMI.  A folder (one level
    deep) produces a partitioned dataset (PDS) XMI whose members are the
    files found in the folder.

    **PDS member naming and encoding (folder input):**

    The file extension is always dropped; the stem is uppercased and
    truncated to 8 characters (``photo.jpg`` → ``PHOTO``,
    ``my-source.asm`` → ``MY-SOUR``).

    Each file is classified as *text* (valid UTF-8) or *binary* (not),
    which drives encoding — the extension itself is not consulted:

    - ``.txt``, ``.jcl``, ``.rexx``, ``.asm``, ``.py``, ``.sh``,
      ``.xml``, ``.json`` and similar — **text**: converted UTF-8 →
      EBCDIC and padded to *lrecl*; lines longer than *lrecl* are
      silently truncated.
    - ``.xmi``, ``.xmit`` — **binary**: nested XMI files that z/OS can
      ``RECEIVE`` again after the outer PDS is restored.
    - ``.jpg``, ``.jpeg``, ``.png``, ``.gif``, ``.zip``, ``.bin`` and
      any other non-UTF-8 content — **binary**: portable only in an
      all-binary folder (see below).

    **All-binary folder** (every file fails UTF-8 decode): *recfm* is
    automatically switched to ``'U'`` and *lrecl* to ``0``.  All
    members are stored as raw bytes with no padding or conversion —
    roundtrip is lossless for every binary type including ``.xmi``.

    **Mixed folder** (text and binary files together): *recfm* stays
    ``'FB'``.  Text members are EBCDIC-converted and padded to *lrecl*
    as normal.  Binary members are stored as raw bytes null-padded to
    the next *lrecl* boundary so that IEBCOPY's fixed-length record
    structure is preserved.  After this padding:

    - ``.xmi`` / ``.xmit`` — **safe**: NETDATA parsing stops at
      ``INMR06``, so trailing nulls beyond the end record are ignored
      by z/OS ``RECEIVE``.
    - ``.jpg`` / ``.jpeg`` — **safe**: JPEG decoders ignore trailing
      data after the ``FFD9`` end-of-image marker.
    - ``.zip`` — **safe**: ZIP readers scan *backward* from EOF for the
      End of Central Directory signature; trailing nulls don't contain
      that signature, so every mainstream tool finds the real EOCD and
      opens the archive normally.
    - ``.png``, ``.bin`` and most other length-delimited formats —
      **not safe**: their parsers derive the file length from the
      container; the extra null bytes cause a format error.

    Args:
        input_path (str): Path to a file or directory to package as XMI.
        output_file (str): Path where the XMI file will be written.  If
            ``None`` the bytes are returned but no file is written.
        dsn (str): Dataset name to embed in the XMI metadata.  Defaults to
            the uppercased stem of *input_path* (max 44 characters).
        lrecl (int): Logical record length used when encoding text to
            fixed-length EBCDIC records.  Default ``80``.
        recfm (str): Record format string (``'FB'``, ``'F'``, ``'VB'``,
            ``'V'``, ``'U'``).  Default ``'FB'``.
        encoding (str): EBCDIC codepage for text encoding.  Default
            ``'cp500'``.
        from_user (str): Originating user ID (max 8 chars).  Also used as
            the userid recorded in PDS member ISPF statistics.
        from_node (str): Originating node name (max 8 chars).
        to_user (str): Destination user ID (max 8 chars).
        to_node (str): Destination node name (max 8 chars).
        message (str): Inline message string displayed on z/OS RECEIVE.
            Use ``\\n`` for line breaks.  Ignored if *message_file* is given.
        message_file (str): Path to a UTF-8 text file whose content is used
            as the message.  Takes precedence over *message*.
        message_format (str): Terminal format preset — ``'80x32'`` (default,
            LRECL 80, 32 lines) or ``'132x27'`` (LRECL 132, 27 lines).
        loglevel: Python logging level.  Default ``logging.WARNING``.

    Returns:
        bytes: Raw XMI file content (only when *output_file* is ``None``).
    '''
    resolved_msg = resolve_message(message, message_file, message_format)
    builder = XMIT(encoding=encoding, loglevel=loglevel)
    xmi_bytes = builder.build_xmi(
        input_path,
        dsn=dsn,
        lrecl=lrecl,
        recfm=recfm,
        from_user=from_user,
        from_node=from_node,
        to_user=to_user,
        to_node=to_node,
        resolved_msg=resolved_msg,
    )
    if output_file:
        Path(output_file).write_bytes(xmi_bytes)
    else:
        return xmi_bytes


class XMIT:
    """
    Mainframe XMI/AWS/HET file class.
    =================================

    This class contains modules to parse the control records for NETDATA,
    AWSTAPE and HET file format as well as methods to parse IEBCOPY records
    for partitioned datasets (PDS) and ISPF statistics. In addition it can
    identify filetypes stored within providing mimetype information. By default
    files will be converted from EBCDIC based on their mimetype as determined
    by libmagic.

    After parsing various functions exist to extract one, many or all files
    and folders (i.e. datasets) contained within. It also provides interfaces
    to gather file information and provides all metadata as json.

    Examples:
        Load an XMI file and extract all contents::

            >>> from xmilib import XMIT
            >>> obj = XMIT(filename="/path/to/FILE100.XMI")
            >>> obj.open()
            >>> obj.set_output_folder("/path/to")
            >>> obj.unload_files()

        Load an AWS file and view metatdata JSON::

            >>> from xmilib import XMIT
            >>> obj = XMIT(filename="/path/to/FILE420.AWS")
            >>> obj.open()
            >>> print(obj.get_json())

        Load an XMI file, extract member (aka file) from paritioned
        dataset (i.e PDS aka folder)::

            >>> from xmilib import XMIT
            >>> obj = XMIT(filename="/path/to/FILE720.XMI")
            >>> obj.parse()
            >>> obj.unload_file("PDS.IN.XMI", "FILE001")

        Load a HET file, extract datasets (PDS or sequential):

            >>> from xmilib import XMIT
            >>> obj = XMIT(filename="/path/to/tapefile01.het")
            >>> obj.parse()
            >>> obj.unload_files("PDS.IN.HET")

        Get a list of all members from a PDS and get their metadata:

            >>> members = obj.get_members("SOME.PDS")
            >>> for m in members:
            >>> info = obj.get_member_info("SOME.PDS", m)
            >>>     for k in info:
            >>>         print(k, info[k])

    Args:
        filename (str): The path and filename of an XMI/AWS/HET file. Defaults
            to None.
        LRECL (int): If record length cannot be determined this value is used
            when converting from EBCDIC to UTF-8. Defaults to 80.
        loglevel (int): Level of logging, based on
            https://docs.python.org/3/library/logging.html#levels.
            Defaults to ``loggin.WARNING``.
        outputfolder (str): Output file path for extracted files.
            Detaults to current working directory.
        encoding (str): EBCDIC codepage used when translating from EBCDIC to
            UTF-8. Defaults to cp1140
        infile (str): folder/file name to use for XMI output instead of the
                detaset name included in the metadata.
        unnum (bool): Some mainframe files have numbers in columns 72-80
            denoting line numbers. If True files converted from EBCDIC
            will have these columns removed. Default to True.
        quiet (bool): Do not print any output messages whle extracting files.
            Default to False.
        force_convert (bool): Converts all files utf-8 ignoring mimetype.
            Defaults to False.
        binary (bool): Extract files as binaries, ignoring mimetype.
            Defaults to False.
        modifydate (bool): If created date/last modified date information is
            available change the last modified date of the extracted file to
            match. Defaults to False.
    """

    def __init__(self,
                 filename=None,
                 LRECL=80,
                 loglevel=logging.WARNING,
                 infile=None,
                 outputfolder="./",
                 encoding='cp500',
                 unnum=True,
                 quiet=False,
                 force_convert=False,
                 binary=False,
                 modifydate=False
                ):

        self.filename = filename
        self.manual_recordlength = LRECL
        self.infile = infile
        self.xmit_object = ''
        self.tape_object = ''
        self.outputfolder = Path(outputfolder)
        self.INMR02_count = 0
        self.INMR03_count = 0
        self.msg = False
        self.file_object = None
        self.force = force_convert
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
            logger_formatter = logging.Formatter(
                '%(levelname)s :: {} :: %(funcName)s'
                ' :: %(message)s'.format(self.filename))
        else:
            logger_formatter = logging.Formatter(
                '%(levelname)s :: %(funcName)s :: %(message)s')
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
        self.logger.debug("force: {}".format(force_convert))
        self.logger.debug("binary: {}".format(binary))

    def open(self, infile=None):
        """ Loads an XMI/AWS/HET file.

        Args:
            infile (str): folder/file name to use for output instead of what
            is included in the metadata. Only used for XMI files.

        Use either ``set_filename(filename=)`` or ``set_file_object(data=)``
        before using this function if you haven't passed ``filename`` when you
        initialized this object.
        """
        if not self.filename and not self.file_object:
            raise Exception(
                "No file or object to load."
                " Use set_filename(filename=) or set_file_object(data=)")

        if self.filename and self.file_object:
            self.logger.debug(
                "XMIT.open() function called with both a filename and a file"
                "object. Using file object.")

        if not self.file_object:
            self.read_file()

        if infile:
            self.set_infile(infile)

        # Is the file an XMI file?

        if self.filetype_is_xmi(self.file_object[0:10]):
            self.xmit = {}
            self.logger.debug("File is an XMIT file")
            self.set_xmit_object(self.file_object)
            self.parse_xmi()
            self.get_xmi_files()
        elif self.filetype_is_tape(self.file_object[0:4]):
            self.tape = {}
            self.logger.debug("File is a Virtual Tape file")
            self.set_tape_object(self.file_object)
            self.parse_tape()
            self.get_tape_files()
        else:
            raise Exception("File is not XMI or Virtual Tape")

    def set_overwrite(self, setting=True):
        '''
        Sets file overwrite. If true extracted files will overwrite existing.
        '''
        self.overwrite = setting

    def set_modify(self, setting=True):
        '''
        Sets file modify date for extracting a file. If true extracted files
        will have their date changed based on their metadata, either created
        date or last modified date if available.
        '''
        self.modifydate = setting

    def set_quiet(self, setting=True):
        '''
        Enables/Disables printing when extracting files.
        '''
        self.quiet = setting

    def set_filename(self, filename):
        '''
        Sets the filename used to load the XMI/AWS/HET file
        '''
        self.filename = filename

    def set_xmit_file(self, filename):
        '''
        Alias of ``set_filename()`` with debug statements.
        '''
        self.logger.debug("Setting XMIT filename to: {}".format(filename))
        self.set_filename(filename)

    def set_tape_file(self, filename):
        '''
        Alias of ``set_filename()`` with debug statements.
        '''
        self.logger.debug("Setting TAPE filename to: {}".format(filename))
        self.set_filename(filename)

    def set_file_object(self, data):
        '''
        Used to open already loaded file bytes instead of file name.
        '''
        # allows you to pass an object
        self.logger.debug("Setting file object")
        self.file_object = data
        self.logger.debug("Total bytes: {}".format(len(self.file_object)))

    def set_xmit_object(self, xmit_data):
        '''
        Used to open already loaded XMI file bytes instead of file name.
        '''
        self.logger.debug("Setting XMIT object")
        self.xmit_object = xmit_data
        self.logger.debug("Total bytes: {}".format(len(self.xmit_object)))

    def set_tape_object(self, virtual_tape_data):
        '''
        Used to open already loaded AWS/HET file bytes instead of file name.
        '''
        self.logger.debug("Setting Virtual Tape object")
        self.tape_object = virtual_tape_data
        self.logger.debug("Total bytes: {}".format(len(self.tape_object)))

    def set_output_folder(self, outputfolder):
        '''
        Default folder to extract to. Defaults to current working directory.
        '''
        self.logger.debug("Setting output folder to: {}".format(outputfolder))
        self.outputfolder = Path(outputfolder)

    def set_codepage(self, codepage='cp1140'):
        '''
        Sets EBCDIC codepage used for EBCDIC to utf-8 conversion.
        Defaults to ``cp1140``.

        Warning:
            If this setting changed after a file has been opened/parsed
            you must reload the file for the changes to take effect.
        '''
        self.logger.debug(
            "Changing codepage from {} to {}".format(self.ebcdic, codepage))
        self.ebcdic = codepage

    def set_force(self, setting=True):
        '''
        When setting=True force all files to be converted to utf-8.

        Warning:
            If this setting changed after a file has been opened/parsed
            you must reload the file for the changes to take effect.
        '''
        self.logger.debug("Setting force file conversion")
        self.force = setting

    def set_binary(self, setting=True):
        '''
        When setting=True ignore mimetype and do not convert any files
        to UTF-8.

        Warning:
            If this setting changed after a file has been opened/parsed
            you must reload the file for the changes to take effect.
        '''
        self.logger.debug("Disabling automated file conversion.")
        self.binary = setting

    def set_unnum(self, setting=True):
        '''
        When set to True plaintext files translted to utf-8 will have their
        number columns removed.

        Some files on mainframes have number in columns 72-80, this library
        removes those columns by default when converting files. To disable this
        feature use ``set_unnum(False)``.

        Warning:
            If this setting changed after a file has been opened/parsed
            you must reload the file for the changes to take effect.
        '''
        self.logger.debug("Disabling unnum.")
        self.unnum = setting

    def read_file(self):
        '''
        Reads the XMI/AWS/HET file but does not parse it.
        '''
        self.logger.debug("Reading file: {}".format(self.filename))
        with open(self.filename, 'rb') as infile:
            self.file_object = infile.read()
        self.logger.debug("Total bytes: {}".format(len(self.xmit_object)))

    def read_xmit_file(self):
        '''
        Reads the XMI file but does not parse it.
        '''
        self.logger.debug("Reading file: {}".format(self.filename))
        with open(self.filename, 'rb') as xmifile:
            self.xmit_object = xmifile.read()
        self.logger.debug("Total bytes: {}".format(len(self.xmit_object)))
        if not self.filetype_is_xmi(self.xmit_object[0:10]):
            raise Exception("File is not an XMI file.")

    def read_tape_file(self):
        '''
        Reads the AWS/HET file but does not parse it.
        '''
        self.logger.debug("Reading file: {}".format(self.filename))
        with open(self.filename, 'rb') as tapefile:
            self.tape_object = tapefile.read()
        self.logger.debug("Total bytes: {}".format(len(self.tape_object)))
        if not self.filetype_is_tape(self.tape_object[0:4]):
            raise Exception("File is not an AWS/HET file.")

    def is_xmi(self, pds, member_name):
        '''
        Returns true if a member is an XMI file.

        Args:
            pds (str): partioned dataset name
            member_name (str): pds member
        '''
        self.check_parsed()
        if self.has_xmi():
            members = self.xmit['file'][pds]['members'][member_name]
        else:
            members = self.tape['file'][pds]['members'][member_name]
        self.logger.debug(
            "Checking if member {} is an XMI file".format(member_name))

        if 'mimetype' in members and members['mimetype'] == 'application/xmit':
            return True
        return False

    def has_xmi(self):
        ''' Returns True if this object has opened an XMI file. '''
        self.check_parsed()
        if self.xmit:
            return True
        return False

    def has_tape(self):
        ''' Returns True if this object has opened an AWS/HET file. '''
        self.check_parsed()
        if self.tape:
            return True
        return False

    def get_file(self):
        ''' Returns the name of the first dataset in the XMI/AWS/HET file.

        This function is useful when used with XMI files as they can only
        contain one file/directory.
        '''
        self.check_parsed()
        return self.get_files()[0]

    def get_files(self):
        '''
        Returns a list of all sequential datasets and partitioned dataset (PDS)
        (i.e. files/folders) contained within opened XMI/AWS/HET files.
        '''
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
        '''
        Returns the last modified date of a dataset. Tape files do not have a
        last modified so create date (if available) is used instead. The
        format of the date is string in ISO format.
        '''
        self.check_parsed()
        if self.has_xmi():
            return self.xmit['INMR01']['INMFTIME']
        elif 'HDR1' in self.tape['file'][filename]:
            return self.tape['file'][filename]['HDR1']['createdate']
        else:
            return ''

    def get_owner(self):
        ''' Returns the username of the dataset owner, if available.'''
        self.check_parsed()
        if self.has_xmi():
            return self.xmit['INMR01']['INMFUID']
        elif 'label' in self.tape:
            return self.tape['label']['owner']
        else:
            return ''

    def get_dataset_size(self, dsn):
        ''' Returns the size of a dataset.'''
        self.check_parsed()
        total_size = 0
        if self.has_xmi():
            info = self.xmit
        elif self.has_tape():
            info = self.tape

        if 'members' in info['file'][dsn]:
            for m in info['file'][dsn]['members']:
                if 'data' in info['file'][dsn]['members'][m]:
                    total_size += len(info['file'][dsn]['members'][m]['data'])
        elif 'data' in info['file'][dsn]:
            total_size = len(info['file'][dsn]['data'])
        return total_size

    def get_total_size(self):
        '''
        Returns the total size of all datasets in the file ignoring metadata.
        '''
        self.check_parsed()
        size = 0

        if self.has_xmi():
            for f in self.xmit['file']:
                size += self.get_dataset_size(f)
        if self.has_tape():
            for f in self.tape['file']:
                size += self.get_dataset_size(f)
        return size

    def get_codecs(self):
        """ Returns supported codecs """
        return ebcdic.codec_names + ebcdic.ignored_codec_names()

    def get_codec(self):
        """ Returns current codec """
        return self.ebcdic

    def has_message(self):
        ''' Returns true if the XMI file has a message.'''
        self.check_parsed()
        return self.msg

    def get_message(self):
        ''' Returns a string containing the XMI file message.'''
        self.check_parsed()
        if self.msg:
            self.convert_message()
            return self.xmit['message']['text']

    def get_num_files(self):
        '''
        Returns the total number of files (datasets and members).
        '''
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
        '''
        Returns an array of all members in the provided partitioned dataset.
        '''
        self.check_parsed()
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
        '''
        Returns a dict containing information about the partitioned dataset
        member.

        The returned dict contains:

            * mimetype (str): the member mimetype
            * extenstion (str): the member extention based on mimetype
            * RECFM (str): the member record format
            * LRECL (int): the member line/record length
            * size (int): size of the member

        If ISPF stats are available it also contains:

            * modified (str): The last modify date of the file in ISO format
            * owner (str): The username of the file owner
            * version (str): The current file version

        If the member is an alias (i.e. symbolic link) it also contains:

            * alias (str): name of the member this alias points to

        If the member is an alias all other information is pulled from the
        member the alias points to.
        '''
        self.check_parsed()
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
        '''
        Alias of ``get_member_info()``.
        '''
        return (self.get_member_info(pds, member))

    def get_file_info_simple(self, filename):
        ''' Returns a dict containing a small subset of metadata for the
        dataset.

        The returned dict contains:

            * mimetype (str): the member mimetype
            * extenstion (str): the member extention based on mimetype
            * modified (str): output from get_last_modified() in ISO format
            * size (int): output from get_dataset_size()
            * owner (str): output from get_owner()
        '''
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
        info['size'] = self.get_dataset_size(filename)
        info['owner'] = self.get_owner()

        return info

    def get_pds_info_simple(self, pds):
        ''' Alias of ``get_file_info_simple()``.'''
        return self.get_file_info_simple(pds)

    def get_file_info_detailed(self, filename):
        ''' Returns a dict with metadata. Currently only supports tape files.


        The returned dict contains the following:

            * mimetype (str): the member mimetype
            * extenstion (str): the member extention based on mimetype
            * size (int): output from get_dataset_size()
            * owner (str): output from get_owner()

        It may also contain the following:

            * dsnser (str): The serial number of the dataset
            * created (str): The date the dataset was created in ISO format
            * expires (str): The date the dataset can be removed in ISO format
            * syscode (str): The system code of the system that generated this
              this tape file
            * jobid (str): The job id used to move this dataset to this tape
            * RECFM (str): the member record format
            * LRECL (int): the member line/record length
        '''

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
            info['size'] = self.get_dataset_size(filename)
            info['mimetype'] = self.tape['file'][filename]['filetype']
            info['extension'] = self.tape['file'][filename]['extension']
        return info

    def get_volser(self):
        ''' Returns the tape volume serial if available. '''
        if self.has_tape() and 'label' in self.tape:
            return self.tape['label']['volser']
        return ''

    def get_user_label(self):
        '''
        Returns all user labels on the tape concatenated together in a string.
        '''
        if self.has_tape() and 'UHL' in self.tape:
            label = ''
            for user_text in self.tape['UHL']:
                label += user_text + "\n"
            return label

        return ''

    def get_member_size(self, pds, member):
        '''Returns the size of a partitioned dataset member (int).'''
        self.check_parsed()
        return (len(self.get_member_decoded(pds, member)))

    def get_member_decoded(self, pds, member):
        '''
        Returns either UTF-8 string or EBCDIC bytes depending on member
        mimetype.
        '''
        self.check_parsed()
        # RECFM 'U' are empty and dont make a 'data' item
        # So we return an empty byte, this is a bug that needs fixing

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
        '''Returns partitioned dataset member as bytes.'''
        self.check_parsed()
        if self.has_xmi() and pds in self.xmit['file']:
            return self.xmit['file'][pds]['members'][member]['data']
        if self.has_tape() and pds in self.tape['file']:
            return self.tape['file'][pds]['members'][member]['data']

    def get_member_text(self, pds, member):
        '''
        Returns paritioned dataset member converted to utf-8 based on current
        codepage (default is cp1141).

        Use ``set_codepage()`` to change current code page.
        '''
        self.check_parsed()
        if self.force:
            return self.get_member_decoded(pds, member)

        if self.has_xmi():
            pds_member = self.xmit['file'][pds]['members'][member]
        else:
            pds_member = self.tape['file'][pds]['members'][member]
        if 'text' in pds_member:
            return pds_member['text']
        else:
            return self.xmit['file'][pds]['members'][member]['data'].decode(self.ebcdic)
        # Translates member from EBCDIC to UTF-8 regardless of mimetype

    def get_file_decoded(self, filename):
        '''
        Returns either UTF-8 string or EBCDIC bytes depending on dataset
        mimetype.
        '''
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

    def get_seq_decoded(self, pds):
        '''Alias of ``get_file_decoded()``.'''
        return self.get_file_decoded(pds)

    def get_file_binary(self, filename):
        '''Returns EBCDIC bytes of dataset.'''
        self.check_parsed()
        if self.has_xmi():
            rfile = self.xmit['file'][filename]
        else:
            rfile = self.tape['file'][filename]

        if 'data' in rfile:
            return rfile['data']
        else:
            return b''

    def get_seq_raw(self, pds):
        '''Alias of ``get_file_binary()``.'''
        return self.get_file_binary(pds)

    def get_file_text(self, filename):
        '''
        Returns dataset converted to utf-8 based on current codepage
        (default is cp1141).

        Use ``set_codepage()`` to change current code page.
        '''
        self.check_parsed()
        if self.force:
            return self.get_file_decoded(filename)

        if self.has_xmi():
            dataset_dict = self.xmit['file'][filename]
        else:
            dataset_dict = self.tape['file'][filename]

        if 'text' in dataset_dict:
            return dataset_dict['text']
        else:
            return dataset_dict['data'].decode(self.ebcdic)

    def is_alias(self, pds, member):
        '''Returns True if the partitioned dataset member is an alias.'''
        self.check_parsed()
        if self.has_xmi():
            return self.xmit['file'][pds]['members'][member]['alias']
        else:
            return self.tape['file'][pds]['members'][member]['alias']

    def is_member(self, pds, member):
        '''
        Returns true if the member exists in the provided partioned dataset.
        '''
        self.check_parsed()

        if self.has_xmi():
            if ('file' in self.xmit and pds in self.xmit['file'] and 'members' in self.xmit['file'][pds] and member in self.xmit['file'][pds]['members']):
                return True
        else:
            if (
                'file' in self.tape
                and pds in self.tape['file']
                and 'members' in self.tape['file'][pds]
                and member in self.tape['file'][pds]['members']
            ):
                return True

        return False

    def is_sequential(self, pds):
        '''
        Returns true if the dataset is a sequential dataset (i.e. file).
        '''
        self.check_parsed()
        if self.has_xmi():
            if (
                'file' in self.xmit
                and pds in self.xmit['file']
                and 'members' not in self.xmit['file'][pds]
            ):
                return True
        else:
            if (
                'file' in self.tape
                and pds in self.tape['file']
                and 'members' not in self.tape['file'][pds]
            ):
                return True
        return False

    def is_file(self, pds):
        '''Alias of ``is_sequential()``.'''
        return self.is_sequential(pds)
        return False

    def is_pds(self, pds):
        '''
        Returns true if the dataset is a partitioned dataset (i.e. folder).
        '''
        self.check_parsed()
        return not self.is_sequential(pds)

    def get_alias(self, pds, member):
        '''Returns the member name that the alias points to if available.'''
        self.check_parsed()
        if self.has_xmi():
            a = self.xmit['file'][pds]
        else:
            a = self.tape['file'][pds]
        alias_ttr = a['members'][member]['ttr']
        self.logger.debug(
            "Getting Alias link for {}({})"
            " TTR: {}".format(pds, member, alias_ttr))
        members = a['members']
        for m in members:
            if (
                'ttr' in members[m]
                and not members[m]['alias']
                and members[m]['ttr'] == alias_ttr
            ):
                self.logger.debug("Found alias to: {}".format(m))
                return m
        return None

    def get_xmi_node_user(self):
        '''
        Returns a list containing XMI file information around nodes/owners.

        The list contains the following strings:

            * Originating node name
            * Originating user name
            * Destination node name
            * Destination user name
        '''
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
        '''If an XMI file has a message prints the message and returns.'''
        if not self.msg:
            self.logger.debug("No message file included in XMIT")
            return

        if 'text' not in self.xmit['message']:
            self.convert_message()

        print(self.xmit['message']['text'])

    def get_json(self, text=False, indent=2):
        '''
        Returns a string containing all available metadata for the file.

        Args:
            text (bool): If True the metadata also includes the file converted
            to utf-8. Default to False.
            indent (int): json file indentation, default to 2
        '''
        if not text:
            return json.dumps(self._get_clean_json_no_text(), default=str, indent=indent)

        return json.dumps(self._get_clean_json(), default=str, indent=indent)

    def get_xmit_json(self):
        '''Alias of ``get_json()``.'''
        return self.get_json()

    def get_tape_json(self):
        '''Alias of ``get_json()``.'''
        return self.get_json()

    def dump_xmit_json(self, json_file_target=None):
        '''
        Extracts all file metadata to filename.json. Where filename is the
        name of the XMI/AWS/HET file.

        Args:
            json_file_target (str): folder where to place json file
        '''
        if not json_file_target:
            json_file_target = self.outputfolder / "{}.json".format(Path(self.filename).stem)

        self.logger.debug("Dumping JSON to {}".format(json_file_target.absolute()))
        json_file_target.write_text(self.get_json())

    def _pprint(self):
        '''Prints the XMI/TAPE object using pprint.'''
        # Prints object dict
        self.check_parsed()
        if self.xmit:
            pprint(self.xmit)
        else:
            pprint(self.tape)

    def _get_clean_json(self):
        '''Returns a dict with binary data from tape/xmi dicts removed and
        appends class information. For use with get_json()'''

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
        output_dict['CONFIG'] = {
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

    def _get_clean_json_no_text(self):
        '''Returns a dict with utf-8 data from tape/xmi dicts removed. For use
        with get_json()'''
        output_dict = self._get_clean_json()

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
        '''Determines if a file is an XMI file.


        NETDATA files must have the first record header INRM01 which
        is located after the record size halfword at the beggening of the
        file.
        '''
        self.logger.debug("Checking for INMR01 in bytes 2-8")
        if current_file[2:8].decode(self.ebcdic) == 'INMR01':
            return True

    def filetype_is_tape(self, current_file):
        '''Determines if a file is a virtual tape (AWS/HET) file.


        Virtual tape files begin with a six byte header which contains three
        halfwords:
        1 the number of bytes in this block
        2 the number of bytes in the previous block
        3 end of file flag

        To check if the file is a virtual tape file it confirms that
        the first record previous bytes header is zero as there cannot be
        any previous bytes at the beggining of the file.
        '''

        self.logger.debug("Checking for 00 00 in bytes 2-4")
        # Determine if a file is a virtual tape file
        if self.__get_int(current_file[2:4]) == 0:
            return True

    def print_details(self, human=True):
        '''Prints a subset of available metadata for all datasets/members
        included in the file.

        Arguments:
            human (bool): Is True converts file sizes to human readable.
                Default is True.
        '''
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
            metadata = []
            for h in headers:
                if h == 'size' and human:
                    metadata.append( "{}".format(self.sizeof_fmt(info[h])))
                elif h in info:
                    metadata.append( "{}".format(info[h]))
                else:
                    metadata.append('')
            table.add_row([f] + metadata[1:])
            if self.is_pds(f):
                for m in self.get_members(f):
                    metadata = []
                    info = self.get_member_info_simple(f, m)
                    for h in headers:
                        if h == 'size' and human:
                            metadata.append( "{}".format(self.sizeof_fmt(info[h])))
                        elif h == 'member':
                            metadata.append(m)
                        elif h in info:
                            metadata.append( "{}".format(info[h]))
                        else:
                            metadata.append('')
                    table.add_row([f] + metadata[1:])
        print(table)

    def print_xmit(self, human=True):
        '''Alias of ``print_details()``.'''
        self.print_details(human=human)

    def print_tape(self, human=True):
        '''Alias of ``print_details()``.'''
        self.print_details(human=human)

    def unload_files(self):
        '''
        Extracts all datasets and members to output folder and appends file
        extensions based on mimetype.

        If there are partitioned datasets folders will be created based on the
        dataset name and all members will be placed in that folder.

        Output folder can be changed with ``set_outputfolder()``, default is
        current working directory.
        '''
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

    def unload_xmit(self):
        '''Alias of ``unload_files()``.'''
        self.unload_files()

    def unload_tape(self):
        '''Alias of ``unload_files()``.'''
        self.unload_files()

    def extract_all(self):
        '''Alias of ``unload_files()``.'''
        self.unload_files()

    def unload_pds(self, pds):
        '''
        Extracts all the dataset or members to output folder and appends
        file extensions based on mimetype.

        If the dataset is a partitioned dataset a folder will be created and
        all members will be placed in that sub-folder.

        Output folder can be changed with set_outputfolder(), default is
        current working directory.
        '''
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

            if 'extension' in info:
                ext = info['extension']
            else:
                ext = '.bin'

            outfile = outfolder / "{}{}".format(m, ext)

            if not self.overwrite and outfile.exists():
                self.logger.debug("File {} exists and overwrite disabled".format(outfile.absolute()))
                continue

            if self.is_alias(pds, m):
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

    def unload_folder(self, pds):
        '''Alias of ``unload_pds()``.'''
        self.unload_pds(pds)

    def extract_pds(self, pds):
        '''Alias of ``unload_pds()``'''
        self.unload_pds(pds)

    def unload_file(self, filename, member=None):
        '''
        Extracts one file to output folder.


        Arguments:
            filename (str): dataset to extract, required.
            member (str): optional member name


        Output folder can be changed with set_outputfolder(), default is
        current working directory.
        '''
        self.check_parsed()

        if not self.outputfolder.exists():
            self.logger.debug(
                "Output folder '{}' does not exist,"
                " creating".format(self.outputfolder.absolute()))
            self.outputfolder.mkdir(parents=True, exist_ok=True)

        if member:
            info = self.get_member_info(filename, member)
            outfile = (
                self.outputfolder / "{}{}".format(member, info['extension']))
            file_data = self.get_member_decoded(filename, member) if not self.binary else self.get_member_binary(filename, member)
        elif not member and self.is_sequential(filename):
            info = self.get_file_info_simple(filename)
            outfile = (
                self.outputfolder / "{}{}".format(filename, info['extension']))
            file_data = self.get_file_decoded(filename) if not self.binary else self.get_file_binary(filename)
        else:
            raise Exception("unload_file() called with a PDS and no member."
                            " Use unload_pds() instead")

        if not self.overwrite and outfile.exists():
            self.logger.debug(
                "File {} exists and overwrite disabled".format(
                    outfile.absolute()))
            return

        if not self.quiet:
            print("{dsn}\t->\t{path}".format(dsn=filename,
                  path=outfile.absolute()))
        if isinstance(file_data, str):
            outfile.write_text(file_data)
        else:
            outfile.write_bytes(file_data)
        if 'modified' in info and info['modified']:
            self.change_outfile_date(outfile, info['modified'])

    def extract_dataset(self, dataset):
        '''Alias of ``unload_file()``.'''
        self.unload_file(dataset)

    # Helper Functions

    def sizeof_fmt(self, num):
        '''Returns human friendly size of int.'''
        for unit in ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']:
            if abs(num) < 1024.0:
                return "{:3.1f}{}".format(num, unit).rstrip('0').rstrip('.')
            num /= 1024.0
        return "{:.1f}{}".format(num, 'Y')

    def convert_text_file(self, ebcdic_text, recl):
        '''
        Converts EBCDIC files to utf-8.

        Arguments:
            * ebcdic_text (bytes): EBCDIC data to convert
            * lrecl (int): How long each line is

        Why lrecl? Mainframe fixed length (RECFM = F) files have no concept of
        line terminators, each line is the exact same length padded to LRECL
        with spaces. Therefore, if ebcdic_text is 160 bytes and an LRECL of 80
        there's two lines in the provided data. Some files can have variable
        length (RECFM = V). The only change is that each line has its own
        record length, but it will still be padded with spaces.

        Some mainframe files may have a numbers column (columns 72-80 have
        numbers in them). By default those columns are removed if they only
        contain numbers. To disable this feature use ``set_unnum(False)``.
        '''
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
        '''Converts XMI file message to utf-8'''
        if not self.msg:
            self.logger.debug("No message file included in XMIT")
            return

        message = self.xmit['message']['file']
        recl = self.xmit['message']['lrecl']
        self.xmit['message']['text'] = self.convert_text_file(message, recl)

    def get_dsorg(self, dsorg):
        '''Returns a string of the dataset organization (DSORG).

        DSORG contains file layout information. Typically datasets will be
        either "PS" or "PO". PS datasets are sequential datasets (i.e. a single
        file), PO dataset are partitioned datasets (i.e. folders).
        '''
        try:
            file_dsorg = self.__get_int(dsorg)
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
        '''Returns a string of the dataset record format (RECFM).

        RECFM contains file layout information and is cummulative.

        The first letter is one of F, V, U where:

            * F = fixed length records
            * V = Variable length records
            * U = Unknown

        The additional letters may be:

            * B = blocked
            * A = ANSI control characters (for printers)
            * M = machine control characters (for printers)
            * S = standard blocks

        For more information see DS1RECFM in
        https://www.ibm.com/support/knowledgecenter/SSLTBW_2.3.0/com.ibm.zos.v2r3.idas300/s3013.htm
        '''
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

        self.logger.debug("Record Format (recfm): {} ({:#06x})".format(rfm, self.__get_int(recfm)))

        return rfm

    def check_parsed(self):
        '''Raises an exception if no XMI/AWS/HET has been opened.'''
        if not self.xmit and not self.tape:
            raise Exception("No XMI or Virtual Tape loaded.")
        if self.xmit and 'INMR01' not in self.xmit:
            raise Exception("No INMR01 in XMI file, has it been parsed yet?")

    def make_int(self, num):
        '''Converts string to integer, mostly used in tape labels.'''
        num = num.strip()
        return int(num) if num else 0

    def ispf_date(self, ispfdate, seconds=0):
        '''Converts ISPF date to ISO format string with microseconds.'''

        # ISPF dates use packed decimal, more information here:
        # https://www.ibm.com/support/knowledgecenter/ssw_ibm_i_74/rzasd/padecfo.htm

        century = 19 + ispfdate[0]
        year = format(ispfdate[1], '02x')
        day = format(ispfdate[2], '02x') + format(ispfdate[3], '02x')[0]
        if day == '000':
            day = '001'
        if len(ispfdate) > 4:
            hours = format(ispfdate[4], '02x')
            minutes = format(ispfdate[5], '02x')
        else:
            hours = '00'
            minutes = '00'

        if seconds != 0:
            seconds = format(seconds, '02x')
        else:
            seconds = '00'

        date = "{}{}{}{}{}{}".format(century, year, day, hours, minutes, seconds)

        try:
            d = datetime.datetime.strptime(date, '%Y%j%H%M%S')
            return(d.isoformat(timespec='microseconds'))
        except:
            self.logger.debug("Cannot parse ISPF date field")
            return ''

    def __get_int(self, bytes, endian='big'):
        return int.from_bytes(bytes, endian)

    def change_outfile_date(self, outfile, date):
        '''
        Modifies extracted files created/last modified date to match metadata

        Arguments:
            outfile (str): path to file
            date (str): ISO format date
        '''
        # outfile: Path object
        # date: iso format date string
        if not self.modifydate:
            return

        self.logger.debug(
            "Changing last modify date to match file records: {}".format(date))
        d = datetime.datetime.fromisoformat(date)
        modTime = time.mktime(d.timetuple())
        os.utime(outfile.absolute(), (modTime, modTime))

    # NETDATA (XMI/TSO TRANSMIT) Files

    def parse_xmi(self):
        '''
        Parses an XMI file collecting metadata and files and stores them in the
        object.

        NETDATA files (otherwise known as XMI files) are composed of control
        records which contain metadata and dataset(s).

        Control Records:

        * INMR01 - Header records
        * INMR02 - File control record(s)
        * INMR03 - Data control record(s)
        * INMR04 - User control record
        * INMR06 - Final record
        * INMR07 - Notification record

        This library only processes INMR01, INRM02, INMR03, INMR04, and INMR06
        records. INMR07 records are notification records and do not contain
        any files.

        INMR records are composed of the name (INMR01, etc) followed by IBM
        text units which contains metadata about the record.
        '''
        self.logger.debug("Parsing XMIT file")
        if not self.xmit_object:
            if self.file_object:
                self.xmit_object = self.file_object
            else:
                self.read_xmit_file()
        self.xmit = {}

        # Get XMI header

        segment_name = self.xmit_object[2:8].decode(self.ebcdic)
        if segment_name != 'INMR01':
            raise Exception('No INMR01 record found in {}.'.format(self.filename))

        record_data = b''
        loc = 0
        while loc < len(self.xmit_object):
            section_length = self.__get_int(self.xmit_object[loc:loc + 1])
            flag = self.__get_int(self.xmit_object[loc + 1:loc + 2])

            if 0x20 != (0x20 & flag):  # If we're not a control record

                if (
                    'INMDSNAM' not in self.xmit['INMR02'][1]
                    and self.msg
                    and len(self.xmit['INMR03']) < 2
                ):

                    if "message" not in self.xmit:
                        self.logger.debug("Message record found")
                        self.xmit['message'] = {}
                        self.xmit['message']['file'] = b''
                        self.xmit['message']['lrecl'] = self.xmit['INMR03'][1]['INMLRECL']
                    self.xmit['message']['file'] += self.xmit_object[loc + 2:loc + section_length]
                    self.filelocation = 2

                else:
                    if 'INMDSNAM' not in self.xmit['INMR02'][self.filelocation]:
                        # XMIT370 doesn't include a dataset name record in sequential datasets
                        # If no dsn is provided to this function use the filename instead
                        self.logger.debug("No INMDSNAM using filename {}".format(Path(self.filename).stem.upper()))
                        self.xmit['INMR02'][self.filelocation]['INMDSNAM'] = Path(self.filename).stem.upper()

                    if self.infile:
                        self.logger.debug("Infile set to {}. Using as filename.".format(infile))
                        dsn = infile
                    else:
                        dsn = self.xmit['INMR02'][self.filelocation]['INMDSNAM']  # filename

                    if 'file' not in self.xmit:
                        self.xmit['file'] = {}

                    if dsn not in self.xmit['file']:
                        self.logger.debug(
                            "{} not recorded creating".format(dsn))
                        self.xmit['file'][dsn] = {}
                        self.xmit['file'][dsn]['data'] = []

                    record_data += self.xmit_object[loc + 2:loc + section_length]  # get the various segments
                    eighty = False
                    forty = False
                    write_length = len(self.xmit_object[loc + 2:loc + section_length])

                    if 0x80 == (0x80 & flag):
                        eighty = True

                    if 0x40 == (0x40 & flag):
                        forty = True
                        self.xmit['file'][dsn]['data'].append(record_data)
                        record_data = b''

                    self.logger.debug("Location: {:8} Writting {:<3} bytes Flag: 0x80 {:<1} 0x40 {:<1} (Section length: {})".format(loc, write_length, eighty, forty, section_length))

            if 0x20 == (0x20 & flag):
                self.logger.debug("[flag 0x20] This is (part of) a control record.")
                record_type = self.xmit_object[loc + 2:loc + 8].decode(self.ebcdic)
                self.logger.debug("Record Type: {}".format(record_type))
                if record_type == "INMR01":
                    self.parse_INMR01(self.xmit_object[loc + 8:loc + section_length])
                elif record_type == "INMR02":
                    self.parse_INMR02(self.xmit_object[loc + 8:loc + section_length])
                elif record_type == "INMR03":
                    self.parse_INMR03(self.xmit_object[loc + 8:loc + section_length])
                elif record_type == "INMR04":
                    self.parse_INMR04(self.xmit_object[loc + 8:loc + section_length])
                elif record_type == "INMR06":
                    self.logger.debug("[INMR06] Processing last record")
                    return

            if 0x0F == (0x0F & flag):
                self.logger.debug("[flag 0x0f] Reserved")
            loc += section_length

    def get_xmi_files(self):
        '''Extracts files from a parsed XMI file and stores them in the XMIT object'''

        # Partitioned datasets are broken up as follows:
        #   * COPYR1 record
        #   * COPYR2 record
        #   * Member metadata (filnames, file stats, etc)
        #   * Files

        magi = magic.Magic(mime_encoding=True, mime=True)
        inrm02num = 1
        if self.msg:
            inrm02num = 2
        filename = self.xmit['INMR02'][inrm02num]['INMDSNAM']
        dsnfile = self.xmit['file'][filename]['data']
        recl = self.xmit['INMR03'][inrm02num]['INMLRECL']
        recfm = self.xmit['INMR02'][inrm02num]['INMRECFM']
        self.xmit['file'][filename].update(self.__get_file_mimetype_and_convert(
            file_name=filename,
            file_data=b''.join(dsnfile),
            vb_file_data=dsnfile,
            recfm=recfm,
            lrecl=recl
        ))

        try:
            self.xmit['file'][filename]['COPYR1'] = self.iebcopy_record_1(dsnfile[0])
        except:
            self.xmit['file'][filename]['filetype'] = self.xmit['file'][filename]['mimetype']
            self.logger.debug("{} is not a PDS leaving".format(filename))
            return
        self.xmit['file'][filename]['filetype'] = "pds/directory"
        self.xmit['file'][filename]['extension'] = None

        self.xmit['file'][filename]['COPYR2'] = self.iebcopy_record_2(dsnfile[1])

        # Directory Info https://www.ibm.com/support/knowledgecenter/SSLTBW_2.3.0/com.ibm.zos.v2r3.idad400/pdsd.htm
        # dir_block_location = 2

        member_dir = b''
        count_dir_blocks = 2

        for blocks in dsnfile[count_dir_blocks:]:
            # End of PDS directory is 12 0x00
            # loop until there and store it
            member_dir += blocks
            count_dir_blocks += 1
            if self.__all_members(member_dir):
                break

        self.xmit['file'][filename]['members'] = self.__get_members_info(member_dir)

        # Now we have PDS directory information
        # Process the member data (which is everything until the end of the file)
        raw_data = b''.join(dsnfile[count_dir_blocks:])
        self.xmit['file'][filename] = self.__process_blocks(filename, raw_data)

    def parse_INMR01(self, inmr01_record):
        '''Parses INMR01 records

        INRM01 records always contain the following text units:

            * INMFTIME - date/time the XMI was created
            * INMLRECL - Record length for this XMI
            * INMFNODE - name of the originating system
            * INMTNODE - name of the target system
            * INMFUID - userid of the person who created the XMI
            * INMTUID - userid of the user this XMI is being sent to

        The following text units are optional:

            * INMFACK - notification receipt
            * INMFVERS - version number
            * INMNUMF - number of files
            * INMUSERP - user options

        XMI files on the mainframe are generated with the TSO TRANSMIT command,
        when an output file is not supplied the XMI file will be sent using
        network job entry (NJE) to the target userid and node. If an output
        file is provided the TRANSMIT command still puts a target user and
        target node in the XMI file.
        '''
        # INMR01 records are the XMIT header and contains information
        # about the XMIT file
        self.xmit['INMR01'] = self.__text_units(inmr01_record)
        if 'INMFTIME' in self.xmit['INMR01']:
            # Changing date format to '%Y%m%d%H%M%S%f'
            self.xmit['INMR01']['INMFTIME'] = self.xmit['INMR01']['INMFTIME'] + "0" * (20 - len(self.xmit['INMR01']['INMFTIME']))
            # Changing date format to isoformat
            INMFTIME = self.xmit['INMR01']['INMFTIME']
            isotime = datetime.datetime.strptime(INMFTIME, '%Y%m%d%H%M%S%f').isoformat(timespec='microseconds')
            self.xmit['INMR01']['INMFTIME'] = isotime

    def parse_INMR02(self, inmr02_record):
        '''Parses INRM02 control records.

        An XMI file may contain multiple INMR02 control records. These records
        always contains the following text units:

        * INMDSORG - dataset organization
        * INMLRECL - Record length
        * INMSIZE - size in bytes
        * INMUTILN - Utility program

        Optional text units are:

        * INMDSNAM - dataset name (messages do not have this text unit)
        * INMCREAT - the date the file was created

        There are multiple other optional text units which can be read here:
        https://www.ibm.com/support/knowledgecenter/en/SSLTBW_2.3.0/com.ibm.zos.v2r3.ikjb400/inmr02.htm

        The utility program defines how the file was generated and it can be
        INMCOPY, IEBCOPY, and AMSCIPHR:

        * INMCOPY - converts a sequential dataset (file) for XMI
        * IEBCOPY - converts a partitioned dataset (folder) for XMI
        * AMSCIPHR - encrypts the files in XMI, this library does not
          support extracting encrypted files.

        Depending on the dataset type the XMI may contain multiple records. The
        process is:

        * If the dataset is sequential - INMCOPY
        * If it is a partitioned dataset - IEBCOPY -> INMCOPY

        Therefore, partitioned datasets will have two INMR02 records.
        '''
        self.INMR02_count += 1
        numfiles = struct.unpack('>L', inmr02_record[0:4])[0]
        if 'INMR02' not in self.xmit:
            self.xmit['INMR02'] = {}
        self.xmit['INMR02'][self.INMR02_count] = self.__text_units(inmr02_record[4:])
        self.xmit['INMR02'][self.INMR02_count]['INMDSORG'] = self.get_dsorg(self.xmit['INMR02'][self.INMR02_count]['INMDSORG'])
        self.xmit['INMR02'][self.INMR02_count]['INMRECFM'] = self.get_recfm(self.xmit['INMR02'][self.INMR02_count]['INMRECFM'])
        self.xmit['INMR02'][self.INMR02_count]['numfile'] = numfiles

    def parse_INMR03(self, inmr03_record):
        '''Parses INMR03 records

        Defines the file format and contains the following text units:

        * INMDSORG - dataset organization
        * INMLRECL - dataset record length
        * INMRECFM - dataset record format
        * INMSIZE - size of the dataset in bytes
        '''
        self.INMR03_count += 1
        if 'INMR03' not in self.xmit:
            self.xmit['INMR03'] = {}
        self.xmit['INMR03'][self.INMR03_count] = self.__text_units(inmr03_record)
        self.xmit['INMR03'][self.INMR03_count]['INMDSORG'] = self.get_dsorg(self.xmit['INMR03'][self.INMR03_count]['INMDSORG'])
        self.xmit['INMR03'][self.INMR03_count]['INMRECFM'] = self.get_recfm(self.xmit['INMR03'][self.INMR03_count]['INMRECFM'])

    def parse_INMR04(self, inmr04_record):
        '''Print debug message for INMR04 records.

        INMR04 records are used to pass data to instalation specific exits
        (i.e. APIs), this function is provided if needed to be overloaded.
        '''
        self.logger.debug("[INMR04]: {}".format(inmr04_record.decode(self.ebcdic)))
        return

    # Virtual Tape Files

    def parse_tape(self):
        '''
        Parses a virtual tape file (AWS or HET) collecting metadata and files
        and stores them in the object.

        Virtual tapes are broken down as follows:

        - Header (3 bytes)

            - Current block size (little endian)
            - Previous block size (little endian)
            - Flag:

                - 0x2000 ENDREC - End of record
                - 0x4000 EOF - tape mark
                - 0x8000 NEWREC - Start of new record
                - HET file flags can also contain compression flags:

                    - 0x02 BZIP2 compression
                    - 0x01 ZLIB compression

        - Labels (optional):

            - VOL1 (80 bytes)

                - Volume serial number
                - Tape owner
                - More information: https://www.ibm.com/support/knowledgecenter/en/SSLTBW_2.1.0/com.ibm.zos.v2r1.idam300/formds1.htm

            - HDR1 (80 bytes):

                - Dataset name
                - Dataset serial number
                - Volume sequence number
                - Dataset sequence number
                - Generation number
                - Version number
                - Created date
                - Expiration date
                - System code (i.e. what OS version)
                - More information: https://www.ibm.com/support/knowledgecenter/en/SSLTBW_2.1.0/com.ibm.zos.v2r1.idam300/formds2.htm

            - HDR2 (80 bytes)

                - Record format
                - Block length
                - Tape density
                - Position
                - Job name and step used to copy files to this tape
                - Tape recording technique
                - Control character, used for printing
                - Block attribute
                - Device serial number
                - Security flag
                - Large block length
                - More information: https://www.ibm.com/support/knowledgecenter/en/SSLTBW_2.1.0/com.ibm.zos.v2r1.idam300/dshead.htm

            - UHL1 - UHL8: (80 bytes):

                - Contains user headers 76 bytes long
                - More info here: https://www.ibm.com/support/knowledgecenter/en/SSLTBW_2.1.0/com.ibm.zos.v2r1.idam300/m3208.htm
        '''

        self.logger.debug("Parsing virtual tape file")
        self.logger.debug("Using LRECL: {}".format(self.manual_recordlength))
        magi = magic.Magic(mime_encoding=True, mime=True)

        if not self.tape_object:
            self.read_tape_file()

        self.tape = {}
        self.tape['file'] = {}
        UHL = []
        loc = 0
        tape_file = b''
        tape_text = ''
        current_record = ''
        file_num = 1
        eof_marker = eor_marker = False
        HDR1 = HDR2 = volume_label = {}

        while loc < len(self.tape_object):
            # tape header
            # Header:
            # blocksize little endian 2 bytes
            # prev blocksize little endian 2 bytes
            # Flags(2 bytes)
            #   0x2000 ENDREC End of record
            #   0x4000 EOF    tape mark
            #   0x8000 NEWREC Start of new record
            #   HET File:
            #     0x02 BZIP2 compression
            #     0x01 ZLIB compression
            # Labels:
            # https://www.ibm.com/support/knowledgecenter/en/SSLTBW_2.1.0/com.ibm.zos.v2r1.idam300/formds1.htm

            cur_blocksize = self.__get_int(self.tape_object[loc:loc+2], 'little')
            self.logger.debug("Current Blocksize: {b} ({b:#06x})".format(b=cur_blocksize))
            prev_blocksize = self.__get_int(self.tape_object[loc+2:loc+4], 'little')
            self.logger.debug("Previous Blocksize: {b} ({b:#06x})".format(b=prev_blocksize))
            flags = self.__get_int(self.tape_object[loc+4:loc+6])
            self.logger.debug("Flags bytes: {b} ({b:#06x})".format(b=flags))

            if 0x4000 == (flags & 0x4000):
                eof_marker = True

            if 0x2000 == (flags & 0x2000):
                eor_marker = True

            if 0x8000 == (flags & 0x8000):
                eof_marker = False
            if flags == 0:
                self.logger.debug("Flag is 0x00. Some tapes have extra zeroes on end")
                break

            if (
                0x8000 != (flags & 0x8000)
                and 0x4000 != (flags & 0x4000)
                and 0x2000 != (flags & 0x2000)
            ):
                raise Exception('Header flag {:#06x} unrecognized'.format(self.__get_int(self.tape_object[loc+4:loc+6])))

            if 0x0200 == (flags & 0x0200):
                # BZLIB Compression
                self.logger.debug("Record compresed with BZLIB")
                tape_file += bz2.decompress(self.tape_object[loc + 6:loc + cur_blocksize + 6])
                current_record = bz2.decompress(self.tape_object[loc + 6:loc + cur_blocksize + 6])
            elif 0x0100 == (flags & 0x0100):
                self.logger.debug("Record compresed with zLIB")
                tape_file += zlib.decompress(self.tape_object[loc + 6:loc + cur_blocksize + 6])
                current_record = zlib.decompress(self.tape_object[loc + 6:loc + cur_blocksize + 6])
            else:
                tape_file += self.tape_object[loc + 6:loc + cur_blocksize + 6]
                current_record = self.tape_object[loc + 6:loc + cur_blocksize + 6]

            if not volume_label and tape_file[:4].decode(self.ebcdic) == 'VOL1':
                volume_label = {
                    'volser'   : tape_file[4:10].decode(self.ebcdic),
                    'owner'   : tape_file[41:51].decode(self.ebcdic),
                    # 'label_id' : tape_file[:4].decode(self.ebcdic),
                }
            if (
                current_record[:4].decode(self.ebcdic) == 'HDR1'
                and len(current_record) == 80
            ):
                t = current_record.decode(self.ebcdic)

                HDR1 = {
                    # 'label_num' : self.make_int(t[3]),
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
            if (
                current_record[:4].decode(self.ebcdic) == 'HDR2'
                and len(current_record) == 80
            ):
                t = current_record.decode(self.ebcdic)
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

            if current_record[:3].decode(self.ebcdic) == 'UHL':
                UHL.append(current_record.decode(self.ebcdic))

            self.logger.debug("Location: {} Blocksize: {} Prev Blocksize: {} EoF: {} EoR: {} Flags: {:#06x} File Size: {}".format(loc, cur_blocksize, prev_blocksize, eof_marker, eor_marker, flags, len(tape_file)))

            if eof_marker:
                if tape_file[:4].decode(self.ebcdic) in ['VOL1', 'HDR1', 'HDR2', 'EOF1', 'EOF2']:
                    self.logger.debug('Skipping VOL/HDR/EOF records type: {}'.format(tape_file[:4].decode(self.ebcdic)))
                    tape_file = b''
                    continue

                # if 'recfm' in HDR2 and 'V' in HDR2['recfm']:
                #     vb_tape_file = self.handle_vb(tape_file)
                #     tape_file = b''.join(vb_tape_file)

                filetype, datatype = magi.from_buffer(tape_file).split('; ')
                datatype = datatype.split("=")[1]
                extention = mimetypes.guess_extension(filetype)
                # eof_marker = False

                if not extention:
                    extention = "." + filetype.split("/")[1]

                # File magic cant detec XMIT files
                if ( filetype == 'application/octet-stream'
                   and len(tape_file) >= 8
                   and tape_file[2:8].decode(self.ebcdic) == 'INMR01'):
                    extention = ".xmi"
                    filetype = 'application/xmit'

                if self.force:
                    extention = ".txt"

                if filetype == 'text/plain' or datatype != 'binary' or self.force:

                    if 'lrecl' in HDR2:
                        if 'F' in HDR2['recfm']:
                            tape_text = self.convert_text_file(tape_file, HDR2['lrecl'])
                        # elif 'V' in HDR2['recfm']:
                        #     for record in vb_tape_file:
                        #         tape_text += self.convert_text_file(record, len(record)).rstrip() + '\n'
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
                        if self.__is_jcl(tape_text):
                            output['extension'] = ".jcl"
                        if self.__is_rexx(tape_text):
                            output['extension'] = ".rexx"

                    if HDR1:
                        output['HDR1'] = HDR1
                        msg = 'HDR1:'
                        for key in HDR1:
                            msg += " {}: {}".format(key, HDR1[key])
                        self.logger.debug(msg)
                    if HDR2:
                        output['HDR2'] = HDR2
                        msg = 'HDR2:'
                        for key in HDR2:
                            msg += " {}: {}".format(key, HDR2[key])
                        self.logger.debug(msg)

                    if UHL:
                        print("!! FOUND UHL !!" * 500)
                        output['UHL'] = UHL
                        for i in UHL:
                            self.logger.debug("User Label: {}".format(i))

                    if 'dsn' in HDR1:
                        self.tape['file'][HDR1['dsn']] = copy.deepcopy(output)
                    else:
                        self.tape['file']['FILE{:>04d}'.format(file_num)] = copy.deepcopy(output)

                    file_num += 1
                    HDR1 = {}
                    HDR2 = {}
                    output = {}
                    UHL = []
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
        '''Extracts files from a parsed AWS/HET file and stores them in the XMIT object'''

        # Partitioned datasets are broken up as follows:
        #   * COPYR1 record
        #   * COPYR2 record
        #   * Member metadata (filnames, file stats, etc)
        #   * File data itself
        # PDSEs are followed by other fields, PDSE support is tenious as best
        # barring a complete rewrite

        for filename in self.tape['file']:
            self.logger.debug('Processing Dataset: {}'.format(filename))

            if 'data' not in self.tape['file'][filename]:
                self.logger.debug("Skipping empty tape")
                continue

            dataset = self.tape['file'][filename]['data']
            copyr1_size = self.__get_int(dataset[:2])
            try:
                self.tape['file'][filename]['COPYR1'] = self.iebcopy_record_1(dataset[:copyr1_size])
                self.logger.debug("Size of COPYR1 Field: {}".format(copyr1_size))
            except:
                self.logger.debug("{} is not a PDS leaving".format(filename))
                continue

            self.tape['file'][filename]['filetype'] = "pds/directory"
            self.tape['file'][filename]['extension'] = None
            copyr2_size = self.__get_int(dataset[copyr1_size:copyr1_size + 2])
            self.logger.debug("Size of COPYR2 Field: {}".format(copyr2_size))

            self.tape['file'][filename]['COPYR2'] = self.iebcopy_record_2(dataset[copyr1_size + 8:copyr1_size + copyr2_size])

            loc = 0
            dataset = dataset[copyr1_size + copyr2_size:]
            member_dir = b''
            while loc < len(dataset):
                block_size = self.__get_int(dataset[loc:loc + 2])
                seg_size = self.__get_int(dataset[loc + 4:loc + 6])
                self.logger.debug("BDW Size: {} SDW Size: {}".format(block_size, seg_size))
                member_dir += dataset[loc + 8:loc + block_size]  # skip BDW and SDW
                loc += block_size
                if self.__all_members(member_dir):
                    break
            self.tape['file'][filename]['members'] = self.__get_members_info(member_dir)
            # Now getting member blocks
            dataset = dataset[loc:]
            loc = 0
            member_files = b''

            while loc < len(dataset):
                # loop until we get to the end of the PDS
                block_size = self.__get_int(dataset[loc:loc + 2])
                seg_size = self.__get_int(dataset[loc + 4:loc + 6])
                self.logger.debug("BDW Size: {} SDW Size: {}".format(block_size, seg_size))
                member_files += dataset[loc + 8:loc + block_size]  # skip BDW and SDW
                loc += block_size
                if member_files[-12:] == b'\x00' * 12:
                    break
            self.logger.debug('Processing PDS: {}'.format(filename))
            self.tape['file'][filename] = self.__process_blocks(filename, member_files)

    def get_tape_date(self, tape_date):
        '''Converts tape label date to ISO format string with microseconds.'''
        self.logger.debug("changing date {}".format(tape_date))
        #   c = century (blank implies 19)
        #  yy = year (00-99)
        # ddd = day (001-366)
        if tape_date[0] == ' ':
            tape_date = '19' + tape_date[1:]
        else:
            tape_date = str(20 + int(tape_date[0])) + tape_date[1:]
            # strfmt is %Y%j
        if tape_date[-1] == '0':
            tape_date = tape_date[:-1] + "1"
        d = datetime.datetime.strptime(tape_date, '%Y%j')
        return d.isoformat(timespec='microseconds')

    # Dataset/partitioned dataset functions

    # iebcopy_record_1: parses COPYR1
    # iebcopy_record_2: parses COPYR2
    # __all_members: Checks if all members have been processed in a directory
    # __get_members_info: gets member stats and info for all members in a directory
    # __process_blocks: process the directory blocks in a PDS
    # handle_vb: Deals with variable record lengths
    # __text_units: Process IBM text units and return info

    def iebcopy_record_1(self, first_record):
        '''Returns a dict containing IEBCOPY COPYR1 metatdata

        More information available here:
        https://www.ibm.com/support/knowledgecenter/SSLTBW_2.2.0/com.ibm.zos.v2r2.idau100/u1322.htm
        '''
        self.logger.debug("IEBCOPY First Record Atributes (COPYR1)")
        # PDS i.e. IEBCOPY
        if self.__get_int(first_record[1:4]) != 0xCA6D0F and self.__get_int(first_record[9:12]) != 0xCA6D0F:
            self.logger.debug("COPYR1 header eyecatcher 0xCA6D0F not found")
            raise Exception("COPYR1 header eyecatcher 0xCA6D0F not found")
        if len(first_record) > 64:
            self.logger.debug("COPYR1 Length {} longer than 64 records".format(len(first_record)))
            raise Exception("COPYR1 Length {} longer than 64 records".format(len(first_record)))

        COPYR1 = {}
        COPYR1['type'] = 'PDS'

        if self.__get_int(first_record[1:4]) != 0xCA6D0F:  # XMIT files omit the first 8 bytes?
            COPYR1['block_length'] = self.__get_int(first_record[0:2])
            COPYR1['seg_length'] = self.__get_int(first_record[4:6])
            first_record = first_record[8:]

        if first_record[0] & 0x01:
            COPYR1['type'] = 'PDSE'

        # Record 1
        # https://www.ibm.com/support/knowledgecenter/SSLTBW_2.2.0/com.ibm.zos.v2r2.idau100/u1322.htm#u1322__nt2

        COPYR1['DS1DSORG'] = self.__get_int(first_record[4:6])
        COPYR1['DS1BLKL'] = self.__get_int(first_record[6:8])
        COPYR1['DS1LRECL'] = self.__get_int(first_record[8:10])
        COPYR1['DS1RECFM'] = self.get_recfm(first_record[10:12])
        COPYR1['DS1KEYL'] = first_record[11]
        COPYR1['DS1OPTCD'] = first_record[12]
        COPYR1['DS1SMSFG'] = first_record[13]
        COPYR1['file_tape_blocksize'] = self.__get_int(first_record[14:16])
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
        COPYR1['DVAOPTS'] = self.__get_int(first_record[16:18])
        COPYR1['DVACLASS'] = first_record[18]
        COPYR1['DVAUNIT'] = first_record[19]
        COPYR1['DVAMAXRC'] = self.__get_int(first_record[20:24])
        COPYR1['DVACYL'] = self.__get_int(first_record[24:26])
        COPYR1['DVATRK'] = self.__get_int(first_record[26:28])
        COPYR1['DVATRKLN'] = self.__get_int(first_record[28:30])
        COPYR1['DVAOVHD'] = self.__get_int(first_record[30:32])
        COPYR1['num_header_records'] = self.__get_int(first_record[36:38])

        if first_record[38:] != (b'\x00'*18):
            # reserved = first_record[38]
            COPYR1['DS1REFD'] = first_record[39:42]
            COPYR1['DS1SCEXT'] = first_record[42:45]
            COPYR1['DS1SCALO'] = first_record[45:49]
            COPYR1['DS1LSTAR'] = first_record[49:52]
            COPYR1['DS1TRBAL'] = first_record[52:54]
            # reserved = first_record[54:]
            COPYR1['DS1REFD'] = "{:02d}{:04d}".format(
                COPYR1['DS1REFD'][0] % 100, self.__get_int(COPYR1['DS1REFD'][1:]))

        self.logger.debug("Record Size: {}".format(len(first_record)))
        for i in COPYR1:
            self.logger.debug("{:<19} : {}".format(i, COPYR1[i]))
        return COPYR1

    def iebcopy_record_2(self, second_record):
        '''Returns a dict containing IEBCOPY COPYR2 metatdata

        More information available here:
        https://www.ibm.com/support/knowledgecenter/SSLTBW_2.2.0/com.ibm.zos.v2r2.idau100/u1322.htm
        '''

        self.logger.debug("IEBCOPY Second Record Atributes (COPYR2)")
        if len(second_record) > 276:
            self.logger.debug("COPYR2 Length {} longer than 276 records".format(len(second_record)))
            raise Exception("COPYR2 Length {} longer than 276 records".format(len(second_record)))

        deb = second_record[0:16]  # Last 16 bytes of basic section of the Data Extent Block (DEB) for the original data set.
        deb_extents = []
        for i in range(0, 256, 16):
            deb_extents.append(second_record[i:i + 16])
        # reserved = second_record[272:276]  # Must be zero

        # https://www.ibm.com/support/knowledgecenter/en/SSLTBW_2.1.0/com.ibm.zos.v2r1.idas300/debfiel.htm#debfiel
        self.logger.debug("DEB: {:#040x}".format( self.__get_int(deb)))
        deb_mask = deb[0]  # DEBDVMOD
        deb_ucb = self.__get_int(deb[1:4])  # DEBUCBA
        # DEBDVMOD31 = deb[4]  # DEBDVMOD31
        # DEBNMTRKHI = deb[5]
        deb_cylinder_start = self.__get_int(deb[6:8])  # DEBSTRCC
        deb_tracks_start = self.__get_int(deb[8:10])   # DEBSTRHH
        deb_cylinder_end = self.__get_int(deb[10:12])  # DEBENDCC
        deb_tracks_end = self.__get_int(deb[12:14])  # DEBENDHH
        deb_tracks_num = self.__get_int(deb[14:])  # DEBNMTRK

        self.logger.debug("Mask {:#04x} UCB: {:#06x} Start CC: {:#06x} Start Tracks: {:#06x} End CC: {:#06x} End Tracks: {:#06x} Num tracks: {:#06x} ".format(deb_mask, deb_ucb, deb_cylinder_start, deb_tracks_start, deb_cylinder_end, deb_tracks_end, deb_tracks_num))
        x = 1
        for i in deb_extents:
            self.logger.debug("DEB Extent {}: {:#040x}".format(x, self.__get_int(i)))
            x += 1
        return {'deb': deb, 'extents' : deb_extents}

    def __all_members(self, members):
        '''If all members in a pds have been processed returns True, otherwise
        False.
        '''
        self.logger.debug('Checking for last member found')
        block_loc = 0
        while block_loc < len(members):
            directory_len = self.__get_int(members[block_loc + 20:block_loc + 22]) - 2  # Length includes this halfword
            directory_members_info = members[block_loc + 22:block_loc + 22 + directory_len]
            loc = 0
            while loc < directory_len:
                if directory_members_info[loc:loc + 8] == b'\xff' * 8:
                    return True
                loc = loc + 8 + 3 + 1 + (directory_members_info[loc + 11] & 0x1F) * 2
            block_loc += 276
        return False

    def __get_members_info(self, directory):
        '''Returns a dict containing metadata and filenames for all members
        contained in a PDS'''
        self.logger.debug("Getting PDS Member information. Directory length: {}".format(len(directory)))
        members = {}

        block_loc = 0
        while block_loc < len(directory):
            # I've commented these out but left them in to describe PDS directory blocks
            # directory_zeroes = directory[block_loc:block_loc + 8]                        # In a PDSe this may be 08 00 00 00 00 00 00 00
            # directory_key_len = directory[block_loc + 8:block_loc + 10]                  # 0x0008
            # directory_data_len = self.__get_int(directory[block_loc + 10:block_loc + 12])  # 0x0100
            # last_referenced_member = directory[block_loc + 12:block_loc + 20]              # last referenced member
            directory_len = self.__get_int(directory[block_loc + 20:block_loc + 22]) - 2   # Length includes this halfword
            directory_members_info = directory[block_loc + 22:block_loc + 22 + directory_len]
            loc = 0
            while loc < directory_len:
                member_name = directory_members_info[loc:loc + 8].decode(self.ebcdic).rstrip()
                if directory_members_info[loc:loc + 8] == b'\xff' * 8:
                    self.logger.debug("End of Directory Blocks. Total members: {}".format(len(members)))
                    loc = len(directory)
                    break
                else:
                    members[member_name] = {
                        'ttr' : self.__get_int(directory_members_info[loc + 8:loc + 11]),
                        'alias' : True if 0x80 == (directory_members_info[loc + 11] & 0x80) else False,
                        'halfwords' : (directory_members_info[loc + 11] & 0x1F) * 2,
                        'notes' : (directory_members_info[loc + 11] & 0x60) >> 5
                    }
                    members[member_name]['parms'] = directory_members_info[loc + 12:loc + 12 + members[member_name]['halfwords']]

                    if len( members[member_name]['parms']) >= 30 and members[member_name]['notes'] == 0:  # ISPF Stats
                        # https://www.ibm.com/support/knowledgecenter/en/SSLTBW_2.1.0/com.ibm.zos.v2r1.f54mc00/ispmc28.htm
                        # ISPF statistics entry in a PDS directory
                        member_parms = members[member_name]['parms']
                        members[member_name]['ispf'] = {
                            'version' : "{:02}.{:02}".format(member_parms[0], member_parms[1]),
                            'flags' : member_parms[2],
                            'createdate' : self.ispf_date(member_parms[4:8]),
                            'modifydate' : self.ispf_date(member_parms[8:14], seconds=member_parms[3]),
                            'lines' : self.__get_int(member_parms[14:16]),
                            'newlines' : self.__get_int(member_parms[16:18]),
                            'modlines' : self.__get_int(member_parms[18:20]),
                            'user' : member_parms[20:28].decode(self.ebcdic).rstrip()
                        }
                        if 0x10 == (members[member_name]['ispf']['flags'] & 0x10):
                            members[member_name]['ispf']['lines'] = self.__get_int(member_parms[28:32])
                            members[member_name]['ispf']['newlines'] = self.__get_int(member_parms[32:36])
                            members[member_name]['ispf']['modlines'] = self.__get_int(member_parms[36:40])

                    else:
                        members[member_name]['ispf'] = False

                    loc = loc + 8 + 3 + 1 + members[member_name]['halfwords']
            block_loc += loc + 24
            if (block_loc % 276) > 0:  # block lengths must be 276
                block_loc = (276 * (block_loc // 276)) + 276

        member_info = ''
        # prints debug information about current member
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

    def __process_blocks(self, filename, member_blocks=b''):
        '''Processes partitioned dataset directory blocks, returns dict
        of members and file data/metadata.

        Args:
            * filename (str): partitioned dataset name
            * member_blocks (bytes): binary PDS file data

        '''

        self.logger.debug("Processing PDS Data Blocks")

        loc = 0
        ttr_location = 0
        member_data = b''
        vb_member_data = []
        deleted_num = 1
        prev_ttr = 0
        record_closed = False

        if self.has_xmi():
            lrecl = self.xmit['file'][filename]['COPYR1']['DS1LRECL']
            recfm = self.xmit['file'][filename]['COPYR1']['DS1RECFM']
            member_dict = self.xmit['file'][filename]
            PDS_or_PDSE = self.xmit['file'][filename]['COPYR1']['type']
        elif self.has_tape():
            lrecl = self.tape['file'][filename]['COPYR1']['DS1LRECL']
            recfm = self.tape['file'][filename]['COPYR1']['DS1RECFM']
            member_dict = self.tape['file'][filename]
            PDS_or_PDSE = self.tape['file'][filename]['COPYR1']['type']
        else:
            raise Exception("No XMI/AWS/HET file opened.")

        self.__fix_circular_alias(filename)
        ttrs = self.__member_ttrs(filename)
        self.logger.debug("Using LRECL: {} RECFM: {}".format(lrecl, recfm))

        # Sort the TTRs
        sorted_ttrs = []
        for i in sorted(ttrs.keys()) :
            sorted_ttrs.append(i)

        while loc < len(member_blocks):
            # Example headers from real XMI files
            # F  M  BB    CC    TT    R  KL DLen
            # 00 00 00 00 04 45 00 09 04 00 03 C0
            # 00 00 00 00 00 3E 00 05 0E 00 00 FB
            # 00 00 00 00 00 3E 00 05 12 00 1D 38
            member_data_len = self.__get_int(member_blocks[loc + 10:loc + 12])
            member_ttr = self.__get_int(member_blocks[loc + 6:loc + 9])

            if PDS_or_PDSE == 'PDSE' and record_closed:
                while True:
                    member_ttr = self.__get_int(member_blocks[loc + 6:loc + 9])
                    member_data_len = self.__get_int(member_blocks[loc + 10:loc + 12])
                    if member_ttr != prev_ttr:
                        break
                    loc += member_data_len + 12
                record_closed = False

            if member_ttr == 0 and member_data_len == 0:
                # skip empty entries
                loc += member_data_len + 12
                continue

            # member_flag = member_blocks[loc]
            member_extent = member_blocks[loc + 1]
            member_bin = member_blocks[loc + 2:loc + 4]
            member_cylinder = self.__get_int(member_blocks[loc + 4:loc + 6])
            member_key_len = member_blocks[loc + 9]

            if ttr_location + 1 > len(sorted_ttrs):

                self.logger.debug("Encoutered more files than members names: Total members: {} Current file: {}".format(len(ttrs), ttr_location+1))
                sorted_ttrs.append("??{}".format(deleted_num))
                ttrs["??{}".format(deleted_num)] = "DELETED??{}".format(deleted_num)
                member_dict['members'][ "DELETED??{}".format(deleted_num)] = { 'alias' : False}
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

            if 'V' in recfm:
                vb_member_data += self.handle_vb(member_blocks[loc + 12:loc + 12 + member_data_len])
                member_data = b''.join(vb_member_data)
            else:
                member_data += member_blocks[loc + 12:loc + 12 + member_data_len]

            if member_data_len == 0:

                if PDS_or_PDSE == 'PDSE':
                    record_closed = True

                member_dict['members'][member_name].update( self.__get_file_mimetype_and_convert(
                    file_name=member_name,
                    file_data=member_data,
                    vb_file_data=vb_member_data,
                    recfm=recfm, lrecl=lrecl
                ))

                member_data = b''
                vb_member_data = []
                # End of member
                ttr_location += 1
                prev_ttr = member_ttr

            loc += member_data_len + 12

        if len(member_data) > 0:
            # sometimes trailing records aren't followed by a zero
            member_dict['members'][member_name].update( self.__get_file_mimetype_and_convert(
                file_name=member_name,
                file_data=member_data,
                vb_file_data=vb_member_data,
                recfm=recfm, lrecl=lrecl
            ))

        return member_dict


    def __get_file_mimetype_and_convert(
                                        self,
                                        file_name,
                                        file_data,
                                        vb_file_data=None,
                                        recfm='F',
                                        lrecl=80
        ):
        '''Guesses file extension based on mimetype and converts to utf-8
        if file is plain/ebcdic

        Args:
            member_name (str): member name
            file_data (bytes, str or list): the file to get mimetype for
            vb_file_data (list): variable block data split in to records
            recfm (str): the record formart (used to convert variable length)
            lrecl (int): record length

        Returns a dict with:
            mimetype (str): file guessed mimetype from libmagic
            datatype (str): either binary, text, etc
            extension (str): file extention with period (i.e. ".txt")
            data (byte): binary file data
            text (str): Is included when force_text is enabled or when the file
            mimetype is determined to be a plain/text file.
        '''

        magi = magic.Magic(mime_encoding=True, mime=True)
        mime_dict = {}

        filetype, datatype = magi.from_buffer(file_data).split('; ')
        datatype = datatype.split("=")[1]
        extention = mimetypes.guess_extension(filetype)

        if not extention:
            extention = "." + filetype.split("/")[1]

        if self.force:
            extention = ".txt"

        # File magic cant detect XMIT files (yet :D)
        if ( filetype == 'application/octet-stream'
            and len(file_data) >= 8
            and file_data[2:8].decode(self.ebcdic) == 'INMR01'):
            extention = ".xmi"
            filetype = 'application/xmit'

        if filetype == 'text/plain' or datatype != 'binary' or self.force:

            if 'V' in recfm:
                vb_member_text = ''
                for record in vb_file_data:
                    vb_member_text += self.convert_text_file(record, len(record)).rstrip() + '\n'
                mime_dict['text'] = vb_member_text

            elif 'F' in recfm:
                mime_dict['text'] = self.convert_text_file(file_data, lrecl)
            elif 'U' in recfm:
                mime_dict['text'] = self.convert_text_file(file_data, self.manual_recordlength)
            else:
                mime_dict['text'] = self.convert_text_file(file_data, lrecl)

            if self.__is_jcl(
                mime_dict['text']
            ):
                extention = '.jcl'

            elif self.__is_rexx(
                mime_dict['text']
            ):
                extention = '.rexx'

        self.logger.debug("File name: {} Mime Type: {} Datatype: {} File ext: {} Size: {}".format(file_name, filetype, datatype, extention, len(file_data)))
        mime_dict['mimetype'] = filetype
        mime_dict['datatype'] = datatype
        mime_dict['extension'] = extention
        mime_dict['data'] = file_data

        return mime_dict

    def __is_jcl(self, text_lines=''):
        '''Returns true if the first line starts with ``//`` and contains
        ``JOB``'''
        job_card = text_lines.splitlines()[0].split()
        if (
            len(job_card) > 1
            and job_card[0].startswith("//")
            and job_card[1] == 'JOB'
        ):
            return True
        return False


    def __is_rexx(self, text_lines=''):
        '''Returns true if the first line starts with ``/*`` and contains
        ``REXX``'''
        maybe_rexx = text_lines.splitlines()[0].lstrip()
        if (
            maybe_rexx.startswith("/*")
            and "REXX" in maybe_rexx.upper()
        ):
            return True
        return False

    def __fix_circular_alias(self, pds):
        '''Some XMI files have circular aliases, this function updates
        the XMIT object to fix them.
        '''

        # Technically they arent circular, TTRs are a pointer to the member
        # data kinda like inodes. But are labelled ALIAS so data isn't replicated
        # similar to hard links. But the library needs at least one fixed
        # member so here we are.

        self.logger.debug("Checking for circular aliases")
        ttrs = self.__member_ttrs(pds)
        for m in self.get_members(pds):
            ttr = self.__get_ttr(pds, m)
            if self.is_alias(pds, m):
                if ttr not in ttrs:
                    self.logger.debug("Found circular reference: {}. Fixed.".format(m))
                    ttrs[ttr] = m
                    if self.has_xmi():
                        self.xmit['file'][pds]['members'][m]['alias'] = False
                    else:
                        self.tape['file'][pds]['members'][m]['alias'] = False

    def __member_ttrs(self, pds):
        '''Returns a dict of TTRs to members.'''
        ttrs = {}
        for m in self.get_members(pds):
            ttr = self.__get_ttr(pds, m)
            if not self.is_alias(pds, m):
                ttrs[ttr] = m
        return ttrs

    def __get_ttr(self, pds, member):
        '''Returns the TTR for a given pds and member.'''
        if self.has_xmi():
            return self.xmit['file'][pds]['members'][member]['ttr']
        else:
            return self.tape['file'][pds]['members'][member]['ttr']

    def handle_vb(self, vbdata):
        self.logger.debug("Processing Variable record format")
        # the first 4 bytes are bdw
        loc = 4
        data = []
        lrecl = 10
        while loc < len(vbdata) and lrecl > 0:
            lrecl = self.__get_int(vbdata[loc:loc + 2])
            data.append(vbdata[loc + 4:loc + lrecl])
            loc += lrecl
        return data

    def __text_units(self, text_records):
        '''Parses IBM text units from XMI control records, returns a dict
        with text unit name and value.

        Text units in INMR## records are broken down like this:

            * First two bytes are the 'key'/type
            * Second two bytes are how many text unit records there are
            * Then records are broken down by size (two bytes) and the data
            * Data can be string, int or hex
        '''

        loc = 0
        tu = {}
        INMDSNAM = ''
        debug = (
            "Key: {k:#06x}, Mnemonic: '{n}', Type: '{t}', Description: '{d}',"
            " Text Unit number: {tun}, length: {l}, Value: '{v}'")
        self.logger.debug("Total record Length: {}".format(len(text_records)))

        while loc < len(text_records):

            key = struct.unpack('>H', text_records[loc:loc + 2])[0]
            num = struct.unpack('>H', text_records[loc + 2:loc + 4])[0]

            if key == 0x1026 and num == 0:
                # this record can be empty so we skip it
                loc = loc + 4

            if key == 0x0028 and num == 0:
                # this record can be empty so we skip it
                self.logger.debug('This is a message')
                self.msg = True
                loc += 4

            for i in range(0, num):
                if i == 0:
                    tlen = self.__get_int(text_records[loc + 4:loc + 6])
                    item = text_records[loc + 6:loc + 6 + tlen]
                else:
                    tlen = self.__get_int(text_records[loc:loc + 2])
                    item = text_records[loc + 2:loc + 2 + tlen]

                if key in IBM_text_units:
                    if IBM_text_units[key]['type'] == 'character':

                        value = item.decode(self.ebcdic)
                        if IBM_text_units[key]['name'] == 'INMDSNAM':
                            INMDSNAM += item.decode(self.ebcdic) + "."
                    elif IBM_text_units[key]['type'] == 'decimal':
                        value = self.__get_int(item)
                        # self.logger.debug("Decimal Unit value: {}".format(value))
                    else:
                        # self.logger.debug("Hex value: {}".format(hex(self.__get_int(item))))
                        value = item

                        if IBM_text_units[key]['name'] == 'INMTYPE':
                            value = self.__get_int(value)
                            if value == 0x80:
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

                    tu[IBM_text_units[key]['name']] = value

                    self.logger.debug(debug.format(
                        k=key,
                        n=IBM_text_units[key]['name'],
                        t=IBM_text_units[key]['type'],
                        d=IBM_text_units[key]['desc'],
                        tun=num,
                        l=tlen,
                        v=value))

                if i == 0:
                    loc += 6 + tlen
                else:
                    loc += 2 + tlen
        self.logger.debug("Final Loc: {}".format(loc))
        return tu


    # ------------------------------------------------------------------ #
    #  XMI CREATION METHODS                                               #
    # ------------------------------------------------------------------ #

    def build_xmi(
            self,
            input_path,
            dsn=None,
            lrecl=80,
            recfm='FB',
            from_user='PYTHON',
            from_node='LOCAL',
            to_user='PYTHON',
            to_node='LOCAL',
            message=None,
            message_file=None,
            message_format='80x32',
            resolved_msg=None,
    ):
        '''Build and return raw XMI (NETDATA) bytes from *input_path*.

        If *input_path* is a file  → sequential dataset XMI.
        If *input_path* is a folder → PDS XMI whose members are the files.

        Args:
            input_path (str): File or directory to package.
            dsn (str): Dataset name in XMI metadata (default: uppercased stem).
            lrecl (int): Logical record length (default: 80).
            recfm (str): Record format — ``'FB'``, ``'F'``, ``'VB'``, ``'V'``, ``'U'``.
            from_user (str): Originating user ID (max 8 chars).
            from_node (str): Originating node name (max 8 chars).
            to_user (str): Destination user ID (max 8 chars).
            to_node (str): Destination node name (max 8 chars).
            message (str): Inline message; ``\\n`` for line breaks.
            message_file (str): Path to UTF-8 message file (takes precedence).
            message_format (str): ``'80x32'`` (default) or ``'132x27'``.
            resolved_msg: Pre-resolved ``(text, lrecl, max_lines)`` tuple;
                pass from ``create_xmi`` to avoid double resolution.

        Returns:
            bytes: Raw XMI file bytes.
        '''
        if resolved_msg is None:
            resolved_msg = resolve_message(message, message_file, message_format)

        p = Path(input_path)
        if not p.exists():
            raise FileNotFoundError("Input path not found: {}".format(input_path))

        if p.is_dir():
            return self._build_pds_xmi(p, dsn=dsn, lrecl=lrecl, recfm=recfm,
                                       from_user=from_user, from_node=from_node,
                                       to_user=to_user, to_node=to_node,
                                       resolved_msg=resolved_msg)
        else:
            return self._build_seq_xmi(p, dsn=dsn, lrecl=lrecl, recfm=recfm,
                                       from_user=from_user, from_node=from_node,
                                       to_user=to_user, to_node=to_node,
                                       resolved_msg=resolved_msg)

    # -- Low-level segment / text-unit builders ------------------------- #

    def _xmi_seg(self, flag, data):
        '''Return one raw XMI segment: length(1) + flag(1) + data(<=253).'''
        length = len(data) + 2
        return bytes([length, flag]) + data

    def _xmi_ctrl_seg(self, record_type, text_unit_bytes, numfiles=0):
        '''Wrap *record_type* + *text_unit_bytes* into control segments.

        Control records use flag 0xE0 for each segment.  The record type
        (e.g. 'INMR01') is placed at the start of the first segment,
        EBCDIC-encoded to exactly 6 characters.

        INMR02 records carry a 4-byte numfiles count between the type name
        and the text units (pass numfiles= to set it).

        Each segment payload is at most 253 bytes.
        '''
        rt_encoded = record_type.upper()[:6].ljust(6).encode(self.ebcdic)
        prefix = struct.pack('>I', numfiles) if numfiles or record_type.upper().startswith('INMR02') else b''
        payload = rt_encoded + prefix + text_unit_bytes
        out = bytearray()
        while payload:
            chunk = payload[:253]
            payload = payload[253:]
            out += self._xmi_seg(0xE0, chunk)
        return bytes(out)

    def _xmi_data_record(self, data):
        '''Segment *data* into XMI data segments with proper flags.

        Flags: single = 0xC0, first = 0x80, middle = 0x00, last = 0x40.
        '''
        out = bytearray()
        chunks = [data[i:i + 253] for i in range(0, len(data), 253)]
        if not chunks:
            chunks = [b'']
        if len(chunks) == 1:
            out += self._xmi_seg(0xC0, chunks[0])
        else:
            out += self._xmi_seg(0x80, chunks[0])
            for chunk in chunks[1:-1]:
                out += self._xmi_seg(0x00, chunk)
            out += self._xmi_seg(0x40, chunks[-1])
        return bytes(out)

    def _xmi_tu(self, key, value_bytes):
        '''Build one IBM text unit: key(2) + count(2) + len(2) + value.'''
        return (struct.pack('>HH', key, 1) +
                struct.pack('>H', len(value_bytes)) +
                value_bytes)

    def _xmi_dsn_tu(self, dsn):
        '''Build an INMDSNAM text unit (0x0002) for a dotted dataset name.

        Each qualifier is a separate text-unit item.
        '''
        parts = [p.upper().encode(self.ebcdic) for p in dsn.split('.')]
        data = struct.pack('>HH', 0x0002, len(parts))
        for i, part in enumerate(parts):
            data += struct.pack('>H', len(part)) + part
        return data

    # -- ISPF statistics helpers ---------------------------------------- #

    def _xmi_ispf_date(self, dt, with_time=False):
        '''Encode a Python datetime as ISPF packed-BCD date bytes.

        4-byte form (creation date):  century | year-BCD | day2-BCD | day3+sign
        6-byte form (modify date+time): same 4 bytes + hour-BCD | minute-BCD

        The encoding mirrors what ispf_date() decodes:
          century byte  = year // 100 - 19   (0 = 1900s, 1 = 2000s)
          year byte     = year % 100 as packed BCD  (2026 → 0x26)
          day bytes 2-3 = 3-digit day-of-year in packed BCD across 1.5 bytes
                          high nibble of byte3 = third digit,
                          low nibble of byte3  = 0xF (packed decimal sign)
                          e.g. day 067: byte2=0x06, byte3=0x7F
        '''
        century = dt.year // 100 - 19
        year_bcd = int('{:02d}'.format(dt.year % 100), 16)
        doy = dt.timetuple().tm_yday
        doy_str = '{:03d}'.format(doy)
        day_byte2 = int(doy_str[0:2], 16)
        day_byte3 = (int(doy_str[2], 16) << 4) | 0x0F  # 0xF = packed decimal sign
        result = bytes([century, year_bcd, day_byte2, day_byte3])
        if with_time:
            hour_bcd = int('{:02d}'.format(dt.hour), 16)
            min_bcd = int('{:02d}'.format(dt.minute), 16)
            result += bytes([hour_bcd, min_bcd])
        return result

    def _xmi_ispf_stats(self, userid, num_lines, dt=None):
        '''Build 30-byte ISPF statistics user data for a PDS directory entry.

        Offsets (from IBM doc):
          0   VV  version (0x01)
          1   MM  modification level (0x00)
          2   flags byte (0x00)
          3   seconds of last modification (hex)
          4   creation date  (4 bytes, packed BCD)
          8   modification date+time (6 bytes, packed BCD)
          14  current line count (uint16)
          16  lines added  (uint16)
          18  lines modified (uint16)
          20  user ID (8 bytes EBCDIC, space-padded)
        Total = 30 bytes (15 halfwords → C-byte lower 5 bits = 0x0F)
        '''
        import datetime as _dt
        if dt is None:
            dt = _dt.datetime.now()
        lines = min(num_lines, 65535)
        uid = userid.upper()[:8].encode(self.ebcdic).ljust(8, b'\x40')
        sec_bcd = int('{:02d}'.format(dt.second), 16)
        return (
            bytes([0x01, 0x00, 0x00, sec_bcd]) +           # VV MM flags seconds  (4)
            self._xmi_ispf_date(dt, with_time=False) +    # creation date         (4)
            self._xmi_ispf_date(dt, with_time=True) +     # modify date+time      (6)
            struct.pack('>HHH', lines, lines, 0) +         # lines, added, mod     (6)
            uid +                                          # userid                (8)
            b'\x40\x40'                                    # reserved (EBCDIC sp)  (2)
        )  # total = 30 bytes (15 halfwords)

    # -- RECFM / blocksize helpers -------------------------------------- #

    def _xmi_recfm_byte(self, recfm):
        '''Return the 2-byte RECFM field from a string like "FB", "VB".'''
        recfm = recfm.upper()
        first = {'F': 0x80, 'V': 0x40, 'U': 0xC0}.get(recfm[0], 0x80)
        if 'B' in recfm:
            first |= 0x10
        return bytes([first, 0x00])

    def _xmi_blksize(self, lrecl, recfm):
        '''Calculate a sensible blocksize for IEBCOPY.'''
        recfm = recfm.upper()
        if recfm.startswith('U'):
            return 6233  # standard safe max block size for RECFM=U
        if recfm.startswith('V'):
            # Use enough blocks to fill ~6144 bytes
            max_rec = lrecl + 4
            n = max(1, 6144 // max_rec)
            return n * max_rec
        # FB/FS: ~3200 bytes aligned to lrecl
        n = max(1, 3200 // lrecl)
        return n * lrecl

    # -- Text-to-EBCDIC encoder ---------------------------------------- #

    def _xmi_text_to_ebcdic(self, text_bytes, lrecl):
        '''Split UTF-8 *text_bytes* into fixed-length EBCDIC records.

        Each record is padded / truncated to *lrecl* bytes.
        Returns raw bytes (concatenated fixed-length records).
        '''
        space = b'\x40'  # EBCDIC space
        lines = text_bytes.replace(b'\r\n', b'\n').replace(b'\r', b'\n').split(b'\n')
        # Drop trailing empty line that split() produces when text ends with \n
        if lines and lines[-1] == b'':
            lines = lines[:-1]
        out = bytearray()
        for line in lines:
            try:
                ebcdic_line = line.decode('utf-8').encode(self.ebcdic)
            except (UnicodeDecodeError, UnicodeEncodeError):
                ebcdic_line = line  # keep raw bytes if conversion fails
            # Pad / truncate to lrecl
            if len(ebcdic_line) >= lrecl:
                out += ebcdic_line[:lrecl]
            else:
                out += ebcdic_line + space * (lrecl - len(ebcdic_line))
        return bytes(out)

    def _xmi_encode_input(self, file_bytes, lrecl, recfm):
        '''Return raw EBCDIC bytes for *file_bytes* as FB/VB/... records.

        For FB/F: text is converted to fixed EBCDIC records.
        For VB/V: text is converted and each record wrapped with RDW.
        For U/binary: file_bytes returned as-is (no conversion).
        '''
        recfm = recfm.upper()
        if recfm.startswith('U'):
            return file_bytes

        # Try to decode as UTF-8 text; fall back to binary pass-through
        try:
            _ = file_bytes.decode('utf-8')
            is_text = True
        except UnicodeDecodeError:
            is_text = False

        if not is_text:
            # FB context: pad to LRECL multiple so IEBCOPY sub-block data_len
            # is LRECL-aligned.  z/OS IEBCOPY can't reconstruct fixed-length
            # records from a chunk whose size isn't a multiple of LRECL.
            # Trailing nulls are harmless — NETDATA parsers stop at INMR06.
            if recfm.startswith('F') and lrecl > 0:
                remainder = len(file_bytes) % lrecl
                if remainder:
                    file_bytes = file_bytes + b'\x00' * (lrecl - remainder)
            return file_bytes

        if recfm.startswith('F'):
            return self._xmi_text_to_ebcdic(file_bytes, lrecl)

        # VB / V: build variable-length records with RDW (4-byte header)
        space = b'\x40'
        lines = file_bytes.replace(b'\r\n', b'\n').replace(b'\r', b'\n').split(b'\n')
        if lines and lines[-1] == b'':
            lines = lines[:-1]
        out = bytearray()
        for line in lines:
            try:
                ebcdic_line = line.decode('utf-8').encode(self.ebcdic)
            except (UnicodeDecodeError, UnicodeEncodeError):
                ebcdic_line = line
            if len(ebcdic_line) > lrecl:
                ebcdic_line = ebcdic_line[:lrecl]
            rdw_len = len(ebcdic_line) + 4
            out += struct.pack('>HH', rdw_len, 0) + ebcdic_line
        return bytes(out)

    # -- Control record builders ---------------------------------------- #

    def _xmi_inmr01(self, from_user, from_node, to_user, to_node, has_message=False):
        '''Build INMR01 control record bytes.'''
        import datetime
        now = datetime.datetime.utcnow()
        timestamp = now.strftime('%Y%m%d%H%M%S').encode(self.ebcdic)

        def _enc(s, maxlen):
            return s.upper()[:maxlen].encode(self.ebcdic)

        numf = 2 if has_message else 1  # message stream counts as an additional file
        tu = (
            self._xmi_tu(0x0042, b'\x50') +              # INMLRECL (required constant in INMR01)
            self._xmi_tu(0x1011, _enc(from_node, 8)) +  # INMFNODE
            self._xmi_tu(0x1012, _enc(from_user, 8)) +  # INMFUID
            self._xmi_tu(0x1001, _enc(to_node, 8)) +    # INMTNODE
            self._xmi_tu(0x1002, _enc(to_user, 8)) +    # INMTUID
            self._xmi_tu(0x1024, timestamp) +            # INMFTIME
            self._xmi_tu(0x102F, struct.pack('>B', numf))  # INMNUMF
        )
        return self._xmi_ctrl_seg('INMR01', tu)

    def _xmi_inmr02_seq(self, dsn, lrecl, recfm, data_len, file_number=1):
        '''Build INMR02 for a sequential dataset (utility=INMCOPY).

        *file_number* is the NETDATA file ordinal (1-based).  Pass 2 when a
        message stream occupies file 1 so z/OS RECEIVE can match this INMR02
        to the correct INMR03/data pair.
        '''
        blksize = self._xmi_blksize(lrecl, recfm)
        recfm_bytes = self._xmi_recfm_byte(recfm)
        dsorg = b'\x40\x00'  # PS

        tu = (
            self._xmi_tu(0x1028, 'INMCOPY'.encode(self.ebcdic)) +       # INMUTILN
            self._xmi_tu(0x102C, struct.pack('>I', data_len)) +         # INMSIZE
            self._xmi_tu(0x003C, dsorg) +                               # INMDSORG
            self._xmi_tu(0x0042, struct.pack('>I', lrecl)) +            # INMLRECL
            self._xmi_tu(0x0030, struct.pack('>I', blksize)) +          # INMBLKSZ
            self._xmi_tu(0x0049, recfm_bytes) +                         # INMRECFM
            self._xmi_dsn_tu(dsn)                                       # INMDSNAM
        )
        return self._xmi_ctrl_seg('INMR02', tu, numfiles=file_number)

    def _xmi_inmr02_pds(self, dsn, lrecl, recfm, num_members, inmsize=0, file_number=1):
        '''Build the two INMR02 records needed for a PDS XMI.

        First:  IEBCOPY record (describes the PDS structure).
        Second: INMCOPY record (transport layer).
        *inmsize*: total byte count of the IEBCOPY data stream.
        *file_number*: NETDATA file ordinal; pass 2 when a message occupies file 1.
        '''
        blksize = self._xmi_blksize(lrecl, recfm)
        recfm_bytes = self._xmi_recfm_byte(recfm)
        dsorg_po = b'\x02\x00'   # PO
        dsorg_ps = b'\x40\x00'   # PS (for INMCOPY wrapper)

        # Number of directory blocks: at least ceil(members / 5)
        dir_blocks = max(1, (num_members + 4) // 5)

        # First INMR02: IEBCOPY
        tu1 = (
            self._xmi_tu(0x1028, 'IEBCOPY'.encode(self.ebcdic)) +      # INMUTILN
            self._xmi_tu(0x102C, struct.pack('>I', inmsize)) +         # INMSIZE
            self._xmi_tu(0x003C, dsorg_po) +                           # INMDSORG
            self._xmi_tu(0x8012, b'\x00') +                            # INMTYPE
            self._xmi_tu(0x0042, struct.pack('>I', lrecl)) +           # INMLRECL
            self._xmi_tu(0x0030, struct.pack('>I', blksize)) +         # INMBLKSZ
            self._xmi_tu(0x0049, recfm_bytes) +                        # INMRECFM
            self._xmi_tu(0x000C, struct.pack('>I', dir_blocks)[1:]) +  # INMDIR (3 bytes)
            self._xmi_dsn_tu(dsn)                                      # INMDSNAM
        )
        # Second INMR02: INMCOPY (transport)
        # LRECL/BLKSIZE are fixed transport values (not dataset values)
        tu2 = (
            self._xmi_tu(0x1028, 'INMCOPY'.encode(self.ebcdic)) +      # INMUTILN
            self._xmi_tu(0x102C, struct.pack('>I', inmsize)) +         # INMSIZE
            self._xmi_tu(0x003C, dsorg_ps) +                           # INMDSORG
            self._xmi_tu(0x0042, struct.pack('>I', 3216)) +            # INMLRECL (transport)
            self._xmi_tu(0x0030, struct.pack('>I', 3220)) +            # INMBLKSZ (transport)
            self._xmi_tu(0x0049, b'\x48\x02')                          # INMRECFM (VS)
        )
        return (self._xmi_ctrl_seg('INMR02', tu1, numfiles=file_number) +
                self._xmi_ctrl_seg('INMR02', tu2, numfiles=file_number))

    def _xmi_inmr03(self, lrecl, inmsize=0, recfm_bytes=b'\x00\x01'):
        '''Build INMR03 control record.

        recfm_bytes defaults to b'\x00\x01' for message streams (the value
        z/OS TRANSMIT uses).  Pass self._xmi_recfm_byte(recfm) for dataset
        streams so z/OS sees the correct RECFM on RECEIVE.
        '''
        dsorg = b'\x40\x00'  # PS

        tu = (
            self._xmi_tu(0x102C, struct.pack('>I', inmsize)) +    # INMSIZE
            self._xmi_tu(0x003C, dsorg) +                         # INMDSORG
            self._xmi_tu(0x0042, struct.pack('>H', lrecl)) +      # INMLRECL (2 bytes)
            self._xmi_tu(0x0049, recfm_bytes)                     # INMRECFM
        )
        return self._xmi_ctrl_seg('INMR03', tu)

    def _xmi_inmr06(self):
        '''Build INMR06 (end-of-transmission) control record.'''
        return self._xmi_ctrl_seg('INMR06', b'')

    def _xmi_message_inmr02(self, text, lrecl):
        '''Build only the INMR02 control record for the embedded message.

        INMTERM (key=0x0028, count=0) marks this as a message stream.
        No INMDSNAM — its absence is how the parser identifies message INMR02s.

        INMLRECL is set to 251 — the z/OS TRANSMIT transport value for terminal
        messages, independent of the actual text LRECL (which lives in INMR03).
        '''
        ebcdic_data = self._xmi_text_to_ebcdic(text.encode('utf-8'), lrecl)
        data_len = len(ebcdic_data)
        dsorg = b'\x40\x00'  # PS

        tu = (
            self._xmi_tu(0x1028, 'INMCOPY'.encode(self.ebcdic)) +   # INMUTILN
            struct.pack('>HH', 0x0028, 0) +                          # INMTERM flag (count=0)
            self._xmi_tu(0x102C, struct.pack('>I', data_len)) +      # INMSIZE
            self._xmi_tu(0x003C, dsorg) +                            # INMDSORG
            self._xmi_tu(0x0042, struct.pack('>I', 251)) +           # INMLRECL=251 (terminal transport value)
            self._xmi_tu(0x0049, self._xmi_recfm_byte('FB'))         # INMRECFM
        )
        return self._xmi_ctrl_seg('INMR02', tu, numfiles=1)

    def _xmi_message_data(self, text, lrecl):
        '''Build INMR03 + DATA segments for the embedded message.

        Each text line is sent as an individual 0xC0 record (matching z/OS
        TRANSMIT output).  Sending all lines as one big block would exceed the
        LRECL declared in the message INMR02 for any multi-line message.

        RECFM=FB is used rather than the 0x0001 value seen in real z/OS XMITs:
        the message data genuinely is fixed-length lrecl-byte records, and
        z/OS RECEIVE uses the first INMR03 it encounters to determine the
        incoming record format — leaving it as RECFM=U causes INMR065I/066I
        on the dataset even though the dataset INMR03 correctly says FB.
        '''
        ebcdic_data = self._xmi_text_to_ebcdic(text.encode('utf-8'), lrecl)
        data_len = len(ebcdic_data)
        out = bytearray()
        out += self._xmi_inmr03(lrecl, inmsize=data_len,
                                recfm_bytes=self._xmi_recfm_byte('FB'))
        # lrecl is 80 or 132 — both fit in a single segment (max 253 bytes)
        for i in range(0, len(ebcdic_data), lrecl):
            out += self._xmi_seg(0xC0, ebcdic_data[i:i + lrecl])
        return bytes(out)

    def _xmi_message_stream(self, text, lrecl):
        '''Build INMR02 + INMR03 + DATA for the embedded message (combined).

        Note: in a complete XMI, INMR02 must appear before all INMR03/DATA
        records (use _xmi_message_inmr02 / _xmi_message_data separately when
        building multi-file streams).
        '''
        return self._xmi_message_inmr02(text, lrecl) + self._xmi_message_data(text, lrecl)


    # -- IEBCOPY data block builders ------------------------------------ #

    def _xmi_copyr1(self, lrecl, recfm, blksize):
        '''Build the 56-byte COPYR1 block.'''
        recfm_byte = self._xmi_recfm_byte(recfm)[0]
        rec = bytearray(56)
        # Eyecatcher at bytes 1-3 (offset 1)
        rec[1] = 0xCA
        rec[2] = 0x6D
        rec[3] = 0x0F
        # DS1DSORG at offset 4: PO = 0x0200
        struct.pack_into('>H', rec, 4, 0x0200)
        # DS1BLKL at offset 6
        struct.pack_into('>H', rec, 6, blksize)
        # DS1LRECL at offset 8
        struct.pack_into('>H', rec, 8, lrecl)
        # DS1RECFM at offset 10
        rec[10] = recfm_byte
        # file_tape_blocksize at offset 14 = INMCOPY transport blocksize (3220)
        struct.pack_into('>H', rec, 14, 3220)
        return bytes(rec)

    def _xmi_copyr2(self):
        '''Return the 276-byte COPYR2 block.

        Byte 0 must be 0x01 (DEB version indicator) for IEBCOPY to accept
        the file.  The remaining 275 bytes are zeros (no disk extent info).
        '''
        rec = bytearray(276)
        rec[0] = 0x01
        return bytes(rec)

    def _xmi_directory_block(self, members, is_last=True):
        '''Build one 276-byte PDS directory block for *members*.

        *members* is a list of (name_str, ttr_int, ispf_bytes_or_None) triples,
        at most 5 per block when ISPF stats are included (42 bytes/entry) or
        8 per block without stats (12 bytes/entry).

        *is_last*: True for the final (or only) directory block — appends the
        end-of-directory sentinel (0xFF*8).  Intermediate blocks must pass
        False so the reader does not stop scanning too early.

        Layout (276 bytes total):
          bytes  0- 7: key area (zeroes)
          bytes  8- 9: key_len = 8
          bytes 10-11: data_len = 256 (0x0100)
          bytes 12-19: last-referenced-member (zeroes)
          bytes 20-21: used_length = entries [+ sentinel] + 2
          bytes 22+  : directory entries [+ end-of-directory sentinel if last]
          remainder  : zero-padded to 276 bytes

        Each entry: 8-byte EBCDIC name + 3-byte TTR + 1-byte C-byte
                    [+ user-data halfwords if C-byte & 0x1F != 0]
        ISPF stats are 30 bytes (C-byte lower 5 bits = 0x0F = 15 halfwords).
        '''
        entries = bytearray()
        for name, ttr, ispf in members:
            ebcdic_name = name.upper()[:8].encode(self.ebcdic).ljust(8, b'\x40')
            ttr_bytes = struct.pack('>I', ttr)[1:]  # 3 bytes big-endian
            if ispf:
                # 15 halfwords (30 bytes) of ISPF user data
                c_byte = bytes([0x0F])
                entries += ebcdic_name + ttr_bytes + c_byte + ispf
            else:
                c_byte = b'\x00'
                entries += ebcdic_name + ttr_bytes + c_byte

        # End-of-directory sentinel: only in the last directory block
        sentinel = b'\xff' * 8 + b'\x00\x00\x00\x00' if is_last else b''

        # used_length includes itself (the +2)
        used_length = len(entries) + len(sentinel) + 2

        # 12-byte IEBCOPY sub-block header: flag=0x08 (directory), TTR=0,
        # key_len=8, data_len=256
        iebcopy_hdr = (
            b'\x08' +           # flag: directory block
            b'\x00' * 5 +       # zeros
            b'\x00\x00\x00' +   # TTR = 0 for directory
            b'\x08' +           # key_len = 8
            b'\x01\x00'         # data_len = 256
        )

        # 8-byte PDS key
        pds_key = b'\xff' * 8

        # 256-byte PDS data: used_length + entries [+ sentinel] + zero-padding
        pds_data = struct.pack('>H', used_length) + bytes(entries) + bytes(sentinel)
        pds_data = pds_data.ljust(256, b'\x00')[:256]

        return iebcopy_hdr + pds_key + pds_data  # 276 bytes

    def _xmi_member_block(self, name, data_bytes, ttr, blksize=3200, recfm='FB'):
        '''Build a list of IEBCOPY member data sub-block records.

        Each sub-block record is a separate XMI logical data record.
        Non-last sub-blocks: 12-byte header + up to *blksize* bytes of data.
        Last sub-block:      12-byte header + remaining data + 12-byte EOM header.

        For VB data, splits on RDW record boundaries rather than arbitrary offsets.
        Returns a list of byte strings (one per XMI data record).
        '''
        ttr_bytes = struct.pack('>I', ttr)[1:]  # 3 bytes big-endian

        def _hdr(flag, data_len):
            return (bytes([flag]) +
                    b'\x00' * 5 +
                    ttr_bytes +
                    b'\x00' +
                    struct.pack('>H', data_len))

        eom = _hdr(0x80, 0)  # member EOM: flag=0x80, data_len=0
        records = []

        if not data_bytes:
            records.append(eom)
            return records

        recfm_upper = recfm.upper()
        if recfm_upper.startswith('V'):
            # VB/VS: split on RDW boundaries so sub-blocks hold complete records
            cur = bytearray()
            off = 0
            while off + 4 <= len(data_bytes):
                rdw = struct.unpack('>H', data_bytes[off:off + 2])[0]
                if rdw < 4:
                    break
                rec_bytes = data_bytes[off:off + rdw]
                if cur and len(cur) + rdw > blksize:
                    records.append(_hdr(0x00, len(cur)) + bytes(cur))
                    cur = bytearray()
                cur += rec_bytes
                off += rdw
            if cur:
                records.append(_hdr(0x00, len(cur)) + bytes(cur))
            records.append(eom)
        else:
            # FB/FS: split on blksize byte boundaries
            offset = 0
            while offset < len(data_bytes):
                chunk = data_bytes[offset:offset + blksize]
                offset += len(chunk)
                records.append(_hdr(0x00, len(chunk)) + chunk)
            records.append(eom)  # EOM as separate record

        return records

    def _xmi_build_iebcopy(self, members_data, lrecl, recfm, from_user='PYTHON'):
        '''Build a list of IEBCOPY logical blocks for *members_data*.

        *members_data*: list of (name_str, file_bytes) tuples.
        *from_user*: userid recorded in each PDS directory entry's ISPF stats.
        Returns a list of byte strings, each to be written as a separate
        XMI logical data record (wrapped with _xmi_data_record).

        Order: COPYR1, COPYR2, directory block(s), member block(s).
        '''
        import datetime as _dt

        blksize = self._xmi_blksize(lrecl, recfm)
        now = _dt.datetime.now()

        blocks = []
        blocks.append(self._xmi_copyr1(lrecl, recfm, blksize))
        blocks.append(self._xmi_copyr2())

        # Assign sequential TTRs starting at 1; pre-encode data so we can
        # count lines for ISPF stats before building the directory.
        ttr_map = []
        for i, (name, raw) in enumerate(members_data):
            ebcdic_data = self._xmi_encode_input(raw, lrecl, recfm)
            recfm_upper = recfm.upper()
            if recfm_upper.startswith('U'):
                # Binary passthrough — line count is meaningless
                num_lines = 0
            elif recfm_upper.startswith('V'):
                # Count RDW-prefixed records
                off, num_lines = 0, 0
                while off + 4 <= len(ebcdic_data):
                    rdw = struct.unpack('>H', ebcdic_data[off:off + 2])[0]
                    if rdw < 4:
                        break
                    num_lines += 1
                    off += rdw
            else:
                num_lines = len(ebcdic_data) // lrecl if lrecl else 0

            ispf = self._xmi_ispf_stats(from_user, num_lines, now)
            ttr_map.append((name, i + 1, ebcdic_data, ispf))

        # Directory block(s): 5 entries/block max with ISPF stats (42 bytes each).
        # Only the last block carries the end-of-directory sentinel (0xFF*8);
        # intermediate blocks must not, or the reader stops scanning too early.
        chunk_size = 5
        dir_entries = [(name, ttr, ispf) for name, ttr, _, ispf in ttr_map]
        chunks = [dir_entries[i:i + chunk_size]
                  for i in range(0, len(dir_entries), chunk_size)]
        for idx, chunk in enumerate(chunks):
            blocks.append(self._xmi_directory_block(chunk, is_last=(idx == len(chunks) - 1)))
        blocks.append(b'\x88' + b'\x00' * 11)  # single IEBCOPY directory EOM

        # Member blocks - each sub-block is a separate XMI data record
        for name, ttr, ebcdic_data, _ in ttr_map:
            for sub_block in self._xmi_member_block(name, ebcdic_data, ttr,
                                                    blksize=blksize, recfm=recfm):
                blocks.append(sub_block)

        return blocks

    # -- Top-level builders --------------------------------------------- #

    def _build_seq_xmi(self, file_path, dsn=None, lrecl=80, recfm='FB',
                       from_user='PYTHON', from_node='LOCAL',
                       to_user='PYTHON', to_node='LOCAL', resolved_msg=None):
        '''Build a sequential dataset XMI from a single file.'''
        raw = Path(file_path).read_bytes()
        ebcdic_data = self._xmi_encode_input(raw, lrecl, recfm)

        if dsn is None:
            dsn = Path(file_path).stem.upper()

        msg_text = resolved_msg[0] if resolved_msg is not None else None
        msg_lrecl = resolved_msg[1] if resolved_msg is not None else 0
        has_message = msg_text is not None

        out = bytearray()
        out += self._xmi_inmr01(from_user, from_node, to_user, to_node,
                                has_message=has_message)
        # All INMR02s must be grouped before any INMR03/DATA (NETDATA protocol)
        if has_message:
            out += self._xmi_message_inmr02(msg_text, msg_lrecl)
        dataset_file = 2 if has_message else 1
        out += self._xmi_inmr02_seq(dsn, lrecl, recfm, len(ebcdic_data),
                                    file_number=dataset_file)
        # Message INMR03+DATA first, then dataset INMR03+DATA
        if has_message:
            out += self._xmi_message_data(msg_text, msg_lrecl)
        out += self._xmi_inmr03(lrecl, inmsize=len(ebcdic_data),
                               recfm_bytes=self._xmi_recfm_byte(recfm))
        out += self._xmi_data_record(ebcdic_data)
        out += self._xmi_inmr06()
        return bytes(out)

    def _build_pds_xmi(self, folder_path, dsn=None, lrecl=80, recfm='FB',
                       from_user='PYTHON', from_node='LOCAL',
                       to_user='PYTHON', to_node='LOCAL', resolved_msg=None):
        '''Build a PDS XMI from a folder (one level deep).'''
        folder = Path(folder_path)
        members_data = []
        for f in sorted(folder.iterdir()):
            if f.is_file():
                name = f.stem.upper()[:8]
                members_data.append((name, f.read_bytes()))

        if not members_data:
            raise ValueError("Folder contains no files: {}".format(folder_path))

        if dsn is None:
            dsn = folder.name.upper()

        # Auto-detect: if EVERY member is binary (cannot be decoded as UTF-8),
        # switch to RECFM=U so the content is stored as-is rather than being
        # mangled into fixed-length EBCDIC records.  A typical case is a folder
        # of XMI files to be received individually on z/OS.
        # Mixed folders (text + binary) are left as FB so text members stay
        # correctly EBCDIC-encoded.
        if not recfm.upper().startswith('U'):
            def _is_binary(raw):
                try:
                    raw.decode('utf-8')
                    return False
                except UnicodeDecodeError:
                    return True

            if all(_is_binary(raw) for _, raw in members_data):
                recfm = 'U'
                lrecl = 0

        iebcopy_blocks = self._xmi_build_iebcopy(members_data, lrecl, recfm,
                                                  from_user=from_user)
        inmsize = sum(len(b) for b in iebcopy_blocks)

        msg_text = resolved_msg[0] if resolved_msg is not None else None
        msg_lrecl = resolved_msg[1] if resolved_msg is not None else 0
        has_message = msg_text is not None

        out = bytearray()
        out += self._xmi_inmr01(from_user, from_node, to_user, to_node,
                                has_message=has_message)
        # All INMR02s must be grouped before any INMR03/DATA (NETDATA protocol)
        if has_message:
            out += self._xmi_message_inmr02(msg_text, msg_lrecl)
        dataset_file = 2 if has_message else 1
        out += self._xmi_inmr02_pds(dsn, lrecl, recfm, len(members_data),
                                    inmsize=inmsize, file_number=dataset_file)
        # Message INMR03+DATA first, then dataset INMR03+DATA
        if has_message:
            out += self._xmi_message_data(msg_text, msg_lrecl)
        out += self._xmi_inmr03(lrecl, inmsize=inmsize,
                               recfm_bytes=self._xmi_recfm_byte(recfm))
        for block in iebcopy_blocks:
            out += self._xmi_data_record(block)
        out += self._xmi_inmr06()
        return bytes(out)


if __name__ == "__main__":
    '''
    If executed lists all datasets and members in a given XMI/AWS/HET file
    '''

    print("XMI/AWS/HET file library.")
    if len(sys.argv) == 1:
        print("Missing argument. Please provide an XMI/AWS/HET file to process")
    else:
        print("Listing all datasets and members in {}".format(sys.argv[1]))
        for dataset_or_member in list_all(mainframe_file=sys.argv[1]):
            print(dataset_or_member)
    print("Done!")
