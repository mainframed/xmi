#!/usr/bin/env python3
"""
Command-line entry points for the xmi library.

  extractxmi  --  open/extract XMI, AWS and HET files
  createxmi   --  create XMI files from local files or folders
"""

import logging
import os
import argparse
from argparse import RawTextHelpFormatter

import xmi


# ---------------------------------------------------------------------------
# extractxmi
# ---------------------------------------------------------------------------

def extract_main():
    desc = 'TSO XMIT and Virtual Tape File utility'
    epilog = '''Examples:
  %(prog)s -pH FILE456.XMI
  %(prog)s -l FILE456.XMI
  %(prog)s FILE456.XMI SYS2.PDS(CATLIST)
  %(prog)s --debug CBT500.AWS
  %(prog)s --debug fortran.het "FILE0001(IEYGEN)"'''

    ap = argparse.ArgumentParser(
        description=desc,
        usage='%(prog)s [options] FILE [MEMBER]',
        formatter_class=RawTextHelpFormatter,
        epilog=epilog,
    )
    ap.add_argument('-u', '--unnum',
        help='Remove number column from text files',
        action='store_false', default=True)
    ap.add_argument('-l', '--list',
        help='List contents and exit',
        action='store_true', default=False)
    ap.add_argument('-j', '--json',
        help='Write metadata to a JSON file',
        action='store_true', default=False)
    ap.add_argument('--jsonfile',
        help='JSON output path (default: current directory)',
        default='./')
    ap.add_argument('-q', '--quiet',
        help="Suppress extraction output",
        action='store_true', default=False)
    ap.add_argument('-d', '--debug',
        help='Print debugging statements',
        action='store_const', dest='loglevel',
        const=logging.DEBUG, default=logging.WARNING)
    ap.add_argument('-p', '--print',
        help='Print detailed file information and exit',
        action='store_true', default=False)
    ap.add_argument('-H', '--human',
        help='Print file sizes as human-readable',
        action='store_true', default=False)
    ap.add_argument('-f', '--force',
        help='Translate all files to plain text regardless of mimetype',
        action='store_true', default=False)
    ap.add_argument('-b', '--binary',
        help='Keep all files as raw EBCDIC binary regardless of mimetype',
        action='store_true', default=False)
    ap.add_argument('--message',
        help='Print message (if present) and exit',
        action='store_true', default=False)
    ap.add_argument('-m', '--modify',
        help='Set extracted file timestamps from ISPF/tape statistics',
        action='store_true', default=False)
    ap.add_argument('--outputdir',
        help='Folder to extract into (default: current directory)',
        default=os.getcwd())
    ap.add_argument('--encoding',
        help='EBCDIC codepage (default: cp1140)',
        default='cp1140')
    ap.add_argument('FILE', help='XMI / AWS / HET file to open')
    ap.add_argument('MEMBER',
        help='Dataset or dataset(member) to extract',
        nargs='?', default=False)

    args = ap.parse_args()

    X = xmi.XMIT(
        filename=args.FILE,
        loglevel=args.loglevel,
        outputfolder=args.outputdir,
        unnum=args.unnum,
        quiet=args.quiet,
        encoding=args.encoding,
        force_convert=args.force,
        binary=args.binary,
        modifydate=args.modify,
    )
    X.open()

    if args.message:
        print(X.get_message())
        return

    if args.list:
        for f in X.get_files():
            if X.is_pds(f):
                for m in X.get_members(f):
                    print('{}({})'.format(f, m))
            else:
                print(f)
        return

    if args.print:
        X.print_xmit(human=args.human)
        return

    if not args.MEMBER:
        X.unload_files()
    else:
        if '(' in args.MEMBER:
            f = args.MEMBER.split('(')[0]
            m = args.MEMBER.split('(')[1][:-1]
            X.unload_file(f, m)
        else:
            X.unload_pds(args.MEMBER)

    if args.json:
        X.dump_xmit_json()


# ---------------------------------------------------------------------------
# createxmi
# ---------------------------------------------------------------------------

def create_main():
    desc = 'Create a TSO XMIT (XMI) file from a local file or folder'
    epilog = '''Examples:
  %(prog)s myfolder/
  %(prog)s myfolder/ -o MY.XMI
  %(prog)s myfolder/ -o MY.XMI --dsn MY.PDS --from-user IBMUSER
  %(prog)s myfile.jcl -o SEQ.XMI --dsn MY.SEQ --recfm FB --lrecl 80
  %(prog)s myfile.jcl -o SEQ.XMI --message "Hello from Python!"
  %(prog)s myfile.jcl -o SEQ.XMI --message-file banner-80x32.txt
  %(prog)s myfile.jcl -o SEQ.XMI --message-file banner.txt --message-format 132x27'''

    from pathlib import Path

    ap = argparse.ArgumentParser(
        description=desc,
        usage='%(prog)s [options] INPUT',
        formatter_class=RawTextHelpFormatter,
        epilog=epilog,
    )
    ap.add_argument('INPUT',
        help='File or folder to package as XMI')
    ap.add_argument('-o', '--output',
        dest='output_file', default=None, metavar='OUTPUT',
        help='Output XMI file path (default: <INPUT>.xmi)')
    ap.add_argument('--dsn',
        default=None, metavar='DSN',
        help='Dataset name in XMI metadata (default: uppercased INPUT stem)')
    ap.add_argument('--lrecl',
        type=int, default=80, metavar='N',
        help='Logical record length for text encoding (default: 80)')
    ap.add_argument('--recfm',
        default='FB', metavar='RECFM',
        help='Record format: FB, F, VB, V, U (default: FB)')
    ap.add_argument('--encoding',
        default='cp500', metavar='CODEPAGE',
        help='EBCDIC codepage for text encoding (default: cp500)')
    ap.add_argument('--from-user',
        default='PYTHON', metavar='USERID',
        help='Originating user ID, also recorded in ISPF stats (default: PYTHON)')
    ap.add_argument('--from-node',
        default='LOCAL', metavar='NODE',
        help='Originating node name (default: LOCAL)')
    ap.add_argument('--to-user',
        default='PYTHON', metavar='USERID',
        help='Destination user ID (default: PYTHON)')
    ap.add_argument('--to-node',
        default='LOCAL', metavar='NODE',
        help='Destination node name (default: LOCAL)')
    ap.add_argument('--message',
        default=None, metavar='TEXT',
        help='Message displayed on z/OS RECEIVE (use \\n for line breaks)')
    ap.add_argument('--message-file',
        dest='message_file', default=None, metavar='PATH',
        help='Read message from a UTF-8 text file (takes precedence over --message)')
    ap.add_argument('--message-format',
        dest='message_format', default='80x32', metavar='FORMAT',
        help='Terminal format: 80x32 (default, LRECL 80, 32 lines) or 132x27 (LRECL 132, 27 lines)')
    ap.add_argument('-d', '--debug',
        help='Print debugging statements',
        action='store_const', dest='loglevel',
        const=logging.DEBUG, default=logging.WARNING)

    args = ap.parse_args()

    output = args.output_file
    if not output:
        output = str(Path(args.INPUT.rstrip('/\\')).with_suffix('.xmi'))

    xmi.create_xmi(
        args.INPUT,
        output_file=output,
        dsn=args.dsn,
        lrecl=args.lrecl,
        recfm=args.recfm,
        encoding=args.encoding,
        from_user=args.from_user,
        from_node=args.from_node,
        to_user=args.to_user,
        to_node=args.to_node,
        message=args.message,
        message_file=args.message_file,
        message_format=args.message_format,
        loglevel=args.loglevel,
    )
    print('Created: {}'.format(output))
