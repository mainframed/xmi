#!/usr/bin/env python3

import xmi
import logging
import os
import argparse
from argparse import RawTextHelpFormatter
def main():

    desc = 'TSO XMIT and Virtual Tape File utility'
    epilog = '''Example:
    %(prog)s -pH FILE456.XMI
    %(prog)s -l FILE456.XMI
    %(prog)s FILE456.XMI SYS2.PDS(CATLIST)
    %(prog)s --debug CBT500.AWS
    %(prog)s --debug fortran.het "FILE0001(IEYGEN)"'''
    arg_parser = argparse.ArgumentParser(description=desc,
                        usage='%(prog)s [options] File [member]',
                        formatter_class=RawTextHelpFormatter,
                        epilog=epilog)
    arg_parser.add_argument('-u', '--unnum', help='Remove number column from text files', action="store_false", default=True)
    arg_parser.add_argument('-l', '--list', help='Lists the contents of the XMI/Virtual Tape and exit', action="store_true", default=False)
    arg_parser.add_argument('-j', '--json', help="Write XMIT file information to json file", action="store_true", default=False)
    arg_parser.add_argument('--jsonfile', help="Dump json file location", default='./')
    arg_parser.add_argument('-q', '--quiet', help="Don't print unload output", action="store_true", default=False)
    arg_parser.add_argument('-d', '--debug', help="Print debugging statements", action="store_const", dest="loglevel", const=logging.DEBUG, default=logging.WARNING)
    arg_parser.add_argument('-p', '--print', help="Print detailed file information and exit", action="store_true", default=False)
    arg_parser.add_argument('-H', '--human', help="Print filesizes as human readable", action="store_true", default=False)
    arg_parser.add_argument('-f', '--force', help="Force all files to be translated to plain text regardless of mimetype", action="store_true", default=False)
    arg_parser.add_argument('-b', '--binary', help="Force all files to remain EBCDIC binary files regardless of mimetype", action="store_true", default=False)
    arg_parser.add_argument('--message', help="Prints message (if available) and exits", action="store_true", default=False)
    arg_parser.add_argument('-m', '--modify', help="Set the extracted file last modify date to match ISPF/Tape statistics if available", action="store_true", default=False)
    arg_parser.add_argument("--outputdir", help="Folder to place tape files in, default is current working directory", default=os.getcwd())
    arg_parser.add_argument("--encoding", help="EBCDIC encoding translation table", default='cp1140')
    arg_parser.add_argument("FILENAME", help="XMIT/Virtual Tape File")
    arg_parser.add_argument("MEMBER", help="file/pds or file[(member)] to extract", nargs="?", default=False)

    args = arg_parser.parse_args()

    XMI = xmi.XMIT(
                filename=args.FILENAME,
                loglevel=args.loglevel,
                outputfolder=args.outputdir,
                unnum=args.unnum,
                quiet=args.quiet,
                encoding=args.encoding,
                force_convert=args.force,
                binary=args.binary,
                modifydate=args.modify
                )

    XMI.open()

    if args.message:
        print(XMI.get_message())
        return

    if args.list:
        for f in XMI.get_files():
            if XMI.is_pds(f):
                for m in XMI.get_members(f):
                    print("{}({})".format(f, m))
            else:
                print(f)
        return

    if args.print:
        XMI.print_xmit(human=args.human)
        return

    if not args.MEMBER:
        XMI.unload_files()
    else:
        if "(" in args.MEMBER:
            f = args.MEMBER.split("(")[0]
            m = args.MEMBER.split("(")[1][:-1]
            XMI.unload_file(f, m)
        else:
            XMI.unload_pds(args.MEMBER)

    if args.json:
        XMI.dump_xmit_json()

main()
