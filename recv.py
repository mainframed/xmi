#!/usr/bin/env python3

#!/usr/bin/env python3

from xmilib import XMIT
import logging
import os
import argparse

def main():

    desc = '''TSO XMIT File Unload utility'''
    arg_parser = argparse.ArgumentParser(description=desc,
                        usage='%(prog)s [options] [tape File]',
                        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    arg_parser.add_argument('-u', '--unnum', help='Remove number column from text files', action="store_false", default=True)
    arg_parser.add_argument('-j', '--json', help="Write XMIT file information to json file", action="store_true", default=False)
    arg_parser.add_argument('--jsonfile', help="Dump json file location", default='./')
    arg_parser.add_argument('-q', '--quiet', help="Don't print unload output", action="store_true", default=False)
    arg_parser.add_argument('-d', '--debug', help="Print lots of debugging statements", action="store_const", dest="loglevel", const=logging.DEBUG, default=logging.WARNING)
    arg_parser.add_argument('-p', '--print', help="Print unload information only (no file creation)", action="store_true", default=False)
    arg_parser.add_argument('-H', '--human', help="Print unload information human readable", action="store_true", default=False)
    arg_parser.add_argument('-f', '--force', help="Force all files to be translated to plain text regardless of mimetype", action="store_true", default=False)
    arg_parser.add_argument('-b', '--binary', help="Store all files as binary", action="store_true", default=False)
    arg_parser.add_argument('-m', '--modify', help="Set the unloaded last modify date to match ISPF statistics if available", action="store_true", default=False)
    arg_parser.add_argument("--outputdir", help="Folder to place tape files in, default is current working directory", default=os.getcwd())
    arg_parser.add_argument("--encoding", help="EBCDIC encoding translation table", default='cp1140')
    arg_parser.add_argument("XMIT_FILE", help="XMIT File")

    args = arg_parser.parse_args()

    XMI = XMIT(filename=args.XMIT_FILE,
                loglevel=args.loglevel,
                outputfolder=args.outputdir,
                unnum=args.unnum,
                quiet=args.quiet,
                encoding=args.encoding,
                force=args.force,
                binary=args.binary,
                modifydate=args.modify
                )

    XMI.parse_xmi()
    XMI.get_xmi_files()

    if args.print:
        XMI.print_xmit(human=args.human)

    XMI.unload()
    if args.json:
        XMI.dump_xmit_json()

main()
