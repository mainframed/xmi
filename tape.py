#!/usr/bin/env python3

from xmilib import XMIT
import logging
import os
import argparse

def main():

    desc = '''AWS/HET Tape Unload utility'''
    arg_parser = argparse.ArgumentParser(description=desc,
                        usage='%(prog)s [options] [tape File]',
                        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    arg_parser.add_argument('-u', '--unnum', help='Do not remove number column from text files', action="store_false", default=True)
    arg_parser.add_argument('-l', '--lrecl', help="Set record length", default=80, type=int)
    arg_parser.add_argument('-q', '--quiet', help="Don't print unload output", action="store_true", default=False)
    arg_parser.add_argument('-d', '--debug', help="Print lots of debugging statements", action="store_const", dest="loglevel", const=logging.DEBUG, default=logging.WARNING)
    arg_parser.add_argument('-p', '--print', help="Print unload information only (no file creation)", action="store_true", default=False)
    arg_parser.add_argument('-H', '--human', help="Print unload information human readable", action="store_true", default=False)
    arg_parser.add_argument('-f', '--force', help="Force all files to be translated to plain text regardless of mimetype", action="store_true", default=False)
    arg_parser.add_argument("--outputdir", help="Folder to place tape files in, default is current working directory", default=os.getcwd())
    arg_parser.add_argument("--encoding", help="EBCDIC encoding translation table", default='cp1140')
    arg_parser.add_argument("tape_file", help="Virtual tape file")

    args = arg_parser.parse_args()

    TAPE = XMIT(filename=args.tape_file,
                loglevel=args.loglevel,
                outputfolder=args.outputdir,
                LRECL=args.lrecl,
                unnum=args.unnum,
                quiet=args.quiet,
                encoding=args.encoding,
                force=args.force
                )

    TAPE.parse_tape()
    TAPE.get_tape_files()

    if args.print:
        print("printing")
        TAPE.print_tape(human=args.human)
    else:
        TAPE.unload()

main()