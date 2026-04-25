#!/usr/bin/env python3
"""
XMI smoke test: build XMI variants, verify with local parser, upload to z/OS.

Usage:
    python tests/zos_smoke.py                    # build + parse only
    python tests/zos_smoke.py --upload           # build + parse + upload
    python tests/zos_smoke.py --upload --hlq MYHLQ.XMILIB
    python tests/zos_smoke.py --upload --host 10.0.0.1 --hlq IBMUSER.XMILIB
"""

import argparse
import ftplib
import getpass
import io
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from xmi import XMIT, create_xmi

DEFAULT_HOST = '192.168.1.141'
DEFAULT_HLQ  = 'IBMUSER.XMILIB'

MESSAGE_80 = (
    'XMI smoke test (80x32)\\n'
    '========================================\\n'
    'Created by Python xmi library\\n'
    'Reply ALL'
)

MESSAGE_132 = (
    'XMI smoke test - WIDE FORMAT (132x27)\\n'
    + '=' * 132 + '\\n'
    + 'Created by Python xmi library\\n'
    + 'This message demonstrates the full 132-column width of a Model 5 / 3278 terminal.\\n'
    + '=' * 132
)

# Each entry: (source_type, message, message_format, expect_msg)
VARIANTS = {
    'XMI1M': ('seq', MESSAGE_80,  '80x32',  True),
    'XMI1':  ('seq', None,        None,     False),
    'XMIPM': ('pds', MESSAGE_80,  '80x32',  True),
    'XMIP':  ('pds', None,        None,     False),
    'XMI1W': ('seq', MESSAGE_132, '132x27', True),
}


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

def build_xmis(work_dir: Path) -> dict[str, bytes]:
    """Return {name: bytes} for all XMI variants."""

    seq_src = work_dir / 'SEQFILE.txt'
    seq_src.write_text('Sequential dataset content\nLine 2\nLine 3\n')

    pds_dir = work_dir / 'PDSDATA'
    pds_dir.mkdir()
    (pds_dir / 'MEMBER1').write_text('First PDS member\nLine 2\n')
    (pds_dir / 'MEMBER2').write_text('Second PDS member\nMore data\n')

    result = {}
    for name, (src_type, msg, fmt, _) in VARIANTS.items():
        src = str(seq_src) if src_type == 'seq' else str(pds_dir)
        kwargs = {}
        if msg:
            kwargs['message'] = msg
        if fmt:
            kwargs['message_format'] = fmt
        result[name] = create_xmi(src, **kwargs)
    return result


# ---------------------------------------------------------------------------
# Verify
# ---------------------------------------------------------------------------

def verify_xmis(xmis: dict[str, bytes], work_dir: Path) -> list[str]:
    """Parse each XMI with the local parser; return list of failure strings."""
    failures = []
    print('\nLocal parser verification:')
    for name, data in xmis.items():
        _, _, _, expect_msg = VARIANTS[name]
        xmi_path = work_dir / f'{name}.xmi'
        xmi_path.write_bytes(data)

        try:
            x = XMIT(filename=str(xmi_path), quiet=True)
            x.open()
            has_msg = x.has_message()

            if has_msg != expect_msg:
                failures.append(
                    f'{name}: has_message={has_msg} but expected {expect_msg}')
            elif expect_msg:
                text = x.get_message() or ''
                if 'smoke test' not in text:
                    failures.append(f'{name}: message parsed but text not found')

            status = 'PASS' if not any(f.startswith(name) for f in failures) else 'FAIL'
            msg_info = f'  message: {x.get_message()!r:.60}' if has_msg else ''
            print(f'  [{status}] {name:6s}  has_message={has_msg}{msg_info}')

        except Exception as exc:
            failures.append(f'{name}: exception during parse: {exc}')
            print(f'  [FAIL] {name:6s}  exception: {exc}')

    return failures


# ---------------------------------------------------------------------------
# Upload
# ---------------------------------------------------------------------------

def upload_to_zos(xmis: dict[str, bytes], host: str, hlq: str) -> None:
    host = input(f'\nz/OS FTP host [{host}]: ').strip() or host
    hlq  = input(f'Dataset HLQ   [{hlq}]: ').strip() or hlq
    user = input(f'Username      [IBMUSER]: ').strip() or 'IBMUSER'
    password = getpass.getpass('Password: ')

    print(f'\nConnecting to {host}...')
    ftp = ftplib.FTP(host, timeout=30)
    ftp.login(user, password)
    print(f'Logged in as {user}')

    for name, data in xmis.items():
        dsn = f'{hlq}.{name}'
        ftp.sendcmd('SITE RECFM=FB LRECL=80 BLKSIZE=3120')
        ftp.storbinary(f"STOR '{dsn}'", io.BytesIO(data))
        print(f'  Uploaded {dsn}  ({len(data):,} bytes)')

    ftp.quit()
    print(f'\nDone. On z/OS, RECEIVE INDSN(\'{hlq}.XMI1M\') etc.')


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description='Build XMI variants, verify with local parser, '
                    'optionally upload to z/OS.')
    parser.add_argument('--upload', action='store_true',
                        help='Upload to z/OS after local verification')
    parser.add_argument('--host', default=DEFAULT_HOST,
                        help=f'z/OS FTP hostname (default: {DEFAULT_HOST})')
    parser.add_argument('--hlq',  default=DEFAULT_HLQ,
                        help=f'Dataset HLQ (default: {DEFAULT_HLQ})')
    args = parser.parse_args()

    with tempfile.TemporaryDirectory() as tmp:
        work = Path(tmp)

        print('Building XMI files...')
        xmis = build_xmis(work)
        for name, data in xmis.items():
            _, _, fmt, has_msg = VARIANTS[name]
            tag = f'msg:{fmt}' if has_msg else 'no msg'
            print(f'  {name:6s}  {len(data):>6,} bytes  ({tag})')

        failures = verify_xmis(xmis, work)

        if failures:
            print('\nFAILURES:')
            for f in failures:
                print(f'  {f}')
            if not args.upload:
                return 1

        if not failures:
            print('\nAll local checks passed.')

        if args.upload:
            if failures:
                print('\nWarning: local checks failed — uploading anyway.')
            upload_to_zos(xmis, args.host, args.hlq)
        else:
            print(f'\nRun with --upload to send to z/OS ({args.host})')
            print(f'  python tests/zos_smoke.py --upload')
            print(f'  python tests/zos_smoke.py --upload --hlq MYHLQ.XMILIB')

    return 1 if failures else 0


if __name__ == '__main__':
    sys.exit(main())
