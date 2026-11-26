"""
Tests for xmi.formats: detection and parsing of MVS / z/OS binary formats.

Real samples (tests/samples/, small files taken from CBT Tape):
    LOADLIB.xmi   CBT624 $LOADLIB: XMI of a PDS with load module TSOUCB
    NOTHING.obj   CBT982 OBJ/NOTHING: 3-card OBJ deck
    ADLER32.obj   CBT880 OBJECT/ADLER32: XOBJ deck (C compiler, XSD records)
    UIMASM.gof    CBT880 OBJECT/UIMASM: GOFF object, fixed 80-byte records
    WORMPH.pnl    CBT483 WORMPH: ISPF panel (code page regression case)

DAF.bin (98 KB, CBT094) is used when the CBT mirror is present at
/home/phil/DEV/CBTTAPEv2; otherwise those tests are skipped.

Everything else is a small synthetic fixture built from the record
layouts in z/OS MVS Program Management: Advanced Facilities (SA23-1392)
and the other manuals cited in xmi/formats.py. Each builder documents the
layout it follows.
"""

import os
import random
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import xmi
from xmi import formats as F

TESTS_DIR = Path(__file__).parent
SAMPLES = TESTS_DIR / 'samples'
DAF_BIN = Path('/home/phil/DEV/CBTTAPEv2/cbt/CBT094/DAF130L/DAF.bin')


def e(s, length=None):
    '''EBCDIC (cp037), optionally blank padded to ``length``.'''
    b = s.encode('cp037')
    return b if length is None else b.ljust(length, b'\x40')


def h(n, size):
    return n.to_bytes(size, 'big')


# ---------------------------------------------------------------------------
# Synthetic fixture builders
# ---------------------------------------------------------------------------

def lm_cesd(first_esdid, entries, modes=True):
    '''CESD record (Figure 13):
        0     X'20' identification
        1     flag: X'80' = byte 12 of entries holds AMODE/RMODE/RSECT
        2-3   zero
        4-5   ESDID of first entry
        6-7   byte count of ESD data (16 per entry, max 240)
        8-    16-byte entries: name(8) type(1) address(3) byte12(1) length(3)
    '''
    data = b''
    for name, etype, addr, b12, length in entries:
        data += e(name, 8) + bytes([etype]) + h(addr, 3) + bytes([b12]) + h(length, 3)
    return b'\x20' + (b'\x80' if modes else b'\x00') + b'\x00\x00' + h(first_esdid, 2) + h(len(data), 2) + data


def lm_idr(subtype, payload):
    '''IDR record (Figure 18): X'80', byte count (includes itself, so
    record length = count + 1), subtype, data.'''
    return b'\x80' + h(len(payload) + 2, 1) + bytes([subtype]) + payload


def lm_control(rid, ctl_pairs, text_addr, text_len, rld_records=0):
    '''Control record (Figure 15):
        0     X'01' control / X'05' EOS / X'0D' EOM
        1-2   zero
        3     count of RLD records after the next text record
        4-5   byte count of control data
        6-7   zero
        8-15  CCW: X'06', 3-byte text address, flags, zero, 2-byte text length
        16-   control data: (CESD id(2), length(2)) pairs
    '''
    ctl = b''.join(h(i, 2) + h(n, 2) for i, n in ctl_pairs)
    ccw = b'\x06' + h(text_addr, 3) + b'\x40\x00' + h(text_len, 2)
    return bytes([rid]) + b'\x00\x00' + bytes([rld_records]) + h(len(ctl), 2) + b'\x00\x00' + ccw + ctl


def lm_rld(rid, rld_data):
    '''RLD record (Figure 16): id X'02'/X'06'/X'0E', bytes 6-7 = RLD byte
    count, bytes 8-15 spare, RLD data from byte 16.'''
    return bytes([rid]) + b'\x00\x00\x00' + b'\x00\x00' + h(len(rld_data), 2) + b'\x00' * 8 + rld_data


def build_load_module():
    '''A 2-CSECT load module: CESD, zap IDR, linkage editor IDR,
    translator IDRs (stream split across two records mid-field),
    control + text, EOM control+RLD + text, RLD-EOM.'''
    cesd = lm_cesd(1, [
        ('MAIN', 0x00, 0x000, 0x02, 0x100),   # SD, AMODE 31 RMODE 24
        ('SUB', 0x00, 0x100, 0x07, 0x080),    # SD, AMODE ANY RMODE ANY
        ('ENTRY2', 0x03, 0x010, 0x00, 0x001),  # LR owned by ESDID 1
        ('EXTRN', 0x02, 0x000, 0x00, 0x000),   # ER
    ])
    zap = lm_idr(0x01, b'\x00' + b'\x00' * 10)
    # linkage editor: name(10) VVMM(2) YYDDD(3) HHMMSS(4)
    le = lm_idr(0x02, e('5695PMB01', 10) + b'\x02\x04' + b'\x24\x03\x5F' + b'\x01\x23\x45\x6F')
    # translator stream: ESDIDs 1, X'8002' (last), count 0 (one desc),
    # desc: name(10) VVMM(2) YYDDD(3)
    stream = h(1, 2) + h(0x8002, 2) + b'\x00' + e('569623400', 10) + b'\x01\x06' + b'\x24\x03\x5F'
    tr1 = lm_idr(0x04, stream[:12])
    tr2 = lm_idr(0x84, stream[12:])   # X'80' bit: last IDR
    text1 = b'\x07\xFE' * 0x80          # 256 bytes
    ctl1 = lm_control(0x01, [(1, 0x100)], 0x000, len(text1))
    text2 = b'\x18\xCF' * 0x40          # 128 bytes
    rld = h(1, 2) + h(1, 2) + b'\x0C' + h(0x104, 3)   # R ptr, P ptr, flag, address
    ctl2 = b'\x0F' + b'\x00\x00\x01' + h(4, 2) + h(len(rld), 2) + \
        b'\x06' + h(0x100, 3) + b'\x40\x00' + h(len(text2), 2) + rld + h(2, 2) + h(0x80, 2)
    rld_eom = lm_rld(0x0E, rld)
    return cesd + zap + le + tr1 + tr2 + ctl1 + text1 + ctl2 + text2 + rld_eom


def obj_card(kind, body):
    '''80-byte OBJ card: col 1 X'02', cols 2-4 type, rest per type.'''
    card = b'\x02' + e(kind) + body
    assert len(card) <= 80
    return card.ljust(80, b'\x40')


def build_obj():
    '''OBJ deck (Figures 6, 7, 9, 11):
    ESD: cols 5-10 blank, 11-12 byte count, 13-14 blank, 15-16 ESDID of
         first non-LD item, 17-64 up to 3 16-byte items
    TXT: col 5 blank, 6-8 address, 9-10 blank, 11-12 byte count, 13-14
         blank, 15-16 ESDID, 17-72 text
    END: col 5 blank, 6-8 entry address, 9-14 blank, 15-16 ESDID, 33 IDR
         flag X'F1', 34-43 translator, 44-45 version, 46-47 release,
         48-49 year, 50-52 day
    '''
    item_sd = e('HELLO', 8) + b'\x00' + h(0, 3) + b'\x02' + h(0x10, 3)    # SD AMODE 31
    item_ld = e('HELLO2', 8) + b'\x01' + h(4, 3) + b'\x40' + h(1, 3)      # LD in ESDID 1
    esd = obj_card('ESD', e('', 6) + h(32, 2) + e('', 2) + h(1, 2) + item_sd + item_ld)
    txt = obj_card('TXT', e(' ') + h(0, 3) + e('  ') + h(4, 2) + e('  ') + h(1, 2) + b'\x07\xFE\x07\xFE')
    end = obj_card('END', e(' ') + h(0, 3) + e('', 6) + h(1, 2) + e('', 16) + b'\x40' * 0 +
                   e('', 0)).ljust(32, b'\x40')
    end = end[:32] + b'\xF1' + e('569623400', 10) + e('01') + e('06') + e('24') + e('062')
    end = end.ljust(80, b'\x40')
    return esd + txt + end


def build_xobj():
    '''XOBJ: an ESD card with SD @@DOPLNK, then an XSD card (Figure in
    "XSD record format"): cols 11-12 byte count, 13-14 flags, 15-16 XSD id,
    17-20 total name length, 21-24 offset of this piece (origin 1), 25
    type, 26-28 address, 29 flags, 30-32 length, 33- name piece.'''
    doplnk = obj_card('ESD', e('', 6) + h(16, 2) + e('', 2) + h(1, 2) +
                      e('@@DOPLNK', 8) + b'\x00' + h(0, 3) + b'\x40' + h(0, 3))
    end = obj_card('END', b'')
    name = 'a_long_c_function_name'.encode('cp037')
    xsd = obj_card('XSD', e('', 6) + h(16 + len(name), 2) + b'\x00\x00' + h(2, 2) +
                   h(len(name), 4) + h(1, 4) + b'\x00' + h(0, 3) + b'\x02' + h(0x20, 3) + name)
    return doplnk + end + xsd + obj_card('END', b'')


def goff_record(rtype, body, cont=0):
    '''Fixed 80-byte GOFF record: byte 0 X'03', byte 1 = type<<4 |
    continuation flags (X'02' continuation, X'01' continued), byte 2
    version X'00'.'''
    return (b'\x03' + bytes([(rtype << 4) | cont]) + b'\x00' + body).ljust(80, b'\x00')


def build_goff():
    '''GOFF: HDR (arch level at 48-51), ESD SD "HELLO", ESD LD with a 30-byte
    name continued onto a second record, END.
    ESD layout (logical record): 3 symbol type, 4-7 ESDID, 8-11 parent,
    16-19 offset, 24-27 length, 40 name space, 70-71 name length, 72- name.'''
    hdr = goff_record(0xF, b'\x00' * 45 + h(1, 4))

    def esd(stype, esdid, parent, name):
        return bytes([stype]) + h(esdid, 4) + h(parent, 4) + b'\x00' * 4 + h(0, 4) + \
            b'\x00' * 4 + h(0, 4) + b'\x00' * 12 + b'\x01' + b'\x00' * 29 + h(len(name), 2) + name

    sd = goff_record(0x0, esd(0, 1, 0, e('HELLO')))
    long_name = e('A_VERY_LONG_ENTRY_POINT_NAME01')
    ld_logical = esd(2, 2, 1, long_name)      # 69 + 30 bytes after the 3-byte PTV
    first = goff_record(0x0, ld_logical[:77], cont=0x01)
    second = goff_record(0x0, ld_logical[77:], cont=0x02)
    end = goff_record(0x4, b'')
    return hdr + sd + first + second + end


def build_iebcopy(prefix='bare'):
    '''IEBCOPY unload (DFSMSdfp Utilities):
    COPYR1 (56 bytes): 0 flags (X'01' PDSE), 1-3 X'CA6D0F', 4-5 DS1DSORG
        (X'0200' = PO), 6-7 DS1BLKL, 8-9 DS1LRECL, 10 DS1RECFM, ...
        36-37 number of header records
    COPYR2 (276 bytes): DEB data, ignored here
    Directory: 276-byte units = count(8) + key length/data length(4) +
        key(8) + 256-byte block (2-byte used length, entries, X'FF'*8 end)
    prefix: 'bare' (as inside XMI), 'rdw' or 'bdw' (downloaded with
    descriptors).'''
    copyr1 = b'\x00\xCA\x6D\x0F' + b'\x02\x00' + h(3120, 2) + h(80, 2) + b'\x90' + b'\x00' * 25 + \
        h(2, 2) + b'\x00' * 18
    assert len(copyr1) == 56
    copyr2 = b'\x00' * 276
    entries = e('ALPHA', 8) + h(0x000105, 3) + b'\x00' + \
        e('BETA', 8) + h(0x000201, 3) + b'\x8F' + b'\x11' * 30 + \
        b'\xff' * 8
    block = h(len(entries) + 2, 2) + entries
    dirunit = b'\x00' * 8 + b'\x08\x00\x01\x00' + b'\xff' * 8 + block.ljust(256, b'\x00')
    assert len(dirunit) == 276
    records = [copyr1, copyr2, dirunit]
    if prefix == 'bare':
        return b''.join(records)
    rdws = [h(len(r) + 4, 2) + b'\x00\x00' + r for r in records]
    if prefix == 'rdw':
        return b''.join(rdws)
    return b''.join(h(len(r) + 4, 2) + b'\x00\x00' + r for r in rdws)


def smf_record(rty, sid='SYSA', subtype=None, hundredths=360000, date=b'\x01\x24\x06\x2F'):
    '''Standard SMF header (MVS System Management Facilities): RDW(4),
    FLG(1), RTY(1), TME(4, binary hundredths since midnight), DTE(4,
    packed 0cyydddF), SID(4); with subtypes (FLG X'40'): SSI(4), STY(2).'''
    flg = 0x5E if subtype is not None else 0x1E
    body = bytes([flg, rty]) + h(hundredths, 4) + date + e(sid, 4)
    if subtype is not None:
        body += e('JES2', 4) + h(subtype, 2)
    body += b'\x00' * 10
    return h(len(body) + 4, 2) + b'\x00\x00' + body


def vb_block(records):
    '''BDW (2-byte length incl. itself, 2 zero bytes) + RDW records.'''
    data = b''.join(records)
    return h(len(data) + 4, 2) + b'\x00\x00' + data


def rdw(body, seg=0):
    return h(len(body) + 4, 2) + bytes([seg, 0]) + body


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestLoadModule(unittest.TestCase):

    def setUp(self):
        self.lm = build_load_module()

    def test_identify(self):
        info = F.identify(self.lm)
        self.assertEqual(info.name, 'load-module')
        self.assertEqual(info.confidence, F.HIGH)
        self.assertEqual(info.extension, '.lmd')

    def test_parse(self):
        p = F.parse_load_module(self.lm)
        self.assertTrue(p['complete'])
        self.assertEqual(p['warnings'], [])
        self.assertEqual(p['csects'], ['MAIN', 'SUB'])
        self.assertEqual(p['main_csect'], 'MAIN')
        self.assertEqual((p['amode'], p['rmode']), ('31', '24'))
        sub = p['esd'][1]
        self.assertEqual((sub['amode'], sub['rmode']), ('ANY', 'ANY'))
        lr = p['esd'][2]
        self.assertEqual((lr['type'], lr['name'], lr['sd_esdid']), ('LR', 'ENTRY2', 1))
        self.assertEqual(p['esd'][3]['type'], 'ER')
        self.assertEqual(p['module_length'], 0x180)
        self.assertEqual(p['text_records'], 2)
        self.assertEqual(p['text_bytes'], 256 + 128)
        self.assertEqual(p['record_counts']['RLD-EOM'], 1)
        le = p['idr']['linkage_editor'][0]
        self.assertEqual(le['program'], '5695PMB01')
        self.assertEqual(le['version'], '02.04')
        self.assertEqual(le['date'], '2024-02-04')   # 24035 -> day 35
        self.assertEqual(le['time'], '12:34:56')
        tr = p['idr']['translators']
        self.assertEqual(len(tr), 1)
        self.assertEqual(tr[0]['esdids'], [1, 2])
        self.assertEqual(tr[0]['translators'][0]['program'], '569623400')

    def test_truncated_raises_parse_error(self):
        for cut in (1, 5, 30, 100, len(self.lm) - 1):
            with self.assertRaises(F.ParseError):
                F.parse_load_module(self.lm[:cut])

    def test_tolerant_mode_keeps_esd(self):
        bad = self.lm[:self.lm.index(b'\x01\x00\x00\x00')] + b'\x00' * 64
        with self.assertRaises(F.ParseError):
            F.parse_load_module(bad)
        p = F.parse_load_module(bad, strict=False)
        self.assertFalse(p['complete'])
        self.assertEqual(p['csects'], ['MAIN', 'SUB'])

    def test_pds2_user_data(self):
        # Real TSOUCB directory user data (CBT624): TTRT, 0, TTRN, NL,
        # ATR1/2, STOR, FTBL, EPA, FTB1, FTB2, FTB3
        ud = bytes.fromhex('000105000000000002e20005f805f800000098000100cb4996240100')
        d = F.parse_pds2_user_data(ud)
        self.assertEqual(d['entry_point'], 0)
        self.assertEqual(d['storage'], 0x5F8)
        self.assertTrue(d['executable'])
        self.assertEqual((d['amode'], d['rmode']), ('24', '24'))
        self.assertIsNone(F.parse_pds2_user_data(b'\x00' * 4))


@unittest.skipUnless(DAF_BIN.exists(), 'CBT094 DAF.bin sample not available')
class TestDAFSample(unittest.TestCase):

    def test_daf(self):
        data = DAF_BIN.read_bytes()
        self.assertEqual(len(data), 98468)
        self.assertEqual(F.identify(data).name, 'load-module')
        p = F.parse_load_module(data)
        self.assertTrue(p['complete'])
        self.assertEqual(p['main_csect'], 'DAF')
        for name in ('DAFINIT', 'DAFPROC', 'DAF014'):
            self.assertIn(name, p['csects'])
        self.assertEqual(len(p['csects']), 114)
        le = p['idr']['linkage_editor'][0]
        self.assertEqual(le['program'], '5695DF108')
        self.assertEqual(le['date'], '2003-08-29')      # the "08/29/03" in the IDR
        self.assertEqual(p['idr']['translators'][0]['translators'][0]['program'], '569623400')


class TestObjectDecks(unittest.TestCase):

    def test_synthetic_obj(self):
        deck = build_obj()
        info = F.identify(deck)
        self.assertEqual((info.name, info.confidence), ('obj', F.HIGH))
        p = F.parse_object_deck(deck)
        self.assertEqual(p['csects'], ['HELLO'])
        self.assertEqual(p['esd'][0]['amode'], '31')
        self.assertEqual(p['esd'][1]['type'], 'LD')
        self.assertEqual(p['text_bytes'], 4)
        self.assertEqual(p['entry_point'], {'address': 0, 'esdid': 1})
        self.assertEqual(p['idr'][0]['translator'], '569623400')
        self.assertEqual(p['idr'][0]['date'], '2024-03-02')

    def test_synthetic_xobj(self):
        deck = build_xobj()
        self.assertEqual(F.identify(deck).name, 'xobj')
        p = F.parse_object_deck(deck)
        self.assertEqual(p['variant'], 'xobj')
        self.assertIn('a_long_c_function_name', [x['name'] for x in p['esd']])

    def test_real_obj(self):
        data = (SAMPLES / 'NOTHING.obj').read_bytes()
        self.assertEqual(F.identify(data).name, 'obj')
        p = F.parse_object_deck(data)
        self.assertEqual(p['csects'], ['NOTHING'])
        self.assertEqual(p['idr'][0]['translator'], '5741SC103')

    def test_real_xobj(self):
        data = (SAMPLES / 'ADLER32.obj').read_bytes()
        self.assertEqual(F.identify(data).name, 'xobj')
        p = F.parse_object_deck(data)
        self.assertIn('adler32', p['csects'])

    def test_bad_length(self):
        with self.assertRaises(F.ParseError):
            F.parse_object_deck(build_obj()[:-1])


class TestGOFF(unittest.TestCase):

    def test_synthetic(self):
        data = build_goff()
        info = F.identify(data)
        self.assertEqual((info.name, info.confidence), ('goff', F.HIGH))
        p = F.parse_goff(data)
        self.assertEqual(p['hdr']['architecture_level'], 1)
        self.assertEqual(p['sections'], ['HELLO'])
        self.assertEqual(p['entry_points'], ['A_VERY_LONG_ENTRY_POINT_NAME01'])
        self.assertEqual(p['record_counts'], {'HDR': 1, 'ESD': 2, 'END': 1})

    def test_variable_with_rdw(self):
        recs = [build_goff()[i:i + 80] for i in (0, 80)]
        data = b''.join(rdw(r) for r in recs)
        self.assertEqual(F.identify(data).name, 'goff')
        self.assertEqual(F.parse_goff(data)['record_format'], 'variable')

    def test_real(self):
        data = (SAMPLES / 'UIMASM.gof').read_bytes()
        self.assertEqual(F.identify(data).name, 'goff')
        p = F.parse_goff(data)
        self.assertEqual(p['sections'], ['UIMASM'])
        self.assertIn('CEESTART', p['externals'])

    def test_dangling_continuation(self):
        data = build_goff()
        with self.assertRaises(F.ParseError):
            F.parse_goff(data[:160 + 80])   # ends on a "continued" record


class TestIEBCOPY(unittest.TestCase):

    def test_all_prefix_forms(self):
        for prefix in ('bare', 'rdw', 'bdw'):
            with self.subTest(prefix=prefix):
                data = build_iebcopy(prefix)
                info = F.identify(data)
                self.assertEqual((info.name, info.confidence), ('iebcopy', F.HIGH))
                p = F.parse_iebcopy(data)
                self.assertEqual(p['type'], 'PDS')
                self.assertEqual((p['recfm'], p['lrecl'], p['blksize']), ('FB', 80, 3120))
                self.assertEqual([m['name'] for m in p['members']], ['ALPHA', 'BETA'])
                self.assertTrue(p['members'][1]['alias'])
                self.assertEqual(len(p['members'][1]['user_data']), 30)

    def test_from_real_xmi(self):
        # The IEBCOPY records inside test_pds.xmi, concatenated bare
        records = []
        orig = xmi.XMIT.iebcopy_record_1

        def grab(self_, first):
            records.append(first)
            return orig(self_, first)
        xmi.XMIT.iebcopy_record_1 = grab
        try:
            x = xmi.XMIT(filename=str(TESTS_DIR / 'test_pds.xmi'), quiet=True)
            x.open()
        finally:
            xmi.XMIT.iebcopy_record_1 = orig
        self.assertEqual(F.identify(records[0] + b'\x00' * 300).name, 'iebcopy')

    def test_truncated(self):
        with self.assertRaises(F.ParseError):
            F.parse_iebcopy(build_iebcopy()[:100])


class TestTerse(unittest.TestCase):
    # Headers per openterse src/header.c

    def test_host_pack(self):
        hdr = b'\x02\x00' + h(80, 2) + b'\x00\x00\x00\x00' + h(0, 4)
        info = F.identify(hdr + b'\x00' * (1024 - 12))
        self.assertEqual(info.name, 'terse')
        p = F.parse_terse_header(hdr)
        self.assertEqual((p['method'], p['record_length'], p['variable']), ('PACK', 80, False))

    def test_host_spack_vb(self):
        hdr = b'\x05\x01' + h(0, 2) + b'\x0C\x00' + h(27998, 2) + h(100000, 4)
        self.assertEqual(F.identify(hdr + b'\x00' * 500).name, 'terse')
        self.assertEqual(F.parse_terse_header(hdr)['record_length'], 100000)

    def test_native_spack(self):
        self.assertEqual(F.identify(b'\x07\x89\x69\xA5' + b'\x00' * 60).confidence, F.HIGH)

    def test_rejects(self):
        for bad in (b'\x02\x02' + b'\x00' * 10,                     # bad variable flag
                    b'\x02\x00' + h(80, 2) + b'\x00\x01' + b'\x00' * 6,  # non-MVS with ratio
                    b'\x02\x00\x00\x50' + b'\x00' * 4 + h(81, 4),   # inconsistent lengths
                    b'\x01\x89\x69'):                               # short
            with self.assertRaises(F.ParseError):
                F.parse_terse_header(bad)

    def test_decompress_needs_openterse(self):
        try:
            import openterse  # noqa: F401
        except ImportError:
            with self.assertRaises(F.ParseError):
                F.decompress_terse(b'\x07\x89\x69\xA5')


class TestVBAndSMF(unittest.TestCase):

    def test_vb(self):
        data = vb_block([rdw(e('HELLO')), rdw(e('WORLD!'))]) + vb_block([rdw(b'\x01\x02')])
        self.assertEqual(F.identify(data).name, 'vb')
        p = F.parse_vb(data)
        self.assertTrue(p['blocked'])
        self.assertEqual(p['records'], [e('HELLO'), e('WORLD!'), b'\x01\x02'])

    def test_vbs_spanned(self):
        data = vb_block([rdw(b'AB', seg=1)]) + vb_block([rdw(b'CD', seg=3), rdw(b'EF', seg=2)])
        self.assertEqual(F.parse_vb(data)['records'], [b'ABCDEF'])
        with self.assertRaises(F.ParseError):
            F.parse_vb(vb_block([rdw(b'AB', seg=1)]))

    def test_bad_descriptors(self):
        with self.assertRaises(F.ParseError):
            F.parse_vb(b'\x00\x10\x00\x00\x00\x08\x00\x00', blocked=True)
        with self.assertRaises(F.ParseError):
            F.parse_vb(b'\x00\x02\x00\x00', blocked=False)

    def test_smf(self):
        recs = [smf_record(30, subtype=4), smf_record(30, subtype=5), smf_record(14),
                smf_record(70, subtype=1, sid='SYSB', date=b'\x01\x24\x06\x3F')]
        data = vb_block(recs[:2]) + vb_block(recs[2:])
        info = F.identify(data)
        self.assertEqual((info.name, info.confidence), ('smf', F.HIGH))
        self.assertEqual(info.candidates[0].name, 'vb')
        p = F.parse_smf(data)
        self.assertEqual(p['records'], 4)
        self.assertEqual(p['types'], {30: 2, 14: 1, 70: 1})
        self.assertEqual(p['subtypes'], {'30.4': 1, '30.5': 1, '70.1': 1})
        self.assertEqual(p['systems'], ['SYSA', 'SYSB'])
        self.assertEqual(p['first'], '2024-03-02T01:00:00')   # 2024 day 062, 360000/100 s = 01:00
        self.assertEqual(p['last'], '2024-03-03T01:00:00')

    def test_smf_rdw_only(self):
        data = smf_record(30) + smf_record(30)
        self.assertEqual(F.identify(data).name, 'smf')
        self.assertEqual(F.parse_smf(data)['records'], 2)

    def test_smf_bad_date_is_not_smf(self):
        bad = smf_record(30, date=b'\x01\x24\x99\x9F') * 2   # day 999
        self.assertNotEqual(F.identify(bad).name, 'smf')


class TestOtherDetectors(unittest.TestCase):

    def test_program_object(self):
        # Unloaded PDSE program objects start with the 'IEWPLMH' eye-catcher
        data = e('IEWPLMH ') + b'\x00\x00\x00\x78\x03' + b'\x00' * 200
        self.assertEqual(F.identify(data).name, 'program-object')

    def test_iebupdte(self):
        deck = e('./ ADD NAME=MEMBER1', 80) + e('//JOB1 JOB', 80) + e('./ ENDUP', 80)
        info = F.identify(deck)
        self.assertEqual(info.name, 'iebupdte')
        self.assertEqual(F.identify(b'./ ADD NAME=FOO,LIST=ALL\n').name, 'iebupdte')

    def test_pds_directory(self):
        # 256-byte block: used length(2), entries name(8) TTR(3) C(1) user data
        entries = e('AAA', 8) + b'\x00\x00\x05\x00' + e('BBB', 8) + b'\x00\x00\x06\x00' + b'\xff' * 8
        block = (h(len(entries) + 2, 2) + entries).ljust(256, b'\x00')
        self.assertEqual(F.identify(block).name, 'pds-directory')
        unsorted = entries.replace(e('AAA', 8), e('ZZZ', 8))
        self.assertNotEqual(F.identify((h(len(unsorted) + 2, 2) + unsorted).ljust(256, b'\x00')).name,
                            'pds-directory')

    def test_text(self):
        self.assertEqual(F.identify(e('HELLO WORLD THIS IS SOME EBCDIC TEXT ', 80) * 3).name, 'ebcdic-text')
        self.assertEqual(F.identify(b'hello world, plain ascii text\n' * 4).name, 'ascii-text')

    def test_unknown(self):
        for data in (b'', b'\x00' * 100, b'\x89PNG\r\n\x1a\n' + b'\x00' * 100, bytes(range(256))):
            info = F.identify(data)
            self.assertEqual(info.name, 'unknown')
            self.assertFalse(info)

    def test_codepage_configurable(self):
        p037 = F.parse_object_deck(build_obj(), codepage='cp037')
        p1047 = F.parse_object_deck(build_obj(), codepage='cp1047')
        self.assertEqual(p037['csects'], p1047['csects'])

    def test_ambiguous(self):
        a = F._info('obj', F.HIGH)
        b = F._info('terse', F.HIGH)
        saved = F._DETECTORS
        F._DETECTORS = [('a', lambda *x: a), ('b', lambda *x: b)]
        try:
            info = F.identify(b'x')
        finally:
            F._DETECTORS = saved
        self.assertEqual(info.name, 'ambiguous')
        self.assertEqual([c.name for c in info.candidates], ['obj', 'terse'])
        self.assertFalse(info)

    def test_non_bytes(self):
        with self.assertRaises(F.ParseError):
            F.identify('text')
        with self.assertRaises(F.ParseError):
            F.parse_load_module('text')


class TestCodepages(unittest.TestCase):
    '''EBCDIC code page detection (detect_ebcdic_codepage) and the narrow
    switching policy used when converting text (choose_codepage).'''

    C_SOURCE = ('#pragma pack(packed)\nstruct lda {\n  int argv[4];\n'
                '  char name[8];\n};\n') * 6

    @staticmethod
    def fb80(text, codepage):
        '''Fixed 80-byte EBCDIC records, as text members are stored.'''
        return b''.join(line.encode(codepage).ljust(80, b'\x40') for line in text.splitlines())

    def test_c_source_cp1047(self):
        data = self.fb80(self.C_SOURCE, 'cp1047')
        found = F.detect_ebcdic_codepage(data, preferred='cp1140')
        self.assertEqual(found['codepage'], 'cp1047')
        self.assertTrue(found['certain'])
        self.assertFalse(F.codepage_fits('cp1140', found))
        self.assertEqual(F.choose_codepage(data, 'cp1140'), 'cp1047')
        self.assertEqual(F.choose_codepage(data, 'cp037'), 'cp1047')

    def test_c_source_cp037_unchanged(self):
        data = self.fb80(self.C_SOURCE, 'cp037')
        self.assertIsNone(F.choose_codepage(data, 'cp1140'))
        self.assertEqual(F.detect_ebcdic_codepage(data, preferred='cp1140')['codepage'], 'cp1140')

    def test_plain_text_keeps_configured_page(self):
        data = self.fb80('//MYJOB JOB (ACCT),CLASS=A\n//STEP1 EXEC PGM=IEFBR14\n' * 10, 'cp037')
        found = F.detect_ebcdic_codepage(data, preferred='cp1140')
        self.assertEqual(found['codepage'], 'cp1140')   # euro variant of cp037 kept
        self.assertGreater(len(found['codepages']), 5)  # many pages fit A-Z text
        self.assertIsNone(F.choose_codepage(data, 'cp1140'))

    def test_mvs_syntax_never_switches_to_national_page(self):
        # ISPF panel attributes and assembler/JCL names use $ # @ ! | which
        # are national letters in other pages. These must not switch.
        panel = (')ATTR DEFAULT(%+_)\n  ! TYPE(OUTPUT) INTENS(LOW)\n  @ TYPE(TEXT)\n'
                 '  $ TYPE(INPUT) CAPS(ON)\n)BODY\n%---- #DCBD @FILE $X ----\n') * 5
        data = self.fb80(panel, 'cp037')
        self.assertIsNone(F.choose_codepage(data, 'cp1140'))

    def test_national_pages_detected_but_not_automatic(self):
        german = 'Die Größe der Datei ist für den Übertrag zu groß, bitte prüfen. ' * 12
        data = german.encode('cp273')
        found = F.detect_ebcdic_codepage(data)
        self.assertEqual(found['codepage'], 'cp273')
        self.assertIsNone(F.choose_codepage(data, 'cp1140'))
        info = F.identify(data)
        self.assertEqual((info.name, info.codepage), ('ebcdic-text', 'cp273'))

    def test_non_latin_scripts(self):
        # Non-Latin pages put letters at the same bytes (Greek cp875 and
        # Cyrillic cp1025; Cyrillic and Arabic cp420), so the text decodes
        # plausibly in another script too: detection ranks the right page
        # first but is not certain, and therefore does not switch.
        for text, cp in (('Το αρχείο δεν μπορεί να ανοίξει λόγω σφάλματος στο σύστημα. ', 'cp875'),
                         ('Файл не может быть открыт из-за ошибки в системе управления. ', 'cp1025')):
            with self.subTest(cp=cp):
                data = (text * 12).encode(cp)
                found = F.detect_ebcdic_codepage(data)
                self.assertEqual(found['codepage'], cp)
                self.assertGreater(len(found['codepages']), 1)
                self.assertFalse(found['certain'])
                self.assertIsNone(F.choose_codepage(data, 'cp1140'))

    def test_tie_reported(self):
        # Hebrew letters sit where cp875 has upper case Greek: a real tie
        data = ('הקובץ לא יכול להיפתח בגלל שגיאה במערכת ההפעלה הזאת. ' * 12).encode('cp424')
        found = F.detect_ebcdic_codepage(data)
        self.assertIn('cp424', found['codepages'])
        self.assertFalse(found['certain'])
        self.assertIsNone(F.choose_codepage(data, 'cp1140'))   # uncertain: no switch
        self.assertNotEqual(F.identify(data).name, 'ascii-text')

    def test_panel_attribute_bytes_are_not_greek(self):
        # Regression: CBT483 WORMPH (tests/samples/WORMPH.pnl), an ISPF
        # panel whose attribute bytes are Greek letters in cp875. cp875
        # scored >5% better than cp037, but only ~11% of its letters are
        # Greek, so it must not switch.
        data = (SAMPLES / 'WORMPH.pnl').read_bytes()
        self.assertEqual(F.detect_ebcdic_codepage(data)['codepage'], 'cp875')
        self.assertIsNone(F.choose_codepage(data, 'cp1140'))

    def test_no_break_space_is_not_greek(self):
        # X'41' is a no-break space in Latin pages and Alpha in cp875
        data = ('Subject:\xa0\xa0MXI 4.3 LPD command\xa0\xa0from John\xa0\xa0' * 20).encode('cp037')
        self.assertIsNone(F.choose_codepage(data, 'cp1140'))

    def test_not_text(self):
        self.assertIsNone(F.detect_ebcdic_codepage(build_load_module()))
        self.assertIsNone(F.detect_ebcdic_codepage(b'hello ascii world\n' * 50))
        self.assertIsNone(F.detect_ebcdic_codepage(b'\x40' * 10))

    def test_xmit_conversion(self):
        data = self.fb80(self.C_SOURCE, 'cp1047')
        convert = '_XMIT__get_file_mimetype_and_convert'
        on = getattr(xmi.XMIT(quiet=True, encoding='cp1140'), convert)(
            file_name='T', file_data=data, recfm='FB', lrecl=80)
        self.assertIn('int argv[4];', on['text'])
        self.assertEqual(on['mvs_codepage'], 'cp1047')
        off = getattr(xmi.XMIT(quiet=True, encoding='cp1140', detect_codepage=False), convert)(
            file_name='T', file_data=data, recfm='FB', lrecl=80)
        self.assertIn('int argvÝ4¨;', off['text'])
        self.assertNotIn('mvs_codepage', off)

    def test_xmit_plain_text_unchanged(self):
        data = self.fb80('//MYJOB JOB (ACCT),CLASS=A\n//STEP1 EXEC PGM=IEFBR14\n' * 10, 'cp037')
        r = getattr(xmi.XMIT(quiet=True, encoding='cp1140'), '_XMIT__get_file_mimetype_and_convert')(
            file_name='T', file_data=data, recfm='FB', lrecl=80)
        self.assertNotIn('mvs_codepage', r)
        self.assertTrue(r['text'].startswith('//MYJOB JOB (ACCT),CLASS=A'))


class TestRobustness(unittest.TestCase):
    '''Corrupt or truncated input must only ever raise ParseError.'''

    PARSERS = [
        (F.parse_load_module, build_load_module),
        (F.parse_object_deck, build_obj),
        (F.parse_object_deck, build_xobj),
        (F.parse_goff, build_goff),
        (F.parse_iebcopy, build_iebcopy),
        (F.parse_smf, lambda: vb_block([smf_record(30, subtype=1), smf_record(14)])),
        (F.parse_vb, lambda: vb_block([rdw(b'abc'), rdw(b'de', seg=1)]) + vb_block([rdw(b'f', seg=2)])),
        (F.parse_terse_header, lambda: b'\x05\x01' + h(80, 2) + b'\x00' * 8),
    ]

    def test_fuzz(self):
        rng = random.Random(1234)
        for parser, build in self.PARSERS:
            good = build()
            for _ in range(300):
                data = bytearray(good)
                choice = rng.random()
                if choice < 0.4:
                    data = data[:rng.randrange(len(data) + 1)]
                else:
                    for _ in range(rng.randint(1, 6)):
                        data[rng.randrange(len(data))] = rng.randrange(256)
                try:
                    parser(bytes(data))
                except F.ParseError:
                    pass
                try:
                    F.inspect(bytes(data))
                except Exception as exc:   # inspect never raises
                    self.fail('inspect raised {!r}'.format(exc))

    def test_random_noise(self):
        rng = random.Random(99)
        for _ in range(500):
            data = bytes(rng.randrange(256) for _ in range(rng.randrange(1, 600)))
            info = F.identify(data)
            self.assertIn(info.confidence, (F.HIGH, F.MEDIUM, F.LOW, F.NONE))


class TestIntegration(unittest.TestCase):

    def test_loadlib_member_labelled(self):
        x = xmi.XMIT(filename=str(SAMPLES / 'LOADLIB.xmi'), quiet=True)
        x.open()
        info = x.get_member_info('SBGOLOB.TSOUCB.LOAD', 'TSOUCB')
        self.assertEqual(info['mvs_format'], 'load-module')
        self.assertEqual(info['extension'], '.lmd')
        self.assertEqual(info['mimetype'], 'application/octet-stream')

        result = x.get_member_format('SBGOLOB.TSOUCB.LOAD', 'TSOUCB')
        d = result['details']
        self.assertEqual(d['main_csect'], 'TSOUCB')
        self.assertEqual(d['directory']['entry_point'], 0)
        self.assertEqual(d['directory']['amode'], '24')
        self.assertEqual(d['idr']['linkage_editor'][0]['program'], '5695PMB01')

    def test_text_members_unchanged(self):
        x = xmi.XMIT(filename=str(TESTS_DIR / 'test_pds.xmi'), quiet=True)
        x.open()
        for m in x.get_members('PYTHON.XMI.PDS'):
            self.assertNotIn('mvs_format', x.get_member_info('PYTHON.XMI.PDS', m))

    def test_unload_uses_extension(self):
        import tempfile
        with tempfile.TemporaryDirectory() as out:
            x = xmi.XMIT(filename=str(SAMPLES / 'LOADLIB.xmi'), quiet=True, outputfolder=out)
            x.open()
            x.unload_files()
            self.assertTrue(os.path.exists(os.path.join(out, 'SBGOLOB.TSOUCB.LOAD', 'TSOUCB.lmd')))


if __name__ == '__main__':
    unittest.main(verbosity=2)
