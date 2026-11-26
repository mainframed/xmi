"""
xmi.formats
~~~~~~~~~~~

Detection and inspection of MVS / z/OS binary file formats that libmagic
(``file``) does not recognise.

These show up constantly when unpacking CBT Tape and similar archives:
unloaded load modules, object decks, GOFF objects, IEBCOPY unloads, TERSEd
files, SMF dumps and so on. libmagic reports all of them as ``data``.

Two entry points:

* :func:`identify` looks at the first few KB only, so it is cheap. It is
  conservative: when nothing matches with confidence it returns ``unknown``,
  and when two unrelated formats match equally well it returns
  ``ambiguous`` together with the candidates.
* :func:`inspect` identifies the data and then runs the matching parser,
  which returns a ``dict`` of structured information.

All parsers treat their input as untrusted bytes. Every read is bounds
checked. Truncated or corrupt input raises :class:`ParseError`, never
``IndexError`` or ``struct.error``. Nothing in the input is ever executed.

Record layouts follow *z/OS MVS Program Management: Advanced Facilities*
(SA23-1392, z/OS 2.5 edition: Appendix A object modules, Appendix B load
modules, Appendix C GOFF, PDS2 directory mapping), *DFSMSdfp Utilities*
(IEBCOPY unload) and *MVS System Management Facilities* (standard SMF
header). See ``docs/formats.rst`` for the signature table.
"""

import logging
from dataclasses import dataclass, field
from typing import Callable, List, Optional, Tuple

import ebcdic  # noqa: F401  (registers the cp1047 codec, among others)

logger = logging.getLogger(__name__)

#: Default code page used to decode EBCDIC names. ``cp037`` is also common;
#: every public function takes a ``codepage`` argument.
DEFAULT_CODEPAGE = 'cp1047'

#: How many bytes :func:`identify` looks at.
SNIFF_SIZE = 4096

HIGH = 'high'
MEDIUM = 'medium'
LOW = 'low'
NONE = 'none'
_RANK = {HIGH: 3, MEDIUM: 2, LOW: 1, NONE: 0}


class ParseError(ValueError):
    '''Raised when MVS format data is truncated, corrupt or not the
    format the parser expects.'''


@dataclass
class FormatInfo:
    '''Result of :func:`identify`.

    Attributes:
        name (str): short format name, e.g. ``load-module``. ``unknown``
            when nothing matched, ``ambiguous`` when several unrelated
            formats matched equally well (see ``candidates``).
        confidence (str): ``high``, ``medium``, ``low`` or ``none``.
        description (str): one-line human readable description.
        extension (str): suggested file extension including the period,
            or ``None``.
        mimetype (str): suggested mimetype (``application/x-mvs-...``).
        candidates (list): other formats that also matched, best first.
        codepage (str): for EBCDIC text, the code page it decodes best
            with (``None`` otherwise).
        codepages (list): for EBCDIC text, every code page that decodes it
            equally well (plain A-Z text fits most Latin pages).
    '''
    name: str
    confidence: str
    description: str
    extension: Optional[str] = None
    mimetype: str = 'application/octet-stream'
    candidates: List['FormatInfo'] = field(default_factory=list)
    codepage: Optional[str] = None
    codepages: List[str] = field(default_factory=list)

    def __bool__(self):
        return self.name not in ('unknown', 'ambiguous')


# Format registry: name -> (description, extension, mimetype)
FORMATS = {
    'load-module': ('MVS load module (unloaded PDS member, RECFM=U)', '.lmd',
                    'application/x-mvs-load-module'),
    'program-object': ('z/OS program object (PDSE binder format, IEWPLMH)', '.pob',
                       'application/x-mvs-program-object'),
    'obj': ('MVS object deck (OBJ, 80-byte card images)', '.obj',
            'application/x-mvs-object'),
    'xobj': ('MVS extended object deck (XOBJ, with XSD records)', '.obj',
             'application/x-mvs-object'),
    'goff': ('GOFF object module (Generalized Object File Format)', '.gof',
             'application/x-mvs-goff'),
    'iebcopy': ('IEBCOPY unloaded PDS/PDSE', '.ieb', 'application/x-mvs-iebcopy'),
    'terse': ('TERSE / AMATERSE / TRSMAIN packed file', '.trs',
              'application/x-mvs-terse'),
    'smf': ('SMF records', '.smf', 'application/x-mvs-smf'),
    'vb': ('Variable-length records with BDW/RDW descriptors', '.vb',
           'application/x-mvs-vb'),
    'rdw': ('Variable-length records with RDW descriptors (no BDW)', '.vb',
            'application/x-mvs-vb'),
    'iebupdte': ('IEBUPDTE input (./ ADD NAME=...)', '.upd',
                 'application/x-mvs-iebupdte'),
    'pds-directory': ('PDS directory blocks (256-byte)', '.dir',
                      'application/x-mvs-pds-directory'),
    'aws': ('AWS virtual tape image', '.aws', 'application/x-aws-tape'),
    'het': ('HET virtual tape image (compressed AWS)', '.het',
            'application/x-het-tape'),
    'pdspack': ('ALGOL 68C packed PDS (PDSUNPK format, AWS-framed)', '.pak',
                'application/x-a68c-packed-pds'),
    'ebcdic-text': ('EBCDIC text', '.txt', 'text/plain'),
    'ascii-text': ('ASCII text', '.txt', 'text/plain'),
}

# Formats that are a more specific reading of another format. When both
# match, the general one is not treated as a competing candidate.
_REFINES = {
    'smf': {'vb', 'rdw'},
    'iebcopy': {'vb', 'rdw'},
    'xobj': {'obj'},
    'iebupdte': {'ebcdic-text', 'ascii-text'},
    'goff': {'rdw'},
}


def _info(name, confidence, extra=''):
    desc, ext, mime = FORMATS[name]
    if extra:
        desc = '{} - {}'.format(desc, extra)
    return FormatInfo(name, confidence, desc, ext, mime)


# ---------------------------------------------------------------------------
# Bounds-checked reading helpers
# ---------------------------------------------------------------------------

class _Reader:
    '''Bounds-checked big-endian access to a bytes object.'''

    __slots__ = ('data', 'what')

    def __init__(self, data, what='data'):
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise ParseError('{}: expected bytes, got {}'.format(what, type(data).__name__))
        self.data = bytes(data)
        self.what = what

    def __len__(self):
        return len(self.data)

    def bytes(self, off, n):
        if off < 0 or n < 0 or off + n > len(self.data):
            raise ParseError('{}: truncated, need {} bytes at offset {} but only {} available'.format(
                self.what, n, off, max(0, len(self.data) - off)))
        return self.data[off:off + n]

    def u8(self, off):
        return self.bytes(off, 1)[0]

    def u16(self, off):
        return int.from_bytes(self.bytes(off, 2), 'big')

    def u24(self, off):
        return int.from_bytes(self.bytes(off, 3), 'big')

    def u32(self, off):
        return int.from_bytes(self.bytes(off, 4), 'big')


def _decode(raw, codepage):
    return raw.decode(codepage, errors='replace')


def _name(raw, codepage):
    '''Decodes an EBCDIC name, dropping trailing blanks and nulls.'''
    return _decode(raw, codepage).rstrip(' \x00')


# Bytes that may appear in an external symbol name: EBCDIC A-Z, 0-9,
# @ # $ _ and blank (padding). Lower case is allowed for long names.
_EBCDIC_NAME = set(range(0xC1, 0xCA)) | set(range(0xD1, 0xDA)) | \
    set(range(0xE2, 0xEA)) | set(range(0xF0, 0xFA)) | \
    set(range(0x81, 0x8A)) | set(range(0x91, 0x9A)) | set(range(0xA2, 0xAA)) | \
    {0x40, 0x7C, 0x7B, 0x5B, 0x6D, 0x4B, 0x60}


def _is_ebcdic_name(raw, allow_blank=False):
    if not raw:
        return False
    if raw == b'\x40' * len(raw) or raw == b'\x00' * len(raw):
        return allow_blank
    if raw[0] == 0x40:  # names are left justified
        return False
    return all(b in _EBCDIC_NAME for b in raw)


def _packed_digits(raw):
    '''Returns the digits of a packed decimal field as a string, sign
    nibble removed. Returns ``None`` if any digit nibble is invalid.'''
    digits = ''
    for i, b in enumerate(raw):
        hi, lo = b >> 4, b & 0x0F
        if hi > 9:
            return None
        digits += str(hi)
        if i == len(raw) - 1:
            if lo < 0x0A:  # last nibble must be a sign
                return None
        else:
            if lo > 9:
                return None
            digits += str(lo)
    return digits


def _unsigned_packed(raw):
    '''Unsigned packed decimal (no sign nibble), e.g. VVMM.'''
    digits = ''
    for b in raw:
        if (b >> 4) > 9 or (b & 0x0F) > 9:
            return None
        digits += '{}{}'.format(b >> 4, b & 0x0F)
    return digits


def _yyddd(raw):
    '''Packed decimal YYDDD (3 bytes, ``0yydddF``-style) -> ISO date
    string. Years 65-99 are 19xx, 00-64 are 20xx (as documented for IDR
    data). Returns ``None`` when the field is empty or invalid.'''
    import datetime
    digits = _packed_digits(raw)
    if not digits or len(digits) < 5:
        return None
    yy, ddd = int(digits[-5:-3]), int(digits[-3:])
    if not 1 <= ddd <= 366:
        return None
    year = 1900 + yy if yy >= 65 else 2000 + yy
    try:
        date = datetime.date(year, 1, 1) + datetime.timedelta(days=ddd - 1)
    except (ValueError, OverflowError):
        return None
    if date.year != year:
        return None
    return date.isoformat()


def _version(raw):
    vvmm = _unsigned_packed(raw)
    if vvmm is None or len(vvmm) != 4:
        return None
    return '{}.{}'.format(vvmm[:2], vvmm[2:])


# ---------------------------------------------------------------------------
# Load modules (Program Management: Advanced Facilities, Appendix B)
# ---------------------------------------------------------------------------

# Record identification byte (byte 0)
LM_SYM = 0x40
LM_CESD = 0x20
LM_SCATTER = 0x10
LM_IDR = 0x80
LM_CONTROL = (0x01, 0x05, 0x0D)        # control; EOS; EOM
LM_RLD = (0x02, 0x06, 0x0E)            # RLD; last of segment; last of module
LM_CONTROL_RLD = (0x03, 0x07, 0x0F)    # control+RLD; EOS; EOM
LM_SIGNING = 0x88                      # signing records after the nominal last record

LM_RECORD_NAMES = {
    0x40: 'SYM', 0x20: 'CESD', 0x10: 'SCATTER', 0x80: 'IDR',
    0x01: 'CONTROL', 0x05: 'CONTROL-EOS', 0x0D: 'CONTROL-EOM',
    0x02: 'RLD', 0x06: 'RLD-EOS', 0x0E: 'RLD-EOM',
    0x03: 'CONTROL-RLD', 0x07: 'CONTROL-RLD-EOS', 0x0F: 'CONTROL-RLD-EOM',
    0x88: 'SIGNING', 'TEXT': 'TEXT',
}

# CESD ESD entry type, low nibble of byte 8 (Figure 13)
CESD_TYPES = {
    0x0: 'SD', 0x3: 'LR', 0x4: 'PC', 0x5: 'CM', 0x6: 'PR', 0x7: 'NULL',
    0x2: 'ER', 0xA: 'WX', 0xD: 'SD', 0xE: 'PC', 0xF: 'CM',
}

AMODES = {0: '24', 1: '24', 2: '31', 3: 'ANY'}


def _lm_record_length(r, pos):
    '''Returns ``(record_length, following_text_length)`` for the load
    module record at ``pos``.

    Load module members are RECFM=U. Once unloaded without RDW/BDW the
    record boundaries are lost, but every record type carries enough
    information to work out its own length:

    =====================  =================================================
    Record (byte 0)        Length
    =====================  =================================================
    SYM X'40'              4 + count at bytes 2-3
    CESD X'20'             8 + count at bytes 6-7 (16-byte entries, max 240)
    Scatter/transl X'10'   4 + count at bytes 2-3
    IDR X'80'              1 + byte count at byte 1 (count includes itself)
    Control X'01/05/0D'    16 + control-data count at bytes 4-5
    RLD X'02/06/0E'        16 + RLD-data count at bytes 6-7
    Ctl+RLD X'03/07/0F'    16 + control count (4-5) + RLD count (6-7)
    Signing X'88'          REC_LENGTH at bytes 4-5
    =====================  =================================================

    A control (or control+RLD) record is followed by exactly one text
    record whose length is the CCW count, the last halfword of the CCW at
    bytes 8-15 (i.e. bytes 14-15).
    '''
    rid = r.u8(pos)
    if rid == LM_CESD:
        return 8 + r.u16(pos + 6), 0
    if rid in (LM_SYM, LM_SCATTER):
        return 4 + r.u16(pos + 2), 0
    if rid == LM_IDR:
        return 1 + r.u8(pos + 1), 0
    if rid in LM_CONTROL:
        return 16 + r.u16(pos + 4), r.u16(pos + 14)
    if rid in LM_RLD:
        return 16 + r.u16(pos + 6), 0
    if rid in LM_CONTROL_RLD:
        return 16 + r.u16(pos + 4) + r.u16(pos + 6), r.u16(pos + 14)
    if rid == LM_SIGNING:
        return r.u16(pos + 4), 0
    raise ParseError('load module: unknown record id X\'{:02X}\' at offset {}'.format(rid, pos))


def iter_load_module_records(data):
    '''Yields ``(record_id, offset, length)`` for each load module record,
    including text records (``record_id == 'TEXT'``).

    Raises :class:`ParseError` on an unknown record id or a record that
    runs past the end of the data.
    '''
    r = _Reader(data, 'load module')
    pos = 0
    while pos < len(r):
        rid = r.u8(pos)
        length, text_len = _lm_record_length(r, pos)
        if length < 4:
            raise ParseError('load module: record X\'{:02X}\' at offset {} has impossible length {}'.format(rid, pos, length))
        r.bytes(pos, length)  # bounds check
        yield rid, pos, length
        pos += length
        if text_len:
            if pos >= len(r):
                raise ParseError('load module: text record expected at offset {} but data ended'.format(pos))
            r.bytes(pos, text_len)
            yield 'TEXT', pos, text_len
            pos += text_len


def _parse_cesd(r, pos, length, codepage):
    flags = r.u8(pos + 1)
    first_id = r.u16(pos + 4)
    count = r.u16(pos + 6)
    if count % 16 or count > 240:
        raise ParseError('load module: CESD at offset {} has bad ESD byte count {}'.format(pos, count))
    has_modes = bool(flags & 0x80)
    entries = []
    for i in range(count // 16):
        e = pos + 8 + i * 16
        raw_type = r.u8(e + 8)
        etype = CESD_TYPES.get(raw_type & 0x0F, 'X{:02X}'.format(raw_type))
        entry = {
            'esdid': first_id + i,
            'name': _name(r.bytes(e, 8), codepage) if etype != 'NULL' else '',
            'type': etype,
            'type_byte': raw_type,
            'address': r.u24(e + 9),
        }
        if (raw_type & 0x17) == 0x14:
            entry['deleted'] = True
        b12 = r.u8(e + 12)
        if etype == 'LR':
            entry['sd_esdid'] = r.u16(e + 14)
        else:
            entry['length'] = r.u24(e + 13)
        if etype in ('SD', 'PC'):
            if has_modes:
                entry['amode'] = '64' if b12 & 0x10 else AMODES[b12 & 0x03]
                entry['rmode'] = '64' if b12 & 0x20 else ('ANY' if b12 & 0x04 else '24')
                entry['rsect'] = bool(b12 & 0x08)
            else:
                entry['segment'] = b12
        elif etype == 'PR':
            entry['alignment'] = b12
        elif etype in ('CM', 'LR') and not has_modes:
            entry['segment'] = b12
        entries.append(entry)
    return entries


def _parse_idr(r, pos, length, codepage):
    '''Parses one CSECT identification record (Figures 18-20).'''
    sub = r.u8(pos + 2)
    idr = {'last': bool(sub & 0x80)}
    data = _Reader(r.bytes(pos + 3, length - 3), 'IDR')
    kind = sub & 0x0F
    if kind == 0x02:
        idr['type'] = 'linkage-editor'
        idr['program'] = _name(data.bytes(0, 10), codepage)
        idr['version'] = _version(data.bytes(10, 2))
        idr['date'] = _yyddd(data.bytes(12, 3))
        if len(data) >= 19:
            hhmmss = _packed_digits(data.bytes(15, 4))
            if hhmmss and len(hhmmss) >= 6:
                hhmmss = hhmmss[-6:]
                idr['time'] = '{}:{}:{}'.format(hhmmss[0:2], hhmmss[2:4], hhmmss[4:6])
    elif kind == 0x04:
        # Translator data is a byte stream that continues across
        # consecutive translator IDRs. An item (even a single date field)
        # can be split at a record boundary, so the caller concatenates
        # ``stream`` from every translator IDR and parses it once with
        # _parse_translator_stream().
        idr['type'] = 'translator'
        idr['stream'] = data.data
    elif kind == 0x08:
        idr['type'] = 'user'
        items = []
        off = 0
        while off < len(data):
            n = data.u8(off + 5)
            items.append({
                'esdid': data.u16(off),
                'date': _yyddd(data.bytes(off + 2, 3)),
                'data': _decode(data.bytes(off + 6, n), codepage).rstrip(),
            })
            off += 6 + n
        idr['items'] = items
    elif kind == 0x01:
        idr['type'] = 'zap'
        fc = data.u8(0)
        idr['continued'] = bool(fc & 0x40)
        zaps = []
        for i in range(fc & 0x3F):
            off = 1 + i * 13
            zaps.append({
                'esdid': data.u16(off),
                'date': _yyddd(data.bytes(off + 2, 3)),
                'data': _name(data.bytes(off + 5, 8), codepage),
            })
        idr['zaps'] = zaps
    else:
        idr['type'] = 'unknown'
        idr['subtype'] = sub
    return idr


def _parse_translator_stream(stream, codepage):
    '''Parses concatenated translator IDR data (Figure 20): a list of
    2-byte ESDIDs (high bit set on the last), a count byte (0 = one
    description, 1 = two), then 15-byte descriptions: program name (10),
    version/modification VVMM (2, unsigned packed), date YYDDD (3, packed).
    '''
    data = _Reader(stream, 'translator IDR')
    items = []
    off = 0
    while off < len(data):
        esdids = []
        while True:
            v = data.u16(off)
            off += 2
            esdids.append(v & 0x7FFF)
            if v & 0x8000:
                break
        ndesc = data.u8(off) + 1
        off += 1
        if ndesc > 2:
            raise ParseError('translator IDR: bad description count {}'.format(ndesc - 1))
        translators = []
        for _ in range(ndesc):
            translators.append({
                'program': _name(data.bytes(off, 10), codepage),
                'version': _version(data.bytes(off + 10, 2)),
                'date': _yyddd(data.bytes(off + 12, 3)),
            })
            off += 15
        items.append({'esdids': esdids, 'translators': translators})
    return items


def parse_load_module(data, codepage=DEFAULT_CODEPAGE, strict=True):
    '''Parses an unloaded load module (the raw RECFM=U records of a PDS
    member concatenated without RDW/BDW).

    Args:
        data (bytes): the member.
        codepage (str): EBCDIC code page for names.
        strict (bool): if ``True`` (default) any unparseable record raises
            :class:`ParseError`. If ``False`` parsing stops at the first bad
            record and the problem is reported in ``warnings`` with
            ``complete`` set to ``False``. Some tools pad short blocks with
            zeros, which makes record boundaries unrecoverable after that
            point. SYM, CESD and IDR records always come before text, so
            they are still returned.

    Returns a dict with:
        * ``esd``: list of CESD entries (esdid, name, type, address,
          length, amode, rmode, ...)
        * ``csects``: names of SD entries in ESDID order
        * ``main_csect``: the first SD (load modules have no entry point
          in their records. The entry point is in the PDS directory, see
          :func:`parse_pds2_user_data`)
        * ``idr``: dict with ``linkage_editor``, ``translators``, ``user``
          and ``zap`` lists
        * ``record_counts``, ``text_records``, ``text_bytes``,
          ``module_length``, ``overlay``, ``signed``, ``complete``,
          ``warnings``
    '''
    r = _Reader(data, 'load module')
    if len(r) == 0:
        raise ParseError('load module: no data')
    first = r.u8(0)
    if first not in (LM_SYM, LM_CESD):
        raise ParseError('load module: first record must be SYM or CESD, found X\'{:02X}\''.format(first))

    result = {
        'esd': [], 'csects': [], 'main_csect': None,
        'idr': {'linkage_editor': [], 'translators': [], 'user': [], 'zap': []},
        'record_counts': {}, 'text_records': 0, 'text_bytes': 0,
        'module_length': 0, 'overlay': False, 'signed': False,
        'complete': True, 'warnings': [], 'size': len(r),
    }
    counts = result['record_counts']
    seen_eom = False
    translator_stream = b''

    def idr_problem(msg):
        # IDR contents never affect record boundaries, so a bad IDR is a
        # warning unless strict.
        if strict:
            raise ParseError(msg)
        result['warnings'].append(msg)

    try:
        for rid, pos, length in iter_load_module_records(r.data):
            name = LM_RECORD_NAMES.get(rid, rid)
            counts[name] = counts.get(name, 0) + 1
            if rid == 'TEXT':
                result['text_records'] += 1
                result['text_bytes'] += length
            elif rid == LM_CESD:
                result['esd'].extend(_parse_cesd(r, pos, length, codepage))
            elif rid == LM_IDR:
                try:
                    idr = _parse_idr(r, pos, length, codepage)
                except ParseError as e:
                    idr_problem('IDR at offset {}: {}'.format(pos, e))
                    continue
                if idr['type'] == 'translator':
                    translator_stream += idr['stream']
                elif idr['type'] == 'linkage-editor':
                    result['idr']['linkage_editor'].append(idr)
                elif idr['type'] == 'user':
                    result['idr']['user'].extend(idr['items'])
                elif idr['type'] == 'zap':
                    result['idr']['zap'].extend(idr['zaps'])
            elif rid in (0x05, 0x06, 0x07):
                result['overlay'] = True
            elif rid == LM_SIGNING:
                result['signed'] = True
            if rid in (0x0D, 0x0F):
                seen_eom = True
    except ParseError as e:
        if strict:
            raise
        result['complete'] = False
        result['warnings'].append(str(e))

    if translator_stream:
        try:
            result['idr']['translators'] = _parse_translator_stream(translator_stream, codepage)
        except ParseError as e:
            idr_problem(str(e))

    if not result['esd']:
        raise ParseError('load module: no CESD entries found')
    if result['complete'] and not seen_eom:
        result['warnings'].append('no end-of-module control record found')

    for e in result['esd']:
        if e['type'] == 'SD':
            result['csects'].append(e['name'])
        if e['type'] in ('SD', 'PC', 'CM') and 'length' in e:
            result['module_length'] = max(result['module_length'], e['address'] + e['length'])
    sds = [e for e in result['esd'] if e['type'] == 'SD']
    if sds:
        result['main_csect'] = sds[0]['name']
        if 'amode' in sds[0]:
            result['amode'] = sds[0]['amode']
            result['rmode'] = sds[0]['rmode']
    return result


def parse_pds2_user_data(user_data):
    '''Decodes the user data of a load module PDS directory entry (PDS2
    mapping, starting at PDS2USRD, i.e. the bytes after the TTR and
    indicator byte). This is where the entry point lives.

    Returns ``None`` if the user data is too short to be a load module
    directory entry.
    '''
    if not isinstance(user_data, (bytes, bytearray)) or len(user_data) < 18:
        return None
    r = _Reader(user_data, 'PDS2')
    atr1, atr2 = r.u8(8), r.u8(9)
    info = {
        'text_ttr': r.u24(0),
        'note_list_entries': r.u8(7),
        'attributes': [n for bit, n in (
            (0x80, 'RENT'), (0x40, 'REUS'), (0x20, 'OVLY'), (0x10, 'TEST'),
            (0x08, 'OL'), (0x04, 'SCTR'), (0x02, 'EXEC'), (0x01, '1BLK')) if atr1 & bit] +
            [n for bit, n in ((0x40, 'ORG0'), (0x20, 'EP0'), (0x10, 'NRLD')) if atr2 & bit],
        'executable': bool(atr1 & 0x02),
        'storage': r.u24(10),
        'first_text_length': r.u16(13),
        'entry_point': r.u24(15),
    }
    if len(r) >= 20:
        ftb1, ftb2 = r.u8(18), r.u8(19)
        # PDS2FTB1 bit 0: processed by VS linkage editor, FTB2 is valid
        if ftb1 & 0x80:
            info['amode'] = {0: '24', 2: '31', 1: '64', 3: 'ANY'}[ftb2 & 0x03]
            info['rmode'] = '64' if ftb2 & 0x20 else ('ANY' if ftb2 & 0x10 else '24')
    return info


def _detect_load_module(head, total_len, codepage):
    r = _Reader(head)
    if len(r) < 24 or head[0] not in (LM_SYM, LM_CESD):
        return None
    if head[0] == LM_CESD:
        # bytes 2-3 spare (zero), count multiple of 16, 16..240
        if head[2:4] != b'\x00\x00' or head[1] & 0x7F:
            return None
        count = r.u16(6)
        if count == 0 or count % 16 or count > 240 or r.u16(4) == 0:
            return None
    else:
        if head[1] not in (0x00, 0x80):
            return None
        count = r.u16(2)
        if count == 0 or count > 240:
            return None
    # Walk the records inside the sniff window. A record that runs past the
    # end of the window is fine (the file is longer); an unknown record id
    # or a malformed CESD inside the window is not.
    records = 0
    cesd_records = sym_records = 0
    cesd_entries = good_names = 0
    seen = set()
    pos = 0
    text_len = 0
    window_full = len(head) < total_len
    while pos < len(head):
        if text_len:
            pos += text_len
            text_len = 0
            seen.add('TEXT')
            continue
        if pos + 16 > len(head) and window_full:
            break
        try:
            length, text_len = _lm_record_length(r, pos)
        except ParseError:
            if window_full and pos + 16 > len(head):
                break
            if records >= 3 and LM_CESD in seen:
                break  # e.g. zero-padded blocks after the CESDs/IDRs
            return None
        rid = head[pos]
        if length < 4:
            return None
        if pos + length > len(head):
            if window_full:
                break
            return None  # truncated
        records += 1
        seen.add(rid)
        if rid == LM_SYM:
            if head[pos + 1] not in (0x00, 0x80) or r.u16(pos + 2) > 240:
                return None
            sym_records += 1
        elif rid == LM_CESD:
            n = r.u16(pos + 6)
            if head[pos + 2:pos + 4] != b'\x00\x00' or head[pos + 1] & 0x7F or n % 16 or n > 240:
                return None
            cesd_records += 1
            for i in range(n // 16):
                e = pos + 8 + i * 16
                t = head[e + 8] & 0x0F
                if t not in CESD_TYPES:
                    return None
                cesd_entries += 1
                if t == 0x7 or _is_ebcdic_name(head[e:e + 8], allow_blank=t in (0x4, 0x5)):
                    good_names += 1
        pos += length

    if cesd_entries and good_names < cesd_entries * 0.9:
        return None
    structural = seen & ({LM_IDR, 'TEXT'} | set(LM_CONTROL) | set(LM_CONTROL_RLD))
    if cesd_entries and (structural or cesd_records >= 4):
        return _info('load-module', HIGH)
    if cesd_entries >= 2:
        return _info('load-module', MEDIUM)
    if not cesd_entries and sym_records >= 8 and window_full and seen == {LM_SYM}:
        # Modules link-edited with TEST start with many SYM records; a long
        # exact chain of them filling the window is good evidence.
        return _info('load-module', MEDIUM, 'SYM records (TEST)')
    return _info('load-module', LOW) if cesd_entries else None


# ---------------------------------------------------------------------------
# Program objects (PDSE)
# ---------------------------------------------------------------------------

_IEWPLMH = 'IEWPLMH'.encode('cp037')


def _detect_program_object(head, total_len, codepage):
    # Unloaded program object members begin with the binder's module header
    # eye-catcher 'IEWPLMH' (EBCDIC). The internal format is not published
    # by IBM, so this is detect-only.
    if head[:7] == _IEWPLMH:
        return _info('program-object', HIGH)
    return None


# ---------------------------------------------------------------------------
# Object decks (Appendix A)
# ---------------------------------------------------------------------------

_OBJ_TYPES = {t.encode('cp037'): t for t in ('ESD', 'TXT', 'RLD', 'END', 'SYM', 'XSD')}

OBJ_ESD_TYPES = {
    0x00: 'SD', 0x01: 'LD', 0x02: 'ER', 0x04: 'PC', 0x05: 'CM', 0x06: 'PR',
    0x0A: 'WX', 0x0B: 'UR', 0x0D: 'SD', 0x0E: 'PC', 0x0F: 'CM',
}


def _obj_card_type(card):
    if len(card) >= 4 and card[0] == 0x02:
        return _OBJ_TYPES.get(bytes(card[1:4]))
    return None


def _detect_obj(head, total_len, codepage):
    if len(head) < 80 or head[0] != 0x02 or _obj_card_type(head[:80]) is None:
        return None
    obj_cards = other = 0
    xsd = False
    for off in range(0, len(head) - 79, 80):
        card = head[off:off + 80]
        t = _obj_card_type(card)
        if t:
            obj_cards += 1
            if t == 'XSD':
                xsd = True
            elif t == 'ESD' and b'@@DOPLNK'.decode('ascii').encode('cp037') in card:
                xsd = True
            # columns 5-10 are blank for ESD/TXT/RLD/END (IBM translators);
            # some others (ALGOL 68C, CBT875) write X'00' there.
            if t == 'ESD' and card[4:10].strip(b'\x40\x00'):
                return None
        elif card[0] >= 0x40:
            other += 1  # control statements (NAME, ENTRY, ...)
        else:
            return None
    if obj_cards < 1 or other > obj_cards:
        return None
    name = 'xobj' if xsd else 'obj'
    if total_len % 80 == 0 and (obj_cards >= 2 or total_len == 80):
        return _info(name, HIGH)
    return _info(name, MEDIUM, 'length is not a multiple of 80')


def parse_object_deck(data, codepage=DEFAULT_CODEPAGE):
    '''Parses an OBJ or XOBJ object deck of 80-byte card images.

    Returns a dict with ``variant`` (``obj``/``xobj``), ``cards``,
    ``record_counts``, ``esd`` (list of entries), ``csects``,
    ``entry_point`` (from the END card), ``idr`` (translator data from
    END cards), ``text_bytes`` and ``other_cards`` (non-object cards such
    as linkage editor control statements).
    '''
    r = _Reader(data, 'object deck')
    if len(r) < 80:
        raise ParseError('object deck: shorter than one 80-byte card')
    if len(r) % 80:
        raise ParseError('object deck: length {} is not a multiple of 80'.format(len(r)))
    res = {'variant': 'obj', 'cards': len(r) // 80, 'record_counts': {}, 'esd': [],
           'csects': [], 'entry_point': None, 'idr': [], 'text_bytes': 0,
           'other_cards': 0, 'modules': 0}
    xsd_names = {}  # esdid -> {total, parts, type, ...}
    blank3 = b'\x40\x40\x40'
    for off in range(0, len(r), 80):
        card = _Reader(r.bytes(off, 80), 'object card at offset {}'.format(off))
        t = _obj_card_type(card.data)
        if not t:
            res['other_cards'] += 1
            continue
        res['record_counts'][t] = res['record_counts'].get(t, 0) + 1
        if t == 'ESD':
            nbytes = card.u16(10)
            if nbytes % 16 or nbytes > 48:
                raise ParseError('object deck: ESD card at offset {} has bad byte count {}'.format(off, nbytes))
            esdid = None if card.bytes(14, 2) == b'\x40\x40' else card.u16(14)
            for i in range(nbytes // 16):
                item = 16 + i * 16
                raw_type = card.u8(item + 8)
                etype = OBJ_ESD_TYPES.get(raw_type, 'X{:02X}'.format(raw_type))
                entry = {'name': _name(card.bytes(item, 8), codepage), 'type': etype}
                if etype != 'LD':
                    entry['esdid'] = esdid
                    if esdid is not None:
                        esdid += 1
                if etype in ('SD', 'PC', 'LD'):
                    entry['address'] = card.u24(item + 9)
                if etype in ('SD', 'PC', 'CM', 'PR'):
                    entry['length'] = card.u24(item + 13)
                if etype == 'LD':
                    entry['sd_esdid'] = card.u24(item + 13)
                if etype in ('SD', 'PC'):
                    b = card.u8(item + 12)
                    if b != 0x40:
                        entry['amode'] = '64' if b & 0x10 else AMODES[b & 0x03]
                        entry['rmode'] = '64' if b & 0x20 else ('ANY' if b & 0x04 else '24')
                        entry['rsect'] = bool(b & 0x08)
                if entry['name'] == '@@DOPLNK':
                    res['variant'] = 'xobj'
                res['esd'].append(entry)
        elif t == 'XSD':
            res['variant'] = 'xobj'
            nbytes = card.u16(10)
            ident = card.u16(14)
            total = card.u32(16)
            start = card.u32(20)
            raw_type = card.u8(24)
            sub = card.bytes(32, max(0, min(nbytes, 56) - 16))
            ent = xsd_names.setdefault(ident, {
                'esdid': ident, 'type': OBJ_ESD_TYPES.get(raw_type, 'X{:02X}'.format(raw_type)),
                'total': total, 'parts': {}, 'address': card.u24(25),
                'length': card.u24(29)})
            ent['parts'][start] = sub
        elif t == 'TXT':
            res['text_bytes'] += card.u16(10)
        elif t == 'END':
            res['modules'] += 1
            ep = card.bytes(5, 3)
            sym = card.bytes(16, 8)
            if ep != blank3:
                res['entry_point'] = {'address': int.from_bytes(ep, 'big'),
                                      'esdid': card.u16(14)}
            elif _is_ebcdic_name(sym):
                res['entry_point'] = {'name': _name(sym, codepage)}
            flag = card.u8(32)
            for i in range({0xF1: 1, 0xF2: 2}.get(flag, 0)):
                o = 33 + i * 19
                res['idr'].append({
                    'translator': _name(card.bytes(o, 10), codepage),
                    'version': _decode(card.bytes(o + 10, 2), codepage),
                    'release': _decode(card.bytes(o + 12, 2), codepage),
                    'date': _ebcdic_yyddd(_decode(card.bytes(o + 14, 5), codepage)),
                })
    for ident, ent in sorted(xsd_names.items()):
        name = b''.join(ent['parts'][k] for k in sorted(ent['parts']))[:ent['total']]
        res['esd'].append({'esdid': ident, 'name': _decode(name, codepage), 'type': ent['type'],
                           'address': ent['address'], 'length': ent['length']})
    res['csects'] = [e['name'] for e in res['esd'] if e['type'] == 'SD' and e['name']]
    return res


def _ebcdic_yyddd(s):
    import datetime
    if len(s) != 5 or not (s.isascii() and s.isdigit()):
        return None
    yy, ddd = int(s[:2]), int(s[2:])
    if not 1 <= ddd <= 366:
        return None
    year = 1900 + yy if yy >= 65 else 2000 + yy
    d = datetime.date(year, 1, 1) + datetime.timedelta(days=ddd - 1)
    return d.isoformat() if d.year == year else None


# ---------------------------------------------------------------------------
# GOFF (Appendix C)
# ---------------------------------------------------------------------------

GOFF_TYPES = {0x0: 'ESD', 0x1: 'TXT', 0x2: 'RLD', 0x3: 'LEN', 0x4: 'END', 0xF: 'HDR'}
GOFF_ESD_TYPES = {0: 'SD', 1: 'ED', 2: 'LD', 3: 'PR', 4: 'ER'}


def _goff_prefix_ok(rec):
    # byte 0 X'03'; byte 1: type in bits 0-3, bits 4-5 reserved (0),
    # bits 6-7 continuation flags; byte 2 version X'00'
    return (len(rec) >= 3 and rec[0] == 0x03 and (rec[1] >> 4) in GOFF_TYPES
            and not rec[1] & 0x0C and rec[2] == 0x00)


def _detect_goff(head, total_len, codepage):
    if len(head) >= 80 and head[:3] == b'\x03\xF0\x00':
        n = 0
        for off in range(0, len(head) - 79, 80):
            if not _goff_prefix_ok(head[off:off + 3]):
                return None
            n += 1
        if total_len % 80 == 0 and n >= 2:
            return _info('goff', HIGH, 'fixed 80-byte records')
        return _info('goff', MEDIUM, 'fixed 80-byte records')
    # Variable-length GOFF with RDWs (no BDW)
    if len(head) >= 8 and head[2:4] == b'\x00\x00' and head[4:7] == b'\x03\xF0\x00':
        try:
            recs = _split_rdw(head, limit=8, partial_ok=True)
        except ParseError:
            return None
        if recs and all(_goff_prefix_ok(x[4:]) for x in recs):
            return _info('goff', HIGH if len(recs) >= 2 else MEDIUM, 'variable-length records with RDW')
    return None


def _goff_logical_records(data):
    '''Yields logical GOFF records (continuations joined) from fixed-80 or
    RDW-prefixed variable data.'''
    r = _Reader(data, 'GOFF')
    if r.bytes(0, 3) == b'\x03\xF0\x00':
        if len(r) % 80:
            raise ParseError('GOFF: fixed-format length {} is not a multiple of 80'.format(len(r)))
        physical = [r.data[o:o + 80] for o in range(0, len(r), 80)]
        fmt = 'fixed80'
    else:
        physical = [x[4:] for x in _split_rdw(r.data)]
        fmt = 'variable'
    current = None
    for i, rec in enumerate(physical):
        if not _goff_prefix_ok(rec):
            raise ParseError('GOFF: bad record prefix {} in record {}'.format(rec[:3].hex(), i + 1))
        cont = rec[1] & 0x02
        if cont:
            if current is None:
                raise ParseError('GOFF: continuation record {} without initial record'.format(i + 1))
            current += rec[3:]
        else:
            if current is not None:
                raise ParseError('GOFF: record {} is not the continuation that record {} announced'.format(i + 1, i))
            current = bytes(rec)
        if not rec[1] & 0x01:
            yield fmt, current
            current = None
    if current is not None:
        raise ParseError('GOFF: last record is marked continued')


def parse_goff(data, codepage=DEFAULT_CODEPAGE):
    '''Parses a GOFF object module (fixed 80-byte or RDW-prefixed variable
    records).

    Returns a dict with ``record_format``, ``hdr`` (architecture level),
    ``record_counts``, ``esd`` (esdid, type, parent, name, offset,
    length), ``sections`` (SD names), ``entry_points`` (LD names) and
    ``externals`` (ER names).
    '''
    res = {'record_format': None, 'hdr': None, 'record_counts': {}, 'esd': [],
           'sections': [], 'entry_points': [], 'externals': []}
    for fmt, rec in _goff_logical_records(data):
        res['record_format'] = fmt
        r = _Reader(rec, 'GOFF record')
        t = GOFF_TYPES[r.u8(1) >> 4]
        res['record_counts'][t] = res['record_counts'].get(t, 0) + 1
        if t == 'HDR':
            res['hdr'] = {'architecture_level': r.u32(48) if len(r) >= 52 else None}
        elif t == 'ESD':
            stype = r.u8(3)
            nlen = r.u16(70)
            if nlen == 0:
                raise ParseError('GOFF: ESD name length is zero')
            entry = {
                'esdid': r.u32(4),
                'type': GOFF_ESD_TYPES.get(stype, 'X{:02X}'.format(stype)),
                'parent': r.u32(8),
                'offset': r.u32(16),
                'length': r.u32(24),
                'name_space': r.u8(40),
                'name': _decode(r.bytes(72, nlen), codepage),
            }
            res['esd'].append(entry)
    if res['record_format'] is None:
        raise ParseError('GOFF: no records')
    for e in res['esd']:
        key = {'SD': 'sections', 'LD': 'entry_points', 'ER': 'externals'}.get(e['type'])
        if key and e['name'].strip():  # a blank SD name is the unnamed section
            res[key].append(e['name'])
    return res


# ---------------------------------------------------------------------------
# Variable-length records: BDW / RDW
# ---------------------------------------------------------------------------

def _split_rdw(data, limit=None, partial_ok=False, start=0, end=None):
    '''Splits RDW-prefixed records (no BDW). Returns record bodies
    including the RDW. Spanned segments are returned as-is.'''
    r = _Reader(data, 'RDW records')
    end = len(r) if end is None else end
    out = []
    pos = start
    while pos < end:
        if limit is not None and len(out) >= limit:
            break
        if partial_ok and pos + 4 > end:
            break
        ln = r.u16(pos)
        if ln < 4 or r.u8(pos + 3) != 0 or r.u8(pos + 2) > 3:
            raise ParseError('RDW at offset {} is invalid ({})'.format(pos, r.bytes(pos, 4).hex()))
        if pos + ln > end:
            if partial_ok:
                break
            raise ParseError('record at offset {} (length {}) runs past end of data'.format(pos, ln))
        out.append(r.data[pos:pos + ln])
        pos += ln
    return out


def _split_bdw(data, limit=None, partial_ok=False):
    '''Splits BDW-blocked data into blocks of RDW records. Returns a list
    of record bodies including RDWs.'''
    r = _Reader(data, 'VB blocks')
    out = []
    pos = 0
    while pos < len(r):
        if limit is not None and len(out) >= limit:
            break
        if partial_ok and pos + 4 > len(r):
            break
        bl = r.u16(pos)
        if r.u8(pos) & 0x80:
            raise ParseError('large-block BDW at offset {} is not supported'.format(pos))
        if bl < 8 or r.bytes(pos + 2, 2) != b'\x00\x00':
            raise ParseError('BDW at offset {} is invalid ({})'.format(pos, r.bytes(pos, 4).hex()))
        if pos + bl > len(r):
            if partial_ok:
                break
            raise ParseError('block at offset {} (length {}) runs past end of data'.format(pos, bl))
        out.extend(_split_rdw(r.data, start=pos + 4, end=pos + bl))
        pos += bl
    return out


def parse_vb(data, blocked=None, join_spanned=True):
    '''Splits variable-length data into records.

    Args:
        data (bytes): raw data.
        blocked (bool): ``True`` if the data has BDWs, ``False`` if only
            RDWs, ``None`` to work it out.
        join_spanned (bool): join VBS segments (RDW byte 2 segment codes:
            0 complete, 1 first, 2 last, 3 middle) into logical records.

    Returns a dict with ``blocked`` and ``records`` (record data without
    RDWs).
    '''
    if blocked is None:
        blocked = _looks_blocked(data)
    segs = _split_bdw(data) if blocked else _split_rdw(data)
    records = []
    pending = None
    for seg in segs:
        code = seg[2] if join_spanned else 0
        body = seg[4:]
        if code == 0:
            if pending is not None:
                raise ParseError('VBS: complete record inside a spanned record')
            records.append(body)
        elif code == 1:
            if pending is not None:
                raise ParseError('VBS: first segment inside a spanned record')
            pending = body
        else:
            if pending is None:
                raise ParseError('VBS: middle/last segment without first segment')
            pending += body
            if code == 2:
                records.append(pending)
                pending = None
    if pending is not None:
        raise ParseError('VBS: data ends inside a spanned record')
    return {'blocked': blocked, 'records': records}


def _looks_blocked(data):
    '''True if the data starts with a BDW whose length is exactly covered
    by the RDWs inside it.'''
    if len(data) < 8 or data[2:4] != b'\x00\x00' or data[6:8] not in (b'\x00\x00', b'\x01\x00', b'\x02\x00', b'\x03\x00'):
        return False
    bl = int.from_bytes(data[0:2], 'big')
    if bl < 8 or bl > len(data):
        return False
    try:
        recs = _split_rdw(data, start=4, end=bl)
    except ParseError:
        return False
    return len(recs) > 0


def _detect_vb(head, total_len, codepage):
    if len(head) < 8:
        return None
    if _looks_blocked(head):
        try:
            recs = _split_bdw(head, partial_ok=total_len > len(head))
        except ParseError:
            return None
        if not recs:
            return None
        blocks = 0
        pos = 0
        while pos + 4 <= len(head):
            bl = int.from_bytes(head[pos:pos + 2], 'big')
            if pos + bl > len(head):
                break
            blocks += 1
            pos += bl
        conf = MEDIUM if blocks >= 2 or (blocks == 1 and total_len == pos) else LOW
        return _info('vb', conf, '{} records in first {} blocks'.format(len(recs), blocks))
    try:
        recs = _split_rdw(head, partial_ok=total_len > len(head))
    except ParseError:
        return None
    if len(recs) >= 4 or (recs and sum(map(len, recs)) == total_len and len(recs) >= 2):
        return _info('rdw', LOW if len(recs) < 8 else MEDIUM)
    return None


# ---------------------------------------------------------------------------
# SMF
# ---------------------------------------------------------------------------

def _smf_header_ok(rec):
    '''``rec`` includes the RDW. Standard SMF header: RDW(4) FLG(1) RTY(1)
    TME(4, binary hundredths since midnight) DTE(4, packed 0cyydddF)
    SID(4, EBCDIC).'''
    if len(rec) < 18:
        return False
    tme = int.from_bytes(rec[6:10], 'big')
    if tme >= 24 * 60 * 60 * 100:
        return False
    dte = rec[10:14]
    if dte[0] not in (0x00, 0x01) or (dte[3] & 0x0F) != 0x0F:
        return False
    digits = _packed_digits(dte)
    if digits is None:
        return False
    ddd = int(digits[-3:])
    if not 1 <= ddd <= 366:
        return False
    sid = rec[14:18]
    return all(b in _EBCDIC_NAME or b == 0x00 for b in sid) and sid[0] not in (0x40, 0x00)


def _detect_smf(head, total_len, codepage):
    if len(head) < 22:
        return None
    try:
        if _looks_blocked(head):
            recs = _split_bdw(head, partial_ok=True)
        else:
            recs = _split_rdw(head, partial_ok=True)
    except ParseError:
        return None
    complete = [x for x in recs if x[2] in (0, 1)]
    if not complete or not all(_smf_header_ok(x) for x in complete):
        return None
    return _info('smf', HIGH if len(complete) >= 2 else MEDIUM)


def parse_smf(data, codepage=DEFAULT_CODEPAGE):
    '''Summarises a file of SMF records (RDW or BDW+RDW, as written by
    IFASMFDP or downloaded with ``quote site rdw``).

    Returns a dict with ``records``, ``types`` ({type: count}),
    ``subtypes`` ({"type.subtype": count}, for records whose header flags
    say subtypes are valid), ``systems`` (SIDs) and ``first``/``last``
    timestamps (local time, ISO format).
    '''
    import datetime
    vb = parse_vb(data)
    res = {'records': 0, 'types': {}, 'subtypes': {}, 'systems': [], 'first': None, 'last': None}
    systems = set()
    for body in vb['records']:
        rec = b'\x00\x00\x00\x00' + body  # put an RDW back so offsets match the manual
        if not _smf_header_ok(rec):
            raise ParseError('SMF: record {} does not have a valid SMF header'.format(res['records'] + 1))
        res['records'] += 1
        rty = rec[5]
        res['types'][rty] = res['types'].get(rty, 0) + 1
        if rec[4] & 0x40 and len(rec) >= 24:
            sty = int.from_bytes(rec[22:24], 'big')
            key = '{}.{}'.format(rty, sty)
            res['subtypes'][key] = res['subtypes'].get(key, 0) + 1
        systems.add(_name(rec[14:18], codepage))
        digits = _packed_digits(rec[10:14])
        century = 1900 + 100 * int(digits[0:2])
        year = century + int(digits[2:4])
        try:
            ts = (datetime.datetime(year, 1, 1) + datetime.timedelta(
                days=int(digits[4:7]) - 1, milliseconds=10 * int.from_bytes(rec[6:10], 'big'))).isoformat()
        except (ValueError, OverflowError):
            ts = None
        if ts:
            res['first'] = ts if res['first'] is None or ts < res['first'] else res['first']
            res['last'] = ts if res['last'] is None or ts > res['last'] else res['last']
    res['systems'] = sorted(systems)
    return res


# ---------------------------------------------------------------------------
# IEBCOPY unload (DFSMSdfp Utilities: IEBCOPY unload data set format)
# ---------------------------------------------------------------------------

_COPYR1_EYE = b'\xCA\x6D\x0F'


def _iebcopy_offset(head):
    '''Returns how many bytes of descriptor precede COPYR1: 0 (bare
    records, as inside XMI), 4 (RDW only) or 8 (BDW + RDW), or ``None``.'''
    for prefix in (0, 4, 8):
        if head[prefix + 1:prefix + 4] == _COPYR1_EYE:
            return prefix
    return None


def _detect_iebcopy(head, total_len, codepage):
    prefix = _iebcopy_offset(head)
    if prefix is None or len(head) < prefix + 12:
        return None
    rec = head[prefix:]
    # byte 0: flags (X'00' PDS unload, X'01' PDSE, X'40'/X'80' other unload
    # flags); bytes 4-5 DS1DSORG must have the PO bit (X'02' in byte 0)
    if rec[0] & 0x3E or not rec[4] & 0x02:
        return None
    pdse = bool(rec[0] & 0x01)
    form = {0: 'bare records', 4: 'RDW', 8: 'BDW+RDW'}[prefix]
    return _info('iebcopy', HIGH, '{}, {}'.format('PDSE' if pdse else 'PDS', form))


_RECFM_BITS = [(0x10, 'B'), (0x08, 'S'), (0x04, 'A'), (0x02, 'M')]


def _recfm(b):
    s = {0x80: 'F', 0x40: 'V', 0xC0: 'U'}.get(b & 0xC0, '?')
    return s + ''.join(c for bit, c in _RECFM_BITS if b & bit)


def parse_iebcopy(data, codepage=DEFAULT_CODEPAGE):
    '''Parses the header and directory of a standalone IEBCOPY unload data
    set.

    Accepts the bare records (COPYR1 56 bytes, COPYR2 276 bytes, then
    directory and member data, as found inside XMI files) or the data
    set as downloaded with BDW/RDW descriptors.

    Returns a dict with ``type`` (PDS/PDSE), ``dsorg``, ``recfm``,
    ``lrecl``, ``blksize``, ``header_records`` and ``members`` (a list of
    ``{name, ttr, alias, user_data}``).

    Use :class:`xmi.XMIT` to extract member contents from XMI/tape files.
    '''
    r = _Reader(data, 'IEBCOPY')
    prefix = _iebcopy_offset(r.data[:16])
    if prefix is None:
        raise ParseError('IEBCOPY: COPYR1 eye-catcher X\'CA6D0F\' not found')
    if prefix == 0:
        copyr1 = _Reader(r.bytes(0, 56), 'COPYR1')
        r.bytes(56, 276)
        rest = r.data[56 + 276:]
    else:
        recs = [x[4:] for x in (_split_bdw(r.data) if prefix == 8 else _split_rdw(r.data))]
        if len(recs) < 2:
            raise ParseError('IEBCOPY: fewer than two records')
        copyr1 = _Reader(recs[0], 'COPYR1')
        rest = b''.join(recs[2:])
    res = {
        'type': 'PDSE' if copyr1.u8(0) & 0x01 else 'PDS',
        'dsorg': copyr1.u16(4),
        'blksize': copyr1.u16(6),
        'lrecl': copyr1.u16(8),
        'recfm': _recfm(copyr1.u8(10)),
        'header_records': copyr1.u16(36) if len(copyr1) >= 38 else None,
        'members': [],
    }
    d = _Reader(rest, 'IEBCOPY directory')
    block = 0
    done = False
    while not done:
        base = block * 276
        used = d.u16(base + 20)
        if used < 2 or used > 256:
            raise ParseError('IEBCOPY: directory block {} has bad used length {}'.format(block + 1, used))
        loc = base + 22
        end = base + 20 + used
        while loc < end:
            name = d.bytes(loc, 8)
            if name == b'\xff' * 8:
                done = True
                break
            ind = d.u8(loc + 11)
            ud_len = (ind & 0x1F) * 2
            res['members'].append({
                'name': _name(name, codepage),
                'ttr': d.u24(loc + 8),
                'alias': bool(ind & 0x80),
                'user_data': d.bytes(loc + 12, ud_len),
            })
            loc += 12 + ud_len
        block += 1
    return res


# ---------------------------------------------------------------------------
# TERSE (header rules from openterse, src/header.c)
# ---------------------------------------------------------------------------

def parse_terse_header(data):
    '''Parses the header of a TERSE / AMATERSE / TRSMAIN file.

    Native SPACK header (4 bytes): X'01' or X'07', then X'8969A5'.
    Host header (12 bytes): X'02' PACK or X'05' SPACK, variable flag
    (X'00'/X'01'), record length (2), flags, ratio, block size (2),
    record length (4, used when > 65535).
    '''
    r = _Reader(data, 'TERSE header')
    v = r.u8(0)
    if v in (0x01, 0x07):
        if r.bytes(1, 3) != b'\x89\x69\xA5':
            raise ParseError('TERSE: bad native SPACK validation bytes')
        return {'method': 'SPACK', 'host': False, 'variable': False, 'record_length': None,
                'header_size': 4}
    if v not in (0x02, 0x05):
        raise ParseError('TERSE: unknown version byte X\'{:02X}\''.format(v))
    var = r.u8(1)
    rl1, flags, ratio, bs, rl2 = r.u16(2), r.u8(4), r.u8(5), r.u16(6), r.u32(8)
    if var not in (0, 1):
        raise ParseError('TERSE: bad variable flag')
    if rl1 == 0 and rl2 == 0:
        raise ParseError('TERSE: record length is zero')
    if rl1 and rl2 and rl1 != rl2:
        raise ParseError('TERSE: inconsistent record lengths')
    if not flags & 0x04 and (flags or ratio or bs):
        raise ParseError('TERSE: non-MVS header has MVS fields set')
    return {'method': 'PACK' if v == 0x02 else 'SPACK', 'host': True, 'variable': bool(var),
            'record_length': rl1 or rl2, 'flags': flags, 'block_size': bs, 'header_size': 12}


def _detect_terse(head, total_len, codepage):
    if len(head) < 4 or head[0] not in (0x01, 0x02, 0x05, 0x07):
        return None
    try:
        h = parse_terse_header(head)
    except ParseError:
        return None
    if not h['host']:
        return _info('terse', HIGH, 'native SPACK')
    conf = HIGH if (h['flags'] & 0x04 and h['block_size']) or total_len % 1024 == 0 else MEDIUM
    return _info('terse', conf, '{} {}, LRECL {}'.format(
        h['method'], 'VB' if h['variable'] else 'FB', h['record_length']))


def decompress_terse(data):
    '''Decompresses a TERSE file using the optional ``openterse`` package
    (https://github.com/mainframed/openterse). xmilib does not depend on
    it. Raises :class:`ParseError` if it is not installed.'''
    try:
        import openterse
    except ImportError:
        raise ParseError('TERSE decompression needs the optional openterse package')
    return openterse.decompress(data)


# ---------------------------------------------------------------------------
# Text-ish formats
# ---------------------------------------------------------------------------

_ASCII_TEXT = set(range(0x20, 0x7F)) | {0x09, 0x0A, 0x0D, 0x0C}


def _detect_text(head, total_len, codepage):
    # ASCII only. EBCDIC text is handled by detect_ebcdic_codepage(), which
    # identify() runs when no other format matched.
    if len(head) < 16:
        return None
    sample = head[:2048]
    # Hebrew/Arabic EBCDIC text can be all X'41'-X'7E' bytes, which are
    # printable ASCII, so require ASCII blanks/newlines to outnumber
    # EBCDIC blanks (X'40').
    spaces = sample.count(0x20) + sample.count(0x0A)
    if all(b in _ASCII_TEXT for b in sample) and spaces > sample.count(0x40):
        return _info('ascii-text', MEDIUM)
    return None


# Single-byte EBCDIC code pages tried by detect_ebcdic_codepage(), with a
# label for each. The order is the tie-break order (most common first):
# when text uses only characters that every page agrees on, the first
# page in the tie wins, unless the caller's own code page is in the tie.
# The euro variants (cp1140-cp1149) differ from their base pages only at
# X'9F' (currency sign vs euro), so they are not listed separately.
# DBCS pages (Japanese cp930/cp939, Chinese, Korean mixed) have no Python
# codec and are not tried. Their single-byte halves (cp290 Katakana, cp833
# Korean) are left out too: they map Latin lower case to other scripts,
# which wins tie-breaks on ordinary text, and SBCS-only text in them is
# rare.
EBCDIC_CODEPAGES = [
    ('cp037', 'US/Canada'),
    ('cp1047', 'Latin-1 / Open Systems (C, USS)'),
    ('cp500', 'International'),
    ('cp273', 'German/Austrian'),
    ('cp277', 'Danish/Norwegian'),
    ('cp278', 'Finnish/Swedish'),
    ('cp280', 'Italian'),
    ('cp284', 'Spanish'),
    ('cp285', 'UK'),
    ('cp297', 'French'),
    ('cp871', 'Icelandic'),
    ('cp870', 'Latin-2 (Central European)'),
    ('cp1026', 'Turkish'),
    ('cp1112', 'Baltic'),
    ('cp1122', 'Estonian'),
    ('cp875', 'Greek'),
    ('cp1025', 'Cyrillic'),
    ('cp424', 'Hebrew'),
    ('cp420', 'Arabic'),
    ('cp838', 'Thai'),
]

# Bytes that are text in every EBCDIC code page: blank, and the newline
# and line-feed controls. Everything below X'40' other than these is a
# control character and counts against the data being text.
_EBCDIC_TEXT_CONTROLS = {0x05, 0x0D, 0x15, 0x25}

# ASCII punctuation that national code pages replace with letters
# (e.g. cp037 '{' X'C0' is cp273 'ä'). Between two letters it is far more
# likely to be a national letter decoded with the wrong page.
_VARIANT_PUNCT = set('{}[]~\\|^`!')

# Non-ASCII Latin letters that are common in real text. Other Latin
# letters (Ý, Ÿ, Ð, ...) are rare and score lower.
_COMMON_LATIN = set('äöüßÄÖÜéèêëàâçùûîïôœÉÈÀÇåæøÅÆØñÑáíóúÁÍÓÚìòÌÒ'
                    'őűŐŰąćęłńśźżĄĆĘŁŃŚŹŻčďěňřšťůžČĎĚŇŘŠŤŮŽğışĞİŞ'
                    'āēīūģķļņĀĒĪŪĢĶĻŅõÕþðÞÐ')

# Symbols that are common in mainframe text even though they are not
# ASCII: not-sign (PL/I, REXX), cent sign, broken bar, currency,
# copyright/registered, degree.
_MAINFRAME_SYMBOLS = set('¬¢¦£€©®°')
_CURRENCY = set('¢£€$')

# Two code pages whose raw scores differ by less than this are a tie: about
# one character's worth. An absolute margin, because a few brackets in a
# long C header are a small fraction of the text but decisive.
_CODEPAGE_TIE_MARGIN = 1.0


def _script(ch):
    import unicodedata
    try:
        return unicodedata.name(ch).split()[0]
    except ValueError:
        return None


def _char_score(text, i):
    '''Score of character ``i`` of decoded text: about +1 if plausible,
    negative if not. Depends only on the character and its neighbours.'''
    import unicodedata
    ch = text[i]
    o = ord(ch)
    n = len(text)
    prev_alpha = i > 0 and text[i - 1].isalpha()
    next_alpha = i + 1 < n and text[i + 1].isalpha()
    if ch in '\n\r\t\x85' or ch == '\xa0':   # X'41' is a no-break space in Latin pages
        return 1
    if 0x20 <= o < 0x7F:
        if ch in _VARIANT_PUNCT and prev_alpha and next_alpha:
            return -0.5
        if ch == '@' and 0 < i < n - 1 and text[i - 1] == ' ' and text[i + 1] == ' ':
            return 0.3   # a lone '@' word is usually a national 'à'/'§'
        return 1
    if ch == '\ufffd' or unicodedata.category(ch) in ('Cc', 'Cf', 'Cn', 'Co', 'Cs'):
        return -1
    if ch.isalpha():
        script = _script(ch)
        if script == 'LATIN':
            if ch in _COMMON_LATIN:
                # a one-letter word is plausible too: French "à", Swedish "å"
                return 1 if (prev_alpha or next_alpha) else 0.9
            return 0.2 if (prev_alpha or next_alpha) else -0.5
        # Greek, Cyrillic, Hebrew, ...: plausible inside a word of the same
        # script, implausible scattered among Latin text.
        same = (i > 0 and _script(text[i - 1]) == script) or \
               (i + 1 < n and _script(text[i + 1]) == script)
        return 1 if same else -0.5
    if ch in _MAINFRAME_SYMBOLS:
        # "£pragma": a currency sign right before a letter is a national
        # '#' decoded with the wrong page
        return -0.5 if (ch in _CURRENCY and next_alpha) else 1
    return -0.5   # other symbols: ¨ ± ° ¤ ...


def _text_score(text, positions=None):
    '''Sum of _char_score() over ``positions`` (default: all), minus a
    penalty for stray closing brackets. See detect_ebcdic_codepage().'''
    if positions is None:
        positions = range(len(text))
    score = sum(_char_score(text, i) for i in positions)
    # Real text and code keep [] and {} balanced. National letters decoded
    # with the wrong page turn into stray brackets ("perch] } gi{").
    return score - 1.5 * _unbalanced_brackets(text)


def _unbalanced_brackets(text):
    '''Counts closing brackets that have no opener, for [] and {}
    separately. Openers that are never closed are not counted: the sample
    is truncated, so the closer may simply be further on.'''
    bad = 0
    for opener, closer in (('[', ']'), ('{', '}')):
        depth = 0
        for ch in text:
            if ch == opener:
                depth += 1
            elif ch == closer:
                if depth:
                    depth -= 1
                else:
                    bad += 1
    return bad


# X'5B' X'7B' X'7C' are $ # @ in US pages and national letters (Å Æ Ø,
# Ä Ö, ...) in others. MVS names use $ # @ all the time, so letters
# decoded from these bytes prove nothing about the code page.
_NATIONAL_NAME_BYTES = {0x5B, 0x7B, 0x7C}


def _in_word_letters(sample, text, positions):
    '''Non-ASCII letters with a letter next to them, ignoring the $ # @
    byte positions. With the right code page a national text's special
    bytes decode to letters inside words; with a wrong Latin page they
    decode to ASCII punctuation instead. ``text`` is ``sample`` decoded
    with a single-byte code page, so character i is byte i.'''
    n = len(text)
    return sum(1 for i in positions
               for ch in (text[i],)
               if ord(ch) > 0x7F and ch.isalpha() and sample[i] not in _NATIONAL_NAME_BYTES
               and ((i > 0 and text[i - 1].isalpha()) or (i + 1 < n and text[i + 1].isalpha())))


# Euro variants and their base pages: they differ only at X'9F'.
EURO_BASE = {
    'cp1140': 'cp037', 'cp1141': 'cp273', 'cp1142': 'cp277', 'cp1143': 'cp278',
    'cp1144': 'cp280', 'cp1145': 'cp284', 'cp1146': 'cp285', 'cp1147': 'cp297',
    'cp1148': 'cp500', 'cp1149': 'cp871',
}

_TABLES = None
_VARIANT = None


def _codepage_tables():
    '''{codepage: 256-character decode table} for the pages Python knows.'''
    global _TABLES
    if _TABLES is None:
        _TABLES = {}
        for cp, _ in EBCDIC_CODEPAGES:
            try:
                _TABLES[cp] = bytes(range(256)).decode(cp, errors='replace')
            except LookupError:
                pass
    return _TABLES


def _variant_bytes():
    '''Byte values that not every page decodes to the same character.'''
    global _VARIANT
    if _VARIANT is None:
        tables = list(_codepage_tables().values())
        _VARIANT = frozenset(b for b in range(256) if len({t[b] for t in tables}) > 1)
    return _VARIANT


def codepage_fits(codepage, found):
    '''True if ``codepage`` (or, for a euro variant such as cp1140, its
    base page) is one of the equally good fits in ``found`` (a result of
    :func:`detect_ebcdic_codepage`).'''
    if not found:
        return True
    return EURO_BASE.get(codepage, codepage) in found['codepages']


# Pages that choose_codepage() may switch to on its own. Latin national
# pages are deliberately absent: they turn the MVS syntax characters
# $ # @ ! | ¢ { } \ ~ (panel attributes, JCL, assembler) into letters, and
# on CBT Tape that misfired far more often than it helped.
_AUTO_LATIN = {'cp1047'}
_LATIN1_FAMILY = {'cp037', 'cp500', 'cp1047'}
_AUTO_SCRIPTS = {'cp875', 'cp1025', 'cp424', 'cp420', 'cp838'}


def choose_codepage(data, configured):
    '''Decides whether EBCDIC text should be converted with a code page
    other than ``configured``. Returns that code page, or ``None`` to keep
    ``configured``.

    This is deliberately narrow, because switching wrongly damages text
    that converts correctly today:

    * cp1047 instead of cp037/cp500/cp1140/cp1148: when it scores at
      least 2 points better, i.e. at least one bracket pair decodes as
      ``[`` ``]`` (cp1047) and not ``Ý`` ``¨`` (cp037). This is the usual
      problem with C, USS and REXX source.
    * Greek, Cyrillic, Hebrew, Arabic or Thai: when detection is certain,
      the page beats ``configured`` by at least 5% of the sample, and at
      least half the letters are in that script (so an ISPF panel whose
      attribute bytes are Greek letters in cp875 does not switch).

    Latin national pages (cp273, cp277, cp297, ...) are never chosen
    automatically; :func:`detect_ebcdic_codepage` still reports them so a
    caller can pass ``encoding=`` explicitly.
    '''
    found = detect_ebcdic_codepage(data, preferred=configured)
    if not found or codepage_fits(configured, found):
        return None
    best = found['codepage']
    base = EURO_BASE.get(configured, configured)
    own = found['scores'].get(base)
    if own is None:
        return None
    gap = found['scores'][best] - own
    if best in _AUTO_LATIN and base in _LATIN1_FAMILY and gap >= 2:
        return best
    if (best in _AUTO_SCRIPTS and found['certain'] and gap >= 0.05 * found['sample_size']
            and _non_latin_letter_share(bytes(data[:found['sample_size']]), best) >= 0.5):
        return best
    return None


def _non_latin_letter_share(sample, codepage):
    '''Share of the letters in ``sample`` (decoded with ``codepage``) that
    are not Latin. Real Greek/Cyrillic/... text is mostly such letters; an
    ISPF panel whose attribute bytes happen to be letters in that page is
    not.'''
    text = sample.decode(codepage, errors='replace')
    letters = [ch for ch in text if ch.isalpha()]
    if not letters:
        return 0.0
    return sum(1 for ch in letters if _script(ch) != 'LATIN') / len(letters)


def detect_ebcdic_codepage(data, preferred=None, sample_size=2048):
    '''Works out whether ``data`` is EBCDIC text, and in which code page.

    Each code page in :data:`EBCDIC_CODEPAGES` is used to decode a sample,
    and the decoded text is scored: ASCII-range characters, common
    accented letters inside words and same-script letters (Greek,
    Cyrillic, ...) score positively; control characters, unassigned code
    points, stray symbols and "letter{letter" patterns (a national letter
    decoded with the wrong page) score negatively.

    Returns ``None`` if the data does not look like EBCDIC text,
    otherwise a dict with:
        * ``codepage``: the best code page
        * ``language``: its label
        * ``score``: fraction of the best possible score (0-1)
        * ``codepages``: every code page that scored within
          ``_CODEPAGE_TIE_MARGIN`` (about one character's worth) of the
          best, the tie-break winners first.
        * ``certain``: ``True`` when the tie-break leaves one code page.
        * ``scores``: raw score per code page; ``sample_size``.

    Ties are common and real, not a weakness to paper over:

    * Text that only uses characters common to all pages (A-Z, 0-9, most
      punctuation) decodes identically in every Latin page.
    * The Latin national pages put each language's most common national
      letters at the same byte positions (cp297 French 'é' is X'C0', which
      is cp277 Danish 'æ'), so short samples often fit several of them.
    * Hebrew (cp424) letters occupy the positions of upper case Greek in
      cp875; Cyrillic (cp1025) and Arabic (cp420) overlap similarly.

    Among pages with exactly the best score, the one that turns the most
    bytes into non-ASCII letters inside words wins; if that is still a tie, ``preferred`` wins
    if it is in it, otherwise the first in :data:`EBCDIC_CODEPAGES`
    order.
    '''
    sample = bytes(data[:sample_size])
    if len(sample) < 16:
        return None
    # Cheap structural gate: EBCDIC text has blanks (X'40'), letters in the
    # X'81'-X'E9' range, and almost no control bytes.
    controls = sum(1 for b in sample if b < 0x40 and b not in _EBCDIC_TEXT_CONTROLS)
    if controls > len(sample) * 0.01:
        return None
    if sample.count(0x40) < len(sample) * 0.03:
        return None

    # Only bytes that some pages decode differently ("variant" bytes) can
    # change the ranking. Characters away from them score the same in every
    # page, so score them once and rescore only variant positions and their
    # neighbours (a character's score looks at its neighbours) per page.
    tables = _codepage_tables()
    variant = _variant_bytes()
    vpos = [i for i, b in enumerate(sample) if b in variant]
    near = sorted({j for i in vpos for j in (i - 1, i, i + 1) if 0 <= j < len(sample)})
    near_set = set(near)
    base_text = sample.decode(EBCDIC_CODEPAGES[0][0], errors='replace')
    invariant_score = sum(_char_score(base_text, i) for i in range(len(sample)) if i not in near_set)

    scores = []
    for cp, label in EBCDIC_CODEPAGES:
        if cp not in tables:
            continue
        text = sample.decode(cp, errors='replace')
        total = invariant_score + _text_score(text, near)
        scores.append((total, _in_word_letters(sample, text, vpos), cp, label))
    raw = {x[2]: round(x[0], 2) for x in scores}
    if not scores:
        return None
    best = max(s[0] for s in scores)
    if best < 0.9 * len(sample):
        return None
    # Candidates: within the margin, best score first (stable: equal
    # scores keep table order). Tie-break only among the exact best
    # scores: the most non-ASCII letters inside words.
    tied = sorted((x for x in scores if best - x[0] < _CODEPAGE_TIE_MARGIN), key=lambda x: -x[0])
    top = [x for x in tied if best - x[0] < 1e-6]
    most = max(x[1] for x in top)
    finalists = [x for x in top if x[1] == most]
    tied = finalists + [x for x in tied if x not in finalists]
    pref = EURO_BASE.get(preferred, preferred)
    chosen = next((x for x in finalists if x[2] == pref), finalists[0])
    return {'codepage': preferred if chosen[2] == pref else chosen[2], 'language': chosen[3],
            'score': round(chosen[0] / len(sample), 4), 'scores': raw, 'sample_size': len(sample),
            'codepages': [x[2] for x in tied], 'certain': len(finalists) == 1}


def _detect_ebcdic_text(head, total_len, codepage):
    found = detect_ebcdic_codepage(head, preferred=codepage)
    if not found:
        return None
    conf = MEDIUM if found['score'] >= 0.97 else LOW
    if not found['certain']:
        extra = 'code page {} or {} other(s) that fit equally well'.format(
            found['codepage'], len(found['codepages']) - 1)
    else:
        extra = 'code page {} ({})'.format(found['codepage'], found['language'])
    info = _info('ebcdic-text', conf, extra)
    info.codepage = found['codepage']
    info.codepages = found['codepages']
    return info


def _detect_iebupdte(head, total_len, codepage):
    for enc in ('cp037', 'ascii'):
        if head[:2] != './'.encode(enc):
            continue
        line = head[:80].decode(enc, errors='replace')
        words = line[2:].split()
        if words and words[0] in ('ADD', 'REPL', 'CHANGE', 'REPRO') and 'NAME=' in line:
            return _info('iebupdte', HIGH, 'EBCDIC' if enc == 'cp037' else 'ASCII')
    return None


def _detect_pds_directory(head, total_len, codepage):
    '''Raw 256-byte PDS directory blocks: 2-byte used length (includes
    itself) followed by entries of name(8) TTR(3) C(1) user data.'''
    if len(head) < 256 or total_len % 256:
        return None
    names = []
    for blk in range(0, min(len(head), 256 * 8) - 255, 256):
        used = int.from_bytes(head[blk:blk + 2], 'big')
        if used < 2 or used > 256:
            return None
        loc = blk + 2
        while loc < blk + used:
            name = head[loc:loc + 8]
            if name == b'\xff' * 8:
                if len(names) >= 1 and names == sorted(names):
                    return _info('pds-directory', HIGH, '{} entries'.format(len(names)))
                return None
            if not _is_ebcdic_name(name) or loc + 12 > blk + used:
                return None
            names.append(name)
            loc += 12 + (head[loc + 11] & 0x1F) * 2
        if loc != blk + used:
            return None
    return None


# ---------------------------------------------------------------------------
# AWS / HET virtual tapes
# ---------------------------------------------------------------------------

#: Mimetypes given to AWS / HET tape images (members, tape files).
TAPE_MIMETYPES = ('application/x-aws-tape', 'application/x-het-tape')
#: Mimetype of an ALGOL 68C packed PDS (see :func:`parse_pdspack`).
PDSPACK_MIMETYPE = 'application/x-a68c-packed-pds'

_TAPE_WALK = 16  # blocks; 8 consistent blocks already give HIGH


def tape_chain(data, total_len=None, max_blocks=_TAPE_WALK):
    '''Walks the AWS/HET block header chain at the start of ``data``.

    Every block starts with a 6-byte header::

        +0  curlen  LE16  bytes of data in this block
        +2  prevlen LE16  curlen of the previous block (0 for the first)
        +4  flags1        X'80' begin record, X'40' tapemark,
                          X'20' end record, X'03' HET compression
        +5  flags2

    Rules: prevlen must equal the previous block's curlen, flags1 must
    not have X'1C' set, and a block with curlen 0 must be a tapemark.
    (CBT875's tapes start with a small data block flagged X'E0', so a
    tapemark bit on a data block is accepted.) An all-zero header after
    at least one good block is padding (tapes stored as PDS members are
    padded to the block size) and ends the walk.

    Only the headers are read, so this is cheap on a large buffer.
    ``data`` may be just the start of a file; pass the real size as
    ``total_len``.

    Returns ``(blocks, reached_end, compressed)``, or ``None`` when the
    chain is inconsistent before ``max_blocks`` (so not a tape).
    '''
    walk = _tape_walk(data, total_len, max_blocks)
    return walk and walk[:3]


def _tape_walk(data, total_len=None, max_blocks=_TAPE_WALK):
    '''tape_chain() plus a 4th value: True when the walk used up the
    whole buffer (the next header starts past it, so it is a sniff of a
    longer file and every header that fits was consistent).'''
    n = len(data)
    total = n if total_len is None else max(total_len, n)
    pos = 0
    prev = 0
    blocks = 0
    compressed = False
    while blocks < max_blocks:
        if pos == total:
            return blocks, True, compressed, False
        if pos + 6 > n:
            # Out of buffer (or a partial header at the very end).
            # used_buffer: the last block ran past the buffer.
            return blocks, False, compressed, total > n and pos > n
        hdr = data[pos:pos + 6]
        cur = hdr[0] | (hdr[1] << 8)
        prv = hdr[2] | (hdr[3] << 8)
        f1 = hdr[4]
        if blocks and bytes(hdr) == b'\x00' * 6:
            return blocks, False, compressed, False  # padding
        if prv != prev or f1 & 0x1C or (cur == 0 and not f1 & 0x40):
            return None
        if pos + 6 + cur > total:
            return None
        if f1 & 0x03:
            compressed = True
        prev = cur
        pos += 6 + cur
        blocks += 1
    return blocks, False, compressed, False


def _detect_tape(data, total_len, codepage=None):
    walk = _tape_walk(data, total_len)
    if not walk:
        return None
    blocks, reached_end, compressed, used_buffer = walk
    if blocks < 2:
        return None  # never on the first header alone
    name = 'het' if compressed else 'aws'
    if not compressed and _pdspack_name(data) is not None:
        name = 'pdspack'
    # HIGH: 8+ consistent blocks, or the chain ends exactly at the end of
    # the data, or (a sniff of a longer file) every header in the buffer
    # was consistent and large blocks carried the walk past its end.
    if blocks >= 8 or reached_end or used_buffer:
        conf = HIGH
    else:
        conf = MEDIUM
    return _info(name, conf, '{}{} consistent blocks'.format(
        blocks, '+' if blocks >= _TAPE_WALK else ''))


def is_virtual_tape(data, total_length=None):
    '''Returns ``'aws'``, ``'het'`` or ``None``: whether ``data`` is an
    AWS/HET virtual tape image (high or medium confidence). Walks at most
    16 block headers, so it is cheap on large buffers.

    An ALGOL 68C packed PDS uses the same block framing but is not a tape
    (no drive would mount it); for that this returns ``None``. Use
    :func:`is_pdspack`.'''
    info = _detect_tape(data, len(data) if total_length is None else total_length)
    return info.name if info and info.name != 'pdspack' else None


# ---------------------------------------------------------------------------
# ALGOL 68C packed PDS (PDSPACK / PDSUNPK)
# ---------------------------------------------------------------------------
#
# CBT Tape file 875 (ALGOL 68C, C.J. Cheney) distributes partitioned
# datasets packed into single FB-80 sequential files, restored on MVS by
# its PDSUNPK program (object code only; the format is not documented).
# Reverse engineered from the 14 packed libraries on that tape, the
# stream uses AWS block headers (<curlen LE16><prevlen LE16><flags1>
# <flags2>, prevlen chained):
#
#   X'E0' block   member name (1-8 EBCDIC characters) - starts a member
#   X'A0' blocks  that member's records, one record per block, at their
#                 real lengths (so RECFM=VB libraries survive packing)
#   ...           next X'E0' name block, ...
#   X'40'         tapemark (length 0) ends the stream; the 80-byte
#                 records of the packed file are zero padded after it.
#
# Records split over blocks (X'80' ... X'20') are joined.

def _pdspack_name(data):
    '''Member name in the first block if it is a PDSPACK name block.'''
    if len(data) < 7 or data[4] != 0xE0 or data[2] or data[3]:
        return None
    n = data[0] | (data[1] << 8)
    if not 1 <= n <= 8 or len(data) < 6 + n:
        return None
    name = bytes(data[6:6 + n])
    if not _is_ebcdic_name(name.ljust(8, b'\x40')):
        return None
    return name


def is_pdspack(data, total_length=None):
    '''True if ``data`` is an ALGOL 68C packed PDS (high/medium match).'''
    info = _detect_tape(data, len(data) if total_length is None else total_length)
    return bool(info) and info.name == 'pdspack'


def parse_pdspack(data, codepage=DEFAULT_CODEPAGE):
    '''Unpacks an ALGOL 68C packed PDS.

    Returns ``{'members': [{'name': str, 'records': [bytes, ...]}, ...],
    'complete': bool}``. ``complete`` is False when the stream ended
    without its tapemark. Raises :class:`ParseError` on a broken block
    chain or data before the first name block.'''
    data = bytes(data)
    n = len(data)
    pos = prev = 0
    members = []
    rec = None
    complete = False
    while pos + 6 <= n:
        cur = data[pos] | (data[pos + 1] << 8)
        prv = data[pos + 2] | (data[pos + 3] << 8)
        f1 = data[pos + 4]
        if prv != prev or f1 & 0x1C:
            if members and data[pos:pos + 6] == b'\x00' * 6:
                break  # padding
            raise ParseError('pdspack: bad block header at offset {}'.format(pos))
        if pos + 6 + cur > n:
            raise ParseError('pdspack: block at offset {} runs past end of data'.format(pos))
        body = data[pos + 6:pos + 6 + cur]
        if f1 == 0xE0 and 1 <= cur <= 8:
            members.append({'name': body.decode(codepage).strip(), 'records': []})
            rec = None
        elif f1 & 0x40:
            if cur:
                raise ParseError('pdspack: tapemark with data at offset {}'.format(pos))
            complete = True
            break
        else:
            if not members:
                raise ParseError('pdspack: data before the first member name')
            if f1 & 0x80 or rec is None:
                rec = bytearray()
            rec += body
            if f1 & 0x20:
                members[-1]['records'].append(bytes(rec))
                rec = None
        prev = cur
        pos += 6 + cur
    if not members:
        raise ParseError('pdspack: no members')
    return {'members': members, 'complete': complete}


# ---------------------------------------------------------------------------
# identify / inspect
# ---------------------------------------------------------------------------

# Order matters only as a tie-break when confidences are equal: most
# specific signatures first.
_DETECTORS: List[Tuple[str, Callable]] = [
    ('iebcopy', _detect_iebcopy),
    ('program-object', _detect_program_object),
    ('goff', _detect_goff),
    ('obj', _detect_obj),
    ('load-module', _detect_load_module),
    ('terse', _detect_terse),
    ('smf', _detect_smf),
    ('iebupdte', _detect_iebupdte),
    ('pds-directory', _detect_pds_directory),
    ('vb', _detect_vb),
    ('text', _detect_text),
]


def identify_all(data, codepage=DEFAULT_CODEPAGE, sniff=SNIFF_SIZE, total_length=None):
    '''Returns every format that matches, best first (may be empty).
    Arguments as for :func:`identify`.'''
    if not isinstance(data, (bytes, bytearray, memoryview)):
        raise ParseError('identify: expected bytes')
    head = bytes(data[:sniff])
    total = len(data) if total_length is None else max(total_length, len(head))
    found = []
    # Tape blocks are often larger than the sniff buffer, so walk the
    # block headers through all of ``data`` (cheap: headers only).
    try:
        info = _detect_tape(data, total)
    except Exception as e:
        logger.debug('detector tape failed: %r', e)
        info = None
    if info:
        found.append(info)
    for name, det in _DETECTORS:
        try:
            info = det(head, total, codepage)
        except ParseError:
            info = None
        except Exception as e:  # a detector bug must never break extraction
            logger.debug('detector %s failed: %r', name, e)
            info = None
        if info:
            found.append(info)
    # Fallback: if nothing matched with at least medium confidence, try
    # the data as EBCDIC text in every supported code page.
    if not any(_RANK[i.confidence] >= _RANK[MEDIUM] for i in found):
        try:
            info = _detect_ebcdic_text(head, total, codepage)
        except Exception as e:
            logger.debug('EBCDIC code page detection failed: %r', e)
            info = None
        if info:
            found.append(info)
    found.sort(key=lambda i: -_RANK[i.confidence])  # stable: keeps detector order
    return found


def identify(data, codepage=DEFAULT_CODEPAGE, sniff=SNIFF_SIZE, total_length=None):
    '''Identifies an MVS / z/OS binary format from the first ``sniff``
    bytes of ``data`` (default 4 KB).

    ``data`` may be just the start of a file. Pass the real file size as
    ``total_length`` so the length checks (e.g. multiple of 80) still work.

    Returns a :class:`FormatInfo`. ``name`` is ``unknown`` when nothing
    matched. When two unrelated formats match with the same top
    confidence, ``name`` is ``ambiguous`` and ``candidates`` lists them.
    Otherwise ``candidates`` holds the other, weaker matches.

    Example:
        >>> from xmi.formats import identify
        >>> identify(open('DAF.bin', 'rb').read()).name
        'load-module'
    '''
    found = identify_all(data, codepage, sniff, total_length)
    if not found:
        return FormatInfo('unknown', NONE, 'Unknown format (no MVS signature matched)')
    best = found[0]
    rest = found[1:]
    refined = _REFINES.get(best.name, set())
    rivals = [i for i in rest if i.confidence == best.confidence and i.name not in refined
              and best.name not in _REFINES.get(i.name, set())]
    if rivals:
        return FormatInfo('ambiguous', best.confidence,
                          'Ambiguous: ' + ', '.join(i.name for i in [best] + rivals),
                          candidates=[best] + rest)
    best.candidates = rest
    return best


def _pdspack_summary(r):
    return {'complete': r['complete'],
            'members': [{'name': m['name'], 'records': len(m['records'])}
                        for m in r['members']]}


_PARSERS = {
    'load-module': lambda d, cp: parse_load_module(d, cp, strict=False),
    'obj': parse_object_deck,
    'xobj': parse_object_deck,
    'goff': parse_goff,
    'iebcopy': parse_iebcopy,
    'terse': lambda d, cp: parse_terse_header(d),
    'smf': parse_smf,
    'pdspack': lambda d, cp: _pdspack_summary(parse_pdspack(d, cp)),
    'vb': lambda d, cp: {k: (len(v) if k == 'records' else v) for k, v in parse_vb(d, blocked=True).items()},
    'rdw': lambda d, cp: {k: (len(v) if k == 'records' else v) for k, v in parse_vb(d, blocked=False).items()},
}


def inspect(data, codepage=DEFAULT_CODEPAGE):
    '''Identifies ``data`` and, when a parser exists for the format,
    parses it.

    Returns a dict with ``format`` (the :class:`FormatInfo`), ``details``
    (the parser's dict, or ``None``) and ``error`` (the
    :class:`ParseError` message if parsing failed, else ``None``).
    '''
    info = identify(data, codepage)
    out = {'format': info, 'details': None, 'error': None}
    parser = _PARSERS.get(info.name)
    if parser:
        try:
            out['details'] = parser(bytes(data), codepage)
        except ParseError as e:
            out['error'] = str(e)
    return out


def to_jsonable(obj):
    '''Converts :func:`inspect` / parser output to JSON-serialisable data
    (``FormatInfo`` -> dict, bytes -> hex string).'''
    if isinstance(obj, FormatInfo):
        d = dict(obj.__dict__)
        d['candidates'] = [to_jsonable(c) for c in obj.candidates]
        return d
    if isinstance(obj, (bytes, bytearray)):
        return bytes(obj).hex()
    if isinstance(obj, dict):
        return {str(k): to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [to_jsonable(v) for v in obj]
    return obj
