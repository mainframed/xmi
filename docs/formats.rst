MVS Binary Formats
==================

libmagic (``file``) reports most mainframe binary formats as ``data``.
``xmi.formats`` recognises them, and for the most useful ones it also
parses them into Python dicts. When xmilib unloads an XMI or tape file,
any member that libmagic calls ``application/octet-stream`` is checked
with :func:`xmi.formats.identify`. A high or medium confidence match sets
``mvs_format`` on the member and gives it the extension listed below.
The mimetype and the text conversion stay exactly as before.

.. code-block:: python

    from xmi import formats

    data = open('DAF.bin', 'rb').read()
    info = formats.identify(data)          # looks at the first 4 KB only
    print(info.name, info.confidence)       # load-module high

    result = formats.inspect(data)          # identify + parse
    print(result['details']['csects'][:3])  # ['DAF', 'DAFINIT', 'DAFPROC']

    # Members of an opened XMI/tape:
    x = xmi.XMIT(filename='LOADLIB.xmi'); x.open()
    x.get_member_info('MY.LOAD', 'PROG')['mvs_format']    # 'load-module'
    x.get_member_format('MY.LOAD', 'PROG')['details']['directory']['entry_point']

From the command line, ``mvsfile FILE...`` works like ``file``, and
``mvsfile -v FILE`` prints the parsed details as JSON.

``identify()`` returns ``unknown`` rather than guessing. When two
unrelated formats match equally well it returns ``ambiguous`` and lists
them in ``candidates``. Parsers raise :class:`xmi.formats.ParseError` on
truncated or corrupt input and never execute anything. Names are decoded
with ``cp1047`` by default; every function takes a ``codepage`` argument
(``cp037`` and others work too).

Supported formats
-----------------

=================  ======  ==========  ================================================================
Name               Ext     Parsed      Signature
=================  ======  ==========  ================================================================
load-module        .lmd    yes         Byte 0 is X'20' (CESD: bytes 2-3 zero, bytes 6-7 a multiple of
                                       16, up to 240) or X'40' (SYM), and the records chain exactly
                                       (see below).
program-object     .pob    detect      EBCDIC ``IEWPLMH`` at offset 0. This is the PDSE binder module
                                       header. IBM does not publish the format.
obj                .obj    yes         80-byte cards with X'02' then ``ESD``/``TXT``/``RLD``/``END``/
                                       ``SYM`` in columns 2-4. Columns 5-10 are blank on ESD cards.
xobj               .obj    yes         As OBJ, plus ``XSD`` cards or an SD named ``@@DOPLNK``.
goff               .gof    yes         X'03F000' (HDR) at offset 0. Every 80-byte record starts X'03',
                                       a known type nibble and version X'00'. RDW-prefixed variable
                                       records are also accepted.
iebcopy            .ieb    yes         X'CA6D0F' at offset 1 (bare records, as inside XMI), 5 (RDW)
                                       or 9 (BDW+RDW), and DS1DSORG has the PO bit.
terse              .trs    header      X'01'/X'07' + X'8969A5' (native SPACK), or X'02' (PACK) / X'05'
                                       (SPACK) with the openterse header rules.
smf                .smf    counts      RDW (optional BDW). Then a time below 8,640,000 hundredths,
                                       a packed date ``0cyydddF`` and an EBCDIC SID.
vb / rdw           .vb     records     BDWs whose RDWs add up exactly to the block length.
iebupdte           .upd    detect      ``./ ADD``/``REPL``/``CHANGE``/``REPRO`` with ``NAME=`` on the
                                       first line, in EBCDIC or ASCII.
pds-directory      .dir    detect      256-byte blocks with a 2-byte used length. Entries are in
                                       sorted order and end with X'FF' * 8.
aws                .aws    detect      AWS virtual tape: a chain of 6-byte block headers (see below).
het                .het    detect      As AWS, with HET compression bits (X'01' zlib, X'02' bzip2) set.
ebcdic-text        .txt    detect      Heuristic. This is only a hint, and libmagic's decision is
                                       kept.
ascii-text         .txt    detect      Heuristic.
=================  ======  ==========  ================================================================

Virtual tapes (AWS / HET)
-------------------------

Every block of an AWS or HET image starts with a 6-byte header:
``<curlen LE16><prevlen LE16><flags1><flags2>``. flags1 is X'80' begin
of record, X'40' tapemark and X'20' end of record; HET also uses X'03'
for compression. :func:`xmi.formats.tape_chain` walks the headers:

* the first block's prevlen is 0, and every later prevlen equals the
  curlen of the block before it;
* flags1 X'1C' must be clear, and a block with curlen 0 must be a
  tapemark;
* an all-zero header after a good block is padding (tapes stored as PDS
  members are padded to the block size) and ends the walk.

Two to seven consistent blocks give medium confidence; eight or more, or
a chain that ends exactly at the end of the data, give high confidence.
One header is never enough. Some real tapes (CBT875) start with a small
data block flagged X'E0'; that is accepted. Only headers are read, at
most 16, so the check is cheap on large members.

``XMIT.open()`` uses the same check, so a binary whose bytes 2-4 happen
to be zero is rejected with "File is not XMI or Virtual Tape" instead of
being parsed as a tape. Tape images found as PDS members, sequential
datasets in an XMI or files on another tape get extension ``.aws`` /
``.het``, mimetype ``application/x-aws-tape`` /
``application/x-het-tape`` and are never converted to text.
``XMIT.is_tape_member(pds, member)`` and ``XMIT.is_tape_file(name)``
report them without sniffing again, so callers can open them
recursively.

**Dataset names.** A standard label (HDR1) holds only the rightmost 17
characters of the DSN, so ``ALGOL.F.COMPILER.MACLIB`` comes back as
``F.COMPILER.MACLIB``. No longer name is available on the tape: HDR2
and the IEBCOPY COPYR1/COPYR2 records do not contain the DSN. When the
HDR1 name fills all 17 characters, the file and its ``HDR1`` dict carry
``dsn_truncated: True`` (also in ``get_file_info_detailed()`` and the
JSON). A name of exactly 17 characters is flagged too, because the two
cases look the same.

**Unlabeled tapes** name their files ``FILE0001``, ``FILE0002`` and so
on. For a single-file unlabeled tape, ``open(infile='NAME')`` uses
``NAME`` instead, e.g. the name of the member the tape came from.

Load module record boundaries
-----------------------------

A load module member is RECFM=U. Unloaded without RDWs, the record
boundaries are gone, but every record says how long it is (*z/OS MVS
Program Management: Advanced Facilities*, Appendix B):

=========================  ==================================================
Record (byte 0)            Length
=========================  ==================================================
SYM X'40'                  4 + count at bytes 2-3
CESD X'20'                 8 + count at bytes 6-7
Scatter/translation X'10'  4 + count at bytes 2-3
IDR X'80'                  1 + byte 1 (the count includes itself)
Control X'01' X'05' X'0D'  16 + control data count at bytes 4-5
RLD X'02' X'06' X'0E'      16 + RLD data count at bytes 6-7
Ctl+RLD X'03' X'07' X'0F'  16 + bytes 4-5 + bytes 6-7
Signing X'88'              REC_LENGTH at bytes 4-5
=========================  ==================================================

A text record follows every control record. Its length is the CCW count:
bytes 14-15 of the control record, the last halfword of the CCW at bytes
8-15. X'x5', X'x6' and X'x7' mark the end of an overlay segment; X'0D',
X'0E' and X'0F' mark the end of the module.

Some modules (4 of 712 on CBT) were written with zero-padded blocks.
Those can't be walked past the padding. ``parse_load_module(strict=False)``
(the default used by ``inspect``) returns what it found, which always
includes the CESD and IDR because they come first, and sets
``complete: False``.

The entry point of a load module is **not** in its records. It is in the
PDS directory entry (PDS2EPA). :func:`xmi.formats.parse_pds2_user_data`
decodes it, and ``XMIT.get_member_format()`` attaches it automatically.

Translator IDR data is one byte stream that continues across consecutive
translator IDR records. Items, even single date fields, can be split at a
record boundary, so the stream is joined before it is parsed.

EBCDIC code pages
-----------------

:func:`xmi.formats.detect_ebcdic_codepage` decodes a 2 KB sample with
each single-byte EBCDIC code page Python knows about. That is 20 pages:
US/International (cp037, cp1047, cp500), the Latin national pages,
Latin-2, Turkish, Baltic, Estonian, Greek, Cyrillic, Hebrew, Arabic and
Thai. It scores how text-like each result is and returns the best page,
every page that fits equally well (``codepages``) and whether one page
clearly won (``certain``). Euro variants (cp1140-cp1149) are treated as
their base pages because they differ only at X'9F'.

Ties are common and reported honestly:

* A-Z text decodes identically in every Latin page.
* The national pages put each language's most frequent national letters
  at the same byte positions. cp297 French ``é`` is X'C0', and so is cp277
  Danish ``æ``.
* Hebrew letters sit where cp875 has upper case Greek, and Cyrillic and
  Arabic overlap.

**When xmilib converts with a different page.** When it extracts text,
xmilib uses the configured ``encoding`` (``extractxmi`` uses cp1140)
unless :func:`xmi.formats.choose_codepage` decides otherwise. That
function is deliberately narrow:

* cp1047 replaces cp037/cp500/cp1140 when it wins by at least one bracket
  pair (``[`` ``]`` against ``Ý`` ``¨``). This is the usual problem with
  C, USS and REXX source.
* Greek, Cyrillic, Hebrew, Arabic or Thai replace it only when detection
  is certain and wins clearly. Because of the overlaps above, that is
  rare.
* National Latin pages are never chosen automatically. MVS text uses
  ``$ # @ ! | ¢ { } \ ~`` as syntax (ISPF panel attributes, JCL and
  assembler names). Those are letters in the national pages, and on CBT
  Tape switching to them was wrong far more often than right. Pass
  ``encoding=`` (``--encoding``) yourself for national-language text;
  ``mvsfile`` and ``identify()`` show the likely page.

When a member was converted with a detected page, ``get_member_info()``
reports it as ``codepage``. Members that libmagic reported as binary but
that are EBCDIC text are now converted to text (with the configured
page). Use ``XMIT(detect_codepage=False)`` or ``extractxmi
--no-detect-codepage`` to turn this off.

Not supported
-------------

* SVC dump / SYSMDUMP and DFSMSdss (ADRDSSU) dump data sets: I found no
  documented constant signature. The ADRDSSU record prefix is only a
  16-byte header with a length byte of 16, which is too weak.
* VMARC and other CMS formats: no verified signature.
* DBCS EBCDIC (Japanese cp930/cp939, Chinese, Korean mixed): Python has
  no codecs for them, so they are not detected.
* TERSE decompression: this works only if the optional
  `openterse <https://github.com/mainframed/openterse>`_ package is
  installed (:func:`xmi.formats.decompress_terse`). It is not a dependency.
