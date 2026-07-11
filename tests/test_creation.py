"""Tests for XMI creation — message embedding feature."""

import io
import logging
import struct
import sys
import tempfile
import unittest
import unittest.mock
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from xmi import resolve_message, MESSAGE_FORMATS, create_xmi, XMIT


class TestResolveMessage(unittest.TestCase):

    def test_unknown_format_raises(self):
        with self.assertRaises(ValueError) as ctx:
            resolve_message(message='hello', message_format='99x99')
        self.assertIn('99x99', str(ctx.exception))

    def test_no_message_returns_none_text(self):
        text, lrecl, max_lines = resolve_message()
        self.assertIsNone(text)
        self.assertEqual(lrecl, 80)
        self.assertEqual(max_lines, 32)

    def test_whitespace_only_returns_none_text(self):
        text, lrecl, max_lines = resolve_message(message='   \n  ')
        self.assertIsNone(text)
        self.assertEqual(lrecl, 80)
        self.assertEqual(max_lines, 32)

    def test_inline_message_returned(self):
        text, lrecl, max_lines = resolve_message(message='Hello World')
        self.assertEqual(text, 'Hello World')
        self.assertEqual(lrecl, 80)
        self.assertEqual(max_lines, 32)

    def test_literal_backslash_n_expanded(self):
        text, _, _ = resolve_message(message='line1\\nline2')
        self.assertIn('\n', text)
        lines = text.splitlines()
        self.assertEqual(lines[0], 'line1')
        self.assertEqual(lines[1], 'line2')

    def test_file_takes_precedence_over_inline(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write('from file')
            fname = f.name
        text, _, _ = resolve_message(message='from inline', message_file=fname)
        self.assertEqual(text, 'from file')
        Path(fname).unlink()

    def test_line_truncation_warning(self):
        long_line = 'A' * 85  # exceeds lrecl=80
        buf = io.StringIO()
        with unittest.mock.patch('sys.stderr', buf):
            text, lrecl, _ = resolve_message(message=long_line)
        self.assertEqual(len(text), 80)
        self.assertIn('truncated', buf.getvalue())
        self.assertIn('80', buf.getvalue())

    def test_line_count_truncation_warning(self):
        many_lines = '\n'.join(['line'] * 35)  # exceeds max_lines=32
        buf = io.StringIO()
        with unittest.mock.patch('sys.stderr', buf):
            text, _, max_lines = resolve_message(message=many_lines)
        self.assertEqual(len(text.splitlines()), 32)
        self.assertIn('truncated', buf.getvalue())
        self.assertIn('32', buf.getvalue())

    def test_format_132x27(self):
        text, lrecl, max_lines = resolve_message(message='hello', message_format='132x27')
        self.assertEqual(lrecl, 132)
        self.assertEqual(max_lines, 27)

    def test_message_formats_constant_has_both_presets(self):
        self.assertIn('80x32', MESSAGE_FORMATS)
        self.assertIn('132x27', MESSAGE_FORMATS)
        self.assertEqual(MESSAGE_FORMATS['80x32'], (80, 32))
        self.assertEqual(MESSAGE_FORMATS['132x27'], (132, 27))


class TestInmr01Message(unittest.TestCase):

    def setUp(self):
        self.builder = XMIT(encoding='cp500')

    def test_inmnumf_is_2_when_has_message(self):
        result = self.builder._xmi_inmr01('USER', 'NODE', 'USER2', 'NODE2', has_message=True)
        # INMNUMF key=0x102F, count=1, len=1, value=2
        self.assertIn(b'\x10\x2f\x00\x01\x00\x01\x02', result)

    def test_inmnumf_is_1_without_message(self):
        result = self.builder._xmi_inmr01('USER', 'NODE', 'USER2', 'NODE2', has_message=False)
        self.assertIn(b'\x10\x2f\x00\x01\x00\x01\x01', result)

    def test_inmterm_not_in_inmr01(self):
        # INMTERM belongs in the message INMR02, not INMR01
        result = self.builder._xmi_inmr01('USER', 'NODE', 'USER2', 'NODE2', has_message=True)
        self.assertNotIn(b'\x00\x28\x00\x00', result)

    def test_seq_dataset_inmr02_has_file_number_2_with_message(self):
        # When a message is present, the dataset is file 2; z/OS RECEIVE uses
        # the numfiles field to match INMR02 descriptors to INMR03/data pairs.
        # Both INMR02s having numfiles=1 leaves file 2 with no descriptor and
        # z/OS reports RECFM=U on the dataset.
        result = self.builder._xmi_inmr02_seq('TEST', 80, 'FB', 100, file_number=2)
        self.assertIn(struct.pack('>I', 2), result[:20])  # numfiles=2 near start

    def test_seq_dataset_inmr02_has_file_number_1_without_message(self):
        result = self.builder._xmi_inmr02_seq('TEST', 80, 'FB', 100, file_number=1)
        self.assertIn(struct.pack('>I', 1), result[:20])  # numfiles=1 near start

    def test_build_seq_xmi_dataset_numfiles_2_with_message(self):
        import tempfile
        from pathlib import Path
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write('data\n')
            src = f.name
        from xmi import resolve_message
        resolved_msg = resolve_message(message='hello')
        xmi = self.builder._build_seq_xmi(Path(src), resolved_msg=resolved_msg)
        Path(src).unlink()
        inmr02_tag = 'INMR02'.encode('cp500')
        pos = 0
        file_numbers = []
        while pos < len(xmi):
            seg_len = xmi[pos]
            if seg_len == 0: break
            data = xmi[pos+2:pos+seg_len]
            if data[:6] == inmr02_tag:
                file_numbers.append(struct.unpack_from('>I', data, 6)[0])
            pos += seg_len
        self.assertEqual(file_numbers, [1, 2],
                         'message INMR02=file1, dataset INMR02=file2')


class TestMessageStream(unittest.TestCase):

    def setUp(self):
        self.builder = XMIT(encoding='cp500')

    def test_stream_contains_inmr02_record(self):
        result = self.builder._xmi_message_stream('Hello', 80)
        self.assertIn('INMR02'.encode('cp500'), result)

    def test_stream_contains_inmr03_record(self):
        result = self.builder._xmi_message_stream('Hello', 80)
        self.assertIn('INMR03'.encode('cp500'), result)

    def test_stream_does_not_contain_inmdsnam(self):
        result = self.builder._xmi_message_stream('Hello', 80)
        # INMDSNAM (key=0x0002) must be absent — parser uses its absence to
        # identify this INMR02 as a message stream rather than a dataset.
        # Check several encodings: single-qualifier, multi-qualifier, one-char minimum.
        self.assertNotIn(self.builder._xmi_dsn_tu('A'), result)
        self.assertNotIn(self.builder._xmi_dsn_tu('TESTDSN'), result)
        self.assertNotIn(self.builder._xmi_dsn_tu('MY.DATASET'), result)

    def test_stream_contains_inmterm_flag(self):
        result = self.builder._xmi_message_stream('Hello', 80)
        # INMTERM (key=0x0028, count=0) must be inside the message INMR02
        self.assertIn(b'\x00\x28\x00\x00', result)

    def test_stream_lrecl_251_in_inmr02(self):
        # Message INMR02 always uses LRECL=251 (z/OS transport value)
        result = self.builder._xmi_message_stream('Hello', 132)
        self.assertIn(struct.pack('>HHHi', 0x0042, 1, 4, 251), result)

    def test_stream_lrecl_132_in_inmr03(self):
        # Actual text LRECL lives in INMR03 as a 2-byte value
        result = self.builder._xmi_message_stream('Hello', 132)
        # INMLRECL key 0x0042 + count 1 + len 2 + value 132 big-endian
        self.assertIn(struct.pack('>HHHh', 0x0042, 1, 2, 132), result)

    def test_stream_individual_records_per_line(self):
        # Each message line must be a separate 0xC0 record, not one big block
        two_line = 'line1\nline2'
        result = self.builder._xmi_message_stream(two_line, 80)
        # Count 0xC0 data-record flags — expect one per line
        segments = []
        loc = 0
        while loc < len(result):
            seg_len = result[loc]
            flag = result[loc + 1]
            if not (flag & 0x20):  # data segment
                segments.append(flag)
            loc += seg_len
        self.assertEqual(segments.count(0xC0), 2)


class TestRoundTrip(unittest.TestCase):
    '''Create XMI with message via low-level builders, parse back, verify.'''

    def _make_seq_xmi_with_message(self, message, message_format='80x32'):
        resolved_msg = resolve_message(message=message, message_format=message_format)
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write('hello world\n')
            src = f.name
        builder = XMIT(encoding='cp500')
        xmi_bytes = builder._build_seq_xmi(
            Path(src), resolved_msg=resolved_msg)
        Path(src).unlink()
        return xmi_bytes

    def _make_pds_xmi_with_message(self, message):
        resolved_msg = resolve_message(message=message)
        with tempfile.TemporaryDirectory() as d:
            (Path(d) / 'MEMBER1').write_text('content\n')
            builder = XMIT(encoding='cp500')
            return builder._build_pds_xmi(Path(d), resolved_msg=resolved_msg)

    def test_seq_xmi_has_message_after_roundtrip(self):
        xmi_bytes = self._make_seq_xmi_with_message('Hello from Python!')
        with tempfile.NamedTemporaryFile(suffix='.xmi', delete=False) as f:
            f.write(xmi_bytes)
            fname = f.name
        x = XMIT(filename=fname, quiet=True)
        x.open()
        self.assertTrue(x.has_message())
        self.assertIn('Hello from Python!', x.get_message())
        Path(fname).unlink()

    def test_pds_xmi_has_message_after_roundtrip(self):
        xmi_bytes = self._make_pds_xmi_with_message('PDS message here')
        with tempfile.NamedTemporaryFile(suffix='.xmi', delete=False) as f:
            f.write(xmi_bytes)
            fname = f.name
        x = XMIT(filename=fname, quiet=True)
        x.open()
        self.assertTrue(x.has_message())
        self.assertIn('PDS message here', x.get_message())
        Path(fname).unlink()

    def test_no_message_xmi_has_no_message(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write('hello\n')
            src = f.name
        builder = XMIT(encoding='cp500')
        xmi_bytes = builder._build_seq_xmi(Path(src))
        Path(src).unlink()
        with tempfile.NamedTemporaryFile(suffix='.xmi', delete=False) as f:
            f.write(xmi_bytes)
            fname = f.name
        x = XMIT(filename=fname, quiet=True)
        x.open()
        self.assertFalse(x.has_message())
        Path(fname).unlink()

    def test_message_format_132x27_lrecl_in_roundtrip(self):
        xmi_bytes = self._make_seq_xmi_with_message('Wide message', message_format='132x27')
        with tempfile.NamedTemporaryFile(suffix='.xmi', delete=False) as f:
            f.write(xmi_bytes)
            fname = f.name
        x = XMIT(filename=fname, quiet=True)
        x.open()
        self.assertTrue(x.has_message())
        self.assertIn('Wide message', x.get_message())
        Path(fname).unlink()

    def test_binary_member_padded_to_lrecl_boundary(self):
        # A binary file (e.g. an inner XMI) stored in an FB PDS must be padded
        # to a multiple of LRECL so the IEBCOPY sub-block data_len is aligned.
        # Unpadded binary caused z/OS IEBCOPY restore to fail (RECFM=FB records
        # with a non-LRECL-multiple chunk length).
        builder = XMIT(encoding='cp500')
        # Manufacture bytes that aren't UTF-8 and aren't LRECL-aligned
        fake_xmi = b'\xff\x80\xfe\x81' * 100 + b'\xdd\xee\xff'  # 403 bytes, non-UTF-8
        assert len(fake_xmi) % 80 != 0, 'test pre-condition'
        result = builder._xmi_encode_input(fake_xmi, 80, 'FB')
        self.assertEqual(len(result) % 80, 0, 'binary member must be padded to LRECL multiple')
        self.assertTrue(result.startswith(fake_xmi), 'original bytes must be preserved')

    def test_nested_xmi_roundtrip(self):
        # Create an inner XMI (PDS), then package it with a text file into an
        # outer PDS XMI, parse the outer back and confirm both members exist.
        with tempfile.TemporaryDirectory() as d:
            inner_dir = Path(d) / 'INNER'
            inner_dir.mkdir()
            (inner_dir / 'MEMBER1').write_text('inner content\n')
            inner_xmi_bytes = create_xmi(str(inner_dir))

        with tempfile.TemporaryDirectory() as d:
            outer_dir = Path(d) / 'OUTER'
            outer_dir.mkdir()
            (outer_dir / 'INNERXMI').write_bytes(inner_xmi_bytes)
            (outer_dir / 'README').write_text('text member\n')
            outer_xmi_bytes = create_xmi(str(outer_dir))

        with tempfile.NamedTemporaryFile(suffix='.xmi', delete=False) as f:
            f.write(outer_xmi_bytes)
            fname = f.name
        x = XMIT(filename=fname, quiet=True)
        x.open()
        members = x.get_members(x.get_files()[0])
        self.assertIn('INNERXMI', members)
        self.assertIn('README', members)
        Path(fname).unlink()

    def test_pds_directory_ebcdic_order(self):
        # Regression: Python sort (ASCII) puts digits before letters, but EBCDIC
        # puts letters before digits.  A folder with names like MEMBERA + MEMBER1
        # must produce directory entries in EBCDIC order or z/OS IEBCOPY reports
        # IEB189I "directory entry out of sequence".
        with tempfile.TemporaryDirectory() as d:
            folder = Path(d) / 'SORTTEST'
            folder.mkdir()
            # These names sort differently in ASCII vs EBCDIC:
            # ASCII:  MEMBER1 < MEMBERA  (digit 0x31 < letter 0x41)
            # EBCDIC: MEMBERA < MEMBER1  (letter 0xC1 < digit 0xF1)
            names_on_disk = ['MEMBER1', 'MEMBERA', 'PART2', 'PARTA']
            for name in names_on_disk:
                (folder / name).write_text('content\n')
            xmi_bytes = create_xmi(str(folder))

        with tempfile.NamedTemporaryFile(suffix='.xmi', delete=False) as f:
            f.write(xmi_bytes)
            fname = f.name
        x = XMIT(filename=fname, quiet=True)
        x.open()
        members = x.get_members(x.get_files()[0])
        # Verify directory order is EBCDIC (letters before digits)
        self.assertEqual(members, sorted(members,
            key=lambda n: n.encode('cp500' if hasattr(x, 'ebcdic') else 'cp500').ljust(8, b'\x40')),
            'directory entries must be in EBCDIC collating order')
        Path(fname).unlink()

    def test_pds_more_than_five_members(self):
        # Regression: >5 members spanned two directory blocks; the reader
        # stopped at the first block's end-of-directory sentinel and dropped
        # the 6th+ members.
        with tempfile.TemporaryDirectory() as d:
            folder = Path(d) / 'BIGPDS'
            folder.mkdir()
            names = ['MEM{}'.format(i) for i in range(7)]
            for name in names:
                (folder / name).write_text('content of {}\n'.format(name))
            xmi_bytes = create_xmi(str(folder))

        with tempfile.NamedTemporaryFile(suffix='.xmi', delete=False) as f:
            f.write(xmi_bytes)
            fname = f.name
        x = XMIT(filename=fname, quiet=True)
        x.open()
        members = x.get_members(x.get_files()[0])
        for name in names:
            self.assertIn(name, members, 'member {} missing from parsed PDS'.format(name))
        Path(fname).unlink()


def _max_netdata_record(data):
    '''Walk a NETDATA segment stream; return the longest logical record.

    Segment: length byte (incl. itself + flag) + flag + payload. Flag
    0x20 = control record; data records span segments via first (0x80) /
    middle (0x00) / last (0x40) / single (0xC0) flags.
    '''
    pos, cur, biggest = 0, 0, 0
    while pos < len(data):
        seglen = data[pos]
        if seglen == 0:
            break
        flag = data[pos + 1]
        if not flag & 0x20:
            cur += seglen - 2
            if flag & 0x40 or (flag & 0xC0) == 0xC0:
                biggest = max(biggest, cur)
                cur = 0
        pos += seglen
    return biggest


class TestTransportSize(unittest.TestCase):
    '''INMCOPY transport LRECL/BLKSIZE must cover the largest record a PDS
    build can actually emit, or a real z/OS RECEIVE/RECV370 allocates its
    work dataset too small and abends IEC036I 002-18 + SC03 the moment a
    full-size final sub-block appears. A local round-trip never catches
    this because parsing ignores the transport values -- these tests
    inspect the declared transport fields directly, and simulate the
    on-target constraint by scanning the actual NETDATA record lengths.
    '''

    def test_transport_size_covers_default_fb_blksize(self):
        builder = XMIT(encoding='cp500')
        lrecl, blksz = builder._xmi_transport_size(3200)
        self.assertEqual(lrecl, 3200 + 12 + 12 + 4)
        self.assertEqual(blksz, lrecl + 4)

    def test_inmr02_pds_declares_computed_transport_size_not_hardcoded(self):
        builder = XMIT(encoding='cp500')
        rec = builder._xmi_inmr02_pds('TEST.PDS', 80, 'FB', 2, inmsize=1000)
        expected_lrecl, expected_blksz = builder._xmi_transport_size(3200)
        # The old hardcoded values must NOT appear as the declared transport
        # size; the computed ones must.
        self.assertNotEqual((expected_lrecl, expected_blksz), (3216, 3220))
        self.assertIn(struct.pack('>I', expected_lrecl), rec)
        self.assertIn(struct.pack('>I', expected_blksz), rec)

    def test_copyr1_transport_blocksize_matches_inmr02(self):
        builder = XMIT(encoding='cp500')
        _, expected_blksz = builder._xmi_transport_size(3200)
        self.assertNotEqual(expected_blksz, 3220)
        copyr1 = builder._xmi_copyr1(80, 'FB', 3200)
        self.assertEqual(struct.unpack('>H', copyr1[14:16])[0], expected_blksz)

    def test_full_last_block_member_fits_declared_transport_lrecl(self):
        # Reproduces the OPNTERSE bug: a binary member whose length is an
        # exact multiple of the PDS blksize, guaranteeing the last IEBCOPY
        # sub-block is completely full -- the worst case for the transport
        # LRECL/BLKSIZE declaration. Mirrors OPNTERSE's actual layout (task
        # JCL text member + nested-XMIT binary member) so the folder is
        # "mixed" and stays RECFM=FB -- an all-binary folder would
        # auto-switch to RECFM=U (blksize 6233) via _build_pds_xmi's
        # auto-detection, which is a different scenario, not this one.
        builder = XMIT(encoding='cp500')
        blksize = builder._xmi_blksize(80, 'FB')
        with tempfile.TemporaryDirectory() as d:
            binary_payload = (b'\xff\xfe\x00\x01' * (blksize // 4)) * 3
            self.assertEqual(len(binary_payload) % blksize, 0)
            (Path(d) / 'BIGMEMBR.bin').write_bytes(binary_payload)
            (Path(d) / 'TASKJCL.jcl').write_text('//TASKJCL JOB\n')
            xmi_bytes = builder.build_xmi(d, dsn='TEST.PDS', from_user='TEST',
                                           from_node='TEST', to_user='TEST',
                                           to_node='TEST')

        declared_lrecl, _ = builder._xmi_transport_size(blksize)
        biggest = _max_netdata_record(xmi_bytes)
        self.assertLessEqual(
            biggest + 4, declared_lrecl,
            'largest NETDATA record {} (+4 RDW) exceeds declared transport '
            'LRECL {} -- a real RECV370/RECEIVE would abend IEC036I 002-18'
            .format(biggest, declared_lrecl))

        # And the file must still parse correctly (round-trip sanity).
        with tempfile.NamedTemporaryFile(suffix='.xmi', delete=False) as f:
            f.write(xmi_bytes)
            fname = f.name
        x = XMIT(filename=fname, quiet=True)
        x.open()
        self.assertIn('BIGMEMBR', x.get_members(x.get_files()[0]))
        Path(fname).unlink()


class TestPublicAPI(unittest.TestCase):

    def test_create_xmi_with_inline_message(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write('dataset content\n')
            src = f.name
        with tempfile.NamedTemporaryFile(suffix='.xmi', delete=False) as out:
            out_path = out.name
        create_xmi(src, output_file=out_path, message='Inline message')
        x = XMIT(filename=out_path, quiet=True)
        x.open()
        self.assertTrue(x.has_message())
        self.assertIn('Inline message', x.get_message())
        Path(src).unlink()
        Path(out_path).unlink()

    def test_create_xmi_message_file_takes_precedence(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write('dataset content\n')
            src = f.name
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as mf:
            mf.write('from file')
            msg_path = mf.name
        with tempfile.NamedTemporaryFile(suffix='.xmi', delete=False) as out:
            out_path = out.name
        create_xmi(src, output_file=out_path,
                   message='from inline', message_file=msg_path)
        x = XMIT(filename=out_path, quiet=True)
        x.open()
        self.assertIn('from file', x.get_message())
        self.assertNotIn('from inline', x.get_message())
        Path(src).unlink()
        Path(msg_path).unlink()
        Path(out_path).unlink()

    def test_create_xmi_no_message_no_inmterm(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write('dataset content\n')
            src = f.name
        xmi_bytes = create_xmi(src)
        self.assertNotIn(b'\x00\x28\x00\x00', xmi_bytes)  # INMTERM key=0x0028, count=0
        Path(src).unlink()

    def test_create_xmi_unknown_format_raises(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write('content\n')
            src = f.name
        with self.assertRaises(ValueError):
            create_xmi(src, message='hello', message_format='bad')
        Path(src).unlink()


class TestCLI(unittest.TestCase):

    def test_cli_message_flag_invokes_create_main(self):
        from xmi.cli import create_main
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write('content\n')
            src = f.name
        with tempfile.NamedTemporaryFile(suffix='.xmi', delete=False) as out:
            out_path = out.name
        with unittest.mock.patch('sys.argv', ['createxmi', src, '-o', out_path,
                                              '--message', 'CLI message test']):
            create_main()
        x = XMIT(filename=out_path, quiet=True)
        x.open()
        self.assertTrue(x.has_message())
        self.assertIn('CLI message test', x.get_message())
        Path(src).unlink()
        Path(out_path).unlink()

    def test_cli_message_format_flag_invokes_create_main(self):
        from xmi.cli import create_main
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write('content\n')
            src = f.name
        with tempfile.NamedTemporaryFile(suffix='.xmi', delete=False) as out:
            out_path = out.name
        with unittest.mock.patch('sys.argv', ['createxmi', src, '-o', out_path,
                                              '--message', 'Wide message',
                                              '--message-format', '132x27']):
            create_main()
        x = XMIT(filename=out_path, quiet=True)
        x.open()
        self.assertTrue(x.has_message())
        self.assertIn('Wide message', x.get_message())
        Path(src).unlink()
        Path(out_path).unlink()
