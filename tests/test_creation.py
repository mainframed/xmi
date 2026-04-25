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
