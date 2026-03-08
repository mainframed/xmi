"""
Parsing tests for XMI, AWS, and HET files.

Each test opens a sample file, calls list_all() to retrieve every dataset
and member, and asserts the expected structure is present.  This replaces
the three bash for-loops in test.sh that previously just checked the script
didn't crash with no content assertions.
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from xmi import list_all, XMIT

TESTS_DIR = Path(__file__).parent


class TestParsing(unittest.TestCase):

    # -- XMI files ---------------------------------------------------------

    def test_pds_xmi(self):
        """test_pds.xmi: one PDS with four members."""
        result = list_all(str(TESTS_DIR / 'test_pds.xmi'))
        self.assertIn('PYTHON.XMI.PDS(JES2HIST)', result)
        self.assertIn('PYTHON.XMI.PDS(JES2JPG)',  result)
        self.assertIn('PYTHON.XMI.PDS(SNAKE)',     result)
        self.assertIn('PYTHON.XMI.PDS(XMIT)',      result)
        self.assertEqual(len(result), 4)

    def test_pds_msg_xmi(self):
        """test_pds_msg.xmi: PDS with an embedded message."""
        result = list_all(str(TESTS_DIR / 'test_pds_msg.xmi'))
        self.assertIn('PYTHON.XMI.PDS(TESTING)', result)
        self.assertIn('PYTHON.XMI.PDS(Z15IMG)',  result)
        self.assertEqual(len(result), 2)

        x = XMIT(filename=str(TESTS_DIR / 'test_pds_msg.xmi'), quiet=True)
        x.open()
        self.assertTrue(x.has_message())

    def test_seq_xmi(self):
        """test_seq.xmi: single sequential dataset, no members."""
        result = list_all(str(TESTS_DIR / 'test_seq.xmi'))
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], 'TEST_SEQ')
        self.assertNotIn('(', result[0])  # sequential → no (MEMBER) suffix

    # -- Virtual tape files (AWS / HET) ------------------------------------

    def _assert_tape_contents(self, result):
        """Shared assertions for AWS and HET (identical logical content)."""
        self.assertIn('PYTHON.XMI.SEQ',          result)
        self.assertIn('PYTHON.XMI.PDS(JES2HIST)', result)
        self.assertIn('PYTHON.XMI.PDS(JES2JPG)',  result)
        self.assertIn('PYTHON.XMI.PDS(SNAKE)',    result)
        self.assertIn('PYTHON.XMI.PDS(XMIT)',     result)
        self.assertIn('PYTHON.SEQ.XMIT',          result)
        self.assertIn('PYTHON.PDS.XMIT',          result)

    def test_tape_aws(self):
        """test_tape.aws: AWSTAPE virtual tape with multiple datasets."""
        result = list_all(str(TESTS_DIR / 'test_tape.aws'))
        self._assert_tape_contents(result)

    def test_tape_het(self):
        """test_tape.het: HET (compressed AWSTAPE) — same content as .aws."""
        result = list_all(str(TESTS_DIR / 'test_tape.het'))
        self._assert_tape_contents(result)


if __name__ == '__main__':
    unittest.main(verbosity=2)
