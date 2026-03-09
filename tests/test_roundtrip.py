"""
Round-trip test: extract test_pds.xmi → create new XMI → extract again → compare.

Steps
-----
1. De-XMI tests/test_pds.xmi into tests/TEST.PDS/  (kept on disk after the run).
2. Build a new XMI from that folder.
3. De-XMI the new XMI into a temporary folder.
4. Assert every member file in TEST.PDS is byte-identical to its counterpart in
   the temp folder.
"""

import sys
import shutil
import tempfile
import unittest
from pathlib import Path

# Make sure the package is importable when run directly from the tests/ dir.
sys.path.insert(0, str(Path(__file__).parent.parent))

from xmi import XMIT, create_xmi

TESTS_DIR   = Path(__file__).parent
SOURCE_XMI  = TESTS_DIR / 'test_pds.xmi'
TEST_PDS    = TESTS_DIR / 'TEST.PDS'


class TestXMIRoundTrip(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Extract test_pds.xmi → TEST.PDS/ (persists after the test run)."""
        if TEST_PDS.exists():
            shutil.rmtree(TEST_PDS)

        x = XMIT(filename=str(SOURCE_XMI), quiet=True)
        x.open()
        x.set_output_folder(str(TESTS_DIR))

        dsn = list(x.xmit['file'].keys())[0]
        cls.dsn = dsn
        x.unload_pds(dsn)

        # The library names the subfolder after the dataset (e.g. PYTHON.XMI.PDS).
        # Rename it to TEST.PDS as expected.
        extracted = TESTS_DIR / dsn
        extracted.rename(TEST_PDS)

        cls.orig_files = {f.stem: f for f in TEST_PDS.iterdir() if f.is_file()}

    def test_all_members_present_after_extraction(self):
        """TEST.PDS must contain at least one member."""
        self.assertTrue(
            len(self.orig_files) > 0,
            "No member files were extracted to TEST.PDS",
        )

    def test_roundtrip_contents_match(self):
        """
        Build a new XMI from TEST.PDS, extract it, and verify every member
        file is byte-identical to the corresponding file in TEST.PDS.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)

            # Step 2 – create new XMI from TEST.PDS
            rt_xmi = tmp / 'roundtrip.xmi'
            create_xmi(str(TEST_PDS), output_file=str(rt_xmi))

            # Step 3 – extract the round-tripped XMI into a temp sub-folder
            out_root = tmp / 'extracted'
            out_root.mkdir()

            x2 = XMIT(filename=str(rt_xmi), quiet=True)
            x2.open()
            x2.set_output_folder(str(out_root))
            dsn2 = list(x2.xmit['file'].keys())[0]
            x2.unload_pds(dsn2)

            rt_folder = out_root / dsn2
            rt_files  = {f.stem: f for f in rt_folder.iterdir() if f.is_file()}

            # Step 4 – compare

            # Same set of member names
            self.assertEqual(
                set(self.orig_files.keys()),
                set(rt_files.keys()),
                "Member names differ after round-trip",
            )

            # Byte-identical content for every member
            for stem, orig_file in self.orig_files.items():
                rt_file = rt_files[stem]
                self.assertEqual(
                    orig_file.read_bytes(),
                    rt_file.read_bytes(),
                    f"Content mismatch for member '{stem}' "
                    f"({orig_file.name} vs {rt_file.name})",
                )


if __name__ == '__main__':
    unittest.main(verbosity=2)
