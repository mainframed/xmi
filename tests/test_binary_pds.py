"""
Test: packaging binary files (XMI files) as PDS members inside a container XMI.

Use-case:
  1. Create XMI1 from a folder.
  2. Create XMI2 from the same folder (different DSN).
  3. Place XMI1 and XMI2 in a new folder (MULTI.PDS).
  4. Create a container XMI from that folder.
  5. Extract the container XMI and verify each member is a valid, readable XMI.
"""

import sys
import shutil
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from xmi import create_xmi, XMIT

TESTS_DIR  = Path(__file__).parent
SOURCE_XMI = TESTS_DIR / 'test_pds.xmi'


class TestBinaryPDS(unittest.TestCase):

    def test_container_xmi_round_trip(self):
        """
        Two XMI files packaged as binary PDS members inside a container XMI
        must survive a create → extract round-trip with their contents intact.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            td = Path(tmpdir)

            # Step 1 & 2 — build two source XMI files
            xmi1 = td / 'XMI1'
            xmi2 = td / 'XMI2'
            create_xmi(str(TESTS_DIR / 'TEST.PDS'), output_file=str(xmi1), dsn='TEST.PDS1')
            create_xmi(str(TESTS_DIR / 'TEST.PDS'), output_file=str(xmi2), dsn='TEST.PDS2')

            # Step 3 — assemble the container folder
            multi = td / 'MULTI.PDS'
            multi.mkdir()
            shutil.copy(xmi1, multi / 'XMI1')
            shutil.copy(xmi2, multi / 'XMI2')

            # Step 4 — create the container XMI
            container = td / 'MULTI.XMI'
            create_xmi(str(multi), output_file=str(container), dsn='MULTI.PDS')
            self.assertTrue(container.exists(), "Container XMI was not created")
            self.assertGreater(container.stat().st_size, 0)

            # Step 5 — open the container and check its members
            x = XMIT(filename=str(container), quiet=True)
            x.open()
            dsn = list(x.xmit['file'].keys())[0]
            members = x.get_members(dsn)
            self.assertIn('XMI1', members)
            self.assertIn('XMI2', members)

            # Step 6 — extract each member and verify it is a valid XMI
            for member_name, expected_inner_dsn in [('XMI1', 'TEST.PDS1'),
                                                    ('XMI2', 'TEST.PDS2')]:
                raw = x.get_member_binary(dsn, member_name)
                self.assertGreater(len(raw), 0,
                    f"Member {member_name} extracted as empty")

                x2 = XMIT(quiet=True)
                x2.set_file_object(raw)
                x2.open()
                inner_dsn = list(x2.xmit['file'].keys())[0]
                self.assertEqual(inner_dsn, expected_inner_dsn,
                    f"Inner DSN mismatch for {member_name}")

                inner_members = x2.get_members(inner_dsn)
                self.assertGreater(len(inner_members), 0,
                    f"No members found inside {member_name}")


if __name__ == '__main__':
    unittest.main(verbosity=2)
