IEBCOPY File Format
===================

``IEBCOPY`` is an IBM program with multiple uses, but for the sake of this
library we will talk about how it produces PDS unloads and their various
controls records as well as the directory structure.

``IEBCOPY`` is used to *unload* partitioned datasets to a tape,
virtual tape, xmi, file etc. Its format is made up of controls records (COPYR1
and COPYR2), a listing of members and their metadata followed by the PDS
members file data.

COPYR1 Control Record
~~~~~~~~~~~~~~~~~~~~~

This record contains information about the file itself. It contains multiple
items that can be used rebuild the PDS.

The COPYR1 record is 64 bytes long. Skipping the first 8 bytes this record
has the *eye catcher* of ``0xCA6D0F`` which identifies it as a COPYR1 record.

After the eye catcher is the following information:

- DS1DSORG - Dataset organization
- DS1BLKL - Block size
- DS1LRECL - Record length for all members
- DS1RECFM - Record format
- DS1REFD - Date last referenced

Multiple other fields are available in the COPYR1 record. Please refer to
the following for more details: https://www.ibm.com/support/knowledgecenter/SSLTBW_2.2.0/com.ibm.zos.v2r2.idau100/u1322.htm

COPYR2 Control Record
~~~~~~~~~~~~~~~~~~~~~

Immediately following the COPYR1 control record is the COPYR2 record. This
record contains information about the Data Extent Block (DEB) for the original
dataset. This information is collected by this library but largely unused.

Directory Information
~~~~~~~~~~~~~~~~~~~~~

Following the control records is the directory information. This section is
variable length and can span multiple blocks. It contains the member names
and metadata about the member. This section begins with::

    00 00 00 00 00 00 00 00

or for PDSE::

    08 00 00 00 00 00 00 00

and the key length, the length of the directory and the
name of the last opened member.

After the header multiple entries exist, one for each member (a member is
essentially a file) in this PDS. Each entry contains the following:

- Member name
- TTR, a pointer to the data for this member
- Number of notes attached to this member
- Alias flag, if enabled it means this member is an alias to another member

On top of this information optional information may be stored in the parameters
field. This information is called "ISPF stats" since it is used mostly in
ISPF when viewing and editing files. It can contain the following:

- The file version, which can be automatically incremented by ISPF
- Created date
- Last modified date (down to the microsecond)
- How many line the original file had
- How many lines have been added
- How many lines have been modified
- The owner of the file




Member Data
~~~~~~~~~~~

After the directory block is the member data which can be broken down as:

- Flag (1 byte)
- Original "extent", labeled as 'M' by IBM, (1 byte)
- Binary number (2 bytes)
- TTR
- Data length

Followed by the member data.

More information about member data here: https://www.ibm.com/support/knowledgecenter/SSLTBW_2.2.0/com.ibm.zos.v2r2.idau100/u1327.htm

Metadata IEBCOPY
~~~~~~~~~~~~~~~~

So what does this all look like on actual PDS? Using this library we can export
the metadata from the XMI file ``test_pds_msg.xmi`` in the ``tests`` folder:

.. code-block:: json

    {
    "PYTHON.XMI.PDS": {
    "COPYR1": {
        "type": "PDS",
        "DS1DSORG": 512,
        "DS1BLKL": 27920,
        "DS1LRECL": 80,
        "DS1RECFM": "FB",
        "DS1KEYL": 0,
        "DS1OPTCD": 32,
        "DS1SMSFG": 0,
        "file_tape_blocksize": 3120,
        "DVAOPTS": 12336,
        "DVACLASS": 32,
        "DVAUNIT": 15,
        "DVAMAXRC": 32760,
        "DVACYL": 10017,
        "DVATRK": 15,
        "DVATRKLN": 58786,
        "DVAOVHD": 0,
        "num_header_records": 2,
        "DS1REFD": "210067",
        "DS1SCEXT": "b'\\x80m\\x10'",
        "DS1SCALO": "b'P\\x00\\x00\\x02'",
        "DS1LSTAR": "b'\\x00\\x02\\x02'",
        "DS1TRBAL": "b'\\x9f>'"
    },
    "COPYR2": {
        "deb": "b'\\x01\\x00\\x00\\x00\\xff\\x00\\x00\\x00\\x8f\\x08\\x80\\x00\\x04\\x8b\\x00'",
        "extents": [
        "b'\\x01\\x00\\x00\\x00\\xff\\x00\\x00\\x00\\x8f\\x08\\x80\\x00\\x04\\x8b\\x00'",
        "b'X\\xf4\\xe8X\\x00\\x00\\x01\\x0e\\x00\\x0b\\x01\\x0f\\x00\\x01\\x00\\x06'",
        "b'\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00'",
        "b'\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00'",
        "b'\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00'",
        "b'\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00'",
        "b'\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00'",
        "b'\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00'",
        "b'\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00'",
        "b'\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00'",
        "b'\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00'",
        "b'\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00'",
        "b'\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00'",
        "b'\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00'",
        "b'\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00'",
        "b'\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00'"
        ]
    },
    "members": {
        "TESTING": {
            "ttr": 8,
            "alias": false,
            "halfwords": 30,
            "notes": 0,
            "parms": "b'\\x01\\x00\\x00)\\x01!\\x06\\x7f\\x01!\\x06\\x7f\"S\\x00\\x02\\x00\\x02\\x00\\x00\\xd7\\xc8\\xc9\\xd3@@@@@@'",
            "ispf": {
                "version": "01.00",
                "flags": 0,
                "createdate": "2021-03-08T00:00:00.000000",
                "modifydate": "2021-03-08T22:53:29.000000",
                "lines": 2,
                "newlines": 2,
                "modlines": 0,
                "user": "PHIL"
            },
        },
        "Z15IMG": {
            "ttr": 10,
            "alias": false,
            "halfwords": 0,
            "notes": 0,
            "parms": "b''",
            "ispf": false,
        }
    }

.. note::

    The actual raw member data has been omitted from this JSON output.

Notice that this file has a record format of Fixed Block (FB) and each line is
80 characters long. Also you can see that the first member ``TESTING`` contains
ISPF information whereas the second file ``Z15IMG`` does not.




