Understanding Virtual Tape Files
================================

AWSTAPE
~~~~~~~

The AWSTAPE file format is used to transfer virtual tape files. Originally
created for P/390 it is used primarily today with virtual tape offerings.
AWS is the short name for these tape file types.

Virtual tape files are fairly simple in design, they contain a 6 bytes header
which contains info on how long the current record is, how long the previous
record was and a flag, followed by EBCDIC data.


Hercules Emulated Tape (HET)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Later the opensource project Hercules created the Hercules Emulated Tape,
or HET, which builds on the AWSTAPE format by adding compression using
either Bzip or ZLIB.

AWS and HET Format
~~~~~~~~~~~~~~~~~~~

Each label/dataset stored on a virtual tape is preceded by a header record
6 bytes long made up of the following:

- Current block size (short, little endian)
- Previous block size (short, little endian)
- Flag (2 bytes):

    - 0x2000 ENDREC - End of record
    - 0x4000 EOF - tape mark
    - 0x8000 NEWREC - Start of new record
    - HET file flags can also contain compression flags:

        - 0x02 BZIP2 compression
        - 0x01 ZLIB compression

Following the header record is data. On some tapes (not all) optional
label records can exist. These records identify metadata about the dataset(s)
on the tape. Each label starts with 3 characters and a number and are 80 bytes
long. In HET files labels are compressed based on the flag.

- VOL1 label (80 bytes)

    - Volume serial number
    - Tape owner
    - More information: https://www.ibm.com/support/knowledgecenter/en/SSLTBW_2.1.0/com.ibm.zos.v2r1.idam300/formds1.htm

- HDR1 label (80 bytes):

    - Dataset name
    - Dataset serial number
    - Volume sequence number
    - Dataset sequence number
    - Generation number
    - Version number
    - Created date
    - Expiration date
    - System code (i.e. what OS version)
    - More information: https://www.ibm.com/support/knowledgecenter/en/SSLTBW_2.1.0/com.ibm.zos.v2r1.idam300/formds2.htm

- HDR2 label (80 bytes)

    - Record format
    - Block length
    - Tape density
    - Position
    - Job name and step used to copy files to this tape
    - Tape recording technique
    - Control character, used for printing
    - Block attribute
    - Device serial number
    - Security flag
    - Large block length
    - More information: https://www.ibm.com/support/knowledgecenter/en/SSLTBW_2.1.0/com.ibm.zos.v2r1.idam300/dshead.htm

- UHL1 - UHL8  label (80 bytes):

    - Contains user headers 76 bytes long
    - More info here: https://www.ibm.com/support/knowledgecenter/en/SSLTBW_2.1.0/com.ibm.zos.v2r1.idam300/m3208.htm

Metadata Virtual Tape
~~~~~~~~~~~~~~~~~~~~~

So what does this look like on actual files? Using this library we can export
the metadata from the virtual tape file ``test_tape.aws`` in the ``tests``
folder:

.. code-block:: json

    {
    "file": {
        "PYTHON.XMI.SEQ": {
        "HDR1": {
            "dsn": "PYTHON.XMI.SEQ",
            "dsnser": "XMILIB",
            "volseq": 1,
            "dsnseq": 1,
            "gennum": 0,
            "version": 0,
            "createdate": "1921-03-09T00:00:00.000000",
            "expirationdate": "1900-01-01T00:00:00.000000",
            "dsnsec": false,
            "block_count_low": 0,
            "system_code": "IBM OS/VS 370",
            "block_count_high": 0
        },
        "HDR2": {
            "recfm": "F",
            "block_len": 3200,
            "lrecl": 80,
            "density": 4,
            "position": "0",
            "jobid": "XMITAPE /COPYPS  ",
            "technique": "  ",
            "control_char": " ",
            "block_attr": "B",
            "devser": " 30001",
            "dsnid": " ",
            "large_block_len": "          "
        }
        },
        "PYTHON.XMI.PDS": {
        "HDR1": {
            "dsn": "PYTHON.XMI.PDS",
            "dsnser": "XMILIB",
            "volseq": 1,
            "dsnseq": 2,
            "gennum": 0,
            "version": 0,
            "createdate": "1921-03-09T00:00:00.000000",
            "expirationdate": "1900-01-01T00:00:00.000000",
            "dsnsec": false,
            "block_count_low": 0,
            "system_code": "IBM OS/VS 370",
            "block_count_high": 0
        },
        "HDR2": {
            "recfm": "V",
            "block_len": 3220,
            "lrecl": 3216,
            "density": 4,
            "position": "0",
            "jobid": "XMITAPE /COPYPO  ",
            "technique": "  ",
            "control_char": " ",
            "block_attr": "S",
            "devser": " 30001",
            "dsnid": " ",
            "large_block_len": "          "
        }
        }
    }

The JCL used to move these two datasets to tape was

.. code-block :: JCL

    //XMITAPE JOB (01),'COPY TO TAPE',CLASS=A,MSGCLASS=H,NOTIFY=HERC01
    //* THIS JOB COPIES THE TEST FILES FOR XMILIB TO TAPES
    //* USE HETINIT TO GENERATE THE TAPES
    //* hetinit -d test_tape.aws XMILIB
    //* THEN SUBMIT THIS JOB
    //* AND ENTER: /devinit 480 test_tape.aws IN THE HERCULES CONSOLE
    //COPYPS EXEC PGM=IEBGENER,REGION=562K
    //SYSPRINT DD SYSOUT=*
    //SYSUT2   DD UNIT=TAPE,DISP=NEW,DSN=PYTHON.XMI.SEQ,
    //            VOL=SER=XMILIB,LABEL=(01,SL)
    //SYSUT1   DD DSN=PYTHON.XMI.SEQ,DISP=SHR
    //SYSIN    DD DUMMY
    //COPYPO EXEC PGM=IEBCOPY,REGION=562K
    //SYSPRINT DD SYSOUT=*
    //TAPE     DD UNIT=TAPE,DISP=NEW,DSN=PYTHON.XMI.PDS,
    //            VOL=SER=XMILIB,LABEL=(02,SL)
    //PDS      DD DSN=PYTHON.XMI.PDS,DISP=SHR
    //SYSUT3   DD UNIT=SYSDA,SPACE=(80,(60,45)),DISP=(NEW,DELETE)
    //SYSIN    DD *
    COPY INDD=PDS,OUTDD=TAPE
    /*
    //



The File Contents AWS/HET
~~~~~~~~~~~~~~~~~~~~~~~~~

After parsing the header records and any labels the actual file contents
follow. If the file is a sequential dataset its easy enough to detect the mime
type using ``file`` and extract its content. If the file is a PDS then that
means it was "unloaded" using ``IEBCOPY`` which is a little more complicated.