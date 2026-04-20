Understanding XMI Files
=======================

NETDATA/XMIT
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

NETDATA files are primarily used to transfer sequential and partitioned
datasets between mainframe environments, sometimes via non mainframe
environments. NETDATA is the official name for the file format of the
output from the z/OS ``TRANSMIT``, z/VM ``NETDATA`` or the opensource
tool ``XMIT370``. However, it is more often referred to as an ``XMI`` file.
This documentation uses XMI and NETDATA interchangeably. Typically third
parties and even IBM will refer to them as XMI files.

.. note::
    Some quick terminology:

    - **Dataset**: a mainframe file, usually referred to as a sequential dataset or seq
    - **Partitioned dataset**: a mainframe folder usually referred to as a PDS
    - **Member**: files in a partitioned dataset
    - **Unload**: extracting data to be used elsewhere
    - **LRECL**: The record length. This is how long each line in a file is,
      padded with spaces.
    - **RECFM**: Record format, where:

        - The first letter is one of F, V, U where:

            * F = fixed length records
            * V = Variable length records
            * U = Unknown

        - And additional letters may be:

            * B = blocked
            * A = ANSI control characters (for printers)
            * M = machine control characters (for printers)
            * S = standard blocks


XMI files contain either a sequential dataset or a partitioned dataset, and
optionally a message. They cannot contain more than one dataset, partitioned
or sequential at a time. They can, however, also include an optional message
which is technically sequential dataset, however the dataset name is lost.

Sequential datasets are 'unloaded' by XMIT using the program ``INMCOPY`` whereas
partitioned datasets are unloaded using a program called ``IEBCOPY``.

Think about XMI files as tar files on Linux but only if you could add one file
or one folder to the tar file. Oftentimes XMI files will contain nested XMI
files due to this limitation.

XMI files are commonly used by IBM, Broadcom, and many other mainframe vendors
to send files to customers. There's also a large collection of software and
programs made available for free using XMI by the amazing **CBTTAPE** project
available at http://cbttape.org/cbtdowns.htm.


Creating XMI Files
~~~~~~~~~~~~~~~~~~

To create a XMI file on z/OS you use the TSO program ``XMIT``/``TRANSMIT``::

    XMIT NODE.USER DATASET('DATASET.TO.SEND') OUTDATASET('OUTPUT.FROM.XMIT.XMI')

    TRANSMIT NODE.USER DATASET('DATASET.TO.SEND') OUTDATASET('OUTPUT.FROM.XMIT.XMI')

You can also add a message to XMI files::

    XMIT NODE.USER DATASET('DATASET.TO.SEND') OUTDATASET('OUTPUT.FROM.XMIT.XMI') MSGDATASET('SEQ.MSG.FILE')

If you are using TK4- you can use ``XMIT370`` and some JCL to generate XMI files:

.. code-block:: JCL

    //XMIMAKE JOB (01),'COPY TO TAPE',CLASS=H,MSGCLASS=H,NOTIFY=HERC01
    //* ------------------------------------------------------------------
    //* CREATES XMILIB TEST XMIT FILES
    //* ------------------------------------------------------------------
    //* EXAMPLE 1: STEP XMITSEQ
    //* CREATES THE XMI FILE PYTHON.XMI.SEQ.XMIT FROM THE
    //* SEQUENTIAL DATASET PYTHON.XMI.SEQ
    //XMITSEQ  EXEC PGM=XMIT370
    //XMITLOG  DD SYSOUT=*
    //SYSPRINT DD SYSOUT=*
    //SYSUDUMP DD SYSOUT=*
    //COPYR1   DD DUMMY
    //SYSIN    DD DUMMY
    //SYSUT1   DD DSN=PYTHON.XMI.SEQ,DISP=SHR
    //SYSUT2   DD DSN=&&SYSUT2,UNIT=3390,
    //         SPACE=(TRK,(255,255)),
    //         DISP=(NEW,DELETE,DELETE)
    //XMITOUT  DD DSN=PYTHON.XMI.SEQ.XMIT,DISP=(,CATLG,DELETE),
    //            UNIT=3350,VOL=SER=KICKS,SPACE=(TRK,(50,50))
    //* EXAMPLE 2: STEP XMIPDS
    //* CREATES THE XMI FILE PYTHON.XMI.PDS.XMIT FROM THE
    //* PARTITIONED DATASET PYTHON.XMI.PDS
    //XMIPDS   EXEC PGM=XMIT370
    //XMITLOG  DD SYSOUT=*
    //SYSPRINT DD SYSOUT=*
    //SYSUDUMP DD SYSOUT=*
    //COPYR1   DD DUMMY
    //SYSIN    DD DUMMY
    //SYSUT1   DD DSN=PYTHON.XMI.PDS,DISP=SHR
    //SYSUT2   DD DSN=&&SYSUT2,UNIT=3390,
    //         SPACE=(TRK,(255,255)),
    //         DISP=(NEW,DELETE,DELETE)
    //XMITOUT  DD DSN=PYTHON.XMI.PDS.XMIT,DISP=(,CATLG,DELETE),
    //            UNIT=3350,VOL=SER=KICKS,SPACE=(TRK,(50,50))

I'll leave generating XMI files on z/VM up to the reader.

Transferring XMI files
~~~~~~~~~~~~~~~~~~~~~~

XMI files (as with most mainframe files) are in EBCDIC, therefore to download
the XMI file from the mainframe you will need to use FTP in binary file
transfer mode. Fortunately enabling binary on FTP is simple, just issue the
FTP command ``binary`` once connected and transfer the XMI file to your machine.

File Structure
~~~~~~~~~~~~~~

XMI files are composed of control records which contain metadata
and dataset information.

Control Records:

* INMR01 - Header records
* INMR02 - File control record(s)
* INMR03 - Data control record(s)
* INMR04 - User control record
* INMR06 - Final record
* INMR07 - Notification record

This library only processes INMR01, INRM02, INMR03, INMR04, and INMR06
records. INMR07 records are notification records and do not contain
any files.

INMR records are composed of the name, two digit number (INMR01, etc)
followed by IBM text units which contains metadata about the record.

Text units in INMR## records are broken down like this:

* First two bytes are the 'key'/type
* Second two bytes are how many text unit records there are
* Then records are broken down by size (two bytes) and the data
* Data can be string, int or hex

More information about text units is available here:
https://www.ibm.com/support/knowledgecenter/en/SSLTBW_2.1.0/com.ibm.zos.v2r1.ikjb400/txunit.htm

INRM01 Records
~~~~~~~~~~~~~~

INRM01 records always contain the following text units:

* INMFTIME - date/time the XMI was created
* INMLRECL - Record length for this XMI
* INMFNODE - name of the originating system
* INMTNODE - name of the target system
* INMFUID - userid of the person who created the XMI
* INMTUID - userid of the user this XMI is being sent to

The following text units are optional:

* INMFACK - notification receipt
* INMFVERS - version number
* INMNUMF - number of files
* INMUSERP - user options


INRM02 Records
~~~~~~~~~~~~~~

An XMI file may contain multiple INMR02 control records. These records
always contains the following text units:

* INMDSORG - dataset organization
* INMLRECL - Record length
* INMSIZE - size in bytes
* INMUTILN - Utility program

Optional text units are:

* INMDSNAM - dataset name (messages do not have this text unit)
* INMCREAT - the date the file was created

There are multiple other optional text units which can be read here:
https://www.ibm.com/support/knowledgecenter/en/SSLTBW_2.3.0/com.ibm.zos.v2r3.ikjb400/inmr02.htm

The utility program (*INMUTILN*) defines how the file was generated and it
can be INMCOPY, or IEBCOPY, and AMSCIPHR:

* INMCOPY - converts a sequential dataset (file) for XMI
* IEBCOPY - converts a partitioned dataset (folder) for XMI
* AMSCIPHR - encrypts the files in XMI, this library does not
  support extracting encrypted files.

Depending on the dataset type the XMI may contain multiple INMR02 records. The
process used when generating an XMI file is:

* If the dataset is sequential - INMCOPY -> Stop
* If it is a partitioned dataset - IEBCOPY -> INMCOPY -> Stop
* If there's also a message the first INMR02 record is INMCOPY and
  doesn't have a dataset name (*INMDSNAM*).

Therefore, partitioned datasets will have two or more INMR02 records.

INRM03 Records
~~~~~~~~~~~~~~

Defines the file format and contains the following text units:

* INMDSORG - dataset organization
* INMLRECL - dataset record length
* INMRECFM - dataset record format
* INMSIZE - size of the dataset in bytes


INRM04 Records
~~~~~~~~~~~~~~

INMR04 records are used to pass data to installation specific exits
(i.e. APIs).


Metadata
~~~~~~~~

Let's take a look at the file ``test_pds_msg.xmi`` (generated with ``XMIT`` on TSO)
in the tests folder. Using this library we can extract the XMI metadata as
json:


.. code-block:: json

    {
    "INMR01": {
        "INMLRECL": 80,
        "INMFNODE": "SMOG",
        "INMFUID": "PHIL",
        "INMTNODE": "XMIT",
        "INMTUID": "PHIL",
        "INMFTIME": "2021-03-09T05:14:41.000000",
        "INMNUMF": 2
    },
    "INMR02": {
        "1": {
            "INMUTILN": "INMCOPY",
            "INMSIZE": 58786,
            "INMDSORG": "PS",
            "INMLRECL": 251,
            "INMBLKSZ": 3120,
            "INMRECFM": "VB",
            "numfile": 1
        },
        "2": {
            "INMUTILN": "IEBCOPY",
            "INMSIZE": 176358,
            "INMDSORG": "PO",
            "INMTYPE": "None",
            "INMLRECL": 80,
            "INMBLKSZ": 27920,
            "INMRECFM": "FB",
            "INMDIR": 6,
            "INMDSNAM": "PYTHON.XMI.PDS",
            "numfile": 2
        },
        "3": {
            "INMUTILN": "INMCOPY",
            "INMSIZE": 176358,
            "INMDSORG": "PS",
            "INMLRECL": 32756,
            "INMBLKSZ": 3120,
            "INMRECFM": "VS",
            "numfile": 2
        }
    },
    "INMR03": {
        "1": {
            "INMSIZE": 176358,
            "INMDSORG": "PS",
            "INMLRECL": 80,
            "INMRECFM": "?"
        },
        "2": {
            "INMSIZE": 176358,
            "INMDSORG": "PS",
            "INMLRECL": 80,
            "INMRECFM": "?"
        }
    }
    }

Notice that ``test_pds_msg.xmi`` had a message, hence there being three INMR02
records. And since it was a PDS it contains the records, ``IEBCOPY`` and
another for ``INMCOPY``.

Now lets look at the sequential dataset ``test_seq.xmi`` in the ``tests``
folder. This XMI file was generated with ``XMIT370``.

.. code-block:: json

    {
    "INMR01": {
        "INMLRECL": 80,
        "INMFNODE": "ORIGNODE",
        "INMFUID": "ORIGUID",
        "INMTNODE": "DESTNODE",
        "INMTUID": "DESTUID",
        "INMFTIME": "2021-03-09T04:53:18.000000",
        "INMNUMF": 1
    },
    "INMR02": {
        "1": {
            "INMUTILN": "INMCOPY",
            "INMSIZE": 0,
            "INMDSORG": "PS",
            "INMLRECL": 80,
            "INMBLKSZ": 3200,
            "INMRECFM": "FB",
            "numfile": 1,
        }
    },
    "INMR03": {
        "1": {
            "INMSIZE": 0,
            "INMDSORG": "PS",
            "INMLRECL": 80,
            "INMRECFM": "?"
        }
    }
    }

Notice how there is only one INMR02 record. Also notice that ``XMIT370`` omits
the *INMDSNAM* text unit for sequential files.

The File Contents XMI
~~~~~~~~~~~~~~~~~~~~~

After parsing the control records the actual file contents follow. If the file
is a sequential dataset its easy enough to detect the mime type using ``file``
and extract its content. If the file is a PDS then that means it was "unloaded"
using ``IEBCOPY`` which is a little more complicated.