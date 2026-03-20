Cookbook
========

Open and extract (unload) XMI/AWS/HET mainframe files.

The most simple way to use this library is to import this module and use
``xmi.open_file()`` to open an XMI, AWS, or HET file::

    import xmi
    xmi_obj = xmi.open_file("/path/to/file.xmi")
    het_obj = xmi.open_file("/path/to/file.het")
    aws_obj = xmi.open_file("/path/to/file.aws")

To list all datasets and dataset members::

    for f in het_obj.get_files():
        if het_obj.is_pds(f):
            for m in het_obj.get_members(f):
                print("{}({})".format(f, m))
        else:
            print(f)

Print JSON metatdata::

    print(xmi_obj.get_json())
    print(het_obj.get_json(text=True)) # Adds plaintext files to json output
    print(aws_obj.get_json(indent=6)) # Increases the json indent

Silently extract all files/folders to ``/tmp/xmi_files/``::

    aws_obj.set_output_folder("/tmp/xmi_files/")
    aws_obj.set_quiet(True)
    aws_obj.extract_all()

Print detailed file information::

    xmi_obj.print_details()
    xmi_obj.print_xmit()  # Same output as previous, print_xmit() is an alias to print_details()
    het_obj.print_tape()  # print_tape() is an alias to print_details()
    aws_obj.print_tape(human=True)  # Converts size to human readable

Print message::

    if xmi_obj.has_message():
        print(xmi_obj.get_message())

or just::

    print(xmi_obj.get_message())  # Prints 'None' if no message

Creating XMI mainframe files (datasets)::

    from xmi import create_xmi 

    create_xmi(
        '/path/to/file/or/folder',
        output_file='/path/to/your/XMI',
        dsn='MY.DS',
        from_user='DADE',
        from_node='PYTHON'
    )


This will create a XMI file at `/path/to/your/XMI` that is 'receivable' on z/OS.
If the `/path/to/file/or/folder`  is a folder it will be stored in the XMI as 
as PDS named `MY.DS`. If it's a single file it will be stored in the XMI as
a sequential file called `MY.DS`. If you omit the `dsn` the datasetname 
inside the XMIT will be `folder` (last part of input) in UPPERCASE.

If you you're having problems with the library or want to see whats happening
behind the scenes you can enable debugging::

    import logging
    import xmi

    xmi_obj = xmi.XMIT(filename="/path/to/file.xmi",loglevel=logging.DEBUG)
    xmi_obj.open()

As you can see, using this library is fairly easy.

Command-line tools
==================

After installation two commands are available from the terminal.

``extractxmi`` — open, list, and extract XMI / AWS / HET files
---------------------------------------------------------------

.. code-block:: bash

    # List all datasets and members
    extractxmi -l FILE.XMI

    # Extract everything to the current directory
    extractxmi FILE.XMI

    # Extract a single PDS member
    extractxmi FILE.XMI "MY.PDS(MEMBER)"

    # Print detailed metadata
    extractxmi -pH FILE.XMI

    # Extract to a specific folder
    extractxmi FILE.XMI --outputdir /tmp/out/

    # Print the embedded message (if any)
    extractxmi --message FILE.XMI

    # Full option list
    extractxmi --help

``createxmi`` — create an XMI file from a local file or folder
--------------------------------------------------------------

.. code-block:: bash

    # Package a folder as a PDS (dataset name defaults to folder name)
    createxmi myfolder/

    # Specify output path and dataset name
    createxmi myfolder/ -o MY.XMI --dsn MY.PDS

    # Set originating user (appears in ISPF statistics on z/OS)
    createxmi myfolder/ -o MY.XMI --dsn MY.PDS --from-user IBMUSER

    # Package a single file as a sequential dataset
    createxmi myfile.jcl -o SEQ.XMI --dsn MY.SEQ

    # Package XMI files inside a PDS (auto-detects binary, uses RECFM=U)
    createxmi xmi_folder/ -o MULTI.XMI --dsn MULTI.PDS

    # Full option list
    createxmi --help
