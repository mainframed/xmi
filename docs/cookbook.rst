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
        if het_obj.is_pds(f)
            for m in het_obj.get_member():
                print("{}({})".format(f, m)
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

If you you're having problems with the library or want to see whats happeneing
behind the scenes you can enable debugging::

    import logging
    import xmi

    xmi_obj = xmi.XMIT(filename="/path/to/file.xmi",logging=logging.DEBUG)
    xmi_obj.open()

As you can see, using this library is fairly easy.


