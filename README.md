# NETDATA, AWSTAPE and HET File Python Library

Open and extract (unload) XMI/AWS/HET mainframe files.

## Installation

You can install the **xmi** library from PyPI using:

```
python3 -m pip install xmi-reader
```

## How to Use

The most simple way to use this library is to import this module and use
`xmi.open_file()` to open an XMI, AWS, or HET file::

```python
    import xmi
    xmi_obj = xmi.open_file("/path/to/file.xmi")
    het_obj = xmi.open_file("/path/to/file.het")
    aws_obj = xmi.open_file("/path/to/file.aws")
```

To list all datasets and dataset members::

```python
    for f in het_obj.get_files():
        if het_obj.is_pds(f)
            for m in het_obj.get_member():
                print("{}({})".format(f, m)
        else:
            print(f)
```

Print JSON metatdata::

```python
    print(xmi_obj.get_json())
    print(het_obj.get_json(text=True)) # Adds plaintext files to json output
    print(aws_obj.get_json(indent=6)) # Increases the json indent
```

Silently extract all files/folders to ``/tmp/xmi_files/``::

```python
    aws_obj.set_output_folder("/tmp/xmi_files/")
    aws_obj.set_quiet(True)
    aws_obj.extract_all()
```

Print detailed file information::

```python
    xmi_obj.print_details()
    xmi_obj.print_xmit()  # Same output as previous, print_xmit() is an alias to print_details()
    het_obj.print_tape()  # print_tape() is an alias to print_details()
    aws_obj.print_tape(human=True)  # Converts size to human readable
```

Print message:

```python
    if xmi_obj.has_message():
        print(xmi_obj.get_message())
```

If you you're having problems with the library or want to see whats happeneing
behind the scenes you can enable debugging:

```python
    import logging
    import xmi

    xmi_obj = xmi.XMIT(filename="/path/to/file.xmi",logging=logging.DEBUG)
    xmi_obj.open()
```

## More Information

- [Documentation](https://xmi.readthedocs.io/)
  - [Installation](https://xmi.readthedocs.io/en/latest/install.html)
  - [API](https://xmi.readthedocs.io/en/latest/xmi.html)
  - [XMI File format](https://xmi.readthedocs.io/en/latest/netdata.html)
  - [AWS/HET File format](https://xmi.readthedocs.io/en/latest/vitualtape.htm)
  - [IEBCOPY File format](https://xmi.readthedocs.io/en/latest/iebcopy.htm)
- [Issues](https://github.com/mainframed/xmi/issues)
- [Pull requests](https://github.com/mainframed/xmi/pulls)
