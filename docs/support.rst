Support
=======

Issues and help with this library should use Github Issues at
https://github.com/mainframed/xmi/issues). If you're going to report an issue
please include a debug log which can be enabled by passing ``logging.DEBUG`` to
the ``logging`` argument upon initialization:

.. code-block:: python

    import xmi
    import logging

    f = xmi.open_file(filename="/path/to/file.xmi", logging=logging.DEBUG)

or

.. code-block:: python

    import xmi
    import logging
    f = xmi.XMIT(filename="/path/to/file.xmi", logging=logging.DEBUG)
    f.open()
