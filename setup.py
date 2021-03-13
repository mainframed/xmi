import pathlib
from setuptools import setup, find_packages
import codecs
import os.path

# The directory containing this file
HERE = pathlib.Path(__file__).parent



def read(rel_path):
    here = os.path.abspath(os.path.dirname(__file__))
    with codecs.open(os.path.join(here, rel_path), 'r') as fp:
        return fp.read()

def get_version(rel_path):
    for line in read(rel_path).splitlines():
        if line.startswith('__version__'):
            delim = '"' if '"' in line else "'"
            return line.split(delim)[1]
    else:
        raise RuntimeError("Unable to find version string.")

# The text of the README file
README = (HERE / "README.md").read_text()

# This call to setup() does all the work
setup(
    name="xmi-reader",
    version=get_version("xmi/__init__.py"),
    description="Open and extract (unload) XMI/AWS/HET mainframe files.",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/mainframed/xmi/",
    author="Philip Young",
    author_email="mainframed767@gmail.com",
    license="GPLv2",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v2 (GPLv2)",
        "Operating System :: OS Independent",
    ],
    packages=find_packages(exclude=['test']),
    install_requires=[
        "ebcdic","prettytable","python-magic"
    ],
    project_urls={
        "Bug Tracker": "https://github.com/mainframed/xmi/issues",
    },
)
