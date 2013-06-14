#!/usr/bin/python
from setuptools import setup

setup(
    name='gcat',
    version='0.0.1',
    description='set of tools for reporting and visualizing Wikipedia Zero traffic',
    url='http://www.github.com/embr/gcat',
    author='Evan Rosen',
    author_email='erosen@wikimedia.org',
    install_requires=[
       "gcat == 0.1.0",
       "limnpy == 0.1.0",
       "pandas >= 0.9.0",
       "squidpy >= 0.1.0",
       "unidecode == 0.04.13",
       "mcc-mnc == 0.0.1"
       ],
    dependency_links=[
        "http://github.com/embr/gcat/tarball/master#egg=gcat-0.1.0",
        "http://github.com/wikimedia/limnpy/tarball/master#egg=limnpy-0.1.0",
        "http://github.com/embr/squidpy/tarball/master#egg=squidpy-0.1.0",
        "http://github.com/embr/mcc-mnc/tarball/master#egg=mcc-mnc-0.0.1",
        ]
    )

