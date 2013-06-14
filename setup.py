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
        "git@github.com:embr/gcat.git#egg=gcat-0.1.0",
        "git@github.com:wikimedia/limnpy.git#egg=limnpy-0.1.0",
        "git@github.com:embr/squidpy.git#egg=squidpy-0.1.0",
        "git@github.com:embr/mcc-mnc.git#egg=mcc-mnc-0.0.1",
        ]
    )

