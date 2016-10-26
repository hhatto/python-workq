#!/usr/bin/env python
# -*- coding: utf-8 -*-

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

exec(open('workq/_version.py').read())

setup(
    name='python-workq',
    version=__version__,
    description="Python Client for Workq",
    long_description=open("README.rst").read(),
    license='MIT License',
    author='Hideo Hattori',
    author_email='hhatto.jp@gmail.com',
    url='https://github.com/hhatto/python-workq',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
    ],
    keywords="job queue",
    packages=['workq'],
    zip_safe=False,
)
