#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Define the setup options."""

try:
    import distribute_setup
    distribute_setup.use_setuptools()
except Exception:
    pass

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup


with open('requirements.txt', 'r') as f:
    requires = [x.strip() for x in f if x.strip()]

setup(
    name='influxdb',
    version='0.1',
    description="InfluxDB async client",
    url='https://github.com/Yaser-Amiri/async-influxdb-python',
    license='MIT License',
    packages=find_packages(exclude=['tests']),
    test_suite='tests',
    install_requires=requires,
    classifiers=(
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Framework :: AsyncIO',
    ),
)
