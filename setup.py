#!/usr/bin/env python

from setuptools import setup

setup(name='hdbbackup',
      version='1.0',
      description='Utility to automate backups in HDB',
      author='Zerpet',
      packages=['hawqbackup'],
      install_requires=['argparse',
		'PyGreSQL(==4.0)',
                ],
      scripts=['scripts/hawqbackup'],
      classifiers=[
          'Development Status :: 4 - Beta',
          'Programming Language :: Python :: 2',
          'Programming Language :: Python :: 2.6',
          ],
)

