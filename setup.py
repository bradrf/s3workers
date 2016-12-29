#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    'Click>=6.6',
    'configstruct>=0.2.0',
    'boto>=2.45.0',
    'future>=0.16.0',
]

test_requirements = [
    # TODO: put package test requirements here
]

setup(
    name='s3workers',
    version='0.1.0',
    description="Helper to simplify concurrent access to object scanning in AWS S3 buckets.",
    long_description=readme + '\n\n' + history,
    author="Brad Robel-Forrest",
    author_email='brad@bitpony.com',
    url='https://github.com/bradrf/s3workers',
    packages=[
        's3workers',
    ],
    package_dir={'s3workers':
                 's3workers'},
    entry_points={
        'console_scripts': [
            's3workers=s3workers.cli:main'
        ]
    },
    include_package_data=True,
    install_requires=requirements,
    license="MIT license",
    zip_safe=False,
    keywords='s3workers',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    test_suite='tests',
    tests_require=test_requirements
)
