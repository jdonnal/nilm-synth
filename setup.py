#!/usr/bin/env python

from setuptools import setup, find_packages

PROJECT = 'nilmsynth'

# Change docs/sphinx/conf.py too!

try:
    long_description = open('README.rst', 'rt').read()
except IOError:
    long_description = ''

setup(
    name=PROJECT,
    version = 0.1,
    description='NILM dataset generator',
    long_description='Create arbitrary labeled datasets to train and benchmark NILM algorithms',
    author='John Donnal',
    author_email='donnal@usna.edu',

    download_url='https://github.com/donnal/load_library',
    license='open source (see LICENSE)',
    classifiers=['Programming Language :: Python',
                 'Environment :: Console',
                 ],
    platforms=['Any'],
    scripts=[],
    provides=[],
    install_requires=['joule'],
    test_suite='tests',
    namespace_packages=[],
    packages=find_packages(exclude=["tests","tests.*"]),
    include_package_data=True,

    entry_points={
        'console_scripts': [
            'nilm-synth = nilm_synth.main:run_main',
            'nilm-exemplars = nilm_synth.extract_exemplars:run_main',
            'nilm-synth-build-docs = nilm_synth.build_docs:main'
        ]
    },
    zip_safe=False,
)
