# Copyright (c) 2016-2017 CEA
#
# This software is governed by the CeCILL license under French law and
# abiding by the rules of distribution of free software. You can use,
# modify and/ or redistribute the software under the terms of the CeCILL
# license as circulated by CEA, CNRS and INRIA at the following URL
# "http://www.cecill.info".
#
# As a counterpart to the access to the source code and rights to copy,
# modify and redistribute granted by the license, users are provided only
# with a limited warranty and the software's author, the holder of the
# economic rights, and the successive licensors have only limited
# liability.
#
# In this respect, the user's attention is drawn to the risks associated
# with loading, using, modifying and/or developing or reproducing the
# software by the user in light of its specific status of free software,
# that may mean that it is complicated to manipulate, and that also
# therefore means that it is reserved for developers and experienced
# professionals having in-depth computer knowledge. Users are therefore
# encouraged to load and test the software's suitability as regards their
# requirements in conditions enabling the security of their systems and/or
# data to be ensured and, more generally, to use and operate it in the
# same conditions as regards security.
#
# The fact that you are presently reading this means that you have had
# knowledge of the CeCILL license and that you accept its terms.

from setuptools import setup


def readme():
    with open('README.rst') as f:
        return f.read()


def license():
    with open('LICENSE') as f:
        return f.read()


setup(
    name='imagen_databank',
    version='0.1.0',
    author='Dimitri Papadopoulos',
    author_email='imagendatabase@cea.fr',
    description='Imagen project databank software',
    long_description=readme(),
    license='CeCILL',
    url='https://github.com/imagen2/imagen_databank',
    packages=['imagen_databank'],
    scripts=[
        'cantab/imagen_cantab_age_at_session_start_time.py',
        'dawba/imagen_dawba_process.py',
        'psytools/imagen_psytools_download_csv.py',
        'psytools/imagen_psytools_download_json.py',
        'psytools/imagen_psytools_process_csv.py',
        'psytools/imagen_psytools_process_json.py',
    ],
    classifiers=[
        "License :: OSI Approved :: CEA CNRS Inria Logiciel Libre License, version 2.1 (CeCILL-2.1)",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Environment :: Console",
        "Development Status :: 4 - Beta",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        "Topic :: Scientific/Engineering :: Medical Science Apps.",
        "Topic :: Utilities",
    ],
    install_requires=[
        'pydicom',
    ],
)
