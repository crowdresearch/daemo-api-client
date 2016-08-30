from __future__ import print_function

import sys
from distutils.core import setup

from setuptools import find_packages

if sys.version_info < (2, 7):
    print('daemo-api-client requires python version >= 2.7.x', file=sys.stderr)
    sys.exit(1)

install_requires = [
    'requests',
    'autobahn',
    'twisted'
]

setup(
    name='daemo-api-client',
    version='1.0.1',
    packages=find_packages(),
    install_requires=install_requires,
    license='MIT',
    author="Daemo",
    author_email="daemo@cs.stanford.edu",
    url="https://github.com/crowdresearch/daemo-api-client/",
    description="Client library for Daemo",
    long_description='Client library for Daemo',
    keywords="daemo crowdsourcing client"
)
