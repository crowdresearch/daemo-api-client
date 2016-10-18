from __future__ import print_function

import sys
from distutils.core import setup

from setuptools import find_packages

if sys.version_info < (2, 7):
    print('daemo-api-client requires python version >= 2.7.x', file=sys.stderr)
    sys.exit(1)

install_requires = [
    'requests==2.11.1',
    'autobahn[twisted]==0.16.0',
    'pyOpenSSL==16.1.0',
    'service_identity==16.0.0',
    'pyyaml==3.12',
    'rainbow_logging_handler==2.2.2'
]

setup(
    name='daemo-api-client',
    version='1.0.8',
    packages=find_packages(exclude=['samples']),
    package_data={'daemo': ['logging.conf']},
    install_requires=install_requires,
    license='MIT',
    author="Daemo",
    author_email="daemo@cs.stanford.edu",
    url="https://github.com/crowdresearch/daemo-api-client/",
    description="Python API Client for Daemo",
    long_description='Python API Client for Daemo Crowdsourcing Platform. More information is available at http://daemo-api-client.readthedocs.io/en/latest/',
    keywords="daemo crowdsourcing client"
)
