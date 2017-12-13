""" Setup file.
"""
import os
from setuptools import setup, find_packages

REQUIREMENTS = [
    'bcrypt',
    'cassandra-driver',
    'cornice',
    'colander',
    'PasteScript',
    'waitress',
    'logmatic-python',
]


here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, 'README.rst')) as f:
    README = f.read()


setup(
    name='moisturizer',
    version='0.1.0',
    description='Cassandra event logging with schema inference.',
    long_description=README,
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Framework :: Pylons",
        "Topic :: Internet :: WWW/HTTP",
        "Topic :: Internet :: WWW/HTTP :: WSGI :: Application"
    ],
    keywords="web services",
    author='Gabriela Surita',
    author_email='gsurita@loggi.com',
    url='https://github.com/gabisurita/moisturizer',
    license='Apache Licence v2.0',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=REQUIREMENTS,
    entry_points="""\
    [paste.app_factory]
    main = moisturizer:main
    """,
    paster_plugins=['pyramid'],
)
