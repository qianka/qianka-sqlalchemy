# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

setup(
    name='qianka-sqlalchemy',
    version='1.0.0',

    packages=find_packages(),

    install_requires=[
        'SQLAlchemy'
    ],
    setup_requires=[],
    tests_require=[],

    author="Qianka Inc.",
    description="",
    url='http://github.com/qianka/qianka-sqlalchemy'
)