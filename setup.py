#!/usr/bin/env python

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="simplerdb",
    version="1.0.0",
    description="A simple, unified interface for multiple database systems",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Subhash Dasyam",
    author_email="YOUR.EMAIL@example.com",  # Replace with your actual email
    url="https://github.com/subhash-dasyam/simplerdb",  # Replace with your actual GitHub URL
    packages=find_packages(),
    license="GPLv2",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Programming Language :: Python",
        "Natural Language :: English",
        "License :: OSI Approved :: GNU General Public License v2 (GPLv2)",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries"
    ],
    install_requires=[
        "mysql-connector-python>=8.0.0",
    ],
    extras_require={
        "mysql": ["mysql-connector-python>=8.0.0"],
        "mariadb": ["mariadb>=1.0.0"],
        "postgres": ["psycopg2-binary>=2.9.0"],
        "pgvector": ["psycopg2-binary>=2.9.0", "pgvector>=0.1.0"],
        "mssql": ["pyodbc>=4.0.0"],
        "oracle": ["cx_Oracle>=8.0.0"],
        "redis": ["redis>=4.0.0"],
        "chromadb": ["chromadb>=0.3.0"],
        "all": [
            "mysql-connector-python>=8.0.0",
            "mariadb>=1.0.0",
            "psycopg2-binary>=2.9.0",
            "pgvector>=0.1.0",
            "pyodbc>=4.0.0",
            "cx_Oracle>=8.0.0",
            "redis>=4.0.0",
            "chromadb>=0.3.0"
        ]
    },
    python_requires=">=3.8",
)