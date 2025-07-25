#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

requirements = []

setup_requirements = []

test_requirements = []

setup(
    author="Raul Sevilla",
    author_email="rsevilla@redhat.com",
    python_requires=">=3.5",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    description="Python Boilerplate contains all the boilerplate you need to create a Python package.",
    entry_points={
        "console_scripts": [
            "data_collector=main:main",
        ],
    },
    install_requires=requirements,
    license="Apache Software License 2.0",
    include_package_data=True,
    keywords="data_collector",
    name="data_collector",
    py_modules=["main"],
    packages=find_packages(include=["data_collector", "data_collector.*"]),
    setup_requires=setup_requirements,
    test_suite="tests",
    tests_require=test_requirements,
    url="https://github.com/rsevilla87/data_collector",
    version="0.1.0",
    zip_safe=False,
)
