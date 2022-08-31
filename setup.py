#!/usr/bin/env python
from setuptools import setup

with open('README.md') as f:
    long_description = f.read()

setup(
    name="tap-kingdee-jdy",
    version="0.0.1",
    description="Singer.io tap for extracting kingdee data",
    long_description=long_description,
    long_description_content_type='text/markdown',
    author="Vibe Inc",
    url="http://github.com/vibeus/tap-kingdee-jdy",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_kingdee_jdy"],
    install_requires=[
        "requests",
        "singer-python",
        "python-dateutil",
    ],
    entry_points="""
    [console_scripts]
    tap-kingdee-jdy=tap_kingdee_jdy:main
    """,
    packages=["tap_kingdee_jdy", "tap_kingdee_jdy.streams"],
    package_data = {"schemas": ["tap_kingdee_jdy/schemas/*.json"]},
    include_package_data=True,
)
