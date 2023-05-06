#!/usr/bin/env python
from setuptools import find_namespace_packages, setup
from pathlib import Path

root_dir = Path(__file__).parent
long_description = (root_dir / "README.md").read_text()

setup(
    name="dbt-odps",
    version="0.0.1",
    description="""The ODPS (MaxCompute) adapter for DBT (data build tool)""",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="eavea",
    author_email="lijingyu68@gmail.com",
    url="https://github.com/ai-excelsior/F2AI",
    packages=find_namespace_packages(include=["dbt", "dbt.*"]),
    include_package_data=True,
    install_requires=[
        "dbt-core~=1.4.0",
        "pyodps~=0.11.3",
    ],
)
