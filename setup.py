#!/usr/bin/env python
from setuptools import find_namespace_packages, setup

package_name = "dbt-odps"
# make sure this always matches dbt/adapters/{adapter}/__version__.py
package_version = "1.4.1"
description = """The ODPS adapter plugin for dbt"""

setup(
    name=package_name,
    version=package_version,
    description=description,
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
