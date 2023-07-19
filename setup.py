import setuptools
from setuptools import setup

setup(
    name="sdRDM Database Utilities",
    version="0.0.0",
    author="Range, Jan",
    author_email="jan.range@simtech.uni-stuttgart.de",
    license="MIT License",
    packages=setuptools.find_packages(),
    include_package_data=True,
    install_requires=["pydantic <= 1.10.11", "ibis-framework[postgres]"],
)
