"""Setup file for the HelmsDeep package."""
from setuptools import find_packages, setup

with open("README.md", encoding="utf-8") as readme_file:
    readme = readme_file.read()

setup(
    name="helmsdeep",
    version="0.1.0",
    author="Max Wang",
    author_email="max@covar.com",
    url="https://github.com/TranslatorSRI/StressTester",
    description="HelmsDeep -- HTTP Endpoint Load Measurement System, "
                "Determining Each Endpoint's Performance",
    long_description_content_type="text/markdown",
    long_description=readme,
    packages=find_packages(),
    include_package_data=True,
    package_data={"helmsdeep": ["*.json"]},
    zip_safe=False,
    license="MIT",
    python_requires=">=3.11",
    install_requires=[
        "locust>=2.38.1",
    ],
    entry_points={
        "console_scripts": [
            "helmsdeep=helmsdeep.cli:main",
        ],
    },
)
