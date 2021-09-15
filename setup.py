from setuptools import setup

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="aqi_livibility_analysis",
    version="0.1.0a1",
    author="Ted Conbeer",
    author_email="ted@shandy.io",
    description="An analysis of EPA AQI data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=["aqi_livibility_analysis"],
    package_dir={"": "src"},
    license="Apache-2.0",
    python_requires=">=3.9.0",
    install_requires=[],
)
