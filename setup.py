from setuptools import setup

setup(
    name="blueflood",
    version="0.0",
    author="james absalon",
    author_email="james.absalon@rackspace.com",
    packages=['blueflood'],
    package_data={'blueflood': ['blueflood/*']},
    long_description="Python client for ingestion and reading of blueflood."
)
