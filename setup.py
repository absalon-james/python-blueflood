from setuptools import setup

setup(
    name="bluefloodclient",
    version="0.01",
    author="james absalon",
    author_email="james.absalon@rackspace.com",
    packages=['bluefloodclient'],
    package_data={'bluefloodclient': ['bluefloodclient/*']},
    long_description="Python client for ingestion and reading of blueflood."
)
