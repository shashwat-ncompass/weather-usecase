from setuptools import setup, find_packages

setup(
    name="custom_airflow",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        "apache-airflow==2.9.1",
    ],
)