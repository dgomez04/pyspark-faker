from setuptools import setup, find_packages

setup(
    name="pyspark-faker",
    version="0.1.0",
    description="A custom PySpark 4.0 DataSource for generating fake data using Faker",
    author_email="dagomezmoreno@outlook.com",
    packages=find_packages(),
    install_requires=[
        "pyspark>=4.0.0",
        "faker>=19.0.0",
    ],
    license="MIT",
    url="https://github.com/dgomez04/pyspark-faker",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
    ],
)