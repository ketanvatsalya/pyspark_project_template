from setuptools import setup, find_packages
setup(
    name="spark_application",
    version="beta",
    packages=find_packages(),
    scripts=[],

    install_requires=[],

    package_data={
    },

    # metadata to display on PyPI
    author="Ketan Vatsalya",
    author_email="vatsalya.ketan@gmail.com",
    description="spark-application template",
    license="MIT",
    keywords="spark pyspark template",
)