from setuptools import find_packages, setup

setup(
    name="kafka_dagster_pipeline",
    packages=find_packages(exclude=["kafka_dagster_pipeline_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
