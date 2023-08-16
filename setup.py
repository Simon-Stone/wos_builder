from setuptools import setup

setup(
    name="wos_builder",
    version="0.1",
    description="Convert the Web of Science XML data into a SQL database",
    url="https://github.com/Simon-Stone/wos_builder",
    author="Simon Stone",
    author_email="simon.stone@dartmouth.edu",
    license="MIT",
    packages=["wos_builder"],
    zip_safe=False,
    scripts=["bin/wos_xml_to_sql"],
)
