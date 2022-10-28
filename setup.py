from distutils.core import setup
from pathlib import Path

install_requires = ["dnspython==2.2.1", "mongita==1.1.1"]
long_description = (Path(__file__).parent / "README.md").read_text()

setup(
    name="mongoclass",
    packages=["mongoclass"],
    version="1.0",
    license="MIT",
    description="A basic ORM like interface for mongodb in python that uses dataclasses.",
    author="Philippe Mathew",
    author_email="philmattdev@gmail.com",
    url="https://github.com/bossauh/mongoclass",
    download_url="https://github.com/bossauh/mongoclass/archive/refs/tags/v_10.tar.gz",
    keywords=["pymongo", "orm"],
    install_requires=install_requires,
    long_description=long_description,
    long_description_content_type="text/markdown",
)
