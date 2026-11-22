"""The build step of `pip install`: the three C libraries are built from
src/core into the packages, so an installed package has its library and
needs no sources. Everything else is in pyproject.toml.

An editable install (`pip install -e .`) builds nothing here: each
package builds its library from src/core the first time it is imported,
as in a plain checkout.
"""
import os
import subprocess

from setuptools import Distribution, setup
from setuptools.command.build_py import build_py

HERE = os.path.dirname(os.path.abspath(__file__))

#: package -> its library's target in src/core/Makefile
LIBRARIES = {"arrow_ring": "libarrow_ring", "parquet_checkpoint": "libpqc",
             "streaming_parquet": "libsp"}


class BuildWithCores(build_py):
    def run(self):
        super().run()
        if getattr(self, "editable_mode", False):
            return
        for package, target in LIBRARIES.items():
            out = os.path.abspath(os.path.join(self.build_lib, package))
            subprocess.run(["make", "-C", os.path.join(HERE, "src", "core"),
                            f"OUT={out}", target], check=True)


class WithLibraries(Distribution):
    """The packages hold compiled libraries: the wheel is for one platform."""

    def has_ext_modules(self):
        return True


setup(cmdclass={"build_py": BuildWithCores}, distclass=WithLibraries)
