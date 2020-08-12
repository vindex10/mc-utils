import os
import sys
import subprocess

from setuptools import setup, Extension
from setuptools.command.build_ext import build_ext


class CMakeExtension(Extension):
    def __init__(self, name, cmake_lists_dir='.', **kwargs):
        super().__init__(name, sources=[], **kwargs)
        self.cmake_lists_dir = os.path.abspath(cmake_lists_dir)


class cmake_build_ext(build_ext):
    def build_extensions(self):
        # Ensure that CMake is present and working
        try:
            subprocess.check_output(['cmake', '--version'])
        except OSError:
            raise RuntimeError('Cannot find CMake executable')

        for ext in self.extensions:

            if not isinstance(ext, CMakeExtension):
                continue

            extdir = os.path.abspath(os.path.dirname(self.get_ext_fullpath(ext.name)))
            cmake_args = [
                # Ask CMake to place the resulting library in the directory
                # containing the extension
                '-DCMAKE_LIBRARY_OUTPUT_DIRECTORY={}'.format(extdir),
                # Other intermediate static libraries are placed in a
                # temporary build directory instead
                '-DCMAKE_ARCHIVE_OUTPUT_DIRECTORY={}'.format(self.build_temp),
                # Hint CMake to use the same Python executable that
                # is launching the build, prevents possible mismatching if
                # multiple versions of Python are installed
                '-DPYTHON_EXECUTABLE={}'.format(sys.executable),
                # Add other project-specific CMake arguments if needed
                # ...
            ]

            if not os.path.exists(self.build_temp):
                os.makedirs(self.build_temp, exist_ok=True)

            # Config
            subprocess.check_call(['cmake', ext.cmake_lists_dir] + cmake_args,
                                  cwd=self.build_temp)

            # Build
            subprocess.check_call(['cmake', '--build', '.'],
                                  cwd=self.build_temp)


setup(name="mc-utils",
      version="9999",
      description="Helper functions to process events",
      url="https://github.com/vindex10/mc-utils",
      author="Victor Ananyev",
      author_email="vindex10@gmail.com",
      packages=["mc_utils"],
      ext_modules=[CMakeExtension("mc_utils.fastlhe", "src/")],
      cmdclass={
          "build_ext": cmake_build_ext
      },
      install_requires=["numpy==1.19.*"])
