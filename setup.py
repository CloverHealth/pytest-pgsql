from setuptools import setup
import sys

# Check the Python version manually because pip < 9.0 doesn't check it for us.
if sys.version_info < (3, 4):
    raise RuntimeError('Unsupported version of Python: ' + sys.version)

setup(
    setup_requires=['pbr'],
    pbr=True,
)
