from setuptools import setup, find_packages

setup(name='pyBlazing',
      version='1.0',
      description='Messaging system for BlazingDB',
      author='BlazingDB Team',
      author_email='blazing@blazingdb',
      url='https://github.com/BlazingDB/pyBlazing',
      packages=find_packages(),
      install_requires=['blazingdb-protocol'],
)
