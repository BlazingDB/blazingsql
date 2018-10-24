from setuptools import setup, find_packages

setup(
    name='PyBlazing',
    version='0.1',
    author='BlazingDB',
    packages=find_packages(),
    install_requires=(
        'numba==0.40.1',  # already depends on numpy-1.15.3
        'pycuda==2018.1.1'
    ),
    zip_safe=False,
)
