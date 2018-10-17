import os
import shutil
import subprocess
import sys
import tempfile

from setuptools import setup, find_packages, Extension
from setuptools.sandbox import DirectorySandbox, _execfile, setup_context
from setuptools.command.build_ext import build_ext

from pkg_resources import working_set


def run(_dir, arg):
  with setup_context(_dir):
    setup = os.path.join(_dir, 'setup.py')
    if not os.path.exists(setup):
      raise ValueError('No distribution on ' + _dir)
    try:
      sys.argv[:] = [setup, arg]
      sys.path.insert(0, _dir)
      working_set.__init__()
      working_set.callbacks.append(lambda d: d.activate())
      GDFSandbox(_dir).run(
        lambda: _execfile(setup,
                          dict(__file__=setup, __name__='__main__')))
    except SystemExit as e:
      if e.args and e.args[0]:
        raise


class GDFSandbox(DirectorySandbox):

  def _ok(self, path):
    return True  # TODO(gcca): remove and evaluate by exceptions


class build_ext(build_ext):

  def run(self):
    # TODO(gcca): check building tools
    for extension in self.extensions:
      self.build_extension(extension)

  def build_extension(self, extension):
    import git

    libGdfUrl = 'https://github.com/rapidsai/libgdf.git'
    self.print('Create temporal directory for LibGDF')
    with tempfile.TemporaryDirectory() as baseDir:
      libGdfDir = os.path.join(baseDir, 'libgdf-src')

      self.print('Cloning LibGDF repository on ' + libGdfDir)
      repo = git.Repo.clone_from(libGdfUrl, libGdfDir)
      for submodule in repo.submodules:
        submodule.update(recursive=True)

      self.print('Prepare building directory LibGDF')
      buildDir = os.path.join(libGdfDir, 'build')
      if not os.path.exists(buildDir):
        os.mkdir(buildDir)

      self.print('Run cmake for LibGDF')
      subprocess.check_call(('cmake', libGdfDir), cwd=buildDir)

      self.print('Build LibGDF')
      subprocess.check_call(('make', 'gdf', '-j4'), cwd=buildDir)

      self.print('Install LibGDF for CFFI')
      subprocess.check_call(('make', 'copy_python'), cwd=buildDir)
      subprocess.check_call(('python', 'setup.py', 'install'), cwd=buildDir)

      libGdfSo = os.path.join(buildDir, 'libgdf.so')
      shutil.copy(libGdfSo, self.build_lib)
      shutil.copy(libGdfSo,
                  os.path.abspath(
                    os.path.dirname(self.get_ext_fullpath(extension.name))))
    print()

  def print(self, s):
    line = '-' * len(s)
    print(line)
    print(s)
    print(line)


class GDFExtension(Extension):

    def __init__(self):
      super().__init__('libgdf', [''])


setup(
  name='PyBlazing',
  version='0.1',
  author='BlazingDB Team',
  packages=find_packages(),
  cmdclass=dict(build_ext=build_ext),
  ext_modules=[
    GDFExtension(),
  ],
  install_requires=(
    'numpy==1.15.2',
    'pycuda==2018.1.1'
    'pygdf@git+https://github.com/rapidsai/pygdf.git#egg=pygdf-master',
  ),
  setup_requires=(
    'GitPython==2.1.11',
  ),
  zip_safe=False,
)
