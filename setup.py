import os
import sys

from setuptools import setup, find_packages
from setuptools.sandbox import DirectorySandbox, _execfile, setup_context
from setuptools.command.install import install

from pkg_resources import working_set


def run(setup, args):
  _dir = os.path.abspath(os.path.dirname(setup))
  with setup_context(_dir):
    try:
      sys.argv[:] = [setup] + args
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
    return True


class install(install):

  def run(self):
    import git
    baseDir = os.getcwd()
    pygdfUrl = 'https://github.com/rapidsai/pygdf.git'
    pygdfDir = os.path.join(baseDir, 'pygdf-src')
    print('Clonning PyGDF repository on ' + pygdfDir)
    git.Repo.clone_from(pygdfUrl, pygdfDir)
    print('Installing PyGdf')
    run(os.path.join(pygdfDir, 'setup.py'), ['install'])
    super().run()


setup(
  name='PyBlazing',
  version='0.1',
  author='BlazingDB Team',
  packages=find_packages(),
  cmdclass=dict(install=install),
  setup_requires=(
    'GitPython==2.1.11',
  ),
  zip_safe=False,
)
