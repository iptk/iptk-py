from setuptools import setup
setup(
  name = 'iptk',
  packages = ['iptk', 'iptk.metadata', 'iptk.utils'],
  version = '0.6',
  description = 'Python interface to the Imaging Pipeline Toolkit',
  author = 'Jan-Gerd Tenberge',
  author_email = 'jan-gerd.tenberge@uni-muenster.de',
  install_requires=[
    'zipstream',
    'requests'
  ],
  url = 'https://github.com/iptk/iptk-py',
  keywords = ['neuroimaging', 'docker'],
  classifiers = [],
)