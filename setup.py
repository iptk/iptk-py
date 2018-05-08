from setuptools import setup
setup(
  name = 'iptk',
  packages = ['iptk', 'iptk.metadata', 'iptk.utils'],
  version = '0.4.1',
  description = 'Python interface to the Imaging Pipeline Toolkit',
  author = 'Jan-Gerd Tenberge',
  author_email = 'jan-gerd.tenberge@uni-muenster.de',
  install_requires=[
    'zipstream',
    'requests'
  ],
  url = 'https://github.com/iptk/iptk-py',
  download_url = 'https://github.com/iptk/iptk-py/archive/0.4.tar.gz', 
  keywords = ['neuroimaging', 'docker'],
  classifiers = [],
)