from setuptools import setup, find_packages

setup(
  name="commonswiki-bulk-downloader",
  version="1.0",
  description="Bulk media downloader for Wikimedia Commons using SQL dumps",
  long_description=open("README.md", encoding='utf-8').read(),
  long_description_content_type='text/markdown',
  author='Jort de Boer',
  packages=find_packages(),
  install_requires=['requests'],
  entry_points={
    'console_scripts': [
      'cwbd=cwbd.main:main'
    ]
  }
)