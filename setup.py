from setuptools import setup

PACKAGE_DIRS = ['public_holidays', 'mta']
PREFIX = 'liquidata_etl'
PACKAGES_TO_PACKAGE_DIRS = {'{}.{}'.format(PREFIX, package): package for package in PACKAGE_DIRS}

setup(name='liquidata-etl-jobs',
      version='0.1',
      packages=PACKAGES_TO_PACKAGE_DIRS.keys(),
      package_dir=PACKAGES_TO_PACKAGE_DIRS,
      install_requires=['doltpy', 'pandas', 'requests'],
      author='Liquidata',
      author_email='oscar@liquidata.co',
      description='Liquidata ETL jobs for loading public data.',
      url='https://github.com/liquidata-inc/liquidata-etl-jobs',
      project_urls={'Bug Tracker': 'https://github.com/liquidata-inc/liquidata-etl-jobs/issues'},
      scripts=['dolt_loader.py'])
