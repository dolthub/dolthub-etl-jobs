from setuptools import setup

PACKAGE_DIRS = ['local_write_example', 'fx_rates_example', 'mta']
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
      project_urls={'Bug Tracker': 'https://github.com/liquidata-inc/liquidata-etl-jobs/issues'})
