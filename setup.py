from setuptools import setup
import os

PACKAGE_DIRS = ['local_write_example', 'fx_rates_example', 'mta', 'ip_to_country', 'wikipedia']
PREFIX = 'liquidata_etl'
PREFIXED_PACKAGES_TO_PACKAGE_DIRS = {'{}.{}'.format(PREFIX, package): os.path.join('airflow_dags', package)
                                     for package in PACKAGE_DIRS}
PACKAGES_TO_PACKAGE_DIRS = {package: os.path.join('airflow_dags', package)
                            for package in PACKAGE_DIRS}

setup(name='liquidata-etl-jobs',
      version='0.1',
      packages=list(PACKAGES_TO_PACKAGE_DIRS.keys()) + list(PREFIXED_PACKAGES_TO_PACKAGE_DIRS.keys()),
      package_dir=dict(list(PACKAGES_TO_PACKAGE_DIRS.items()) + list(PREFIXED_PACKAGES_TO_PACKAGE_DIRS.items())),
      install_requires=['doltpy', 'pandas', 'requests'],
      author='Liquidata',
      author_email='oscar@liquidata.co',
      description='Liquidata ETL jobs for loading public data.',
      url='https://github.com/liquidata-inc/liquidata-etl-jobs',
      project_urls={'Bug Tracker': 'https://github.com/liquidata-inc/liquidata-etl-jobs/issues'})
