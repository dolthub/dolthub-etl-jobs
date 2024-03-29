from setuptools import setup
import os

PACKAGE_DIRS = ['fx_rates_example',
                'mta',
                'wikipedia',
                'five_thirty_eight',
                'coin_metrics']
PREFIXED_PACKAGES_TO_PACKAGE_DIRS = {'{}'.format(package): os.path.join('loaders', package)
                                     for package in PACKAGE_DIRS}
PACKAGES_TO_PACKAGE_DIRS = {package: os.path.join('loaders', package)
                            for package in PACKAGE_DIRS}

setup(name='dolthub-etl-jobs',
      version='0.1',
      packages=list(PACKAGES_TO_PACKAGE_DIRS.keys()) + list(PREFIXED_PACKAGES_TO_PACKAGE_DIRS.keys()),
      package_dir=dict(list(PACKAGES_TO_PACKAGE_DIRS.items()) + list(PREFIXED_PACKAGES_TO_PACKAGE_DIRS.items())),
      install_requires=['doltpy', 'pandas', 'requests'],
      author='Liquidata',
      author_email='oscar@liquidata.co',
      description='Liquidata ETL jobs for loading public data.',
      url='https://github.com/liquidata-inc/liquidata-etl-jobs',
      project_urls={'Bug Tracker': 'https://github.com/liquidata-inc/liquidata-etl-jobs/issues'})
