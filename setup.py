# Automatically created by: shub deploy

from setuptools import setup, find_packages

setup(
    name         = 'dgbt',
    version      = '1.0',
    packages     = find_packages(),
    package_data={
        'dgbt': ['resources/*.csv', 'resources/*.json']
    },
    scripts      = ['dgbt/scripts/updts.py'],
    entry_points = {'scrapy': ['settings = dgbt.settings']},
    zip_safe=False,
)
