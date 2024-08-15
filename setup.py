import os
import re

from setuptools import find_packages, setup

#- Begin setup
setup_keywords = dict()
#
setup_keywords['name'] = 'snews_cs'
setup_keywords['description'] = 'An alert application for supernovae neutrino coincidences'
setup_keywords['author'] = 'SNEWS 2.0 Collaboration',
setup_keywords['author_email'] = 'snews2.0@lists.bnl.gov',
setup_keywords['license'] = 'BSD'
setup_keywords['url'] = 'https://github.com/SNEWS2/SNEWS_Coincidence_System'
#
#- version
setup_keywords['version'] = ''
with open("snews_cs/_version.py", "r") as f:
    version_file = f.read()
    version_match = re.search(r"^version = ['\"]([^'\"]*)['\"]", version_file, re.M)
    version = version_match.group(1)
    setup_keywords['version'] = version
#
#- Use README.md as a long description
if os.path.exists('README.md'):
    with open('README.md', 'rb') as readme:
        setup_keywords['long_description'] = readme.read().decode().strip()
    setup_keywords['long_description_content_type'] = 'text/markdown'
#
#- Other keywords for setup
setup_keywords['provides'] = [setup_keywords['name']]
setup_keywords['python_requires'] = '>=3.11'
setup_keywords['zip_safe'] = False
setup_keywords['setup_requires'] = ['pbr']
setup_keywords['pbr'] = True
setup_keywords['packages'] = find_packages()
setup_keywords['include_package_data'] = True
setup_keywords['package_dir'] = {'': '.'}
setup_keywords['package_data'] = {'' : ['etc/*.json', 'etc/*.env']}
setup_keywords['test_suite'] = 'snews_cs.test.snews_cs_test_suite.snews_cs_test_suite'
#
#- Requirements
requires = []
with open('requirements.txt', 'r') as f:
    for line in f:
        if line.strip():
            requires.append(line.strip())
#
setup_keywords['install_requires'] = requires
setup_keywords['extras_require'] = {
    'dev': [
        'autopep8',
        'flake8',
        'mongomock',
        'pytest < 6.3.0',
        'pytest-console-scripts',
        'pytest-cov',
        'pytest-mongodb',
        'pytest-runner',
        'twine',
    ],
    'docs': [
        'sphinx',
        'sphinx_rtd_theme',
        'sphinxcontrib-programoutput'
    ],
}
#
#- Command-line access
setup_keywords['entry_points'] = {
    'console_scripts': [
        'snews_cs = snews_cs.__main__:main',
    ],
}
#
#- Classifiers list
setup_keywords['classifiers'] = [
    'Development Status :: 2 - Pre-Alpha',
    'Programming Language :: Python',
    'Programming Language :: Python :: 3',
    'Intended Audience :: Science/Research',
    'Intended Audience :: Developers',
    'Natural Language :: English',
    'Topic :: Scientific/Engineering',
    'Topic :: Scientific/Engineering :: Astronomy',
    'Topic :: Scientific/Engineering :: Physics',
    'Operating System :: POSIX',
    'Operating System :: Unix',
    'Operating System :: MacOS',
    'License :: OSI Approved :: BSD License',
]
#
#- Run the setup command:
setup(**setup_keywords)
