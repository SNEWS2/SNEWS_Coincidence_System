import os
import re

from setuptools import find_packages, setup

# read in README
this_dir = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_dir, 'README.md'), 'rb') as f:
    long_description = f.read().decode().strip()

# load version
with open("snews_cs/_version.py", "r") as f:
    version_file = f.read()
version_match = re.search(r"^version = ['\"]([^'\"]*)['\"]", version_file, re.M)
version = version_match.group(1)

# requirements
install_requires = []
    # "hop-client=0.8.0",
    # "jsonschema=4.4.0",
    # "numpy=1.26.3",
    # "pymongo=4.6.1",
    # "python-dotenv=0.19.2",
    # "pandas=2.2.0",
    # "slack-sdk=3.26.2",
# ]

with open('requirements.txt', 'r') as f:
    for line in f:
        if line.strip():
            install_requires.append(line.strip())

# def read_requirements():
#     with open('requirements.txt') as req:
#         content = req.read()
#         requirements = content.split('\n')
#     return install_requires.append(requirements)


extras_require = {
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

setup(
    name='snews_cs',
    version=version,
    description='An alert application for observing supernovas.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/SNEWS2/SNEWS_Coincidence_System.git',
    author='Sebastian Torres-Lara, Melih Kara',
    author_email='sebastiantorreslara17@gmail.com, karamel.itu@gmail.com',
    license='BSD 3-Clause',
    setup_requires=['pbr'],
    pbr=True,
    packages=find_packages(),
    include_package_data=True,

    entry_points={
        'console_scripts': [
            'snews_cs = snews_cs.__main__:main',
        ],
    },

    python_requires='>=3.9.*',
    install_requires=install_requires, #read_requirements(),
    extras_require=extras_require,

    classifiers=[
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
    ],
    
    package_dir = {'' : ''},
    test_suite='snews_cs.test.snews_cs_test_suite.snews_cs_test_suite',

)
