import setuptools

with open('README.md', 'r') as fh:
    long_description = fh.read()

VERSION = '1.2.1'

setuptools.setup(
    name='taskchain',
    version=VERSION,
    author='Jiří `Thran` Řihák',
    author_email='jiri.rihak@flowerchecker.com',
    description='Utility for running data and ML pipelines',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/flowerchecker/taskchain',
    download_url=f'https://github.com/flowerchecker/taskchain/archive/refs/tags/{VERSION}.tar.gz',
    packages=setuptools.find_packages('src'),
    package_dir={'': 'src'},
    classifiers=[
        'Development Status :: 4 - Beta',
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.8',
    install_requires=[
        'h5py',
        'tqdm',
        'pandas',
        'pytest',
        'pyyaml',
        'networkx',
        'seaborn',
        'filelock',
        'icecream',
        'tabulate',
        'pyparsing<3,>=2.0.2'       # TODO remove - temporary fix for version conflict from package `packaging`
    ],
    setup_requires=['pytest-runner'],
    tests_require=['pytest'],
)
