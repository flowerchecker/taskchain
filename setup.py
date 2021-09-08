import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="taskchain",
    version="1.0.3",
    author="Jiří Thran Řihák",
    author_email="jiri.rihak@plant.id",
    description="Pipeline for ML and data processing",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://gitlab.org/flowerchecker/taskchain",
    packages=setuptools.find_packages('src'),
    package_dir={'': 'src'},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
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
    ],
    setup_requires=['pytest-runner'],
    tests_require=['pytest'],
)
