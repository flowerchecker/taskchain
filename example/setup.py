import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="taskchain-example",
    version="1.0.0",
    author="Jiří Thran Řihák",
    author_email="jiri.rihak@plant.id",
    description="Example project using TaskChain",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://gitlab.org/flowerchecker/taskchain/-/tree/master/example",
    packages=setuptools.find_packages('src'),
    package_dir={'': 'src'},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.8',
    install_requires=[
    ],
    setup_requires=['pytest-runner'],
    tests_require=['pytest'],
)
