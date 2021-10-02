
install:
	python setup.py install

develop:
	python setup.py develop

test:
	python setup.py test

version-patch:
	bump2version patch

version-minor:
	bump2version minor

publish:
	rm dist/*
	python setup.py sdist
	twine check dist/*
	twine upload dist/*

docs-develop:
	mkdocs serve

docs-build:
	mkdocs build --strict --verbose
