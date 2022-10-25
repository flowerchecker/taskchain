
install:
	conda env create
	poetry install

develop:
	poetry install

test:
	pytest

version-patch:
	bump2version patch

version-minor:
	bump2version minor

publish:
	poetry build
	poetry publish

docs-develop:
	mkdocs serve

docs-build:
	mkdocs build --verbose --site-dir public

docs-publish:
	mkdocs gh-deploy
