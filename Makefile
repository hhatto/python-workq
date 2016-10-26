all:
	@echo "usage"

clean:
	rm -rf build dist *.egg-info
	rm -rf */__pycache__

pypireg:
	python setup.py register
	python setup.py sdist upload
	python setup.py bdist_wheel upload
