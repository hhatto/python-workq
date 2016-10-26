all:
	@echo "usage"

clean:
	rm -rf build dist *.egg-info
	rm -rf */__pycache__

pypireg:
	${PYTHON} setup.py register
	${PYTHON} setup.py sdist upload
