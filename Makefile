all:
	@echo "usage"

pypireg:
	${PYTHON} setup.py register
	${PYTHON} setup.py sdist upload
