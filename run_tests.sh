#!/bin/bash
# Ensure src is discoverable by adding project root to PYTHONPATH
export PYTHONPATH=$(pwd):$PYTHONPATH

# Discover and run all tests in the 'tests' directory
python -m unittest discover -v tests
