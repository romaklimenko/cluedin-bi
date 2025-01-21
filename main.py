# pylint: disable=missing-module-docstring disable=missing-function-docstring
import sys

from commands.fetch import fetch
from commands.process import process

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: main.py fetch|process')
        sys.exit(1)

    if sys.argv[1] == 'fetch':
        fetch()
    elif sys.argv[1] == 'process':
        process()
