#!/usr/bin/env python
from setuptools import setup, Command

class PyTest(Command):
    user_options = []
    def initialize_options(self):
        pass
    def finalize_options(self):
        pass
    def run(self):
        import sys,subprocess
        errno = subprocess.call([sys.executable, 'runtests.py'])
        raise SystemExit(errno)

def get_version():
	build_version = 1
	return build_version

setup(
	name='pettingzoo',
	version='0.1.%s' % get_version(),
	url = 'https://wiki.knewton.net/index.php/Tech',
	author='Devon Jones',
	author_email='devon@knewton.com',
	license = 'Proprietary',
	packages=['pettingzoo'],
    cmdclass = {'test': PyTest},
	install_requires=[
		'k.config>=0.1',
		'zc.zk>=0.7.0',
		'zkpython>=0.4'
	],
	tests_require=[
		'cov-core>=1.3,<2.0',
		'coverage>=3.5.1,<3.6',
		'py>=1.4.3,<1.5',
		'pytest>=2.0.0,<2.3',
		'pytest-cov>=1.5,<1.6',
	],
	description = 'Python edition of the PettingZoo framework.',
	long_description = '\n' + open('README').read(),
)

