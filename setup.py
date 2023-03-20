from pip.download import PipSession
from pip.req import parse_requirements
from setuptools import find_packages, setup

from squiggler import __version__ as squiggler_version

reqs = parse_requirements('requirements.txt', session=PipSession())
requirements = [str(req.req) for req in reqs]

setup(
    name='Squiggler',
    author='Casey Weed & Sam Phillips',
    author_email='casey@caseyweed.com',
    version=squiggler_version,
    description='Automate rollover API calls and index pattern creation',
    py_modules=['squiggler'],
    install_requires=requirements,
    tests_require=['pytest'],
    extras_require={'test': ['pytest']},
    entry_points="""
        [console_scripts]
        squiggler=squiggler:main
    """
)
