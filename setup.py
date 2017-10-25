from setuptools import setup, find_packages
import os

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.rst')).read()
NEWS = open(os.path.join(here, 'NEWS.txt')).read()


version = '0.1.1'

install_requires = [
    'prettytable>=0.7.2',
    'ipython>=1.0',
    'kusto_client>=0.4.0',
    'six',
    'pgspecial',
    'ipython-genutils>=0.1.0',
]


setup(name='jupyter-kql-magic',
    version=version,
    description="Kusto access via Jupyter magic",
    long_description=README + '\n\n' + NEWS,
    classifiers=[
        'Development Status :: 1 - Alpha',
        'Environment :: Console',
        'License :: OSI Approved :: Apache License 2.0',
        'Topic :: Database',
        'Topic :: Database :: Front-Ends',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 2',
    ],
    keywords='database ipython jupyter kql kusto ',
    author='Michael Binshtock',
    author_email='mbnshtck@gmail.com',
    url='https://pypi.python.org/pypi/jupyter-kql-magic',
    license='Apache License 2.0',
    packages=find_packages('src'),
    package_dir = {'': 'src'},
    include_package_data=True,
    zip_safe=False,
    install_requires=install_requires,
)
