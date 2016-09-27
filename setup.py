#!/usr/bin/env python

import setuptools


setuptools.setup(
    name='coworker',
    version='1.0.0',

    author='Max Zheng',
    author_email='maxzheng.os @t gmail.com',

    description='Generic worker that performs concurrent tasks using coroutine.',
    long_description=open('README.rst').read(),

    url='https://github.com/maxzheng/coworker',

    install_requires=open('requirements.txt').read(),

    license='MIT',

    package_dir={'': 'src'},
    packages=setuptools.find_packages('src'),
    include_package_data=True,

    setup_requires=['setuptools-git'],

    classifiers=[
        'Development Status :: 5 - Production/Stable',

        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',

        'License :: OSI Approved :: MIT License',

        'Programming Language :: Python :: 3.5',
    ],

    keywords='coroutine concurrent worker',
)
