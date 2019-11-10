import setuptools


requirements = []


long_description = (
    'Aiopika is a pure-Python implementation of the popular Pika module '
    'implmentd using asyncio. Pika was developed primarily for use with '
    'RabbitMQ, but should also work with other AMQP 0-9-1 brokers.'
)


setuptools.setup(
    name='aiopika',
    version='0.1.0',
    description='Aiopika Python AMQP Asyncio Client Library',
    long_description=open('README.rst').read(),
    maintainer='Gianmarco Fantinuoli',
    maintainer_email='gianmarco.fanti@gmail.com',
    packages=setuptools.find_packages(include=['aiopika', 'aiopika.*']),
    #license='BSD',
    install_requires=requirements,
    package_data={'': ['LICENSE', 'README.rst']},
    extras_require={
        'uvloop': ['uvloop'],
    },
    python_requires='>=3.6',
    classifiers=[
        'Development Status :: 5 - Alpha release',
        'Intended Audience :: Developers',
        #'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Topic :: Communications', 'Topic :: Internet',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: System :: Networking'
    ],
    zip_safe=True
)