from setuptools import setup, find_packages

requires = ["psycopg2>=2.8.6"]


setup(
    name='pgsupporter',
    version='1.0.0',
    description='pgsupporter',
    url='https://github.com/KiharaTakahiro/pgsupporter/tree/main',
    author='Takahiro Kihara',
    author_email='takahirokihara123@gmail.com',
    license='MIT',
    packages=find_packages(),
    install_requires=requires,
    classifiers=[
        'Programming Language :: Python :: 3.6',
    ],
)