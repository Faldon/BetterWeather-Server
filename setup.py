from setuptools import setup, find_packages

setup(
    name='BetterWeather',
    packages=find_packages(),
    version="1.0",
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'flask', 'click',
    ],
)