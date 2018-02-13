from setuptools import setup

setup(
    name='BetterWeather',
    packages=['betterweather'],
    include_package_data=True,
    install_requires=[
        'flask',
    ],
)