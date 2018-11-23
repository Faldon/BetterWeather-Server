from setuptools import setup

setup(
    name='BetterWeather',
    packages=['betterweather'],
    version="1.0",
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'flask', 'click',
    ],
)