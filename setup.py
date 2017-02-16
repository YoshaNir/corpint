from setuptools import setup, find_packages

setup(
    name='corpint',
    version='0.1',
    long_description="Corporate data OSINT toolkit",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
    ],
    keywords='',
    author='Friedrich Lindenberg',
    author_email='pudo@occrp.org',
    url='https://occrp.org',
    license='MIT',
    packages=find_packages(exclude=['ez_setup', 'examples', 'test']),
    namespace_packages=[],
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'normality',
        'dataset>=0.8',
        'requests<=2.11.1',
        'countrynames',
        'fingerprints',
        'unicodecsv',
        'lxml',
        'googlemaps',
        'python-Levenshtein',
        'mwclient',  # wikipedia
        'rdflib',  # wikidata
        'SPARQLWrapper',  # wikidata
        'zeep',  # bvd orbis (soap)
        'urlnorm',  # document crawler
        'py2neo',
        'click',
        'Flask'
    ],
    test_suite='nose.collector',
    entry_points={
        'console_scripts': [
            'corpint = corpint.cli:main',
        ],
        'corpint.enrich': [
            'wikipedia = corpint.enrich.wikipedia:enrich',
            'wikidata = corpint.enrich.wikidata:enrich',
            'opencorporates = corpint.enrich.opencorporates:enrich',
            'aleph = corpint.enrich.aleph:enrich',
            'alephdocuments = corpint.enrich.aleph:enrich_documents',
            'bvdorbis = corpint.enrich.bvdorbis:enrich',
            'gmaps = corpint.enrich.gmaps:enrich'
        ]
    },
    tests_require=['coverage', 'nose']
)
