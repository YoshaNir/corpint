# corpint

Corporate open-source intelligence toolkit for data-driven investigations.

A common use case in investigative reporting is to research a given set of companies
or people by searching for their ownership, control and other relationships in
online databases. ``corpint`` augments that process by automating look-ups in
web services and building a network graph out of the resulting set of links. It
also provides an explicit way to accept and reject results from online research,
thus making sure the entire resulting graph is fact-checked.

## Installation

To run ``corpint`` you will want to have Python and PostgreSQL installed. You
may also want to install Neo4J if you intend to use the graph exporter feature.

It's recommended to run ``corpint`` inside a Python virtual environment. When you
have a [virtualenv](https://python-guide.readthedocs.io/en/latest/dev/virtualenvs/)
set up, clone the git repository and install the package:

```bash
$ git clone https://github.com/alephdata/corpint.git
$ cd corpint
$ pip install -e .
```

## Usage

Most of the usage of ``corpint`` is handled via a command-line utility, which
allows users to enrich data from external sources, find duplicates proposed for
merging and generate output formats such as a Neo4J graph.

### Configuration

Some configuration is required to make ``corpint`` connect to the correct
database and to the right subset of the data in there.

* ``CORPINT_PROJECT`` is the title of the current investigation, in a slug
  form, e.g. ``foo`` or ``panama_papers``.
* ``DATABASE_URI`` is an environment variable containing a database connection
  URI of the form ``postgresql://user:password@host/database``.
* ``NEO4J_URI`` is the URL for Neo4J, usually
  ``http://neo4j:neo4j@localhost:7474/``.

### Loading data

Unfortunately, loading data still requires some manual mapping of the data into
the structure expected by ``corpint``. This can be done via a Python script:

```python
from corpint import project, csv
from corpint import PERSON, COMPANY

# name the source, used to distinguish from enrichment results:
origin = project.origin('mysource')
# delete previous load:
origin.clear()

# lets assume a data file with the names of companies and their directors
with open('data.csv', 'r') as fh:
  for row in csv(fh):
    # get a row:
    director_name = row.get('director_name')
    # important: you need to generate unique IDs (uids) for each entity you
    # load.
    director_id = origin.uid(director_name)

    # check for empty names:
    if director_id is not None:
      origin.emit_entity({
        'uid': director_id,
        'name': director_name,
        'schema': PERSON,
        # this will enable company look-ups for the entity:
        'tasked': True
      })

    # same for the companies
    company_name = row.get('company_name')
    company_id = origin.uid(company_name)
    if company_id is not None:
      origin.emit_entity({
        'uid': company_id,
        'name': company_name,
        'schema': Company,
        # maybe add an extra property:
        'country': row.get('company_country'),
        'tasked': True
      })

    # now, create a link:
    if company_id is not None and director_id is not None:
      origin.emit_link({
        'source_uid': director_id,
        'target_uid': company_id,
        'schema': 'DIRECTOR',
        'summary': row.get('director_role')
      })

```

### Cleaning up the data

Once the data is loaded, you might want to start by checking if there are
duplicates within the source list:

```bash
$ corpint mappings generate -o mysource -t 0.8
```

This will generate all duplicate candidates with a ranking better than 80%.

You can then go and use the web interface to manually cross-check duplicates:

```bash
$ corpint webui
```

This will expose the web interface on port 5000 of the local machine.

### Enriching data from external sources

To run all the loaded entities against an online source, such as OpenCorporates,
and store the resulting matches, run the following command:

```bash
$ corpint enrich -o mysource opencorporates
```

Valid enrichers currently include ``opencorporates``, ``aleph``,
``alephdocuments``, ``gmaps``, and ``bvdorbis``. Some of these enrichers may
work better if API keys are provided:

* ``OPENCORPORATES_APIKEY`` a valid API key from OpenCorporates.
* ``GMAPS_APIKEY`` a Google Maps API key
* ``ALEPH_APIKEY``, ``ALEPH_HOST`` to specify an Aleph instance other than
  ``data.occrp.org``.

## License

The MIT License (MIT)

Copyright (c) 2017 Journalism Development Network, Inc.

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
