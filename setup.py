import setuptools

authors = [
    'Nila Moenig',
]

description = 'python functions query agglomeration graph information from ' \
              'BigQuery tables. Intends to replace some of the Brainmaps API ' \
              'functionality that is broken for large read-only agglomeration ' \
              'graphs'

setuptools.setup(
    name='graph_from_bigquery',
    version='0.0.1',
    author=authors,
    packages=setuptools.find_packages(),
    description=description,
    install_requires=[
        'google-cloud-bigquery >= 3.9.0',
        'google-auth-oauthlib >= 0.7.1 '
    ],
)