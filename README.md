# cqapi

cqapi (ConqueryApi) is a Python Api for the [conquery backend](https://github.com/ingef/conquery).  
Next to interacting with Conquery via the ConqueryConnection-Class it has functionality to 
read, edit and write the conquery queries which are specified in json. 

## Installation
- Some functionality of `cqapi` requires Python version `>= 3.8` and `<=3.9`
- Installation is done with poetry. Poetry is a dependency manager for python. It is used to install all dependencies and to create a virtual environment. To install poetry, follow the instructions on the [poetry website](https://python-poetry.org/docs/#installation). 
  After installing poetry, run the following commands in the root directory of the project:

```bash
poetry install
```

## Notes on Jupyter Notebooks

## Documentation

Refer to [the docs](doc/doc.md) for usage examples.

## Running Tests

`python -m pytest tests/`

## Usage
There are multiple Notebooks for Guidance in the examples-Folder in the Forms-Repo

### Basic functionality
Establish a connection to a conquery instance
```python
from cqapi import ConqueryConnection

cq = ConqueryConnection("http://conquery-base.url:9082")
```
When a user builds and executes a query in the Conquery Editor, that query is then stored in the eva backend with a unique query_id.
When the user drops that query_id in a Jupyend-Form, we receive that query_id as a string.

We can access meta information about that query:
```python
cq.get_query_info(query_id)
```

We can also get the data of all the people in the query group with the selected covariates:
```python
data = cq.get_query_result(query_id)
```
# TODO:
- ConqueryIds
- QueryEditor
- Create Query
- Query vs Query Id
- Which queries are executable
- Types of queries (and their corresponding editor queries)



