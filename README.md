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

### Usage
There are multiple Notebooks for Guidance in the examples-Folder
```python
from cqapi import ConqueryConnection

with ConqueryConnection("http://conquery-base.url:9082") as cq:
    query = cq.get_query_info("demo", "query.id")
    query_execution_id = cq.execute_query("demo", query)
    query_result = cq.get_query_result("demo", query_execution_id)
```

## Notes on Jupyter Notebooks

## Documentation

Refer to [the docs](doc/doc.md) for usage examples.

## Running Tests

`python -m pytest tests/`