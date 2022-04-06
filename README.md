# cqapi

## Installation

Some functionality of `cqapi` requires Python version `>= 3.7`.

To install the latest version of `cqapi` on the `master` branch run:

```
pip install git+ssh://git@github.com/bakdata/cqapi@master
```

### Usage

The ConqueryConnection class connects to the backend and among other things execute queries and download results.
For login specify the parameters `url` and `token`. You can also specify a dataset. Otherwise most methods of
this class accept a dataset parameter that is guessed when not provided.
```python
from cqapi import ConqueryConnection

conquery = ConqueryConnection(url="http://conquery-base.url:9082", token="ConqueryToken", dataset="demo")

# get result of already existing query
query_id = cq.get_query_id(label="Query that has been executed before (maybe by using the conquery frontend")
query_result = conquery.get_query_result(query_id)
```

Create and execute own queries:
To create and structure a query we need information from the concepts. Concepts are structured by an hierachical id system.
Each id starts with the dataset and are structured with a "." separator.
Example: <dataset-id>.<concept-id> => demo.age
```python
from cqapi.queries.base_elements import create_query
concepts = conquery.get_concepts()
age_query = create_query(concept_id="demo.age", concepts=concepts, concept_query=True)
age_query_id = conquery.execute_query(age_query)
age_query_result = conquery.get_query_result(age_query_id)
```

Queries can be joined and adjusted just as it is possible in the conquery frontend.
For easier use, you can use the QueryEditor
```python
from cqapi.queries.editor import QueryEditor

editor = QueryEditor(age_query)

editor.and_query(create_query(concept_id="demo.icd.a", concepts=concepts))

editor.date_restriction(start_date="2013-01-01", end_date="2019-12-31")

editor.concept_query()

query_id = eva.conn.execute_query(editor.query, label="air_pte vte")

eva.conn.get_query_result(query_id=query_id)
```