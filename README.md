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
When a user builds and executes a query in the Conquery Editor, that query is then stored in the conquery backend with a unique query_id.
When the user drops that query_id in a Jupyend-Form, we receive that query_id as a string.

We can access meta information about that query:
```python
cq.get_query_info(query_id)
```

We can also get the data of all the people in the query group with the selected covariates:
```python
data = cq.get_query_result(query_id)
```

### Concepts (described by ConqueryIds)
Concepts are listed on the left hand side in Conquery.
Each Concept is described by an ID.

The class ConqueryId and the classes for each ID-type that inherit from it
are defined in ```cqapi.conquery_ids```
It is clear from the structure of the classes, how the IDs can be built and what combinations
are possible. 

To provide an overview, these are the possible ID combinations including examples.
```
Dataset - Concept
-> fdb_demo.alter

Dataset - Concept - Connector
-> fdb_demo.icd.kh_diagnose_icd_code
 
Dataset - Child - Child - (...)
-> fdb_demo.icd.c00-d48.c00-c14.c01

Dataset - Concept - Select
-> fdb_demo.ops.exists
 
Dataset - Concept - Connector - Select
-> fdb_demo.icd.au_fall.anzahl_au_faelle

Dataset - Concept - Connector - Date
-> fdb_demo.atc.atc.verordnungsdatum

Dataset - Concept - Connector - Filter
-> fdb_demo.icd.arzt_diagnose_icd_code.diagnosesicherheit

Dataset - SecondaryId
-> fdb_demo.drg

Dataset - Table - Column  # only am_300
-> fdb_demo.am300_belegpositionen.beleg_brutto_summe
```
Each ConqueryId only describes one part of the ID, such as the ConnectorId, and defines a base
that it is built on, such as a ConceptId. This goes down to the DatasetId, which cannot have a base.

To initiate a ConqueryId such as ```fdb_demo.ops.exists``` either use the from_str method
or initiate all the bases of the ID.

Example:
```python
from cqapi.conquery_ids import DatasetId, ConceptId, SelectId

ops_exists_select = SelectId.from_str("fdb_demo.ops.exists")

# or

dataset = DatasetId("fdb_demo")
ops_concept = ConceptId("ops", dataset)
ops_exists_select = SelectId("exists", ops_concept)
```

### Editor Queries
In the Editor we can put together queries and evaluate them to get the group of insurants that fulfil the
specifications.
We can specify the same queries (with some more liberties than we have in the graphical editor) with the cqapi.

An editor query can consist of multiple building blocks.
One possible building block are ConceptElement's. A ConceptElement must contain at least an Id and a ConceptTable, 
that describes which table the Id is based on.
Other building blocks are ExternalQuery's (user-uploaded queries in the backend) and SavedQuery's (previously executed and 
stored queries in the backend).

```
Building blocks:
- ConceptElement
    - ConceptTable
- ExternalQuery
- SavedQuery
```

Each building block can be (but does not have to be) wrapped into a DateRestriction, a Negation or 
an OrElement / AndElement.

```
Building block wrappers (optional):
- Date Restriction
- Negation
- AndElement
- OrElement
```

Finally, the building blocks must be wrapped in a ConceptQuery (regular query with one row for each insurant) 
or a SecondaryIdQuery (allocating a secondary id to the query, so that there is a row for each insurant and each value
of the secondary id).

```
Editor query wrappers:
- ConceptQuery
- SecondaryIdQuery
```

These editor queries can be executed and we get the query_id returned.
```python
from cqapi.api import ConqueryConnection
ConqueryConnection.execute_query()
```

For example, take this editor query:

![img.png](img.png)

**Note: This is a very base level approach, which is explained only to illustrate the functionality.
There are more user-friendly ways of creating queries, which are described in "Create Query" and "Query Editor",
however those functions use the same functionality under the hood.**

First we have to initiate the building blocks and then use the ```OrElement, AndElement``` to make a logical statement
which we can then wrap in a ConceptQuery.

```python
from cqapi.queries.base_elements import ConceptElement, OrElement, AndElement, ConceptQuery, ConceptTable
from cqapi.conquery_ids import ChildId, SelectId, ConnectorId

# ConceptElement ATC A
atc_a = ChildId.from_str("fdb_demo.atc.a")
atc_exists = SelectId.from_str("fdb_demo.atc.exists")
atc_connector = ConnectorId.from_str("fdb_demo.atc.atc")
atc_a_concept_element = ConceptElement(ids=[atc_a], concept_selects=[atc_exists],
                                       tables=[ConceptTable(connector_id=atc_connector)])

# Concept Element ICD C00-D48
icd_c00_d48 = ChildId.from_str("fdb_demo.icd.c00-d48")
icd_exists = SelectId.from_str("fdb_demo.icd.exists")
icd_kh_connector = ConnectorId.from_str("fdb_demo.icd.kh_diagnose_icd_code")
icd_c00_d48_concept_element = ConceptElement(ids=[icd_c00_d48], concept_selects=[icd_exists],
                                       tables=[ConceptTable(connector_id=icd_kh_connector)])

# Concept Element ATC B
atc_b = ChildId.from_str("fdb_demo.atc.b")
atc_b_concept_element = ConceptElement(ids=[atc_b], concept_selects=[atc_exists],
                                       tables=[ConceptTable(connector_id=atc_connector)])

# Or Element, AndElement
or_query = OrElement(children=[icd_c00_d48_concept_element, atc_b_concept_element])

and_query = AndElement(children=[atc_a_concept_element, or_query])

# ConceptQuery
concept_query = ConceptQuery(root=and_query)

# Execute query
query_id = cq.execute_query(query=concept_query)

```

### Form Queries



# TODO:
- Types of queries (and their corresponding editor queries)
- Create Query
- QueryEditor
- Query vs Query Id
- Which queries are executable
- what can you put in get data (for each query, flussdiagramm examples from create query to get data)

