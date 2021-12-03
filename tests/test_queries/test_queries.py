import cqapi.queries.queries as queries
from cqapi.datasets import set_test_datasets
from copy import deepcopy
import json
from pathlib import Path
import os

set_test_datasets()
test_queries_path = Path(os.path.dirname(os.path.realpath(__file__))) / "test_queries"


def test_get_selects_from_query():
    query = {
        "type": "OR",
        "children": [
            {
                "type": "CONCEPT",
                "tables": [
                    {
                        "selects": [
                            "dataset1.icd.kh_diagnose_icd_code.anzahl_krankenhausfaelle",
                            "dataset1.icd.kh_diagnose_icd_code.liste_icd",
                            "dataset1.icd.kh_diagnose_icd_code.liste_entlassungstage"
                        ]
                    },
                    {
                        "selects": []
                    }
                ],
                "selects": [
                    "dataset1.icd.icd_exists"
                ]
            }
        ]
    }
    assert set(queries.get_selects_from_query(query)) == {
        "dataset1.icd.kh_diagnose_icd_code.anzahl_krankenhausfaelle",
        "dataset1.icd.kh_diagnose_icd_code.liste_icd",
        "dataset1.icd.kh_diagnose_icd_code.liste_entlassungstage",
        "dataset1.icd.icd_exists"}

    query = {
        "type": "OR",
        "children": [
            {
                "type": "CONCEPT",
                "tables": [
                    {
                        "selects": []
                    }
                ],
                "selects": [
                    "dataset1.icd.icd_exists"
                ]
            },
            {
                "type": "CONCEPT",
                "tables": [
                    {
                        "selects": []
                    },
                    {
                        "selects": [
                            "dataset1.icd.au_fall.anzahl_au_faelle"
                        ]
                    },
                    {
                        "selects": []
                    }
                ],
                "selects": [
                    "dataset1.icd.icd_exists",
                    "dataset1.icd.dates"
                ]
            }
        ]
    }

    assert set(queries.get_selects_from_query(query)) == {"dataset1.icd.icd_exists",
                                                          "dataset1.icd.au_fall.anzahl_au_faelle",
                                                          "dataset1.icd.icd_exists",
                                                          "dataset1.icd.dates"
                                                          }


def test_remove_selects_from_query():
    from copy import deepcopy
    query = {
        "type": "OR",
        "children": [
            {
                "type": "CONCEPT",
                "tables": [
                    {
                        "selects": ["dataset1.table1.select1",
                                    "dataset1.table1.select2",
                                    "dataset1.table1.select3"]
                    },
                    {
                        "selects": ["dataset1.table2.select1",
                                    "dataset1.table2.select2",
                                    "dataset1.table2.select3"]
                    },
                    {
                        "selects": []
                    }
                ],
                "selects": ["dataset1_select1",
                            "dataset1_select2"]
            }
        ]
    }
    query_output = {
        "type": "OR",
        "children": [
            {
                "type": "CONCEPT",
                "tables": [
                    {
                        "selects": []
                    },
                    {
                        "selects": []
                    },
                    {
                        "selects": []
                    }
                ],
                "selects": []
            }
        ]
    }
    assert queries.remove_selects_from_query(deepcopy(query)) == query_output
    query_output = {
        "type": "OR",
        "children": [
            {
                "type": "CONCEPT",
                "tables": [
                    {
                        "selects": []
                    },
                    {
                        "selects": ["dataset1.table2.select2"]
                    },
                    {
                        "selects": []
                    }
                ],
                "selects": ["dataset1_select2"]
            }
        ]
    }
    assert queries.remove_selects_from_query(deepcopy(query),
                                             selects_to_keep=["dataset1.table2.select2",
                                                              "dataset1_select2"]) == query_output


def test_separate_query_by_selects():
    # query without selects
    query = {
        "type": "OR",
        "children": [
            {
                "type": "CONCEPT",
                "tables": [
                    {
                        "selects": []
                    },
                    {
                        "selects": []
                    },
                    {
                        "selects": []
                    }
                ],
                "selects": []
            }
        ]
    }

    query_output = [
        {
            "type": "OR",
            "children": [
                {
                    "type": "CONCEPT",
                    "tables": [
                        {
                            "selects": []
                        },
                        {
                            "selects": []
                        },
                        {
                            "selects": []
                        }
                    ],
                    "selects": []
                }
            ]
        }
    ]
    assert queries.separate_query_by_selects(deepcopy(query)) == query_output
    # query with one select
    query = {
        "type": "OR",
        "children": [
            {
                "type": "CONCEPT",
                "tables": [
                    {
                        "selects": ["dataset1.table1.select1"]
                    },
                    {
                        "selects": []
                    },
                    {
                        "selects": []
                    }
                ],
                "selects": []
            }
        ]
    }
    query_output = [
        {
            "type": "OR",
            "children": [
                {
                    "type": "CONCEPT",
                    "tables": [
                        {
                            "selects": ["dataset1.table1.select1"]
                        },
                        {
                            "selects": []
                        },
                        {
                            "selects": []
                        }
                    ],
                    "selects": []
                }
            ]
        }
    ]
    assert queries.separate_query_by_selects(deepcopy(query)) == query_output

    # query with select in table and concept select
    query = {
        "type": "OR",
        "children": [
            {
                "type": "CONCEPT",
                "tables": [
                    {
                        "selects": ["dataset1.table1.select1"]
                    },
                    {
                        "selects": []
                    },
                    {
                        "selects": []
                    }
                ],
                "selects": ["dataset1.select1"]
            }
        ]
    }
    query_output = [
        {
            "type": "OR",
            "children": [
                {
                    "type": "CONCEPT",
                    "tables": [
                        {
                            "selects": ["dataset1.table1.select1"]
                        },
                        {
                            "selects": []
                        },
                        {
                            "selects": []
                        }
                    ],
                    "selects": []
                }
            ]
        },
        {
            "type": "OR",
            "children": [
                {
                    "type": "CONCEPT",
                    "tables": [
                        {
                            "selects": []
                        },
                        {
                            "selects": []
                        },
                        {
                            "selects": []
                        }
                    ],
                    "selects": ["dataset1.select1"]
                }
            ]
        }
    ]

    assert queries.separate_query_by_selects(deepcopy(query)) == query_output

    # query with two selects in same table
    query = {
        "type": "OR",
        "children": [
            {
                "type": "CONCEPT",
                "tables": [
                    {
                        "selects": ["dataset1.table1.select1"]
                    },
                    {
                        "selects": ["dataset1.table2.select1"]
                    },
                    {
                        "selects": []
                    }
                ],
                "selects": []
            }
        ]
    }
    query_output = [
        {
            "type": "OR",
            "children": [
                {
                    "type": "CONCEPT",
                    "tables": [
                        {
                            "selects": ["dataset1.table1.select1"]
                        },
                        {
                            "selects": []
                        },
                        {
                            "selects": []
                        }
                    ],
                    "selects": []
                }
            ]
        },
        {
            "type": "OR",
            "children": [
                {
                    "type": "CONCEPT",
                    "tables": [
                        {
                            "selects": []
                        },
                        {
                            "selects": ["dataset1.table2.select1"]
                        },
                        {
                            "selects": []
                        }
                    ],
                    "selects": []
                }
            ]
        }
    ]
    assert queries.separate_query_by_selects(deepcopy(query)) == query_output


def test_get_concept_elements_from_query():
    query = {'type': 'AND',
             'children': [{'type': 'CONCEPT_QUERY',
                           'root': {'type': 'SAVED_QUERY',
                                    'query': 'dataset1.6da7e516-f052-478b-b015-05195717fb49'}},
                          {'type': 'DATE_RESTRICTION',
                           'dateRange': {'min': '2020-01-01', 'max': '2020-12-31'},
                           'child': {'type': 'OR',
                                     'children': [{'type': 'CONCEPT',
                                                   'ids': ['dataset1.atc.a'],
                                                   'label': 'A',
                                                   'tables': [{'id': 'dataset1.atc.atc',
                                                               'dateColumn': {
                                                                   'value': 'dataset1.atc.atc.abgabedatum'},
                                                               'selects': [],
                                                               'filters': []}],
                                                   'selects': []}]}}]}

    queries.get_concept_elements_from_query(query)

    query = json.load((test_queries_path / "saved_icd_au_kh.json").open("r"))

    concept_elements = json.load((test_queries_path / "saved_icd_au_kh.concept_elements.json").open("r"))

    assert concept_elements == queries.get_concept_elements_from_query(query)


def test_remove_connectors_from_query():
    query = json.load((test_queries_path / "saved_icd_au_kh_au.json").open("r"))
    query_without_kh = json.load((test_queries_path / "saved_icd_au_kh_au.removed_connector_kh.json").open("r"))

    assert query_without_kh == queries.remove_connectors_from_query(query, ["icd.kh_fall"])

    querys = [{'type': 'OR',
               'children': [{'type': 'CONCEPT',
                             'ids': ['dataset1.icd.z00-z99'],
                             'label': 'fix_sel:SELECTED@original_label:Z00 - Z99',
                             'tables': [{'id': 'dataset1.icd.kh_diagnose_icd_code',
                                         'dateColumn': {
                                             'value': 'dataset1.icd.kh_diagnose_icd_code.entlassungsdatum'},
                                         'selects': [],
                                         'filters': []},
                                        {'id': 'dataset1.icd.au_fall',
                                         'dateColumn': {'value': 'dataset1.icd.au_fall.au-beginn'},
                                         'selects': [],
                                         'filters': []},
                                        {'id': 'dataset1.icd.arzt_diagnose_icd_code',
                                         'dateColumn': None,
                                         'selects': [],
                                         'filters': []}],
                             'selects': []}]},
              {'type': 'OR',
               'children': [{'type': 'CONCEPT',
                             'ids': ['dataset1.icd.x'],
                             'label': 'fix_sel:SELECTED@original_label:x',
                             'tables': [{'id': 'dataset1.icd.kh_diagnose_icd_code',
                                         'dateColumn': {
                                             'value': 'dataset1.icd.kh_diagnose_icd_code.entlassungsdatum'},
                                         'selects': [],
                                         'filters': []},
                                        {'id': 'dataset1.icd.au_fall',
                                         'dateColumn': {'value': 'dataset1.icd.au_fall.au-beginn'},
                                         'selects': [],
                                         'filters': []},
                                        {'id': 'dataset1.icd.arzt_diagnose_icd_code',
                                         'dateColumn': None,
                                         'selects': [],
                                         'filters': []}],
                             'selects': []}]}]

    for query in querys:
        queries.remove_connectors_from_query(query, ["icd.kh_diagnose_icd_code",
                                                     "icd.au_fall"])


def test_add_connector_select_to_query():
    query = json.load((test_queries_path / "saved_icd_au_kh.json").open("r"))

    query_with_au_select = json.load((test_queries_path / "saved_icd_au_kh.add_au_days_select.json").open("r"))

    assert query_with_au_select == queries.add_connector_select_to_query(query, concept_id="icd.c00-d48",
                                                                         connector_id="icd.au_fall",
                                                                         select_id="icd.au_fall_days")


def test_add_filter_to_query():
    query = {'type': 'CONCEPT',
             'ids': ['dataset1.atc.a.a10.a10a'],
             'tables': [{'id': 'dataset1.atc.atc',
                         'dateColumn': {'value': 'dataset1.atc.atc.abgabedatum'},
                         'selects': [],
                         'filters': []}],
             'selects': []}

    query_val = deepcopy(query)
    query_val["tables"][0]["filters"] = [{
        "filter": "dataset1.atc.atc.rezeptnummer",
        "type": "BIG_MULTI_SELECT",
        "value": ["1", "2", "3"]
    }]

    query_out = queries.add_filter_to_query(query, concept_id="atc.a.a10.a10a",
                                            connector_id="atc.atc",
                                            filter_id="atc.atc.rezeptnummer",
                                            filter_obj={
                                                "filter": "dataset1.atc.atc.rezeptnummer",
                                                "type": "BIG_MULTI_SELECT",
                                                "value": ["1", "2", "3"]
                                            })

    assert query_val == query_out
