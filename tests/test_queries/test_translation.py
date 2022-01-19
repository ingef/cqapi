from copy import deepcopy
from cqapi.queries.base_elements import ConceptQuery, ConceptElement
from cqapi.queries.translation import translate_query
import json
from cqapi import ConqueryConnection
import cqapi.datasets

cqapi.datasets.set_dataset_list(["dataset1", "dataset2", "dataset3"])


icd_object = {'children': ['dataset3.icd.a00-b99', 'dataset3.icd.c00-d48'],
              'label': "ICD",
              'selects': [{
                  "id": "dataset3.icd.exists",
                  "label": "ICD liegt vor",
                  "description": None
              }],
              'tables': [
                  {'id': 'dataset3.kh_diagnose',
                   'label': "KH Diagnose",
                   'connectorId': 'dataset3.icd.kh_diagnose_icd_code',
                   'dateColumn': {
                       'options': [
                           {
                               'value': 'dataset3.icd.kh_diagnose_icd_code.entlassungsdatum'},
                           {
                               'value': 'dataset3.icd.kh_diagnose_icd_code.aufnahmedatum'},
                           {
                               'value': 'dataset3.icd.kh_diagnose_icd_code.aufenthaltsdauer'}]},
                   'selects': [{'id': 'dataset3.icd.kh_diagnose_icd_code.liste_icd',
                                'label': 'Ausgabe ICD-Code (TP4a)',
                                'description': None},
                               {'id': 'dataset3.icd.kh_diagnose_icd_code.anzahl_krankenhausfaelle',
                                'label': 'Anzahl Krankenhausfälle'}],
                   'filters': [{'id': 'dataset3.icd.kh_diagnose_icd_code.diagnoseart',
                                'label': 'Diagnoseart',
                                'type': 'MULTI_SELECT',
                                'description': 'Art der Diagnose',
                                'options': [{'label': 'Hauptdiagnose',
                                             'value': '1'},
                                            {'label': 'Nebendiagnose',
                                             'value': '2'},
                                            {'label': 'Aufnahmediagnose',
                                             'value': '3'}]},
                               {'id': 'dataset3.icd.kh_diagnose_icd_code.diagnose-anhang',
                                'label': 'Diagnose-Anhang',
                                'type': 'MULTI_SELECT',
                                'description': 'Klassifizierung in primäre und sekundäre Diagnosen',
                                'options': [{'label': 'sekundärer ICD-Schlüssel (!)',
                                             'value': '!'},
                                            {'label': 'primärer ICD-Schlüssel (#)',
                                             'value': '#'},
                                            {'label': 'sekundärer ICD-Schlüssel (*)',
                                             'value': '*'}]}]},
                  {'id': 'dataset3.au_diagnose',
                   'label': 'AU Diagnose',
                   'connectorId': 'dataset3.icd.au_fall',
                   'dateColumn': {
                       'options': [{
                           'value': 'dataset3.icd.au_fall.au-beginn'},
                           {'value': 'dataset3.icd.au_fall.au-ende'},
                           {'value': 'dataset3.icd.au_fall.au-zeit'}]}},
                  {'id': 'dataset3.arzt_diagnose',
                   'connectorId': 'dataset3.icd.arzt_diagnose_icd_code',
                   'dateColumn': None}]}


def test_translate_query():
    eva_url = "http://localhost:9292"
    eva_token = "eva_token"

    with ConqueryConnection(eva_url, eva_token) as cq:

        query = {
            "type": "CONCEPT_QUERY",
            "root": {
                "type": "AND",
                "children": [
                    {
                        "type": "DATE_RESTRICTION",
                        'dateRange': {'max': None, 'min': None},
                        "child": {
                            "type": "OR",
                            "children": [
                                {
                                    "type": "CONCEPT",
                                    "ids": [
                                        "dataset1.alter"
                                    ],
                                    "tables": [
                                        {
                                            "id": "dataset1.alter.alter",
                                            "dateColumn": {
                                                "value": "dataset1.alter.alter.versichertenzeit"
                                            },
                                            "filters": [
                                                {'filter': "dataset1.alter.alter.alterseinschr$c3$a4nkung",
                                                 'type': 'INTEGER_RANGE',
                                                 'value': {'min': 21, 'max': None}}
                                            ]}]}]}}]}}
        concepts_new_dataset = {
            "dataset2.alter": {
                'children': [],
                'tables': [{'id': 'dataset2.vers_stamm',
                            'connectorId': 'dataset2.alter.alter',
                            'dateColumn': {'defaultValue': 'dataset2.alter.alter.versichertenzeit',
                                           'options': [{'label': 'Versichertenzeit',
                                                        'value': 'dataset2.alter.alter.versichertenzeit'},
                                                       {'label': 'Erster Tag',
                                                        'value': 'dataset2.alter.alter.erster_tag'},
                                                       {'label': 'Letzter Tag',
                                                        'value': 'dataset2.alter.alter.letzter_tag'}]},
                            "filters": [{
                                "id": "dataset2.alter.alter.alterseinschr$c3$a4nkung",
                                "label": "Alterseinschränkung",
                                "type": "INTEGER_RANGE",
                                "unit": None,
                                "description": "Alter zur gegebenen Datumseinschränkung"
                            }]}]}}
        output_new_query = json.loads(json.dumps(query).replace('dataset1', 'dataset2'))
        query_before = deepcopy(query)
        new_query, old_query = \
            translate_query(ConceptQuery.from_dict(query), concepts_new_dataset, cq, return_removed_ids=False)

        assert output_new_query == new_query.to_dict()
        assert query_before == old_query.to_dict()

        query = {
            "type": "CONCEPT",
            "ids": [
                "dataset1.icd.c00-d48"
            ],
            "selects": ["dataset1.icd.exists", "dataset1.icd.test"],
            "tables": [
                {
                    "id": "dataset1.icd.kh_diagnose_icd_code",
                    "dateColumn": {
                        "value": "dataset1.icd.kh_diagnose_icd_code.entlassungsdatum"
                    },
                    "selects": ['dataset1.icd.kh_diagnose_icd_code.anzahl_krankenhausfaelle',
                                'dataset1.icd.kh_diagnose_icd_code.test'],
                    "filters": []
                },
                {
                    "id": "dataset1.icd.au_fall",
                    "dateColumn": {
                        "value": "dataset1.icd.au_fall.au-beginn"
                    },
                    "selects": [],
                    "filters": []
                },
                {
                    "id": "dataset1.icd.arzt_diagnose_icd_code",
                    "selects": [],
                    "filters": []
                },
                {
                    "id": "dataset1.icd.au_fall_21c",
                    "dateColumn": {
                        "value": "dataset1.icd.au_fall_21c.au-beginn_(21c)"
                    },
                    "selects": [],
                    "filters": []
                }]
        }

        query_old_val = {
            "type": "CONCEPT",
            "ids": [
                "dataset1.icd.c00-d48"
            ],
            "selects": ["dataset1.icd.exists"],
            "tables": [
                {
                    "id": "dataset1.icd.kh_diagnose_icd_code",
                    "dateColumn": {
                        "value": "dataset1.icd.kh_diagnose_icd_code.entlassungsdatum"
                    },
                    "selects": ['dataset1.icd.kh_diagnose_icd_code.anzahl_krankenhausfaelle'],
                },
                {
                    "id": "dataset1.icd.au_fall",
                    "dateColumn": {
                        "value": "dataset1.icd.au_fall.au-beginn"
                    },
                },
                {
                    "id": "dataset1.icd.arzt_diagnose_icd_code"
                }]
        }

        concepts_new_dataset = {'dataset3.icd': icd_object}

        query_new_val = json.loads(json.dumps(query_old_val).replace('dataset1', 'dataset3'))

        new_query, old_query = \
            translate_query(ConceptElement.from_dict(query), concepts_new_dataset, cq, return_removed_ids=False)

        output_new_query["selects"] = ["dataset3.icd.exists"]
        assert query_new_val == new_query.to_dict()
        assert query_old_val == old_query.to_dict()
