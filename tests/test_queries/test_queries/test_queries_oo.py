from cqapi.queries.queries_oo import *
from unittest import TestCase


def test_from_write_query():
    concept_element = {
        "type": "CONCEPT",
        "ids": [
            "dataset1.atc"],
        "tables": [
            {
                "id": "dataset1.atc.atc",
                "dateColumn": {
                    "value": "dataset1.atc.atc.abgabedatum"},
                "selects": [
                    "dataset1.atc.atc.anzahl_apotheken"],
                "filters": [
                    {
                        "filter": "dataset1.atc.atc.anzahl_packungen_f",
                        "type": "REAL_RANGE",
                        "value": {
                            "min": 10,
                            "max": 20}}]}],
        "selects": []}
    TestCase().assertDictEqual(concept_element, ConceptElement.from_query(concept_element).write_query())

    or_element = {
        "type": "OR",
        "children": [concept_element,
                     {
                         "type": "CONCEPT",
                         "ids": [
                             "dataset1.alter"],
                         "tables": [
                             {
                                 "id": "dataset1.alter.alter",
                                 "dateColumn": {
                                     "value": "dataset1.alter.alter.versichertenzeit"},
                                 "selects": [],
                                 "filters": []}],
                         "selects": [
                             "dataset1.alter.dates"]}]}

    TestCase().assertDictEqual(or_element, OrElement.from_query(or_element).write_query())

    date_restriction = {
        "type": "DATE_RESTRICTION",
        "dateRange": {
            "min": "2020-01-01",
            "max": "2020-12-31"},
        "child": or_element}

    TestCase().assertDictEqual(date_restriction, DateRestriction.from_query(date_restriction).write_query())

    secondary_id_query = {"type": "SECONDARY_ID_QUERY",
                          "secondaryId":
                              "dataset1.beh_fall_id",
                          "root": {"type": "AND",
                                   "children": [date_restriction]}}

    TestCase().assertDictEqual(secondary_id_query, SecondaryIdQuery.from_query(secondary_id_query).write_query())


def test_and_query():
    query_1 = {'type': 'DATE_RESTRICTION',
               'child': {'type': 'CONCEPT',
                         'ids': ['dataset1.atc.a.a10.a10a'],
                         'selects': ['atc.atc.liste_rezepte', 'atc.atc.liste_pzn'],
                         'tables': [{'id': 'dataset1.atc.atc',
                                     'dateColumn': {'value': 'dataset1.atc.atc.abgabedatum'},
                                     'filters': [],
                                     'selects': []}]},
               'dateRange': {'min': '2020-01-01', 'max': '2020-03-31'}}
    query_2 = {'type': 'AND',
               'children': [{'type': 'OR',
                             'children': [{'type': 'CONCEPT',
                                           'ids': ['dataset1.alter'],
                                           'excludeFromSecondaryIdQuery': True,
                                           'excludeFromTimeAggregation': False,
                                           'selects': [],
                                           'tables': [{'id': 'dataset1.alter.alter',
                                                       'dateColumn': {
                                                           'value': 'dataset1.alter.alter.versichertenzeit'},
                                                       'filters': [],
                                                       'selects': []}]}]}]}

    query_editor = QueryEditor(query_1)
    query_editor.and_query(QueryEditor(query_2))
    and_query_out = query_editor.write_query()
    and_query_val = {"type": "AND", "children": [query_1, query_2]}

    TestCase().assertDictEqual(and_query_val, and_query_out)


def test_create_query():
    concepts = {
        "dataset1.alter": {'parent': 'dataset1.sozdem_infonet_struc',
                           'label': 'Alter',
                           'description': None,
                           'active': True,
                           'children': [],
                           'additionalInfos': [],
                           'matchingEntries': 0,
                           'dateRange': {'min': None, 'max': None},
                           'tables': [{'id': 'dataset1.vers_stamm',
                                       'connectorId': 'dataset1.alter.alter',
                                       'label': 'Alter',
                                       'dateColumn': {'defaultValue': 'dataset1.alter.alter.versichertenzeit',
                                                      'options': [{'label': 'Versichertenzeit',
                                                                   'value': 'dataset1.alter.alter.versichertenzeit',
                                                                   'templateValues': None,
                                                                   'optionValue': None},
                                                                  {'label': 'Erster Tag',
                                                                   'value': 'dataset1.alter.alter.erster_tag',
                                                                   'templateValues': None,
                                                                   'optionValue': None},
                                                                  {'label': 'Letzter Tag',
                                                                   'value': 'dataset1.alter.alter.letzter_tag',
                                                                   'templateValues': None,
                                                                   'optionValue': None}]},
                                       'filters': [{'id': 'dataset1.alter.alter.alterseinschr$c3$a4nkung',
                                                    'label': 'Alterseinschränkung',
                                                    'type': 'INTEGER_RANGE',
                                                    'unit': None,
                                                    'description': 'Alter zur gegebenen Datumseinschränkung',
                                                    'options': [],
                                                    'min': None,
                                                    'max': None,
                                                    'template': None,
                                                    'pattern': None,
                                                    'allowDropFile': None}],
                                       'selects': [{'id': 'dataset1.alter.alter.alter_select',
                                                    'label': 'Ausgabe Alter',
                                                    'description': 'Automatisch erzeugter Zusatzwert.',
                                                    'resultType': {'type': 'INTEGER'}},
                                                   {'id': 'dataset1.alter.alter.liste_geburtsdatum',
                                                    'label': 'Ausgabe Geburtsdatum',
                                                    'description': None,
                                                    'resultType': {'type': 'DATE'}},
                                                   {'id': 'dataset1.alter.alter.dates',
                                                    'label': 'Datumswerte',
                                                    'description': None,
                                                    'resultType': {'type': 'LIST',
                                                                   'elementType': {'type': 'DATE_RANGE'}}}],
                                       'supportedSecondaryIds': []}],
                           'detailsAvailable': True,
                           'codeListResolvable': False,
                           'selects': [{'id': 'dataset1.alter.dates',
                                        'label': 'Datumswerte',
                                        'description': None,
                                        'resultType': {'type': 'LIST', 'elementType': {'type': 'DATE_RANGE'}}}]}
    }

    query_val = {"type": "CONCEPT", "ids": ["dataset1.alter"], "selects": [],
                 "tables": [{'id': 'dataset1.alter.alter', 'filters': [], 'selects': []}]}
    query_out = create_query(concept_id="dataset1.alter", concepts=concepts).write_query()
    TestCase().assertDictEqual(d1=query_val, d2=query_out)
