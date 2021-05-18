from cqapi.queries.editor import QueryEditor
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
                 "tables": [
                     {'id': 'dataset1.alter.alter', 'filters': [], 'selects': ["dataset1.alter.alter.ausgabe_alter"]}]}
    query_out = create_query(concept_id="dataset1.alter", concepts=concepts,
                             connector_select_ids=["dataset1.alter.alter.ausgabe_alter"]).write_query()
    TestCase().assertDictEqual(d1=query_val, d2=query_out)


def test_translate_query():
    # translate table
    icd_au_table = {
        "id": "dataset1.au_diagnose",
        "connectorId": "dataset1.icd.au_fall",
        "label": "AU-Diagnose",
        "dateColumn": {
            "defaultValue": "dataset1.icd.au_fall.au-beginn",
            "options": [
                {
                    "label": "AU-Beginn",
                    "value": "dataset1.icd.au_fall.au-beginn"
                }
            ]
        },
        "filters": [
            {
                "id": "dataset1.icd.au_fall.fall_id",
                "label": "Fallnummer",
                "type": "BIG_MULTI_SELECT",
                "description": "Fallnummer"
            }
        ],
        "selects": [
            {
                "id": "dataset1.icd.au_fall.liste_fall_id",
                "label": "Ausgabe Fallnummer"
            }
        ]
    }
    icd_kh_table = {
        "id": "dataset1.kh_diagnose",
        "connectorId": "dataset1.icd.kh_diagnose_icd_code",
        "label": "KH-Diagnose",
        "dateColumn": {
            "defaultValue": "dataset1.icd.kh_diagnose_icd_code.entlassungsdatum",
            "options": [
                {
                    "label": "Entlassungsdatum",
                    "value": "dataset1.icd.kh_diagnose_icd_code.entlassungsdatum"
                }
            ]
        },
        "filters": [
            {
                "id": "dataset1.icd.kh_diagnose_icd_code.fallzahl",
                "label": "Fallzahl",
                "type": "INTEGER_RANGE",
                "unit": "Fälle",
                "description": "Anzahl der Fälle"
            },
            {
                "id": "dataset1.icd.kh_diagnose_icd_code.anzahl_quartale",
                "label": "Anzahl Quartale",
                "type": "INTEGER_RANGE",
                "unit": "Quartale",
                "description": "Anzahl der Quartale mit Diagnose"
            }
        ],
        "selects": [
            {
                "id": "dataset1.icd.kh_diagnose_icd_code.liste_erster_entlassungstag",
                "label": "Ausgabe erster Entlassungstag"
            },
            {
                "id": "dataset1.icd.kh_diagnose_icd_code.liste_letzter_entlassungstag",
                "label": "Ausgabe letzter Entlassungstag"
            }
        ]
    }
    concepts = {"dataset1.icd": {"tables": [icd_au_table, icd_kh_table],
                                 "children": ["dataset1.icd.a", "dataset1.icd.b"],
                                 "selects": [{
                                     "id": "dataset1.icd.icd_exists",
                                     "label": "ICD liegt vor"
                                 }]
                                 }}
    table = ConceptTable(connector_id="dataset2.icd.au_fall",
                         date_column_id="dataset2.icd.au_fall.au-beginn",
                         select_ids=["dataset2.icd.au_fall.liste_fall_id",
                                     "dataset2.icd.au_fall.select_to_drop"],
                         filter_objs=[{"filter": "dataset2.icd.au_fall.fall_id"},
                                      {"filter": "dataset2.icd.au_fall.filter_to_drop"}])

    removed_ids = ConqueryIdCollection()
    new_table = table.translate(concepts=concepts, removed_ids=removed_ids)

    # check old table
    table_val = ConceptTable(connector_id="dataset2.icd.au_fall",
                             date_column_id="dataset2.icd.au_fall.au-beginn",
                             select_ids=["dataset2.icd.au_fall.liste_fall_id"],
                             filter_objs=[{"filter": "dataset2.icd.au_fall.fall_id"}])
    assert table == table_val

    # check removed ids
    removed_ids_val = ConqueryIdCollection()
    removed_ids_val.add(ConqueryId("dataset2.icd.au_fall.select_to_drop", "connector_select"))
    removed_ids_val.add(ConqueryId("dataset2.icd.au_fall.filter_to_drop", "filter"))
    assert removed_ids == removed_ids_val

    # check new table
    new_table_val = ConceptTable(connector_id="dataset1.icd.au_fall",
                                 date_column_id="dataset1.icd.au_fall.au-beginn",
                                 select_ids=["dataset1.icd.au_fall.liste_fall_id"],
                                 filter_objs=[{"filter": "dataset1.icd.au_fall.fall_id"}])

    assert new_table == new_table_val

    # test concept element
    removed_ids = ConqueryIdCollection()
    table_1 = ConceptTable(connector_id="dataset2.icd.au_fall")
    table_2 = ConceptTable(connector_id="dataset2.icd.table_to_drop")
    concept_element = ConceptElement(ids=["dataset2.icd.a", "dataset2.icd.a.a",
                                          "dataset2.icd.a.to_drop", "dataset2.icd.concept_id_to_drop"],
                                     tables=[table_1, table_2],
                                     exclude_from_time_aggregation=True,
                                     exclude_from_secondary_id=True,
                                     label="test",
                                     concept_selects=["dataset2.icd.icd_exists", "dataset2.icd.select_to_drop"])
    new_concept_element = concept_element.translate(concepts=concepts, children_ids=["dataset1.icd.a.a",
                                                                                     "dataset1.icd.a.b"],
                                                    removed_ids=removed_ids)

    assert concept_element == ConceptElement(ids=["dataset2.icd.a", "dataset2.icd.a.a"],
                                             tables=[ConceptTable(connector_id="dataset2.icd.au_fall")],
                                             exclude_from_secondary_id=True,
                                             exclude_from_time_aggregation=True,
                                             label="test",
                                             concept_selects=["dataset2.icd.icd_exists"])

    removed_ids_val = ConqueryIdCollection()
    removed_ids_val.add(ConqueryId("dataset2.icd.a.to_drop", id_type="concept"))
    removed_ids_val.add(ConqueryId("dataset2.icd.concept_id_to_drop", id_type="concept"))
    removed_ids_val.add(ConqueryId("dataset2.icd.table_to_drop", id_type="connector"))
    removed_ids_val.add(ConqueryId("dataset2.icd.select_to_drop", id_type="concept_select"))

    assert removed_ids == removed_ids_val

    assert new_concept_element == ConceptElement(ids=["dataset1.icd.a", "dataset1.icd.a.a"],
                                                 tables=[ConceptTable(connector_id="dataset1.icd.au_fall")],
                                                 exclude_from_secondary_id=True,
                                                 exclude_from_time_aggregation=True,
                                                 label="test",
                                                 concept_selects=["dataset1.icd.icd_exists"])




test_translate_query()
