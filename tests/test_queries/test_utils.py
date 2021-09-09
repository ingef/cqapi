from cqapi.queries.elements import *
from unittest import TestCase

from cqapi.queries.utils import create_query


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

    query_val = {"type": "CONCEPT", "ids": ["dataset1.alter"],
                 "tables": [
                     {'id': 'dataset1.alter.alter', 'selects': ["dataset1.alter.alter.ausgabe_alter"]}]}
    query_out = create_query(concept_id="dataset1.alter", concepts=concepts,
                             connector_select_ids=["dataset1.alter.alter.ausgabe_alter"]).to_dict()
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
    new_table, remaining_table = table.translate(concepts=concepts, removed_ids=removed_ids)

    # check old table
    table_val = ConceptTable(connector_id="dataset2.icd.au_fall",
                             date_column_id="dataset2.icd.au_fall.au-beginn",
                             select_ids=["dataset2.icd.au_fall.liste_fall_id"],
                             filter_objs=[{"filter": "dataset2.icd.au_fall.fall_id"}])
    assert remaining_table == table_val

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
    new_concept_element, remaining_concept_element = \
        concept_element.translate(concepts=concepts, children_ids=["dataset1.icd.a.a",
                                                                   "dataset1.icd.a.b"],
                                  removed_ids=removed_ids)

    assert remaining_concept_element == ConceptElement(ids=["dataset2.icd.a", "dataset2.icd.a.a"],
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

    # negation
    negation = Negation(child=ConceptElement(ids=["dataset2.icd.a", "dataset2.icd.to_drop"],
                                             tables=[ConceptTable(connector_id="dataset2.icd.au_fall")]))
    negation_val = Negation(child=ConceptElement(ids=["dataset2.icd.a"],
                                                 tables=[ConceptTable(connector_id="dataset2.icd.au_fall")]))
    new_negation, remaining_negation = negation.translate(concepts=concepts, removed_ids=ConqueryIdCollection(),
                                                          children_ids=[])
    new_negation_val = Negation(child=ConceptElement(ids=["dataset1.icd.a"],
                                                     tables=[ConceptTable(connector_id="dataset1.icd.au_fall")]))

    assert remaining_negation.to_dict() == negation_val.to_dict()
    assert new_negation.to_dict() == new_negation_val.to_dict()

    # test no survivors
    concept_element = ConceptElement(ids=["dataset2.icd.to_drop"],
                                     tables=[ConceptTable(connector_id="dataset2.icd.au_fall")])

    assert concept_element.translate(concepts=concepts, removed_ids=removed_ids, children_ids=[]) == (None, None)

    concept_query = ConceptQuery(root=ConceptElement(ids=["dataset2.icd.to_drop"],
                                                     tables=[ConceptTable(connector_id="dataset2.icd.au_fall")]))
    assert concept_query.translate(concepts=concepts, removed_ids=removed_ids, children_ids=[]) == (None, None)

    # test only one survivor
    child_1 = ConceptElement(ids=["dataset2.icd.to_drop"],
                             tables=[ConceptTable(connector_id="dataset2.icd.au_fall")])
    child_2 = ConceptElement(ids=["dataset2.icd.a"],
                             tables=[ConceptTable(connector_id="dataset2.icd.au_fall")])
    and_element = AndElement(children=[child_1, child_2])
    new_and_element, remaining_and_element = and_element.translate(concepts=concepts,
                                                                   removed_ids=ConqueryIdCollection(),
                                                                   children_ids=[])

    child_2_val = ConceptElement(ids=["dataset2.icd.a"],
                                 tables=[ConceptTable(connector_id="dataset2.icd.au_fall")])
    and_element_val = AndElement(children=[child_2_val])
    new_child_2_val = ConceptElement(ids=["dataset1.icd.a"],
                                     tables=[ConceptTable(connector_id="dataset1.icd.au_fall")])
    new_and_element_val = AndElement(children=[new_child_2_val])

    assert remaining_and_element.to_dict() == and_element_val.to_dict()
    assert new_and_element.to_dict() == new_and_element_val.to_dict()
