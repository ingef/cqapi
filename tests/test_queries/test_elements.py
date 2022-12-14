from unittest.case import TestCase

from cqapi.conquery_ids import ConceptId, DatasetId, ConnectorId, SelectId, ChildId, DateId, FilterId
from cqapi.queries.editor import QueryEditor
from cqapi.queries.base_elements import ConceptElement, OrElement, DateRestriction, SecondaryIdQuery, \
    ConceptQuery, ConceptTable, create_query
from cqapi.queries.form_elements import EntityDateExportForm, FullExportForm, RelativeExportForm
from cqapi.namespace import Keys, QueryType


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
                            "max": 20}}]}]}

    TestCase().assertDictEqual(concept_element, ConceptElement.from_dict(concept_element).to_dict())

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
                                     "value": "dataset1.alter.alter.versichertenzeit"}, }],
                         "selects": [
                             "dataset1.alter.dates"]}]}

    TestCase().assertDictEqual(or_element, OrElement.from_dict(or_element).to_dict())

    date_restriction = {
        "type": "DATE_RESTRICTION",
        "dateRange": {
            "min": "2020-01-01",
            "max": "2020-12-31"},
        "child": or_element}

    TestCase().assertDictEqual(date_restriction, DateRestriction.from_dict(date_restriction).to_dict())

    secondary_id_query = {"type": "SECONDARY_ID_QUERY",
                          "secondaryId":
                              "dataset1.beh_fall_id",
                          "root": {"type": "AND",
                                   "children": [date_restriction]}}

    TestCase().assertDictEqual(secondary_id_query, SecondaryIdQuery.from_dict(secondary_id_query).to_dict())


def test_and_query():
    query_1 = {'type': 'DATE_RESTRICTION',
               'child': {'type': 'CONCEPT',
                         'ids': [ChildId.from_str('dataset1.atc.a.a10.a10a')],
                         'selects': [SelectId.from_str('atc.atc.liste_rezepte'),
                                     SelectId.from_str('atc.atc.liste_pzn')],
                         'tables': [{'id': ConnectorId.from_str('dataset1.atc.atc'),
                                     'dateColumn': {'value': DateId.from_str('dataset1.atc.atc.abgabedatum')}}]},
               'dateRange': {'min': '2020-01-01', 'max': '2020-03-31'}}

    query_1_out = {'type': 'DATE_RESTRICTION',
                   'child': {'type': 'CONCEPT',
                             'ids': ['dataset1.atc.a.a10.a10a'],
                             'selects': ['atc.atc.liste_rezepte',
                                         'atc.atc.liste_pzn'],
                             'tables': [{'id': 'dataset1.atc.atc',
                                         'dateColumn': {'value': 'dataset1.atc.atc.abgabedatum'}}]},
                   'dateRange': {'min': '2020-01-01', 'max': '2020-03-31'}}

    query_2 = {'type': 'AND',
               'children': [{'type': 'OR',
                             'children': [{'type': 'CONCEPT',
                                           'ids': [ConceptId.from_str('dataset1.alter')],
                                           'excludeFromSecondaryIdQuery': True,
                                           'excludeFromTimeAggregation': False,
                                           'tables': [{'id': ConnectorId.from_str('dataset1.alter.alter'),
                                                       'dateColumn': {
                                                           'value': DateId.from_str(
                                                               'dataset1.alter.alter.versichertenzeit')}}]}]}]}
    query_2_out = {'type': 'AND',
                   'children': [{'type': 'OR',
                                 'children': [{'type': 'CONCEPT',
                                               'ids': ['dataset1.alter'],
                                               'excludeFromSecondaryIdQuery': True,
                                               'excludeFromTimeAggregation': False,
                                               'tables': [{'id': 'dataset1.alter.alter',
                                                           'dateColumn': {
                                                               'value':
                                                                   'dataset1.alter.alter.versichertenzeit'}}]}]}]}

    query_editor = QueryEditor(query_1)
    query_editor.and_query(QueryEditor(query_2))
    and_query_out = query_editor.write_query()
    and_query_val = {"type": "AND", "children": [query_1_out, query_2_out]}

    TestCase().assertDictEqual(and_query_val, and_query_out)


def test_remove_selects():
    concept1 = ConceptId("concept1", DatasetId("dataset1"))
    table1 = ConnectorId("table1", concept1)
    select1 = SelectId("select1", table1)
    select2 = SelectId("select2", table1)
    concept_select1 = SelectId("select1", concept1)
    query_object_1 = ConceptElement(ids=[concept1],
                                    tables=[ConceptTable(connector_id=table1,
                                                         select_ids=[select1, select2])],
                                    concept_selects=[concept_select1])
    or_query = OrElement(children=[query_object_1.copy()])
    concept_query = ConceptQuery(root=or_query)
    query_editor = QueryEditor(query=concept_query)
    query_editor.remove_all_selects()
    assert query_editor.query.root.children[0].selects == list()
    assert query_editor.query.root.children[0].tables[0].selects == list()


def test_relativ_export_form():
    query_object_1 = ConceptElement(ids=[ConceptId("concept1", DatasetId("dataset1"))])
    query_object_2 = ConceptQuery(root=ConceptElement(ids=[ConceptId("concept2", DatasetId("dataset1"))]))

    export_form = RelativeExportForm(
        query_id="dataset1.query_id",
        resolutions=["QUARTERS"],
        features=[query_object_1, query_object_2],
        time_count_after=2
    )

    export_form_out = export_form.to_dict()
    export_form_val = {
        "type": "EXPORT_FORM",
        "queryGroup": "dataset1.query_id",
        "resolution": ["QUARTERS"],
        "alsoCreateCoarserSubdivisions": True,
        "features": [query_object_1.to_dict(), query_object_2.root.to_dict()],
        "timeMode": {
            "value": "RELATIVE",
            "timeUnit": "QUARTERS",
            'timeCountBefore': 1,
            'timeCountAfter': 2,
            'indexSelector': 'EARLIEST',
            'indexPlacement': 'BEFORE',
        }
    }

    TestCase().assertDictEqual(export_form_val, export_form_out)


def test_concept_element():
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
    concepts = {"dataset1.icd": {"tables": [icd_kh_table],
                                 "children": ["dataset1.icd.a", "dataset1.icd.b"],
                                 "selects": [{
                                     "id": "dataset1.icd.icd_exists",
                                     "label": "ICD liegt vor"
                                 }]
                                 }}
    dataset_id = DatasetId("dataset1")
    concept_id = ConceptId("icd", dataset_id)
    connector_id = ConnectorId("kh_diagnose_icd_code", concept_id)
    filter_id = FilterId("fallzahl", connector_id)
    connector_select_id = SelectId("liste_erster_entlassungstag", connector_id)

    query = create_query(concept_id=concept_id, concepts=concepts,
                         connector_ids=[connector_id])

    # test add connector
    query.remove_all_tables()
    query.add_connector(connector_id=connector_id, concepts=concepts)
    query.add_connector(connector_id=connector_id, concepts=concepts)  # check that it is not added twice

    query.add_filter({"type": "MULTI_SELECT",
                      "value": "test",
                      "filter": filter_id})

    query.add_connector_select(connector_select_id)

    assert query.to_dict() == {'type': 'CONCEPT',
                               'ids': ['dataset1.icd'],
                               'tables': [{'id': 'dataset1.icd.kh_diagnose_icd_code',
                                           'filters': [{'type': 'MULTI_SELECT', 'value': 'test',
                                                        'filter': 'dataset1.icd.kh_diagnose_icd_code.fallzahl'}],
                                           'selects': [
                                               'dataset1.icd.kh_diagnose_icd_code.liste_erster_entlassungstag']}]}


def test_entity_date_export_form():
    query_object_1 = ConceptElement(ids=[ConceptId("concept1", DatasetId("dataset1"))])
    query_object_2 = ConceptQuery(root=ConceptElement(ids=[ConceptId("concept2", DatasetId("dataset1"))]))

    export_form = EntityDateExportForm(
        query_id="dataset1.query_id",
        resolutions=["QUARTERS"],
        features=[query_object_1, query_object_2],
        date_aggregation_mode="MERGE",
        date_range={"min": "2020-01-01", "max": "2020-12-31"},
        alignment_hint="YEAR"
    )

    export_form_out = export_form.to_dict()
    export_form_val = {
        "type": "EXPORT_FORM",
        "queryGroup": "dataset1.query_id",
        "resolution": ["QUARTERS"],
        "alsoCreateCoarserSubdivisions": True,
        "features": [query_object_1.to_dict(), query_object_2.root.to_dict()],
        "timeMode": {
            "value": "ENTITY_DATE",
            Keys.alignment_hint: "YEAR",
            "dateAggregationMode": "MERGE",
            "dateRange": {"min": "2020-01-01", "max": "2020-12-31"}
        }
    }
    test = TestCase()
    test.maxDiff = None
    test.assertDictEqual(export_form_val, export_form_out)


def test_full_export_form():
    concept = {Keys.tables: [{
        Keys.connector_id: "dataset1.alter.alter"
    }]}
    alter_concept_id = ConceptId("alter", DatasetId("dataset1"))
    alter_connector_id = ConnectorId("alter", alter_concept_id)
    full_export_form = FullExportForm(query_id="dataset1.query_id",
                                      concept_id=alter_concept_id,
                                      concept=concept,
                                      start_date="2020-01-01",
                                      end_date="2020-12-31",
                                      validity_date_ids=[DateId("alter_validity_date_id", base=alter_connector_id)])
    form_out = full_export_form.to_dict()
    form_val = {
        Keys.type: "FULL_EXPORT_FORM",
        Keys.query_group: "dataset1.query_id",
        Keys.date_range: {"min": "2020-01-01", "max": "2020-12-31"},
        Keys.tables: [{Keys.type: QueryType.CONCEPT.value,
                       Keys.ids: ["dataset1.alter"],
                       Keys.tables: [{Keys.id: "dataset1.alter.alter"}]}]
    }
    test_case = TestCase()
    test_case.maxDiff = None
    test_case.assertDictEqual(form_val, form_out)
