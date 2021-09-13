from unittest.case import TestCase
from cqapi.queries.editor import QueryEditor
from cqapi.queries.base_elements import ConceptElement, OrElement, DateRestriction, SecondaryIdQuery, \
    ConceptQuery, ConceptTable
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
    TestCase().assertDictEqual(concept_element, ConceptElement.from_query(concept_element).to_dict())

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

    TestCase().assertDictEqual(or_element, OrElement.from_query(or_element).to_dict())

    date_restriction = {
        "type": "DATE_RESTRICTION",
        "dateRange": {
            "min": "2020-01-01",
            "max": "2020-12-31"},
        "child": or_element}

    TestCase().assertDictEqual(date_restriction, DateRestriction.from_query(date_restriction).to_dict())

    secondary_id_query = {"type": "SECONDARY_ID_QUERY",
                          "secondaryId":
                              "dataset1.beh_fall_id",
                          "root": {"type": "AND",
                                   "children": [date_restriction]}}

    TestCase().assertDictEqual(secondary_id_query, SecondaryIdQuery.from_query(secondary_id_query).to_dict())


def test_and_query():
    query_1 = {'type': 'DATE_RESTRICTION',
               'child': {'type': 'CONCEPT',
                         'ids': ['dataset1.atc.a.a10.a10a'],
                         'selects': ['atc.atc.liste_rezepte', 'atc.atc.liste_pzn'],
                         'tables': [{'id': 'dataset1.atc.atc',
                                     'dateColumn': {'value': 'dataset1.atc.atc.abgabedatum'}}]},
               'dateRange': {'min': '2020-01-01', 'max': '2020-03-31'}}
    query_2 = {'type': 'AND',
               'children': [{'type': 'OR',
                             'children': [{'type': 'CONCEPT',
                                           'ids': ['dataset1.alter'],
                                           'excludeFromSecondaryIdQuery': True,
                                           'excludeFromTimeAggregation': False,
                                           'tables': [{'id': 'dataset1.alter.alter',
                                                       'dateColumn': {
                                                           'value': 'dataset1.alter.alter.versichertenzeit'}}]}]}]}

    query_editor = QueryEditor(query_1)
    query_editor.and_query(QueryEditor(query_2))
    and_query_out = query_editor.write_query()
    and_query_val = {"type": "AND", "children": [query_1, query_2]}

    TestCase().assertDictEqual(and_query_val, and_query_out)

test_and_query()
def test_remove_selects():
    query_object_1 = ConceptElement(ids=["dataset1.concept1"],
                                    tables=[ConceptTable(connector_id="dataset1.concept1.table1",
                                                         select_ids=["dataset1.concept1.table1.select1",
                                                                     "dataset1.concept1.table1.select2"])],
                                    concept_selects=["dataset1.concept1.select1"])
    or_query = OrElement(children=[query_object_1.copy()])
    concept_query = ConceptQuery(root=or_query)
    query_editor = QueryEditor(query=concept_query)
    query_editor.remove_all_selects()
    assert query_editor.query.root.children[0].selects == list()
    assert query_editor.query.root.children[0].tables[0].selects == list()


def test_relativ_export_form():
    query_object_1 = ConceptElement(ids=["dataset1.concept1"])
    query_object_2 = ConceptQuery(root=ConceptElement(ids=["dataset1.concept2"]))

    export_form = RelativeExportForm(
        query_id="dataset1.query_id",
        resolution="QUARTERS",
        before_index_queries=[query_object_1, query_object_2],
        after_index_queries=None,
        time_count_after=2
    )

    export_form_out = export_form.to_dict()
    export_form_val = {
        "type": "EXPORT_FORM",
        "queryGroup": "dataset1.query_id",
        "resolution": "QUARTERS",
        "timeMode": {
            "value": "RELATIVE",
            "timeUnit": "QUARTERS",
            'timeCountBefore': 1,
            'timeCountAfter': 2,
            'indexSelector': 'EARLIEST',
            'indexPlacement': 'BEFORE',
            'features': [query_object_1.to_dict(), query_object_2.root.to_dict()],
            'outcomes': []
        }
    }

    TestCase().assertDictEqual(export_form_val, export_form_out)


def test_entity_date_export_form():
    query_object_1 = ConceptElement(ids=["dataset1.concept1"])
    query_object_2 = ConceptQuery(root=ConceptElement(ids=["dataset1.concept2"]))

    export_form = EntityDateExportForm(
        query_id="dataset1.query_id",
        resolution="QUARTERS",
        features=[query_object_1, query_object_2],
        date_aggregation_mode="MERGE",
        date_range={"min": "2020-01-01", "max": "2020-12-31"},
        alignment_hint="YEAR"
    )

    export_form_out = export_form.to_dict()
    export_form_val = {
        "type": "EXPORT_FORM",
        "queryGroup": "dataset1.query_id",
        "resolution": "QUARTERS",
        "timeMode": {
            "value": "ENTITY_DATE",
            Keys.alignment_hint: "YEAR",
            "dateAggregationMode": "MERGE",
            "dateRange": {"min": "2020-01-01", "max": "2020-12-31"},
            "features": [query_object_1.to_dict(), query_object_2.root.to_dict()]
        }
    }
    test = TestCase()
    test.maxDiff = None
    test.assertDictEqual(export_form_val, export_form_out)


def test_full_export_form():
    concept = {Keys.tables: [{
        Keys.connector_id: "dataset1.alter.alter"
    }]}

    full_export_form = FullExportForm(query_id="dataset1.query_id",
                                      concept_id="dataset1.alter",
                                      concept=concept,
                                      start_date="2020-01-01",
                                      end_date="2020-12-31")
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
