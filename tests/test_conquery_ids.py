from cqapi.conquery_ids import ConqueryIdCollection, ConqueryId
from cqapi import ConqueryConnection

conquery_token = "token"
conquery_url = "http://localhost:8000"
conn = ConqueryConnection(conquery_url, conquery_token)
concepts = conn.get_concepts(dataset="dataset1")


def test_label():
    ids = ConqueryIdCollection()

    ids.add(ConqueryId("dataset1.icd", "concept"))
    ids.add(ConqueryId("dataset1.icd.au_fall_21c.sum_au", "connector_select"))
    ids.add(ConqueryId("dataset1.icd.au_fall_21c.krankheitsursache", "filter"))
    ids.add(ConqueryId("dataset1.atc.atc", "connector"))

    print(ids.create_label_dicts(concepts=concepts))

    table = ids.print_id_labels_as_table(concepts=concepts)
    print("hi")


test_label()
