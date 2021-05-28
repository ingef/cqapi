from cqapi.conquery_ids import ConqueryIdCollection, ConqueryId
from cqapi import ConqueryConnection

def test_label():
    if False:
        concepts = dict()
        ids = ConqueryIdCollection()

        ids.add(ConqueryId("dataset1.icd", "concept"))
        ids.add(ConqueryId("dataset1.icd.au_fall_21c.sum_au", "connector_select"))
        ids.add(ConqueryId("dataset1.icd.au_fall_21c.krankheitsursache", "filter"))
        ids.add(ConqueryId("dataset1.atc.atc", "connector"))

        print(ids.create_label_dicts(concepts=concepts))

        table = ids.print_id_labels_as_table(concepts=concepts)
