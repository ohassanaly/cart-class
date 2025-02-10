from edstoolbox import SparkApp
import pandas as pd 
import edsnlp
from pyspark.sql.functions import col
from settings import *

app = SparkApp("get_entities")

@app.submit
def run(spark, sql, config):
    """
    Arguments
    ---------
    spark :
        Spark session object, for querying tables
    sql :
        Spark sql object, for querying tables
    config :
        Dictionary containing the configuration
    """
    # #load here the appropriate clinical note_df
    # note_df = sql("select * from {}.note".format(path))
    
    #converter
    def get_entities(doc):
        entities=[]
        for ent in doc.ents:
            d = dict(
                person_id=doc._.person_id,
                visit_occurrence_id=doc._.visit_occurrence_id,
                note_id=doc._.note_id,
                note_class_source_value = doc._.note_class_source_value,
                note_title=doc._.note_title,
                note_datetime=doc._.note_datetime,
                care_site_id=doc._.care_site_id,
                start=ent.start_char,
                end=ent.end_char,
                label=ent.label_,
                lexical_variant=ent.text,
                sentence = ent.sent.text,
                negation = ent._.negation,
                history = ent._.history,
                hypothesis = ent._.hypothesis,
                family = ent._.family,
            )
            entities.append(d) 
        return entities

    #regexp we are looking for
    regex = dict(
    hospitalisation = r"hospit.*(\n.*)?\n?.*(car[-\s]?t[-\s]?[c\s]|(" + drug_pattern + "))",
    injection = r"((inject|administr|infusion|j0).*(car[-\s]?t[-\s]?[c\s]|(" + drug_pattern + ")))",
    cancer = r"lymphome|ldgcb|dlbcl|lcm|mcl|myelome|leucemie|\slal\s",
    apherese = "aph[ée]r[èe]se",
    lymphodepletion = "(lymphod[ée]pl[ée]tion|conditionnement|fludarabine|endoxan|cyclophosphamide)",
    bridge = "bridg",
    bilan = r"(suivi|bilan|dosage|expansion|persistance|absence|ph[eé]notypage|population|d[eé]tection|recherche).*(car[-\s]?t[-\s]?[c\s]|(" + drug_pattern + "))",
    )

    nlp = edsnlp.blank("eds")
    nlp.add_pipe("eds.normalizer")
    nlp.add_pipe("eds.sentences")
    nlp.add_pipe(
        "eds.matcher",
        config=dict(
            regex=regex,
            attr="NORM",
        ),
    )
    nlp.add_pipe("eds.negation")
    nlp.add_pipe("eds.history")
    nlp.add_pipe("eds.hypothesis")
    nlp.add_pipe("eds.family")

    docs_iter = edsnlp.data.from_spark(data=note_df, converter="omop", 
                                       doc_attributes=["person_id","visit_occurrence_id","note_id","note_class_source_value","note_title","note_datetime","care_site_id"]
                                      )

    docs = nlp.pipe(docs_iter)
    
    #optimizing computation time using backend="spark"
    docs = docs.set_processing(backend="spark")
    note_nlp = docs.to_spark(converter=get_entities)
    note_nlp.toPandas().to_pickle(path = export_path)

if __name__ == "__main__":
    app.run()
