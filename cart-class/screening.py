import pandas as pd
import edsnlp
from pyspark.sql.functions import col
from datetime import datetime
from settings import *
from edsnlp.pipelines.misc.sections.patterns import sections
from spacy.tokens import Span
from typing import List, Optional
import re

##Dates processing

#récupère les dates dans la même sentence que l'ent
def candidate_dates(ent: Span) -> List[Span]:
    """Return every dates in the same sentence as the entity"""
    return [date for date in ent.doc.spans["dates"] if date.sent == ent.sent]

#calcule la date la plus proche de l'ent parmi les candidates
def get_event_date(ent: Span) -> Optional[Span]:
    """Link an entity to the closest date in the sentence, if any"""

    dates = candidate_dates(ent)  # 

    #if not dates: #pas dates dans la phrase de l'ent ; dans ce cas on prend la date la plus proche dans le CR
    #    dates=ent.doc.spans["dates"]

    if not dates : #pas de dates tout court dans le CR ; dans ce cas on est coincé
        return
    
    dates = sorted(
        dates,
        key=lambda d: min(abs(d.start - ent.end), abs(ent.start - d.end)),
    )

    return dates[0]  #

#cette fonction permet d'obtenir n'importe quelle date extraite par la pipeline en datetime
def normalize_date(date,note_date) :
    
    note_datetime = datetime(
    year=note_date.year, 
    month=note_date.month,
    day=note_date.day)

    if not date :
        return(pd.NaT)
    else :
        x = date._.date.to_datetime(note_datetime=note_datetime,infer_from_context=True,tz=None,default_day=15).date() #récupère la date en datetime.datedatetime(
        return(datetime(int(x.year), int(x.month), int(x.day))) #on résout ici le problème de typage avec les objects pendulum
    
#Handcrafted converter

def get_entities(doc) :
    entities=[]
    for ent in doc.ents:
        try:
            ent_date = normalize_date(get_event_date(ent) ,doc._.note_date)
        except AttributeError:
            #print(doc._.note_id)
            #ent_date = None
            ent_date = "error"
            
        d = dict(
            note_id=doc._.note_id,
            person_id=doc._.person_id,
            visit_id=doc._.visit_occurrence_id,
            care_site=doc._.care_site_name,
            visit_start_date=doc._.visit_start_date,
            # note_date=doc._.note_date,
            note_class = doc._.note_class_source_value,
            note_title=doc._.note_title,
            note_datetime=doc._.note_datetime,
            start=ent.start_char,
            end=ent.end_char,
            label=ent.label_,
            lexical_variant=ent.text,
            ent_date = ent_date,
            ent_section = ent._.section
            negation=ent._.negation,
            hypothesis=ent._.hypothesis,
            family=ent._.family,
        )
        entities.append(d)
        
    return entities

#building note embeddings
def add_title_criteria(note_df, note_points, col, regex) :
    note_df2=note_df.dropna(subset=["note_title"])
    note_df2[col] = True
    return(note_points.merge(note_df2[note_df2.note_title.str.contains(pat=regex, regex= True)][["note_id",col]], on="note_id", how = "left").fillna(False))

def add_class_criteria(note_df, note_points, col, class_list) :
    note_df2=note_df.copy()
    note_df2[col] = True
    return(note_points.merge(note_df2[note_df2.note_class_source_value.isin(class_list)][["note_id",col]], on="note_id", how = "left").fillna(False))

def add_section_criteria(note_nlp, note_points, col, label, section) :
    note_nlp2 = note_nlp[(note_nlp.label==label)]
    note_nlp2 = note_nlp2.dropna(subset = ["ent_section"])
    note_nlp2[col] = True
    return(note_points.merge(note_nlp2[note_nlp2.ent_section==section].drop_duplicates(subset = ["note_id"])[["note_id", col]], on ="note_id", how = "left").fillna(False))

def add_date_criteria(note_nlp, note_points, col, label) :
    note_nlp2 = note_nlp[(note_nlp.label==label)]
    note_nlp2 = note_nlp2.dropna(subset = ["ent_date"])
    note_nlp2[col] = True
    return(note_points.merge(note_nlp2.drop_duplicates(subset = ["note_id"])[["note_id", col]], on ="note_id", how = "left").fillna(False))

def build_note_points(note_df, note_nlp) :
    #initialisation
    note_nlp['value'] = True
    note_points = note_nlp.pivot_table(index='note_id', columns='label', values='value', aggfunc='first', fill_value=False)
    
    note_points = add_title_criteria(note_df, note_points, "hospit_title", "(?i)hospit")
    note_points = add_title_criteria(note_df, note_points, "hemato_title", "(?i)hemato")
    note_points = add_title_criteria(note_df, note_points, "crh_title", "(?i)crh")
    note_points = add_class_criteria(note_df, note_points, "crh_class", [ 'CRH-HOSPI', 'CRH-HEMATO', "CRH-S", "CRH-J"])
    note_points = add_section_criteria(note_nlp,note_points, "hospit_in_motif", "hospitalisation", "motif")
    note_points = add_section_criteria(note_nlp,note_points, "hospit_in_intro", "hospitalisation", "introduction")
    note_points = add_section_criteria(note_nlp,note_points, "hospit_in_ccl", "hospitalisation", "conclusion")
    note_points = add_section_criteria(note_nlp,note_points, "inject_in_j0", "injection", "J0 CART")
    note_points = add_section_criteria(note_nlp,note_points, "inject_in_evol", "injection", "evolution")
    note_points = add_section_criteria(note_nlp,note_points, "cancer_in_hist", "cancer", "histoire de la maladie")
    note_points = add_date_criteria(note_nlp,note_points, "injection_date", "injection")
    
    return(note_points.merge(note_df[["note_id", "visit_occurrence_id"]], on = "note_id", how= "left"))

def build_visit_points(note_points) :
    return(note_points.groupby('visit_occurrence_id').agg(lambda x: any(x)).drop('note_id', axis=1))

def get_embedding(note_df, note_nlp) :
    
    note_df2 = note_df.filter(col('note_id').isin(note_nlp.note_id.unique().tolist())).toPandas()

    note_points = build_note_points(note_df2, note_nlp)

    visit_embeddings = build_visit_points(note_points)
    
    return(visit_embeddings)

def define_nlp(drugs, regex_dict, sections):

    drug_pattern = "|".join(re.escape(drug).replace("\\-", "-") for drug in drugs["drug"])

    nlp = edsnlp.blank("eds")
    nlp.add_pipe("eds.normalizer")
    nlp.add_pipe("eds.sentences")
    nlp.add_pipe("eds.dates")
    nlp.add_pipe("eds.matcher", config=dict(regex=regex_dict,
                                            attr="NORM"))
    nlp.add_pipe("eds.sections", config = dict(sections = sections))
    
    return(nlp)

def nlp_pipe(src, df, nlp) : 
    if src == "pandas" :
        docs_iter = edsnlp.data.from_pandas(data=df, converter="omop", doc_attributes=["note_title","note_date","note_datetime","note_class_source_value",
                                                                                   "care_site_name","visit_occurrence_id","person_id","visit_start_date"])
    if src =="spark" :
        docs_iter = edsnlp.data.from_spark(data=df, converter="omop", doc_attributes=["note_title","note_date","note_datetime","note_class_source_value",
                                                                               "care_site_name","visit_occurrence_id","person_id","visit_start_date"])
    docs = nlp.pipe(docs_iter)
    docs = docs.set_processing(num_cpu_workers=4, show_progress=True)
    note_nlp = docs.to_pandas(converter=get_entities)
    
    return(note_nlp)

