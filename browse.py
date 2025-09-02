import os
import shutil
import tqdm 
import gzip
import json

import pandas as pd 

BASEDIR = "/opt/backup/OpenAlex/openalex-snapshot/data/works"
OUTPUTDIR ="/opt/backup/OpenAlex/"


def get_list_of_filenames():
    """ Return a list of all filenames in the BASEDIR. """
    filenames = []
    for root, _, files in os.walk(BASEDIR):
        for filename in files:
            if filename.endswith(".gz"):
                filenames.append(os.path.join(root, filename))
    return filenames


def get_set_of_IDs():
    """ return set of relevant IDs """
    FILENAME = "../map_novelty_pap_alex/scopus_id_to_alex.txt"
    ids = set()
    with open(FILENAME, "r") as f:
        first = True 
        # first line is a header, use lines[1:]
        for line in f:
            if first:
                first = False 
                continue
            references = line.split(", ")[1].strip()
            ids.update(references.split(";"))
    return ids


def extract_citations(wanted_ids, filename, citations=None):
    """ For each ID from wanted_ids find set of citations. Returns dictionary. """

    if citations is None:
        citations = dict()

    with gzip.open(filename, "rt", encoding="utf-8") as f:
        for line in f:
            rec = json.loads(line)
            id = rec["id"]
            references = rec["referenced_works"]

            cited_ids = wanted_ids.intersection(set(references))

            if cited_ids:
                for cited_id in cited_ids:
                    if cited_id not in citations:
                        citations[cited_id] = []
                    citations[cited_id].append(id)
                    #print("Added citation from:", id, "to:", cited_id)

        return citations 


def collect_citations():
    filenames = get_list_of_filenames()

    wanted_ids = get_set_of_IDs()
    print("Number of all papers:", len(wanted_ids))

    #FILENAME=r"/opt/backup/OpenAlex/openalex-snapshot/data/works/updated_date=2025-05-17/part_001.gz"

    citations = dict() 
    for filename in tqdm.tqdm(filenames):
        citations = extract_citations(wanted_ids, filename, citations=citations)

        citation_file = os.path.join(OUTPUTDIR, "citations.json")

        if os.path.exists(citation_file):
            shutil.move(citation_file, citation_file + ".bak")
        with open(citation_file, "w") as f:
            json.dump(citations, f)

        with open(os.path.join(OUTPUTDIR, "processed_files.txt"), "a") as f:
            print(filename, file=f)


def process_citations():
    citation_file = os.path.join(OUTPUTDIR, "citations.json")
    with open(citation_file, "r") as f:
        citations = json.load(f)

    scopus2alex_filename = "../map_novelty_pap_alex/scopus_id_to_alex.txt"
    scopus2alex = dict()
    with open(scopus2alex_filename, "r") as f:
        for line in f:
            scopus_id, alex_id_list = line.strip().split(", ")
            scopus2alex[scopus_id] = alex_id_list.split(";")

    all_citations = dict()
    for key in scopus2alex.keys():
        all_citations[key] = set()
        for alex_id in scopus2alex[key]:
            if alex_id in citations:
                all_citations[key].update(citations[alex_id])
        all_citations[key] = list(all_citations[key])

    with open(os.path.join(OUTPUTDIR, "scopus_to_alex_citations.json"), "w") as f:
        json.dump(all_citations, f)
    print("Done.")

def convert_references():
    references_filename = "../map_novelty_pap_alex/references.txt"
    references = dict()
    with open(references_filename, "r") as f:
        first = True
        for line in f:
            if first:
                first = False
                continue
            scopus_id, alex_id_list = line.split(", ")
            references[scopus_id] = alex_id_list.strip().split(";")
    with open(os.path.join(OUTPUTDIR, "scopus_to_alex_references.json"), "w") as f:
        json.dump(references, f)
    print("Done.")

def calculate_DI():
    references_filename = os.path.join(OUTPUTDIR,"scopus_to_alex_references.json")
    citations_filename = os.path.join(OUTPUTDIR,"scopus_to_alex_citations.json")

    with open(references_filename, "r") as f:
        references = json.load(f)

    with open(citations_filename, "r") as f:
        citations = json.load(f)

    ids = list(references.keys())

    id2DI = dict()
    for scopus_id in ids:
        refs = references[scopus_id]
        cits = citations[scopus_id] 

if __name__ == "__main__":
    collect_citations()
    # process_citations()
    # convert_references()  
    # calculate_DI()