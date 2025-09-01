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



if __name__ == "__main__":

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