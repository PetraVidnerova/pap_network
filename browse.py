import tqdm 
import gzip
import json

import pandas as pd 

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
                    print("Added citation from:", id, "to:", cited_id)

            else:
                print("No cited IDs found for:", id)
    return citations 



if __name__ == "__main__":

    wanted_ids = get_set_of_IDs()
    print("Number of all papers:", len(wanted_ids))


    FILENAME=r"/opt/backup/OpenAlex/openalex-snapshot/data/works/updated_date=2025-05-17/part_001.gz"
    filenames = [FILENAME]

    citations = dict() 
    for filename in filenames:
        citations = extract_citations(wanted_ids, filename, citations=citations)
    
    with open("citations.json", "w") as f:
        json.dump(citations, f)