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

def get_set_of_IDs_v2():
    FILENAME = "../map_novelty_pap_alex/references.txt"
    ids = set()
    with open(FILENAME, "r") as f:
        first = True
        for line in f:
            if first:
                first = False
                continue
            scopus_id, alex_id_list = line.split(", ")
            ids.update(alex_id_list.strip().split(";"))
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


def collect_citations(v=None):
    filenames = get_list_of_filenames()

    if v is None:
        wanted_ids = get_set_of_IDs()
    else:
        wanted_ids = get_set_of_IDs()
        wanted_ids.update(get_set_of_IDs_v2())

    print("Number of all papers:", len(wanted_ids))
    
    #FILENAME=r"/opt/backup/OpenAlex/openalex-snapshot/data/works/updated_date=2025-05-17/part_001.gz"

    citations = dict() 
    for filename in tqdm.tqdm(filenames):
        citations = extract_citations(wanted_ids, filename, citations=citations)

        ver = "" if v is None else f"_{v}"    
        citation_file = os.path.join(OUTPUTDIR, f"citations{ver}.json")

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

def collect_references():
    """
    Returns set of references of focused papers. 
    ( * we only need references of the focused papers
      * references of references not needed
      * references of citing papers not needed since
     we have citations of references of focused papers)
    """
    wanted_ids = get_set_of_IDs()

    references = dict()
    filenames = get_list_of_filenames()
    for filename in tqdm.tqdm(filenames):
        with gzip.open(filename, "rt", encoding="utf-8") as f:
            for line in f:
                rec = json.loads(line)
                id = rec["id"]
                if id in wanted_ids:
                    if id not in references:
                        references[id] = rec["referenced_works"]
                    else:
                        print("Duplicate ID:", id)
                        raise ValueError("Duplicate ID")

    with open(os.path.join(OUTPUTDIR, "references.json"), "w") as f:
        json.dump(references, f)
    print("Done.")

def calculate_DI():
    citation_filename = os.path.join(OUTPUTDIR, "citations_2.json")
    references_filename = os.path.join(OUTPUTDIR, "references.json")
    
    print("Loading citations...")
    with open(citation_filename, "r") as f:
        citations = json.load(f)
    print("Done.")
    
    print("Loading references...")
    with open(references_filename, "r") as f:
        references = json.load(f)
    print("Done.")

    
    DI_values = dict()
    # go through references keys,those are the focused papers
    for paper_id in tqdm.tqdm(references.keys()):
        citing_papers = citations.get(paper_id, [])
        referenced_papers = references[paper_id] # all focused papers should be in references
        citing_of_references = set()
        for ref_id in referenced_papers:
            citing_of_references.update(
                citations.get(ref_id, set())
            )
        # P_i = citing p but not its references  
        P_i = set(citing_papers).difference(citing_of_references)
        # P_j = citing p and its references
        P_j = set(citing_papers).intersection(citing_of_references)
        # P_k = citing references of p but not p
        P_k =  citing_of_references.difference(citing_papers)

        n_i = len(P_i)
        n_j = len(P_j)
        n_k = len(P_k)

        
        di = (n_i - n_j)
        if di != 0:
            di /= (n_i + n_j + n_k)
        DI_values[paper_id] = di
        
    print("DI calcuation finished.")
    with open("DI_values.json", "w") as f:
        json.dump(DI_values, f)
    print("DI_values.json saved.")

def create_set_of_all_involved_papers():
    citation_filename = os.path.join(OUTPUTDIR, "citations_2.json")
    references_filename = os.path.join(OUTPUTDIR, "references.json")
    
    print("Loading citations...")
    with open(citation_filename, "r") as f:
        citations = json.load(f)
    print("Done.")
    
    print("Loading references...")
    with open(references_filename, "r") as f:
        references = json.load(f)
    print("Done.")

    all_papers = set()
    all_papers.update(references.keys())


    for paper_id in tqdm.tqdm(references.keys()):
        # add citations 
        citing_papers = citations.get(paper_id, [])
        all_papers.update(citing_papers)
        # add references
        referenced_papers = references[paper_id] 
        all_papers.update(referenced_papers)
        # add citations of references
        citing_of_references = set()
        for ref_id in referenced_papers:
            citing_of_references.update(
                citations.get(ref_id, set())
            )
        all_papers.update(citing_of_references) 

    print("Total number of involved papers:", len(all_papers))
    with open("all_involved_papers.txt", "w") as f:
        for paper_id in all_papers:
            print(paper_id, file=f)
    print("all_involved_papers.txt saved.")
    
def find_year_for_papers():
    ...


if __name__ == "__main__":
    # collect_citations(v=2)
    ## process_citations()
    ## convert_references()  
    #collect_references()
    # calculate_DI()
    #create_set_of_all_involved_papers()