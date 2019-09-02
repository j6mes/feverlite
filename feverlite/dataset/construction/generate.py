import logging
import argparse
import json
import os
from allennlp.common import file_utils
from tqdm import tqdm

from feverlite import logconfig
from feverlite.dataset.reader.document_database import FEVERDocumentDatabase

logger = logging.getLogger(__name__)

def clean_line_text(text):
    clean_text =  text.split("\t")[1] if len(text.split("\t")) > 1 else ""
    assert len(clean_text.strip()) > 0
    return clean_text

def read_evidence(db, instance):
    evidence_groups = instance["evidence"]
    for evidence_group in evidence_groups:
        yield [clean_line_text(db.get_doc_lines(page)[line_number]) for annotation_id, evidence_id, page, line_number in evidence_group]


def claim_reader(db, in_file_path):
    logger.info("Reading claims from {}".format(in_file_path))
    downloaded_file = file_utils.get_from_cache(in_file_path)

    with open(downloaded_file,"r") as in_file:
        for line in in_file:
            instance = json.loads(line)

            if instance["label"] != "NOT ENOUGH INFO":
                evidence_groups = read_evidence(db, instance)
                instance["evidence_groups"] = evidence_groups
                yield instance


def make_instance(db,in_file_path):
    for instance in claim_reader(db, in_file_path):
        yield from [{"claim": instance["claim"],
                     "label": instance["label"],
                     "evidence": list(instance["evidence_groups"])
                     }]


def process(db, in_file_path, out_file_path):
    logger.info("Processing {} to {}".format(in_file_path, out_file_path))

    if not os.path.exists(os.path.dirname(out_file_path)):
        logger.warning("Path {} does not exist - creating!".format(os.path.dirname(out_file_path)))
        os.makedirs(os.path.dirname(out_file_path),exist_ok=True)

    with open(out_file_path,"w+") as out_file:
        for instance in tqdm(make_instance(db,in_file_path)):
            out_file.write(json.dumps(instance)+"\n")


if __name__ == "__main__":
    logconfig.setup()

    parser = argparse.ArgumentParser()
    parser.add_argument("--db",default="https://s3-eu-west-1.amazonaws.com/fever.public/wiki_index/fever.db")
    parser.add_argument("--train", required=True)
    parser.add_argument("--validation", required=True)
    parser.add_argument("--test", required=True)
    parser.add_argument("--master-train", required=False,
                        default="https://s3-eu-west-1.amazonaws.com/fever.public/train.jsonl")
    parser.add_argument("--master-validation", required=False,
                        default="https://s3-eu-west-1.amazonaws.com/fever.public/paper_dev.jsonl")
    parser.add_argument("--master-test", required=False,
                        default="https://s3-eu-west-1.amazonaws.com/fever.public/paper_test.jsonl")
    args = parser.parse_args()
    logger.info(args)

    db = FEVERDocumentDatabase(args.db)
    process(db, args.master_train, args.train)
    process(db, args.master_validation, args.validation)
    process(db, args.master_test, args.test)

