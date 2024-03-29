import logging
import sqlite3
import unicodedata

from allennlp.common.file_utils import cached_path

logger = logging.getLogger(__name__)
class FEVERDocumentDatabase(object):
    # An implementation inspired by the Facebook DrQA sqlite document database reader

    def __init__(self, path: str):
        logger.info("Opening FEVER database from {}".format(path))
        self._connection = self.connect(cached_path(path))

    @staticmethod
    def connect(path:str):
        try:
            return sqlite3.connect(path)
        except sqlite3.Error as e:
            logging.exception(e)
            logging.critical("Unable to load sqlite database")
            raise e

    def get_doc_lines(self,page_title:str) -> str:
        cursor = self._connection.cursor()
        cursor.execute("SELECT lines FROM documents WHERE id = :page_title",
                       ({
                           "page_title":unicodedata.normalize("NFD",page_title,)
                       }))
        result = cursor.fetchone()
        cursor.close()

        if result is None:
            raise Exception("Document not found")

        return result[0].split("\n")

    def get_doc_ids(self):
        cursor = self._connection.cursor()
        cursor.execute("SELECT id FROM documents")
        results = [result[0] for result in cursor.fetchall()]
        cursor.close()

        return results