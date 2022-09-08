import pandas as pd
from os.path import join

from kiblib.utils.db import DbConn
from kiblib.utils.conf import Config
from kiblib.utils.email_sender import send_email
from kiblib.utils.code2libelle import Code2Libelle
from kiblib.document import Document

db_conn = DbConn().create_engine()
c2l = Code2Libelle(db_conn)
c2l.get_val()

query = """
SELECT
    av.lib AS "Espace",
    i.itemcallnumber AS "Cote",
    i.barcode AS "Code-barre",
    b.author AS "Auteur",
    b.title AS "Titre",
    ExtractValue(m.metadata, '//datafield[@tag="200"]/subfield[@code="h"]') AS "Volume",
    i.copynumber AS "Volume perios",
	i.itemlost,
    DATE(i.timestamp) AS "Date derniere modif"
FROM koha_prod.items i
LEFT JOIN koha_prod.biblio b ON i.biblionumber = b.biblionumber
LEFT JOIN koha_prod.biblioitems bi ON i.biblionumber = bi.biblionumber
LEFT JOIN koha_prod.biblio_metadata m ON m.biblionumber = bi.biblionumber
LEFT JOIN koha_prod.authorised_values av ON av.authorised_value = i.location
WHERE i.notforloan = 4
AND i.itemlost != 0
AND DATE(i.timestamp) >= CURDATE() - INTERVAL 6 MONTH
ORDER BY i.location, i.itemcallnumber
LIMIT 100
"""

df = pd.read_sql(query, con=db_conn)
document = Document(df=df, con=db_conn, c2l=c2l.dict_codes_lib)
print(len(document.df))
