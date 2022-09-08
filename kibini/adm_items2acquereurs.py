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
    i.itemnumber,
    i.barcode,
    i.dateaccessioned,
    i.price,
    i.homebranch,
    i.holdingbranch,
    i.location,
    i.ccode,
    i.itemcallnumber,
    i.notforloan,
    i.damaged,
    i.withdrawn,
    i.withdrawn_on,
    i.itemlost,
    i.itemlost_on,
    i.onloan,
    i.datelastborrowed,
    i.biblionumber,
    b.title as titre,
    b.author,
    ExtractValue(m.metadata, '//datafield[@tag="200"]/subfield[@code="h"]') AS "volume",
    i.copynumber AS "volume_perio",
    bi.publicationyear,
    bi.itemtype,
    DATE(i.timestamp)
FROM koha_prod.items i
JOIN koha_prod.biblioitems bi ON i.biblionumber = bi.biblionumber
JOIN koha_prod.biblio b ON i.biblionumber = b.biblionumber
JOIN koha_prod.biblio_metadata m ON m.biblionumber = bi.biblionumber
WHERE i.notforloan = 4
AND i.itemlost != 0
AND DATE(i.timestamp) >= CURDATE() - INTERVAL 6 MONTH
ORDER BY i.location, i.itemcallnumber
"""

df0 = pd.read_sql(query, con=db_conn)
document = Document(df=df0, con=db_conn, c2l=c2l.dict_codes_lib)
document.get_doc_statdb_data()
document.get_doc_es_data()
document.get_doc_list_data()
df = document.doc_list_data

query = """
SELECT
    biblionumber, "oui" as "réservation"
FROM koha_prod.reserves
WHERE waitingdate IS NULL
"""
resa = pd.read_sql(query, con=db_conn)
df = df.merge(resa, how='left', left_on='doc_biblio_id', right_on='biblionumber')
df = df.drop(columns=['biblionumber'])

r = len(df)
if r > 0:
    dir_data = Config().get_config_data()
    file_out = join(dir_data, "sortisCollectionsPerdus.xlsx")
    df.to_excel(file_out, header=True, index=False)

    subject = "[Kibini] TEST TEST Documents sortis des collections (perdus, non restitués)"
    fromaddr = 'PICHENOT François <fpichenot@ville-roubaix.fr>'
    to = ', '.join(['PICHENOT François <fpichenot@ville-roubaix.fr>'])
    #acquereurs = Config().get_config_acquereurs()
    #courriels = [f"{acquereur['courriel']}@ville-roubaix.fr" for acquereur in acquereurs]
    #to = ', '.join(courriels)

    content = f"""\
        TEST TEST Documents sortis des collections au cours des 30 derniers jours car perdus ou non restitués.
        {r} documents concernés.
    """
    send_email(fromaddr, to, subject, content, file_out)
