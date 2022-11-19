import pandas as pd
from os.path import join

from kiblib.utils.db import DbConn
from kiblib.utils.conf import Config
from kiblib.utils.email_sender import send_email

db_conn = DbConn().create_engine()

query = """
SELECT b.cardnumber, i.barcode
FROM koha_prod.items i
JOIN koha_prod.old_issues iss ON iss.itemnumber = i.itemnumber
JOIN koha_prod.borrowers b ON iss.borrowernumber = b.borrowernumber
WHERE i.itemlost = 3 AND DATE(iss.returndate) = CURDATE() - INTERVAL 1 DAY
"""

df = pd.read_sql(query, con=db_conn)
r = len(df)
if r > 0:
    dir_data = Config().get_config_data()
    file_out = join(dir_data, "items_nonRestituesPlus_retours.xlsx")
    df.to_excel(file_out, header=True, index=False)

    subject = "[Kibini] Exemplaires non restitués + rendus la veille"
    fromaddr = 'PICHENOT François <fpichenot@ville-roubaix.fr>'
    to = ', '.join(['PICHENOT François <fpichenot@ville-roubaix.fr>'])
    content = f"""\
        Exemplaires non restitués + rendus la veille
        {r} documents sont concernés.
        """
    send_email(fromaddr, to, subject, content, file=file_out)
