import pandas as pd
from os.path import join

from kiblib.utils.db import DbConn
from kiblib.utils.conf import Config
from kiblib.utils.email_sender import send_email

db_conn = DbConn().create_engine()

query = """
SELECT i.itemnumber
FROM koha_prod.items i
JOIN koha_prod.issues iss ON iss.itemnumber = i.itemnumber
WHERE i.itemlost != 3 AND iss.date_due < CURDATE() - INTERVAL 90 DAY
"""

df = pd.read_sql(query, con=db_conn)
r = len(df)
if r > 0:
    dir_data = Config().get_config_data()
    file_out = join(dir_data, "items_nonRestituesPlus.csv")
    df.to_csv(file_out, header=False, index=False)

    subject = "[Kibini] Exemplaires à passer en non restitué +"
    fromaddr = 'PICHENOT François <fpichenot@ville-roubaix.fr>'
    to = ', '.join(['PICHENOT François <fpichenot@ville-roubaix.fr>'])
    content = f"""\
        Exemplaires à passer en non restitué +.
        Documents avec un retard de plus de 90 jours.
        {r} documents sont concernés.
        """
    send_email(fromaddr, to, subject, content, file=file_out)
