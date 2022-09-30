import datetime
import re

from kiblib.utils.email_sender import send_email

today = datetime.datetime.today()
year_today = str(today.year)
month_today = str(today.month)

file = "/home/kibini/kibini_prod/data/dumps_koha/vendangeur/to_dedupl.txt"

arks_to_dedupl = []

with open(file, 'r') as f:
    logs = f.readlines()

for log in logs:
    year = log[30:34]
    if year == year_today:
        month = log[35:37]
        month = month.lstrip("0")
        if month == month_today:
            arks = re.findall(r'(/ark:/12148/cb\w*)\.', log)
            if len(arks) > 0:
                ark = 'https://catalogue.bnf.fr' + arks[0]
                arks_to_dedupl.append(ark)
                
r = len(arks_to_dedupl)

if r > 0:
    subject = "[Kibini] Vendangeur : autorités à dédoublonner"
    fromaddr = 'PICHENOT François <fpichenot@ville-roubaix.fr>'
    to = ', '.join(['PICHENOT François <fpichenot@ville-roubaix.fr>'])
    content = f"""
        Autorités à dédoublonner.
        {r} autorités sont concernées.
        
        {'n'.join(arks_to_dedupl)}
        """
    send_email(fromaddr, to, subject, content)