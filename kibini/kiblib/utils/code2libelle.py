import pandas as pd
from kiblib.utils.db import DbConn


class Code2Libelle():
    def __init__(self, db_conn):
        self.dict_codes_lib = {}
        self.db_conn = db_conn

    def get_val(self):
        self.get_val_age()
        self.get_val_age_code()
        self.get_val_ccode()
        self.get_val_iris()
        self.get_val_carte()
        self.get_val_site()
        self.get_val_attribut()
        self.get_val_localisation()
        self.get_val_doc_statut()
        self.get_val_doc_statut_abime()
        self.get_val_doc_statut_desherbe()
        self.get_val_doc_statut_perdu()
        self.get_val_doc_support()

    def get_val_age(self):
        query = """
            SELECT CONCAT("a", age) as age, acode, trinsee, trmeda, trmedb
            FROM statdb.lib_age2
        """
        df = pd.read_sql(query, con=self.db_conn)
        df = df.set_index('age')
        self.dict_codes_lib['age'] = df.to_dict('index')

    def get_val_age_code(self):
        query = """
            SELECT acode, trinsee, trmeda, trmedb
            FROM statdb.lib_age2
            GROUP BY acode
        """
        df = pd.read_sql(query, con=self.db_conn)
        df = df.set_index('acode')
        self.dict_codes_lib['adh_age_code'] = df.to_dict('index')

    def get_val_ccode(self):
        query = """
            SELECT ccode, lib1, lib2, lib3, lib4, lib
            FROM statdb.lib_collections2
        """
        df = pd.read_sql(query, con=self.db_conn)
        df = df.set_index('ccode')
        self.dict_codes_lib['ccode'] = df.to_dict('index')

    def get_val_iris(self):
        query = """
            SELECT irisInsee, irisNom, quartier, secteur
            FROM statdb.iris_lib
        """
        df = pd.read_sql(query, con=self.db_conn)
        df = df.set_index('irisInsee')
        self.dict_codes_lib['iris'] = df.to_dict('index')

    def get_val_carte(self):
        query = """
            SELECT categorycode, carte, carte_type, category_type, carte_gratuite, carte_prix, carte_personnalite
            FROM statdb.lib_categories
        """
        df = pd.read_sql(query, con=self.db_conn)
        df = df.set_index('categorycode')
        self.dict_codes_lib['carte'] = df.to_dict('index')

    def get_val_site(self):
        self.dict_codes_lib['site'] = {
            'MED': 'M??diath??que',
            'BUS': 'Z??bre',
            'MUS': 'Mus??e Diligent'}

    def get_val_attribut(self):
        self.dict_codes_lib['attributs'] = {
            "AM01": "Action ??ducative",
            "AM02": "Ap??ro culture",
            "AM03": "Eveil au livre",
            "AM04": "M??diation",
            "AM05": "Espace multim??dia",
            "AM06": "Nouveaux habitants",
            "AM07": "Personnel Ville de Roubaix",
            "AM08": "Personnel \"La Redoute\"",
            "AM09": "Visite de classe",
            "B00": "Arr??ts foyers logements",
            "B01": "Arr??t Bus Rue Louis Braille",
            "B02": "Arr??t Bus Rue de Lannoy",
            "B03": "Arr??t Bus Place du Travail",
            "B04": "Arr??t Bus Rue du Danemark",
            "B05": "Arr??t Bus Place du Progr??s",
            "B06": "Arr??t Bus Rue du Stand de tir",
            "B07": "Arr??t Bus Place Carnot",
            "B08": "Arr??t Bus Rue de France",
            "B09": "Arr??t Bus Rue de Rome",
            "B10": "Arr??t Bus Rue L??on Blum",
            "B11": "Arr??t Bus Place la de la Nation",
            "B12": "Arr??t Bus Rue de Philippeville",
            "B13": "Arr??t Bus Rue de la Fraternit??",
            "B14": "Arr??t Bus Rue Jacques Pr??vert",
            "B15": "Arr??t Bus Rue Jean-Baptiste Vercout??re",
            "B16": "Arr??t Bus Avenue du Pr??sident Coty",
            "B17": "Arr??t Bus Rue Montgolfier",
            "B18": "Arr??t Bus Place Roussel",
            "B19": "Arr??t Bus Boulevard de Fourmies",
            "B20": "Arr??t Bus Rue d'Alger",
            "B21": "Arr??t Bus Rue L??o Lagrange",
            "B22": "Arr??t Bus Rue de Beaumont",
            "B23": "Arr??t Bus Rue d'Oran",
            "B24": "Arr??t Bus Rue Drouot",
            "B25": "Arr??t Bus Avenue Julien Lagache",
            "COL01": "Maternelle",
            "COL02": "El??mentaire",
            "COL03": "Structure petite enfance",
            "COL04": "Centre social",
            "COL05": "Accueil sp??cialis??",
            "COL06": "ALSH",
            "COL07": "P??riscolaire",
            "COL08": "secondaire",
            "PCS01": "Agriculteurs exploitants",
            "PCS02": "Artisans, commer??ants et chefs d'entreprise",
            "PCS03": "Cadres et professions intellectuelles sup??rieures",
            "PCS04": "Professions Interm??diaires",
            "PCS05": "Employ??s",
            "PCS06": "Ouvriers",
            "PCS07": "Retrait??s",
            "PCS08": "Lyc??ens",
            "PCS09": "Etudiants",
            "PCS10": "Autres personnes sans activit?? professionnelle"
        }

    def get_val_localisation(self):
        query = """
            SELECT authorised_value, lib
            FROM koha_prod.authorised_values
            WHERE category = 'LOC'
        """
        df = pd.read_sql(query, con=self.db_conn)
        df = df.set_index('authorised_value')
        self.dict_codes_lib['localisation'] = df.to_dict('index')

    def get_val_doc_statut(self):
        query = """
            SELECT authorised_value, lib
            FROM koha_prod.authorised_values
            WHERE category = 'ETAT'
        """
        df = pd.read_sql(query, con=self.db_conn)
        df = df.set_index('authorised_value')
        self.dict_codes_lib['doc_statut'] = df.to_dict('index')

    def get_val_doc_statut_abime(self):
        query = """
            SELECT authorised_value, lib
            FROM koha_prod.authorised_values
            WHERE category = 'DAMAGED'
        """
        df = pd.read_sql(query, con=self.db_conn)
        df = df.set_index('authorised_value')
        self.dict_codes_lib['doc_statut_abime'] = df.to_dict('index')

    def get_val_doc_statut_desherbe(self):
        query = """
            SELECT authorised_value, lib
            FROM koha_prod.authorised_values
            WHERE category = 'RETIRECOLL'
        """
        df = pd.read_sql(query, con=self.db_conn)
        df = df.set_index('authorised_value')
        self.dict_codes_lib['doc_statut_desherbe'] = df.to_dict('index')

    def get_val_doc_statut_perdu(self):
        query = """
            SELECT authorised_value, lib
            FROM koha_prod.authorised_values
            WHERE category = 'LOST'
        """
        df = pd.read_sql(query, con=self.db_conn)
        df = df.set_index('authorised_value')
        self.dict_codes_lib['doc_statut_perdu'] = df.to_dict('index')

    def get_val_doc_support(self):
        query = """
            SELECT authorised_value, lib
            FROM koha_prod.authorised_values
            WHERE category = 'TYPEDOC'
        """
        df = pd.read_sql(query, con=self.db_conn)
        df = df.set_index('authorised_value')
        self.dict_codes_lib['doc_biblio_support'] = df.to_dict('index')
