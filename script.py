import pandas as pd
pd.options.mode.chained_assignment = None

data = pd.read_csv("./csv/data.csv", sep=";")
data['Date'] = pd.to_datetime(data['Date'], format = '%d/%m/%Y')
data = data.sort_values(by=['Date'])
data['Date'] = data['Date'].dt.strftime('%d/%m/%Y')


def get_year(str):
    return str[-4:]

def get_month(str):
    return str[-7:]

### Année ###
df_annees = pd.DataFrame(columns=['id_annee', 'annee'])
annees = data['Date'].apply(get_year).unique()
id = 1
for i in range(len(annees)):
    df_annees.loc[i] = [id, annees[i]]
    id+=1
#############

### Mois ###
df_mois = pd.DataFrame(columns=['id_mois', 'mois', 'id_annee'])
mois = data['Date'].apply(get_month).unique()
id = 1
for i in range(len(mois)):
    m = mois[i]
    a = mois[i][-4:]
    id_annee = df_annees.loc[df_annees['annee'] == a].values[0][0]
    df_mois.loc[i] = [id, m, id_annee]
    id+=1
############

### Jour ###
df_jours = pd.DataFrame(columns=['id_jour', 'jour', 'id_mois'])
jours = data['Date'].unique()
id = 1
for i in range(len(jours)):
    jour = jours[i]
    mois = jours[i][3:]
    annee = jours[i][-4:]
    id_mois = df_mois.loc[df_mois['mois'] == mois].values[0][0]
    df_jours.loc[i] = [id, jour, id_mois]
    id+=1
print(df_jours)
############

### Période ###
df_periodes = pd.DataFrame({'id_periode' : [1,2,3,4],
                            'periode' : ['Première Vague', 'Été 2020', 'Deuxième Vague', 'Troisième Vague'],
                            'start' : [data['Date'].iat[0], '11/05/2020', '01/10/2020', '01/01/2021'],
                            'end' : ['10/05/2020', '30/09/2020', '31/12/2020', data['Date'].iat[-1]]},
                            columns = ['id_periode', 'periode', 'start', 'end'])
###############

### Département ###
df_dpt = data[['Code du Département', 'Nom département']].drop_duplicates()
df_dpt.columns = ['codedpt', 'nomdpt']
id_dpt = []
id = 1
for i in df_dpt.index:
    id_dpt.append(id)
    id+=1
df_dpt['id_dpt'] = id_dpt
df_dpt = df_dpt[['id_dpt', 'codedpt', 'nomdpt']]
print(df_dpt)
###################

### Données ###
df_donnees = data[['Date', 'Code du Département', 'Nb Quotidien Admis Hospitalisation', 'Nb actuellement en soins intensifs', 'Nb Quotidien Décès']]
df_donnees.columns = ['id_jour', 'id_dpt', 'nbhosp', 'nbintens', 'nbdeces']
for i in df_donnees.index:
    df_donnees.at[i, 'id_jour'] = df_jours.loc[df_jours['jour'] == df_donnees.at[i, 'id_jour']].values[0][0]
    df_donnees.at[i, 'id_dpt'] = df_dpt.loc[df_dpt['codedpt'] == df_donnees.at[i, 'id_dpt']]['id_dpt']

df_dates = pd.DataFrame(columns=['Date'])
df_dates['Date'] = pd.to_datetime(data['Date'], format = '%d/%m/%Y')
for i in df_dates.index:
    date = df_dates.at[i, 'Date']
    for j in df_periodes.index:
        start = pd.to_datetime(df_periodes.at[j, 'start'], format='%d/%m/%Y')
        end = pd.to_datetime(df_periodes.at[j, 'end'], format='%d/%m/%Y')
        if date >= start and date <= end:
           df_dates.at[i, 'Date'] = df_periodes.at[j, 'id_periode']

df_donnees['id_periode'] = df_dates['Date']
print(df_donnees)
###############

##### SQL Importing #####

from sqlalchemy import create_engine
engine = create_engine('mysql+mysqldb://root:@localhost:3306/sid', echo=False)
df_annees.to_sql(name='annee', con=engine, if_exists='replace', index=False)
df_mois.to_sql(name='mois', con=engine, if_exists='replace', index=False)
df_jours.to_sql(name='jour', con=engine, if_exists='replace', index=False)
df_periodes.to_sql(name='periode', con=engine, if_exists='replace', index=False)
df_dpt.to_sql(name='departement', con=engine, if_exists='replace', index=False)
df_donnees.to_sql(name='donnees', con=engine, if_exists='replace', index=False)

with engine.connect() as con:
    con.execute('ALTER TABLE annee ADD PRIMARY KEY (id_annee);')
    con.execute('ALTER TABLE mois ADD PRIMARY KEY (id_mois);')
    con.execute('ALTER TABLE mois ADD FOREIGN KEY (id_annee) REFERENCES annee(id_annee);')
    con.execute('ALTER TABLE jour ADD PRIMARY KEY (id_jour);')
    con.execute('ALTER TABLE jour ADD FOREIGN KEY (id_mois) REFERENCES mois(id_mois);')
    con.execute('ALTER TABLE periode ADD PRIMARY KEY (id_periode);')
    con.execute('ALTER TABLE departement ADD PRIMARY KEY (id_dpt);')
    con.execute('ALTER TABLE donnees ADD FOREIGN KEY (id_jour) REFERENCES jour(id_jour);')
    con.execute('ALTER TABLE donnees ADD FOREIGN KEY (id_periode) REFERENCES periode(id_periode);')
    con.execute('ALTER TABLE donnees ADD FOREIGN KEY (id_dpt) REFERENCES departement(id_dpt);')

#########################