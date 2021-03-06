import pandas as pd
import numpy as np
from datetime import datetime
pd.options.mode.chained_assignment = None

data = pd.read_csv("./csv/data.csv", sep=";")
data = data[data['Sexe']=='Tous']
data['Date'] = pd.to_datetime(data['Date'], format = '%d/%m/%Y')
data = data.sort_values(by=['Date'])
data['Date'] = data['Date'].dt.strftime('%d/%m/%Y')

datafr = pd.read_csv("./csv/datafr.csv", sep=";")
datafr = datafr[['jour', 'incid_hosp', 'incid_rea', 'incid_dc']]
datafr = datafr.groupby(['jour']).sum().reset_index()
datafr['jour'] = pd.to_datetime(datafr['jour'], format = '%Y-%m-%d')
datafr = datafr.sort_values(by=['jour'])
datafr['jour'] = datafr['jour'].dt.strftime('%d/%m/%Y')
data_ariege = data[data['Code du Département'] == 9].join(datafr.set_index('jour'), on='Date')
data['incid_hosp'] = np.nan
data['incid_rea'] = np.nan
data['incid_dc'] = np.nan
data[data['Code du Département'] == 9] = data_ariege

print(data)


def get_year(str):
    return str[-4:]

def get_month(str):
    return str[-7:]

### Période ###
df_periodes = pd.DataFrame({'id_periode' : [1,2,3,4],
                            'periode' : ['Première Vague', 'Été 2020', 'Deuxième Vague', 'Troisième Vague']},
                            columns = ['id_periode', 'periode'])
###############

### Mois ###
df_mois = pd.DataFrame(columns=['id_mois', 'mois', 'id_periode'])
mois = data['Date'].apply(get_month).unique()
id = 1
for i in range(len(mois)):
    m = mois[i]
    id_periode = 0
    d = datetime.strptime(m, '%m/%Y')
    if d > datetime.strptime('01/2021', '%m/%Y'):
        id_periode = 4
    elif d > datetime.strptime('10/2020', '%m/%Y'):
        id_periode = 3
    elif d > datetime.strptime('11/05/2020', '%d/%m/%Y'):
        id_periode = 2
    else:
        id_periode = 1
    df_mois.loc[i] = [id, m, id_periode]
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
############

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

df_dpt['pop'] = [171000, # Lot
                189000, # gers
                261500, # Tarn-Et-Garonne
                152000, # Ariège
                745000, # Gard
                277900, # Aveyron
                1390000, # Haute-Garonne
                387600, # Tarn
                225000, # Hautes-Pyrénées
                481691, # Pyrénées-Orientales
                368000, # Aude
                1165000, # Hérault
                75700] # Lozère
###################

### Données ###
df_donnees = data[['Date', 'Code du Département', 'Nb Quotidien Admis Hospitalisation', 'Nb actuellement en soins intensifs', 'Nb Quotidien Décès', 'incid_hosp', 'incid_dc']]
df_donnees.columns = ['id_jour', 'id_dpt', 'nbhosp', 'nbintens', 'nbdeces', 'nbhospfr', 'nbdecesfr']
for i in df_donnees.index:
    df_donnees.at[i, 'id_jour'] = df_jours.loc[df_jours['jour'] == df_donnees.at[i, 'id_jour']].values[0][0]
    df_donnees.at[i, 'id_dpt'] = df_dpt.loc[df_dpt['codedpt'] == df_donnees.at[i, 'id_dpt']]['id_dpt']

df_donnees['reacapacite'] = np.nan
# Lot
df_donnees['reacapacite'].loc[df_donnees['id_dpt'] == 1] = 88
# Gers
df_donnees['reacapacite'].loc[df_donnees['id_dpt'] == 2] = 13
# Tarn-et-Garonne
df_donnees['reacapacite'].loc[df_donnees['id_dpt'] == 3] = 100 #122
# Ariège
df_donnees['reacapacite'].loc[df_donnees['id_dpt'] == 4] = 100 #175
# Gard
df_donnees['reacapacite'].loc[df_donnees['id_dpt'] == 5] = 100 #131
# Aveyron
df_donnees['reacapacite'].loc[df_donnees['id_dpt'] == 6] = 91
# Haute-Garonne
df_donnees['reacapacite'].loc[df_donnees['id_dpt'] == 7] = 64
# Tarn
df_donnees['reacapacite'].loc[df_donnees['id_dpt'] == 8] = 100 #121
# Hautes-Pyrénées
df_donnees['reacapacite'].loc[df_donnees['id_dpt'] == 9] = 100 #108
# Pyrénées-Orientales
df_donnees['reacapacite'].loc[df_donnees['id_dpt'] == 10] = 100 #106
# Aude
df_donnees['reacapacite'].loc[df_donnees['id_dpt'] == 11] = 100 #106
# Hérault
df_donnees['reacapacite'].loc[df_donnees['id_dpt'] == 12] = 77
# Lozère
df_donnees['reacapacite'].loc[df_donnees['id_dpt'] == 13] = 100
###############

##### SQL Importing #####

from sqlalchemy import create_engine
engine = create_engine('mysql+mysqldb://root:@localhost:3306/sid', echo=False)
df_mois.to_sql(name='mois', con=engine, if_exists='replace', index=False)
df_jours.to_sql(name='jour', con=engine, if_exists='replace', index=False)
df_periodes.to_sql(name='periode', con=engine, if_exists='replace', index=False)
df_dpt.to_sql(name='departement', con=engine, if_exists='replace', index=False)
df_donnees.to_sql(name='donnees', con=engine, if_exists='replace', index=False)

with engine.connect() as con:
    con.execute('ALTER TABLE periode ADD PRIMARY KEY (id_periode);')
    con.execute('ALTER TABLE mois ADD PRIMARY KEY (id_mois);')
    con.execute('ALTER TABLE mois ADD FOREIGN KEY (id_periode) REFERENCES periode(id_periode);')
    con.execute('ALTER TABLE jour ADD PRIMARY KEY (id_jour);')
    con.execute('ALTER TABLE jour ADD FOREIGN KEY (id_mois) REFERENCES mois(id_mois);')
    con.execute('ALTER TABLE departement ADD PRIMARY KEY (id_dpt);')
    con.execute('ALTER TABLE donnees ADD FOREIGN KEY (id_jour) REFERENCES jour(id_jour);')
    con.execute('ALTER TABLE donnees ADD FOREIGN KEY (id_dpt) REFERENCES departement(id_dpt);')

#########################