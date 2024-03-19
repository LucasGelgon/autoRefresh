#!/usr/bin/python3
# -*- coding: utf-8 -*- 

import os                               # import for operating system commands
import sys                              # import system commands
import json                             # import for json functions
import argparse                         # use argparse to parse arguments
import time                             # import time functions
import logging                          # standard library for logging   
import pathlib             

from google.oauth2 import service_account
from google.cloud import bigquery

from deepdiff import DeepDiff
from datetime import datetime

######################################################
# Parametrage du parser d'arguments
###################################################### 

curworkdir = os.getcwd()

parser=argparse.ArgumentParser()
parser.add_argument('--log', help='Log level', choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], default="INFO")
parser.add_argument('--dataset', help='Dataset Name', required=True)
parser.add_argument('--credpath', help='Path du credential GCP de production (Production sa-replication.json)', required=True)
parser.add_argument('--repertoire_config', help='Nom du répertoire de configuration', required=True)
parser.add_argument('--fichier_config', help='Nom du fichier de configuration', required=True)
parser.add_argument('--exceptions', nargs='+', help='Liste des tables à ignorer')
parser.add_argument('--logFile', help='Path du fichier de log', default=curworkdir+"/logs/"+os.path.splitext(os.path.basename(__file__))[0]+time.strftime("_%Y%m%d_%H%M%S")+".log")

args = parser.parse_args()

######################################################
# Parametrage du logging
######################################################

# création du répertoire de logs si non existant
curlogdir = f"{curworkdir}/logs"
if not os.path.exists(curlogdir):
    os.makedirs(curlogdir)

# create logger
logger = logging.getLogger(os.path.basename(__file__))
logger.setLevel(args.log)

# create formatter
formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)-5s %(message)s')

# create console handler with a higher log level
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(formatter)
logger.addHandler(ch)

# create file handler which logs even debug messages
fh = logging.FileHandler(args.logFile)
fh.setFormatter(formatter)
logger.addHandler(fh)


######################################################
# Fonctions
######################################################

######################################################
# get_tables_list : lister pour un datset la liste de ses tables
# In  : Bigquery client 
#     : Dataset spécifié
# Out : tables list
def get_tables_list(client, dataset_name):
    tables_list = []
    dataset = client.get_dataset(dataset_name)
    # Liste des tables dans le dataset spécifié
    tables = client.list_tables(dataset)
    for table in tables:
        tables_list.append(table)
    return tables_list

######################################################
# save_file : enregistrer la liste des tables avec
# les informations complémentaires dans le 
# fichier de configuration
# In  : Liste de Tables
#       fichier de configuration
def save_file(tables_info, file):
    for table_info in tables_info:
        file.write(str(table_info) + "\n")
    file.close()
    
######################################################
# get_partition : vérifie si la table est partitionnée
# si c'est le cas, renvoie la clé de partition et son 
# type. Sinon, renvoie False
# In  : Table
# Out : infos de partition (clé de partition et type)
def get_partition(table):
    partition_info = {}
    if table.partitioning_type:
        partition_info['partitioned'] = True
        partition_info['partition_key'] = table.time_partitioning.field
        partition_info['partition_type'] = table.time_partitioning.type_
    else:
        partition_info['partitioned'] = False

    return partition_info

######################################################
# ignore_tables : vérifie si une table doit être 
# ignorée selon les règles d'exception
# In  : Table name
#       Liste d'exceptions
# Out : True si la table doit être ignorée, False sinon
def ignore_tables(table_name, exceptions):
    if exceptions is not None :
        for exception in exceptions :
                if exception in table_name :
                    return True
    return False

######################################################
# lecture credential json pour extraire le projet GCP et le mail du compte de service
# In  : json credential auth GCP
# Out : GCP project ID
#       Service account email
#       GCP Credential 
def get_credential( json_cred_file):
    # lecture du json
    with open(json_cred_file) as cred_file:
      data = json.load(cred_file)
    # récupération du project_id
    project_id = data['project_id']
    # récupération du mail du compte de service
    sa_account = data['client_email']
    # authentif GCP
    gcp_cred = service_account.Credentials.from_service_account_file( json_cred_file)
    logger.debug(f"  GCP Project ID = {project_id}")
    logger.debug(f"  GCP Service Account = {sa_account}")
    
    return project_id, sa_account, gcp_cred




################################################################################################################
# main
################################################################################################################
if __name__ == "__main__":

    # Récupération du projet GCP
    # Authentification GCP avec credential json 
    logger.info(f"Lecture GCP credential")
    try : 
        gcp_project_id, gcp_sa_account, gcp_credentials = get_credential(args.credpath)
    except :
        logger.error("Fichier credential inexistant : "+args.credpath)
        exit()

    # Ajout de périodes au fichier credential

    client = bigquery.Client(credentials = gcp_credentials, project = gcp_project_id)

    # Récupération de la liste des tables
    logger.info("Récupération des tables")
    try :
        tables_list = get_tables_list(client, args.dataset)
    except :
        print("Dataset non présent dans le projet")
        exit()

    # Liste pour stocker les informations des tables et de partitionnement
    tables_info = []

    # Liste des tables à ignorer
    exceptions = args.exceptions 
    
    # Obtention des informations sur le partitionnement pour chaque table
    logger.info(f"Obtention des informations sur le partitionnement pour chaque table")
    for table in tables_list:
        partition_info = get_partition(table)
        table_name = table.table_id
        # Vérifier si la table doit être ignorée
        if not ignore_tables(table_name, exceptions):
            if  table.partitioning_type: 
                if table.time_partitioning.type_ == 'DAY': 
                    partition_info['date_debut'] = "debut_DAY"
                    partition_info['date_fin'] = "fin_DAY"
                elif table.time_partitioning.type_ == 'NUMBER':
                    partition_info['date_debut'] = "debut_NUM"
                    partition_info['date_fin'] = "fin_NUM"
            tables_info.append({
                'table_id': table_name,
                'dataset_id': table.dataset_id,
                'partition_info': partition_info
            })


    # Enregistrement de la liste dans le fichier configuration
    # Ouvre le fichier si il existe, le crée sinon
    if not os.path.exists(f'{args.repertoire_config}/{args.fichier_config}'): 
        try :
            logger.info(f"Création du fichier configuration")
            file =  open(f'{args.repertoire_config}/{args.fichier_config}', 'w')
        except :
            logger.info(f"Création du répertoire configuration")
            os.mkdir(f'{args.repertoire_config}')
            logger.info(f"Création du fichier configuration")
            file =  open(f'{args.repertoire_config}/{args.fichier_config}', 'w')
        file.write('---HEADER--- \n')
        file.write('debut_DAY=\n')
        file.write('fin_DAY=\n')
        file.write('debut_NUM=\n')
        file.write('fin_NUM=\n')
        file.write('-----------\n')
        save_file(tables_info, file)

    # Création du nom de fichier avec horodatage
    date = time.strftime('%Y_%m_%d_%H_%M_%S')
    fichier_prod = f"{date}.txt"
    # Enregistrement des informations du fichier de production avec le nouveau nom
    with open(f'{args.repertoire_config}/{fichier_prod}', 'w') as file:
        file.write('---HEADER--- \n')
        file.write('debut_DAY=\n')
        file.write('fin_DAY=\n')
        file.write('debut_NUM=\n')
        file.write('fin_NUM=\n')
        file.write('-----------\n')
        save_file(tables_info, file)


    # Compare le fichier de production et le fichier configuration actuel afin de corriger les éventuelles erreurs
    logger.info(f"Comparaison des fichiers de production et de configuration")

    # Chargement du contenu du fichier de production
    with open(f'{args.repertoire_config}/{fichier_prod}', 'r') as file:
        prod_content = file.read()

    # Chargement du contenu du fichier de configuration
    with open(f'{args.repertoire_config}/{args.fichier_config}', 'r') as file:
        conf_content = file.read()

    # Utilisation de DeepDiff pour comparer les contenus des fichiers
    diff = DeepDiff(prod_content, conf_content)

   # Vérification s'il y a des différences entre les fichiers
    if diff:
        logger.info("Mise à jour nécessaire du fichier de configuration")
        # Charger les nouvelles données du fichier de production
        new_data = prod_content.split('\n')
        with open(f'{args.repertoire_config}/{args.fichier_config}', 'r') as file:
            conf_lines = file.readlines()
        # Écrire uniquement les nouvelles données dans le fichier de configuration
            i = 0
            for data in new_data:
                # Passe le header
                i = i+1
                if i<=6 :
                    continue
                else : 
                    if data not in conf_content:
                        file.write(data + '\n') 

    else:
        logger.info("Aucune mise à jour nécessaire du fichier de configuration")