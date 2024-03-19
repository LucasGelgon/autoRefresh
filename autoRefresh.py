#!/usr/bin/python3
# -*- coding: utf-8 -*- 

import os                               # import for operating system commands
import sys                              # import system commands
import json                             # import for json functions
import argparse                         # use argparse to parse arguments
import time                             # import time functions
import logging                          # standard library for logging   
import pathlib             

######################################################
# Parametrage du parser d'arguments
###################################################### 

curworkdir = os.getcwd()

parser=argparse.ArgumentParser()
parser.add_argument('--log', help='Log level', choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], default="INFO")
parser.add_argument('--project', help='Project Name', required=True)
parser.add_argument('--subenv', help='Sous-environnement', required=True)
parser.add_argument('--target_env', help='Environnement cible', required=True)
parser.add_argument('--repertoire_bash', help='Repertoire du fichier Bash', required=True)
parser.add_argument('--emplacement_config', help='Chemin du fichier de configuration', required=True)
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
# extract_partition_info : retourne les informations de partitionnement
# In  : parition_info 
# Out : informations de partition. Si la table est partitionnée : clé, type, date_debut, date_fin. Si elle ne l'est pas : quatre variables vides
def extract_partition_info(partition_info):
    if partition_info['partitioned']:
        partition_key = partition_info['partition_key']
        partition_type = partition_info['partition_type']
        partition_start = partition_info.get('date_debut', '')
        partition_end = partition_info.get('date_fin', '')
        return partition_key, partition_type, partition_start, partition_end
    else:
        return '', '', '', ''
    
######################################################    
# extract_header : extrait les valeurs du header
# In  : fichier de configuration
# Out : header values, dictionnaire contenant les informations du header
#       debut, date de debut afin de la renseigner dans le titre
#       fin, date de fin afin de la renseigner dans le titre
def extract_header(config_file):
    header_values = {}
    # Lecture du fichier
    logger.info(f"Lecture du fichier de configuration")
    lines = config_file.readlines()
    # Extraction du header
    logger.info(f"Extraction des valeurs du header")
    for line in lines:
        if line.strip() == '-----------':
            break
        elif line.startswith('debut_DAY='):
            header_values['debut_DAY'] = line.strip().split('=')[1]
        elif line.startswith('fin_DAY='):
            header_values['fin_DAY'] = line.strip().split('=')[1]
        elif line.startswith('debut_NUM='):
            header_values['debut_NUM'] = line.strip().split('=')[1]
            debut = line.strip().split('=')[1]
        elif line.startswith('fin_NUM='):
            header_values['fin_NUM'] = line.strip().split('=')[1]
            fin = line.strip().split('=')[1]
    return header_values, debut, fin

################################################################################################################
# main
################################################################################################################
if __name__ == "__main__":

    # Lire le fichier de configuration et extraire les valeurs du header
    header_values = {}
    if os.path.exists(f'{args.emplacement_config}'): 
        with open(args.emplacement_config, 'r') as config_file:
            header_values, debut, fin = extract_header(config_file)
    else :
        logger.error("Fichier de configuration non existant : "+args.emplacement_config)
        exit()

    with open(args.emplacement_config, 'r') as config_file:
        lines = config_file.readlines()

    # Récupère l'identifiant du dataset
    i= 0
    for line in lines:
        i = i+1
        # Ignore les lignes du header
        if i<=6 :
            continue
        if i > 7 :
            break
        else :
            if line.strip(): # Vérifie si la ligne n'est pas vide
                line_data = eval(line.strip()) # Évaluer la ligne comme une expression Python
                dataset_id = line_data['dataset_id']

    # Nomme le fichier
    nom_fichier = f"refresh_{dataset_id}_{debut}_{fin}.sh"

    # Écrire les arguments dans le fichier bash
    logger.info("Création et remplissage du script bash")

    try :
        if not os.path.exists(f'{args.repertoire_bash}'): 
            logger.info(f"Création du repertoire Bash")
            os.mkdir(f'{args.repertoire_bash}')           
    except:
        logger.error(f"Nom de fichier déjà existant : {nom_fichier}")
        exit()
    with open(f'{args.repertoire_bash}/{nom_fichier}', 'w') as bash_script:
        bash_script.write('''#!/bin/bash 

# Détecte si le DryRun est désactivé ou non
if [ "$1" = "False" ]; then
    vDryRun=False
    echo "Mode Dryrun désactivé : Rafraîchissement des tables"
else
    vDryRun=True
    echo "Mode Dryrun activé : Test à blanc, affiche uniquement les informations"   
fi              

# Activation de l'environnement virtuel                          
source ./venv/bin/activate     

# Paramétrage de la fonction log
vScriptDir="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"  

vTsLog=`date '+%Y%m%d_%H%M%S'`                                                                  

vLogFile=${vScriptDir}/logs/autoRefresh_${vTsLog}.log
vLogFileError=${vScriptDir}/logs/autoRefresh_errors_${vTsLog}.log                          

# Création d'une fonction log 
# Cette fonction permet d'afficher des informations durant le déroulement du script  
# Cela permet de voir les rafraîchissement qui ont fonctioné et ceux qui ont n'ont pas fonctionné                      
function log () {
    
    vCurrentLogDate=`date '+%Y-%m-%d %H:%M:%S'` 
    echo "$1 - " "${vCurrentLogDate} - $2"
    
    echo "$1 - " "${vCurrentLogDate} - $2" >> ${vLogFile}
    
    if [[ "$1" == "ERR" ]]
    then
        echo "$1 - " "${vCurrentLogDate} - $2" >> ${vLogFileError}
        vNbError=$((vNbError + 1))
    fi
    
}

# Compteur d'erreur, est incrémenté de 1 à chaque fois qu'un rafraîchissement d'une table ne fonctionne pas                         
error=0                          

''')
        i= 0
        for line in lines:
            i = i+1
            # Ignore les lignes du header
            if i<=6 :
                continue
            else :
                if line.strip(): # Vérifie si la ligne n'est pas vide
                    line_data = eval(line.strip()) # Évaluer la ligne comme une expression Python
                    table_id = line_data['table_id']
                    dataset_id = line_data['dataset_id']
                    partition_key, partition_type, partition_start, partition_end = extract_partition_info(line_data['partition_info'])
                    # Vérifier si la table est partitionnée avant d'écrire les paramètres de partition
                    if line_data['partition_info']['partitioned']:
                        if partition_type == 'DAY' :
                            partition_params = f"--partition_date_start {header_values.get('debut_DAY', '')} --partition_date_end {header_values.get('fin_DAY', '')}"
                        else :
                            partition_params = f"--partition_date_start {header_values.get('debut_NUM', '')} --partition_date_end {header_values.get('fin_NUM', '')}"
                    else:
                        partition_params = "" 
                # Écrire la ligne de commande dans le fichier bash
                    bash_script.write(f'''
# Teste si mode dryrun est actif ou non : si oui => affichage seulement de la commande sans exécuter le refresh
if [ "$vDryRun" = "True" ]; then
    echo "python3 ./refreshSubEnv.py --project {args.project} --subenv {args.subenv} --target_env {args.target_env} --datasets {dataset_id} --tables {table_id} {partition_params} "
else 
    python3 ./refreshSubEnv.py --project {args.project} --subenv {args.subenv} --target_env {args.target_env} --datasets {dataset_id} --tables {table_id} {partition_params}                                        
    if [ $? -ne 0 ]; then
        log "ERR" "Erreur de connexion au projet {args.project}, au sous-environnement {args.subenv} ou à l'environnement cible {args.target_env}"
        error=$((error + 1))
    else
        log "INFO" "Connexion au projet {args.project}, le sous-environnement est {args.subenv} et l'environnement cible est {args.target_env}"
    fi
fi
''')
        bash_script.write('''


if [ $error -eq 0 ]; then
    # Code retour signifiant que tous les rafraîchissements ont fonctionné
    exit 0
else    
     # Code retour signifiant la présence d'erreurs dans un ou plusieurs rafraîchissements  
    echo "Nombre d'erreurs : $error"        
    exit 3
fi
''')
