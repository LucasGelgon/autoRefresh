# 🔁 autoRefresh

Outil d’automatisation du rafraîchissement d’univers BigQuery développé lors de mon stage chez **U Iris**.

⚠️ Ce dépôt contient une **version anonymisée**, non fonctionnelle en dehors de l’environnement de travail de l’entreprise.

---

## 📌 Objectif

- Automatiser la génération de scripts de rafraîchissement pour les tables BigQuery
- Lire une configuration, déterminer les partitions à traiter, générer un script bash d’exécution
- Centraliser les logs, gérer les erreurs, et offrir un mode dry-run

---

## ⚙️ Dépendances

- Python 3.10+
- Google Cloud BigQuery (`google-cloud-bigquery`)
- Authentification GCP (`google-auth`)
- `deepdiff` pour la comparaison des fichiers
- Environnement virtuel Python pour exécuter les scripts bash
- Accès GCP avec un compte de service (clé `.json`)

---

## 🧩 Fonctionnement général

1. Extraction de la configuration GCP à partir d’un fichier `.json`
2. Lecture des tables d’un dataset BigQuery
3. Détection des partitions temporelles
4. Génération de fichiers de configuration pour chaque période
5. Création d’un script bash automatisé pour exécuter les rafraîchissements
6. Gestion des logs, des erreurs et d’un mode dry-run sécurisé

---

## 🗂️ Structure simplifiée

```
📦 autoRefresh/
 ┣ autoRefresh.py            # Script principal d’automatisation
 ┣ genConfig.py              # Générateur de fichiers de configuration
 ┣ logs/                     # Répertoire de logs
 ┣ config/                   # Répertoire contenant les fichiers .txt de configuration
 ┣ venv/                     # Environnement Python local
 ┗ README.md
```

---

## 📝 Exemple de lancement

```bash
python3 autoRefresh.py \
  --project mon_projet \
  --subenv dev \
  --target_env prod \
  --repertoire_bash ./scripts \
  --emplacement_config ./config/refresh_config.txt \
  --log DEBUG
```

---

## 🙏 Remerciements

Ce projet a été développé lors de mon stage chez **U IRIS**.  
Merci à toute l’équipe pour leur confiance et pour m’avoir autorisé à publier une version anonymisée sur mon GitHub.

---

## 👨‍💻 Auteur

Lucas Gelgon  
