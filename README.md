Analyse des Contributions Scientifiques Marocaines

Description
-----------
Ce projet propose une solution complète pour analyser et valoriser la production scientifique marocaine. 
Il combine un pipeline ETL, une base de données relationnelle PostgreSQL, des analyses thématiques avancées (Topic Modeling) et des visualisations interactives via Power BI.

L’objectif est de fournir aux chercheurs, institutions et décideurs une vision stratégique et exploitable des tendances et collaborations scientifiques nationales et internationales.


Technologies et Outils
---------------------
- Langages : Python, SQL
- Bases de données : PostgreSQL
- Visualisation : Power BI
- Modélisation thématique : BERTopic, SBERT, HDBSCAN, UMAP
- Large Language Model : Llama 2 pour la labellisation des thèmes

Fonctionnement
--------------
1. Collecte des données via les API OpenAlex et HAL Maroc
2. Nettoyage et transformation (ETL) : suppression des doublons, normalisation et structuration dans PostgreSQL
3. Analyse thématique (Topic Modeling) : BERTopic + Llama 2 pour identifier les thèmes principaux
4. Visualisation : Power BI pour les tableaux de bord interactifs

Liens Utiles
------------
- OpenAlex API: https://openalex.org
- HAL Maroc: https://hal.archives-ouvertes.fr/
- Power BI: https://powerbi.microsoft.com/

Auteur et Encadrant
------------------
Réalisé par :
- BAKADIRI WIDAD
- EL ALAOUI EL ISSMALLI OUSSAMA
- EL MOUSSAOUI AMINE
- ELAMRANI MARIEM

Encadré par :
- Pr. SARA OUALD CHAÏB

Licence
-------
Ce projet est fourni à titre académique pour usage personnel ou éducatif.
