## Projet Furets du MS Big Data Télécom Paris 2020

Réalisé par Samuel Bakebeck, Emmanuel Blanchard, Rodolphe Calvet et Jean-Philippe Quach


# Obectif du projet : Déployer des datas GDELT Google ("Our global world in real-time") sur cluster NoSQL pour analyses

http://andreiarion.github.io/projet2021.html

http://andreiarion.github.io/Projet2021-intro.html#/data-cleanup-wranglingmunging

https://blog.gdeltproject.org/gdelt-2-0-our-global-world-in-realtime/

# Synthèse du process de ce projet

Le groupe a consolidé ses connaissances pratiques et théoriques dans l'approche NoSql en procédant comme suit:
- Exploration de l'environnement AWS,
- Revue des différents clusters exploitables (Spark + [ EMR, AWS MongoDB semi managé et AWS MongoDB Atlas fully managé, ou Cassandra]),
- Revue des outils de développements adaptés : Jupyter nb et Zeppelin nb adossés à EMR, IDE IntelliJ scala/sbt installé sur machines de TP ou linux perso (install windows réputée difficile à ce jour),
- Revue des langages utilisables (Python avec PySpark ou Scala).

En temps restreint, les circonstances nous on enjoints de d'abord :
- lancer un code client élémentaire fonctionnel,
- configurer en local ou à distance une instance Spark a minima pour valider le code et la prise en main des outils.

Les deux premières étapes suivantes nous ont permis de commencer à coder en Spark / scala, dans l'attente d'éclaircissements sur les difficultés liées aux compte AWS de la promo.

- Ainsi la première instance fonctionnelle a été faite avec un Zeppelin sous Docker / Windows et MongoDB Atlas, solution fully managée de MongoDB hébergée chez AWS (les autres fournisseurs de cloud proposent aussi des clusters MongoDB, et cette solution est d'ailleurs la première à avoir développé un hébergement physique multi providers).. Rapidement des problèmes de dépendances et d'erreurs peu/pas loggées sont apparus.

- La deuxième instance fonctionnelle a été intelliJ (scala/sbt) et MongoDB Atlas. Ayant progressé plus loin, jusqu'à l'écriture des buckets s3, leur lecture a posé des problèmes d'accès insolubles : la version de Spark 3.0 ou une incompatibilité avec le Spark 2 de AWS pourrait être à l'origine de ce problème, comme du précédent.

Par la suite des soucis de portabilité du code IntelliJ sur notebook jupyter et zeppelin sont encore apparus : Nous avons donc concentré nos efforts, dans un troisième temps, sur le déploiement d'un cluster EMR dans le but d'accéder au notebook zeppelin associé (sans autre utilisation du cluster à proprement parler).
C'est là que nous avons pu commencer à coder ensemble avec un environnement commun.

Après écriture sur MongoDB et un premier travail sur les datas, nous avons donc testé notre code sur le cluster Atlas avec satisfaction.
Nous avons effectué les requêtes demandées avec 500MB de quota cluster (2 jours de data events, mentions et gkg).
Mais cette solution temporaire, bien qu'excellente en ce qu'elle nous a permis de travailler et de répondre aux questions posées, s'est avérée définitve, dans le mesure où nous n'avons finalement pas réussi à déployer à un cluster semi-managé MongoDB sur AWS.

Ainsi après réussite à cette étape, les points suivants seraient rendus possibles : 
- D'abord, comme évoqué lors de la soutenance, un retour sur les choix d'écriture sur la DB avec, cette fois, des dataframes issus des requêtes et non des tables complètes issues des CSV s3 même simplifiés en schémas,
- Visualisation des résultats (soit avec le Zeppelin AWS, soit avec l'outil intégré à MongoDB),
- Réécriture éventuelle de requête avec désormais requêtes regex possibles, qui ne sont pas disponibles dans notre version gratuite fully managée telle que présentée,
- Paramétrage poussé de la configuration MongoDB,
- Observation pratique des conséquences des choix de config à mesure que le volume de datas est poussé.

Actuellement ce problème de déploiement subsiste (origine non identifiée, car une création de pile s'était faite avec succès au début du mois..), et nous sommes preneurs d'infos à ce sujet.

Le groupe est néanmoins extrêmement satisfait des réalisations obtenues.

Le repo AnalyseBigDataCloudAWSMongoDBAtlas de ce github comprend le projet IntelliJ scala sbt mis en place en première partie, et les deux json du présent répo sont les deux notebook finaux présentés en soutenance sur Zeppelin EMR.

Merci de votre attention ;)
