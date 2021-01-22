## Projet Furets du MS Big Data Télécom Paris 2020

Réalisé par


# Obectif du projet

http://andreiarion.github.io/projet2021.html
http://andreiarion.github.io/Projet2021-intro.html#/data-cleanup-wranglingmunging

Synthèse du process de ce projet
Le groupe a consolidé ses connaissances pratiques et théoriques dans l'approche NoSql en procédant comme suit:
- Exploration de la création de compte AWS Student,
- Revue des différents clusters exploitables (Spark + [ EMR, AWS MongoDB semi managé et AWS MongoDB Atlas fully managé, ou Cassandra]),
- Revue des outils de développements adaptés : Jupyter nb et Zeppelin nb adossés à EMR, IDE IntelliJ scala/sbt installé sur machines de TP ou linux perso (install windows réputée difficile à ce jour),
- Revue des langages utilisables (Python avec PySpark ou Scala).

En temps restreint, les circonstances nous on enjoints de rapidement:
- lancer un code client élémentaire fonctionnel,
- configurer en local ou à distance une instance Spark a minima pour valider le code et la prise en main des outils.


Les deux premières étapes suivantes nous ont permis de commencer sans trop tarder à coder en Spark / scala, dans l'attente d'éclaircissements sur les difficultés liées aux compte AWS de la promo.

- Ainsi la première instance fonctionnelle a été faite avec un Zeppelin sous Docker / Windows et MongoDB Atlas, solution fully managée de MongoDB hébergée chez AWS (les autres fournisseurs de cloud proposent aussi des clusters MongoDB, et cette solution est d'ailleurs la première à avoir développer un hébergement physique multi providers).. Rapidement des problèmes de dépendances et d'erreurs mal identifiables sont apparus.

- La deuxième instance fonctionnelle a été intelliJ (scala/sbt) et MongoDB Atlas. Ayant progressé plus loin, jusqu'à l'écriture des buckets s3, leur lecture a posé des problèmes d'accès insolubles : la version de Spark 3.0 ou une incompatibilité avec le Spark 2 de AWS pourrait être à l'origine de cette deuxième déception.

Par la suite des soucis de portabilité du code n'ont pas cessé d'apparaître : la version d'intelliJ scala Spark étant 3.0, sa duplication sur notebooks jupyter et zeppelin s'est avérée impossible.

Nous avons donc concentré nos efforts sur le déploiement d'un cluster EMR dans le but d'accéder au notebook zeppelin associé (sans autre utilisation du cluster à proprement parler).
C'est là que nous avons pu commencer à coder ensemble avec un environnement commun.

Après écriture sur MongoDB et un premier travail sur les datas, nous avons donc testé notre code sur le cluster Atlas avec satisfaction.

Nouas avons effectuées les requêtes demandées avec 500MB de quota cluster (2 jours de data events, mentions et gkg).
Cependant nous n'avons pas pu faire de visualisation des résultats, ni d'optimisation de requêtes, ni de paramétrage poussé de la configuration MongoDB, et le transfert de notre code dans un cluster AWS MongoDB n'a finalement pas pu se faire, après de nombreuses tentatives (origine non identifiée, car une création de pile s'était faite avec succès au début du mois..).

Le groupe est néanmoins extrêmement satisfait des réalisations obtenues.

Le repo AnalyseBigDataCloudAWSMongoDBAtlas de ce github comprend le projet IntelliJ scala sbt mis en place en première partie, et les deux json du présent repo sont les deux notebook finaux présentés en soutenance sur Zeppelin EMR.
