Hallo!

Dieses Projekt hat die Daten von der Website www.immowelt.de mittels Web Scraping gesammelt. Das Projekt fokussiert sich auf Wohnungen und Häuser in Deutschland, die zum Verkauf stehen. Alle verwendeten Services (Postgres, Airflow, Metabase, pgAdmin) sind innerhalb von Docker Compose enthalten, und beim ersten Docker Compose up werden die Ordner dags, logs und plugins erstellt. Es wurden zwei DAGs programmiert. Der erste dient zum Initialen Laden der Datenbank. Er fasst alle initialen CSVs zusammen, transformiert sie und importiert sie am Ende in die Datenbank. Die nach dem ersten Laden erstellte Datenbank folgt dem Star Schema, also enthält sie eine Fact Table und viele Dimension Tables. Der zweite DAG dient zum Aktualisieren der Datenbank durch Web Scraping auf der Website von Immowelt. Beide DAGs werden von Airflow orchestriert. Am Ende beider DAGs wird eine Nachricht über einen Discord-Kanal geschickt, die informiert, dass der DAG erfolgreich ausgeführt wurde. Im Ordner csvs gibt es drei weitere Unterordner, nämlich back_up_csvs, initial_csvs und temporary_csvs. In back_up_csvs werden Back-up-Dateien abgelegt, die mit dem jeweiligen Update zu tun haben und als Archive dienen, und in temporary_csvs Dateien, die während des Update-Laufs zum Austausch von Daten zwischen den Tasks des DAGs dienen. Der Ordner temporary_csvs wird nach jedem Update geleert. In initial_csvs sind die für den ersten DAG relevanten web-scraped Dateien zu finden, die Rohdaten enthalten.Das Erstellen aller Ordner und Unterordner sind Teil der ersten Task vom Dag:Initialization.

Viele Grüße,
Konstantinos Mitalloulis



Sure, here's the translation of the provided text into English:

Hello!

This project has collected data from the website www.immowelt.de using web scraping. The project focuses on apartments and houses in Germany that are for sale. All used services (Postgres, Airflow, Metabase, pgAdmin) are included within Docker Compose, and the folders dags, logs, and plugins are created upon the first Docker Compose up. Two DAGs have been programmed. The first one is for the initial loading of the database. It consolidates all initial CSVs, transforms them, and finally imports them into the database. The database created after the first load follows the star schema, so it contains a fact table and many dimension tables. The second DAG is for updating the database through web scraping on the Immowelt website. Both DAGs are orchestrated by Airflow. At the end of both DAGs, a message is sent via a Discord channel, informing that the DAG has run successfully. In the csvs folder, there are three additional subfolders, namely back_up_csvs, initial_csvs, and temporary_csvs. In back_up_csvs, backup files related to the respective update are stored and archived, and in temporary_csvs, files that serve for data exchange between the tasks of the DAG during the update run are stored. The temporary_csvs folder is emptied after each update. In initial_csvs, the web-scraped files relevant for the first DAG, containing raw data, can be found. The creation of all folders and subfolders is part of the first task of the DAG: Initialization.

Best regards,
Konstantinos Mitalloulis
