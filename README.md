Hallo !
Folgendes Prokekt hat die Daten von der Website www.immowelt.de über Web Scraping gesammelt.
Das Projekt fokusiert sich auf Wohnungen und Häuser in Deutschland , die zu verkaufen sind.
Alle verwendeten Sercives (postgres,airflow,metabase,pgadmin) sind innerhalb des Docker Compose enthalten und bei dem ersten Docker Compose up werden die Folders dags,logs und plugins kreiert.
Es wurden zwei Dags programmiert . Erstes dient zum First Load der Datenbank . Es fasst alle initial csvs zusammen , transformiert sie und sie am Ende in die Datenbank importiert.
Die gebildete nach dem ersten Load Datenbank folgt dem Star Schema, also sie hat ein Fact_Table und viele Dimension_Tables.
Das zweite Dag dient zum Aktialisieren von der Datenbank über Web Scraping auf der Webseite von Immowelt.
Beide Dags werden von Airflow orchestriert. Am Ende beider Dags wird eine Nachricht über ein Discord Kanal geschickt , die informiert , dass das Dag erfolgreich gelaufen ist.
Im Folder csvs gibt es drei weiteren Folder , nämlich back_up_csvs , initial_csvs unf temporary_csvs.
In back_up_csvs werden back up Dateien abgelegt , die mit dem jeweleigen Update zu tun haben und in temporary_csvs Dateien , die während des Update-Laufs zum Austausch von Daten zwischen den Tasks des Dags dienen. Der Folder temporary_csvs wird nach jedem Update geleert.
In initial_csvs sind die für das erste Dag web-scraped relevante Dateien zu finden, die Rohdaten enthalten.


