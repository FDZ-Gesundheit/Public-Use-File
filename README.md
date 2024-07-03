## Public Use File für Datenmodelle 1 und 2

### Ziel
Das Public Use File (PUF) soll es ermöglichen, Auswertungsskripte zu entwickeln, die für die DaTraV Daten geeignet sind, sowie deren technische Möglichkeiten zu evaluieren.
Konkret heißt das, dass Nutzungsberechtigte bereits vor der Antragsstellung einen Datensatz mit gleicher Struktur und inhaltlicher Nähe zur Verfügung gestellt bekommen, der vollumfänglich anonym ist. So können Skripte schon vor oder während der Antragstellung entwickelt werden um die Gesamtdauer des Forschungsvorhabens deutlich zu verkürzen werden.

Aufgrund der angestrebten und notwendigen Anonymität wird es nicht möglich sein, mit dem PUF aussagekräftige Ergebnisse zu erzeugen! 
Alle Korrelationen werden während der Erstellung zerstört. Inferenz auf der Grundlage des PUF ist nicht möglich.

### Methoden zur Erstellung des Public Use File

Das PUF ist ein öffentliches Datenprodukt des [Forschungsdatenzentrum Gesundheit](https://www.forschungsdatenzentrum-gesundheit.de/). Um die Publikation des Datenprodukts zu ermöglichen, muss der Datensatz anonym sein. Um Anonymität zu erreichen wurden, haben wir folgende Methoden angewendet:

* Aufläsung aller Korrelationen zwischen Variablen
* Vergröberung von Variablen, bei denen ein Wert nicht mindesten k-mal vorkommt
* Ersetzung der Pseudonyme durch zufällige (aber theoretisch gültige) Werte
* Veröffentlichung einer Stichprobe
* Veröffentlichung nur eines Berichtsjahres

![Übersicht der Public Use File Erstellung](<puf_erstellung.png>)

Figure 1: Übersicht der Public Use File (PUF) Erstellung.

#### Korrelationen auflösen
Im PUF sind alle Korrelationen zwischen Variablen aufgelöst. Dieser Effekt wird erreicht, indem für jede Variable - unabhängig von allen anderen Variablen - eine Stichprobe ohne Zurücklegen in Größe des Datensatzes gezogen wird.
Um eine Rückrechnung der einzelnen Stichproben zu verhindern, muss die Zufallsquelle für das Sample kryptografisch sicher sein. Als Zufallsquelle wird bei der Erzeugung des PUF "/dev/random" verwendet.
Bei diesem Prozess bleiben alle univariaten Verteilungen und Fehler erhalten, alle Verbindungen zwischen Variablen werden jedoch bewusst zerstört.

Nicht alle Variablen sind durch diesen Prozess ausreichend geschützt. Daher werden zusätzlich alle Identifier (mit denen Personen über Jahre und Tabellen identifiziert werden können) durch neue Identifier, die lediglich in der Struktur den ursprünglichen Identifiern entsprechen ersetzt. Die Anzahl der Individuen bleibt dabei erhalten.
Eine abschließende Liste der Variablen und der genutzten Technik sind in der Datei [variable_processing.csv](https://git.public.bfarm.de/fdz/puf_dm12/-/blob/main/variable_processing.csv) aufgeführt.

#### k-Anonymität

Der beschriebene Prozess zur Auflösung der Inter-Variablen-Korrelationen stellt sicher, dass keine Informationen über Einzelpersonen im Datensatz mehr enthalten sind. Lediglich univariate Verteilungen auf Populationsebene sind noch vorhanden. Die einzige Ausnahme von dieser Sicherheit sind Extremwerte, die innerhalb der Population selten oder sogar nur einmal vorkommen. Um auch für diese Sonderfälle einen Schutz der Daten zu gewährleisten, wird auf jede Variable k-Anonymität angewendet. So muss beispielsweise bei `k = 5` jeder Wert einer Variable mindestens fünf mal in der Population vorkommen, ansonsten wird der Wert während der Erstellung des PUF abgeändert.

Besonders wichtig bei diesem Schritt ist es, die Struktur der Daten nicht zu verändern. Um beide Ziele zu erreichen, wurden folgende Methoden angewandt:
- Lokales Runden von numerischen Variablen
  - Numerische Werte, die seltener als k-mal vorkommen, werden auf den nächsten Wert gerundet, der k erfüllt
- Nominal skalierte Werte mit einer Hierarchie
  - Werte werden auf die nächste Hierarchiestufe vergröbert
  - Alternativ wird aus den vorhandenen Hierarchiestufen ein Wert gezogen und als neuer Wert gesetzt
- Andere nominal skalierte Variablen
  - Gruppen, die nicht dem geforderten k entsprechen, werden in die Gruppe "andere" zusammengefasst
  - Ist nur eine Gruppe nicht dem k entsprechend oder die Gruppe "andere" enthält weniger als k Personen, wird mit der nächstgrößeren Gruppe zusammengefasst, die k erfüllt

#### IDs ersetzen

Im Rahmen der Datenlieferung nach DaTraV werden jahresbezogene und jahresübergreifende Pseudonyme erzeugt. Diese Pseudonyme werden verwendet, um über Tabellen hinweg den Bezug zu einer natürlichen Person herstellen zu können. Um die Pseudonyme zu schützen, wird für das Public Use File jedes Pseudonym durch ein neues, im gleichen Format zufällig erstelltes Pseudonym ersetzt. Die Zuordnung wird nicht gespeichert und kann nicht revidiert werden. Es wird keine Kollisionsprüfung bei der Neuerstellung der Pseudonyme vorgenommen. Zwei Pseudonyme im Original können somit zufällig das gleiche neue Pseudonym erhalten. Es wird nicht nachgehalten, ob und wie oft dieser Fall eingetreten ist.

#### Stichprobe

Als weitere Schutzmaßnahme wird eine 1%-Stichprobe des vollständigen Datensatzes eines zufälligen Jahres veröffentlicht. Es wird eine einfach zufällige Stichprobe ohne Zurücklegen der Personen-ID in der Größe 1% der gesamten Personen-IDs gezogen.

### Struktur der Daten

Die Struktur der Daten entspricht dem Datenmodell 1 und 2 der DaTraV Daten des Forschungsdatenzentrum Gesundheit. Eine vollständige Beschreibung dieser Daten ist [hier verfügbar](https://fdz-gesundheit.github.io/datensatzbeschreibung_fdz_gesundheit/).

### Struktur dieses Repositories
  
Dieses Repository enthält die folgenden Dateien:
- [generate_puf.py](https://git.public.bfarm.de/fdz/puf_dm12/-/blob/main/generate_puf.py?ref_type=heads): Das ist die Hauptdatei, in der das PUF erstellt und gespeichert wird. Sie verwendet Funktionen, die in 
- [functions.py](https://git.public.bfarm.de/fdz/puf_dm12/-/blob/main/functions.py?ref_type=heads) enthalten sind. 
- [helpers.py](https://git.public.bfarm.de/fdz/puf_dm12/-/blob/main/helpers.py?ref_type=heads) enthält Funktionen, um die Datenbankverbindung aufzubauen, und Informationen über bestimmte Variablen.
- [data_types.csv](dhttps://git.public.bfarm.de/fdz/puf_dm12/-/blob/main/data_types.csv?ref_type=heads) enthält eine Liste aller Variablen und Datentypen.
  
Zusätzlich gibt es die Skripte [pre_tests.py](https://git.public.bfarm.de/fdz/puf_dm12/-/blob/main/pre_tests.py?ref_type=heads) und [post_tests.py](https://git.public.bfarm.de/fdz/puf_dm12/-/blob/main/post_tests.py?ref_type=heads). Diese enthalten Unittests um die entwickelten Methoden zu evaluieren. 
  
Um den Code laufen zu lassen, stellen wir künstlich erzeugte Testdaten in einer SQLite Datenbank-Datei bereit. Die Testdaten werden mit dem Jupyter Notebook [generate_test_data.ipynb](https://git.public.bfarm.de/fdz/puf_dm12/-/blob/main/generate_test_data.ipynb?ref_type=heads).
Voraussetzung zum Testen ist daher die Installation von SQLite:  
Es kann [hier](https://www.sqlite.org/download.html) heruntergeladen werden. Eine Anleitung für Windows gibt es [hier](https://dev.to/dendihandian/installing-sqlite3-in-windows-44eb).

  
### Wie beitragen

Wir freuen uns ausdrücklich über Feedback und Hinweise zum Prozess, den angewandten Methoden und der Dokumentation des PUF.

Wenn Sie einen Fehler im Code, der Dokumentation oder ähnlichem finden, eröffnen Sie bitte ein Issue in diesem Repository und beschreiben (so gut es geht) den gefundenen Fehler oder das Problem. 
Wir benötigen mindestens:
- Den Ort des Problems (Dateiname, Zeile oder Name der Methode/Funktion).
- Eine Beschreibung des gefundenen Problems.

Wir freuen uns sehr, wenn Sie bei gefundenen Problemen auch einen konstruktiven Lösungsvorschlag einbringen.

Gerne möchten wir Sie dementsprechend auch aktiv zum Beitragen einladen. Wenn Sie konkrete Verbesserungen oder Korrekturen vornehmen möchten, öffnen Sie auch hierfür ein Issue mit einer Beschreibung des Fehlers und senden uns einen Pull Request mit Ihrer vorgeschlagenen Lösung.