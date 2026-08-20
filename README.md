# CampusAlliance ZIM-Projektplanung

## Persistenz auf Railway

Alle Projekte werden über die zentrale REST-API in PostgreSQL gespeichert. Der
Service startet absichtlich nicht ohne `DATABASE_URL`, damit niemals unbemerkt
eine flüchtige Container-Datei zur Projektdatenbank wird. In Railway muss daher
ein PostgreSQL-Service verbunden und dessen `DATABASE_URL` für den Webservice
verfügbar sein.

Die Tabelle `projects` wird beim Start automatisch angelegt. Ihr JSONB-Feld
`state` enthält stets den vollständigen, bestehenden Projektzustand; `revision`
schützt parallele Bearbeitungen vor stillem Überschreiben.
