# OnTheGo Database

OnTheGo Database is ontstaan uit de wens om een eenvoudige, maar betrouwbare en duurzame database-engine te bouwen. Deze engine ondersteunt essentiële database-operaties zoals DDL (aanmaken, verwijderen van databases/tabellen) en DML (selecteren, invoegen, bijwerken, verwijderen). Bovendien wordt er een JDBC-driver meegeleverd, wat een naadloze integratie met Java-toepassingen mogelijk maakt. 

De aanleiding voor dit project was mijn nieuwsgierigheid naar de interne werking van databases. Ik was ervan overtuigd dat het bouwen van een eigen database een ideaal praktijkvoorbeeld zou zijn om diepgaande kennis op te doen over compilertechnieken en datastructuren. Daarnaast werd ik geïnspireerd door toonaangevende softwareontwikkelaars om software te creëren die daadwerkelijk van waarde is voor anderen. 

Na diverse iteraties stond er uiteindelijk een lichtgewicht, robuuste database waarmee ik niet alleen een functioneel product maakte, maar ook een dieper begrip ontwikkelde van hoe databases van binnen werken. 

Toch is dit pas het begin. Om van OnTheGo Database een volwaardige, moderne database te maken, zijn er nog de nodige stappen te zetten. Functionaliteiten als indexering, joins, query-optimalisatie, synchronisatie en een netwerkdaemon staan nog op mijn roadmap. Ondanks deze uitdagingen ben ik trots op mijn voortgang: dit project weerspiegelt mijn oprechte passie voor softwareontwikkeling en mijn ambitie om met technologie anderen vooruit te helpen. 

## Vereisten

1. Above Java 17
2. Gradle

## Functionaliteiten

1. Multiple Transaction(begin, commit, rollback) Support
2. DDL(create, drop), DML(select, insert, update, delete) Support 
3. JDBC Driver Support

## Uitvoeringstappen
### 1. De Github-Opslagplaats van OnTheGo Database Project Klonen
```
$> git clone https://github.com/nieuwmijnleven/OnTheGoDatabase.git
$> cd ./OnTheGoDatabase/onthego.database
```
### 2. OnTheGo Database Opbowen
```
$> ./gradlew clean build
```
### 3. OnTheGo Database Starten
```
$> java -jar ./onthego.database/app/build/libs/app-1.0.0.jar
```

## Schermafbeelden

### 1. create database / create table / insert
![image](https://github.com/nieuwmijnleven/OnTheGoDatabase/assets/56591823/9c58c583-2c0f-44a1-880f-48b40c6399dc)

### 2. update / delete
![image](https://github.com/nieuwmijnleven/OnTheGoDatabase/assets/56591823/c4861fa8-3380-489b-a007-5736c92afb10)

## Autheur

* **Jeon, Cheol** 
