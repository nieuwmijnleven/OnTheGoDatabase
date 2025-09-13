# OnTheGo Database

OnTheGo database is een simpel duurzame database engine. Die ondersteunt basis database operaties. Zoals DDL (create, drop database/table) en DML (select, insert, update, delete). Daarnast verstrekt dit ook de JDBC-driver die OnTheGo Database aankunt voor Java Software. De reden waarom Ik OnTheGo Database project begon is dat I heb gedacht dat een database is een goed voorbeeld om compiler en datastructuren te studeren. Verder, ik wilde iets wat handig is voor mensen maken als uitstekend softwareontwikkelaren deden. Eindelijk had ik een simpel duurzame database gemaakt en kreeg ik een diep verstand van binnenstructuren van database.  

Er zijn veel dingen die ik zouden moeten doen om OnTheGo database een authentieke database te maken. OnTheGo database is gebrek aan sommige funtionaliteiten. Zoals index, join, optimizer, synchronization, end network daemon. Ik denk dat dit project een bewijs van mijn zuivere enthousiasme van softwareontwikkeling is  

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
