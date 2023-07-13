# OnTheGo Database

OnTheGo database is een simpel duurzame database engine. Die ondersteunt basis database operaties. Zoals DDL (create, drop database/table) en DML (select, insert, update, delete). Daarnast verstrekt dit ook de JDBC-driver die OnTheGo Database aankunt voor Java Software. De reden waarom Ik OnTheGo Database project begon is dat I heb gedacht dat een database is een goed voorbeeld om compiler en datastructuren te studeren. Verder, ik wilde iets wat handig is voor mensen maken als uitstekend softwareontwikkelaren deden. Eindelijk had ik een simpel duurzame database gemaakt en kreeg ik een diep verstand van binnenstructuren van database.  

Er zijn veel dingen die ik zouden moeten doen om OnTheGo database een authentieke database te maken. OnTheGo database is gebrek aan sommige funtionaliteiten. Zoals index, join, optimizer, synchronization, end network daemon. Ik denk dat dit project een bewijs van mijn zuivere enthousiasme van softwareontwikkeling is  

## Vereisten

1. Above Java 8 
2. Maven

## Functionaliteiten

1. Multiple Transaction(begin, commit, rollback) Support
2. DDL(create, drop), DML(select, insert, update, delete) Support 
3. JDBC Driver Support

## Het Verloop van Uitvoering
### 1. De Github-Opslagplaats van OnTheGo Database Project Klonen
```
$> git clone https://github.com/nieuwmijnleven/OnTheGoDatabase.git
$> cd ./OnTheGoDatabase/onthego.database
```
### 2. OnTheGo Database Opbowen
```
$> mvn package
```
### 3. OnTheGo Database Starten
```
$> java -jar onthego_database.jar
```

## Schermafbeelden

### 1. create database / create table / insert
![image](https://github.com/nieuwmijnleven/OnTheGoDatabase/assets/56591823/507462dd-9b8e-4c9f-b8dc-76df1d722c83)

### 2. update / delete
![image](https://github.com/nieuwmijnleven/OnTheGoDatabase/assets/56591823/30f5b09f-d44e-458d-9a79-c17bdaaabfaf)

## Autheur

* **Jeon, Cheol** 
