# OnTheGo Database

OnTheGo Database is a small embedded database engine. It supports basic database operations  such as DDL(create, drop database/table) and DML(select, insert, update, delete). Aside from which, it also provides the JDBC driver that can deal with OnTheGo Database for Java Software. 
 The reason why I started to write the OnTheGo Database is that I have thought a database is a good example to study compilers and data structures as well as I have been eager to make something to be useful for people as outstanding programmers did. Moreover, I earned profound understand about database internals. Since the project was started, I have written 8,845 lines of java codes to it for about two weeks. The OnTheGo database will be steadily updated on schedule.

## Prerequisites

1. Above Java 8 
2. Maven

## Functionalities

1. Multiple Transaction(begin, commit, rollback) Support
2. DDL(create, drop), DML(select, insert, update, delete) Support 
3. JDBC Driver Support

## To be provided (scheduled)

1. The Full Standard SQL Support
2. Constraint Conditions
3. Index Support
4. Inner/Outer Join Operation Support
5. Optimizer/Execution Plan Support 
6. Synchronization support for multiple thread/process environments
7. Network Daemon

## Example

1. create database / create table / insert
![OnTheGo-Example-Create-Insert](https://user-images.githubusercontent.com/56591823/69252370-78c7d580-0be5-11ea-85f4-f113d720941a.png)

2. update / delete
![OnTheGo-Example-Update-Delete](https://user-images.githubusercontent.com/56591823/69252577-d2c89b00-0be5-11ea-9d94-58009ba98d04.png)

## Authors

* **Jeon, Cheol** - *Initial work* - [](https://github.com/)

See also the list of [contributors](https://github.com/your/project/contributors) who participated in this project.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* Hat tip to anyone whose code was used
* Inspiration
* etc
