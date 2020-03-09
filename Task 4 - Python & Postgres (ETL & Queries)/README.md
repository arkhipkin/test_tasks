# Data Engineer Take Home challenge

Before you start you'll need a local instance of postgres.
 

 
### The Python part:

#### Set up:
- create two databases, one called 'source' and one called 'target'
- run the queries in `set_up_python.sql` while connected to the 'source' database

#### Task
- Create an ETL process in Python that will extract the `address` and `company` tables from the source database and load them into the 'target' database
- the loading process is insert only and the ETL process should not insert a row if it already exists in the target table
- the ETL process should have some way of tracking when it last successfully ran and only extract data that was created after it last successfully ran
- write tests to show your code works as expected
- please add a requirements file for any libraries you use


### SQL part:

#### Set up:

- run the queries in `set_up_sql.sql` while connected to a (postgres) database

#### Task

Write queries to generate the following two result sets:

1. Query an alphabetically ordered list of all names in __OCCUPATIONS__, immediately followed by the first letter of each profession as a parenthetical (i.e.: enclosed in parentheses). For example: `AnActorName(A)`, `ADoctorName(D)`, `AProfessorName(P)`, and `ASingerName(S)`.
2. Query the number of ocurrences of each occupation in __OCCUPATIONS__. Sort the occurrences in ascending order, and output them in the following format:

> There are a total of [occupation_count] [occupation]s.
    
where [occupation_count] is the number of occurrences of an occupation in __OCCUPATIONS__ and [occupation] is the lowercase occupation name. If more than one __OCCUPATIONS__ has the same [occupation_count], they should be ordered alphabetically.

Note: There will be at least two entries in the table for each type of occupation.

#### Input Format

![occupation_ddl ](images/occupation_ddl.png)


The __OCCUPATIONS__ table is described as follows:  
- Occupation will only contain one of the following values: __Doctor__, __Professor__, __Singer__ or __Actor__.

#### Sample Input

An __OCCUPATIONS__ table that contains the following records:

![occupations](images/occupations.png)


#### Sample Output

    Ashely(P)
    Christeen(P)
    Jane(A)
    Jenny(D)
    Julia(A)
    Ketty(P)
    Maria(A)
    Meera(S)
    Priya(S)
    Samantha(D)
    There are a total of 2 doctors.
    There are a total of 2 singers.
    There are a total of 3 actors.
    There are a total of 3 professors.    
    

