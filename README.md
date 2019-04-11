# homework

This project tries to accomplish the following subtasks:

- MapReduce job (Java) for creating test csv data with this structure: FirstName, LastName, Location, BirthDate.
    All the fields are generated randomly with the help of Java Faker: https://github.com/DiUS/java-faker
    The arguments of the job are Count (number of rows) and Output (folder of result).

- SparkJob (Java, using HBase Java API) that processes the resulted csv:
    reads the resulted csv,
    counts these triplets (FirstName, LastName, Location),
    creates an HBase table,
    loads the records into that HBase table.
    The arguments of the job are Input (resulted csv) and Tablename (HBase table).

- Bash scripts for each step:
    1. generate.sh size output_folder : this should generate the data of given size into output_folder

    2. process.sh input_folder table_name :
         this should process the data with spark,
         compute the counts,
         create HBase table,
         load the counts into the table and create the Phoenix table

    3. query_count_location.sh table_name location : this should run the appropriate query by Phoenix sqlline.py

    4. query_count_firstname_location.sh firstname location : this should run the appropriate query by Phoenix sqlline.py

    5. query_count_lastname_location.sh lastname location : this should run the appropriate query by Phoenix sqlline.py
    
Everything should be able to run in an HDP sandbox (see this page: https://hortonworks.com/downloads/#sandbox )

The main goal to run Phoenix SQL queries against this table, we would like to run these 3 types of queries: 
 How many person was born in a specified location?
 How many person with specified first name was born in a specified location? 
 How many person with specified last name was born in a specified location?  

For this use the sqlline.py CLI tool in Phoenix project.

 