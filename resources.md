
# Database Technology

1. SQLLite
2. Postgres
3. Redshift
4. NoSQL (DynamoDB)
5. Neo4J / Apache GraphX

# Getting Data Loaded into DynamoDB

* [Configuring the Amazon Cloud Command Line Interface](http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-welcome.html)
* [Batch loading data into DynamoDB Tables](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/SampleData.LoadData.html)
* [aws configure command line variable values list](http://docs.aws.amazon.com/cli/latest/topic/config-vars.html)

This is how I import the .json amazon data (needs to have region configured to work)

* [But First, you need to configure the tables](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/SampleData.CreateTables.html)

    aws dynamodb batch-write-item --request-items file://all.json 1>json.log 2>json.err &

* There is a particular .json format for loading batch data. Regular .json will not be loaded using the command above (See Amazon's example for the ProductCatalog table)

    {
            "PutRequest": {
                "Item": {
                    "Id": {
                        "N": "205"
                    },
                    "Title": {
                        "S": "18-Bike-204"
                    },
                    "Description": {
                        "S": "205 Description"
                    },
                    "BicycleType": {
                        "S": "Hybrid"
                    },
                    "Brand": {
                        "S": "Brand-Company C"
                    },
                    "Price": {
                        "N": "500"
                    },
                    "Color": {
                        "L": [
                            {
                                "S": "Red"
                            },
                            {
                                "S": "Black"
                            }
                        ]
                    },
                    "ProductCategory": {
                        "S": "Bicycle"
                    }
                }
            }

* To import from a regular .json file, like the one I have from Amazon

# Amazon RedShift

* [Database Loader](https://blogs.aws.amazon.com/bigdata/post/Tx24VJ6XF1JVJAA/A-Zero-Administration-Amazon-Redshift-Database-Loader)
* [AWS Copy Command](http://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html)

* Loading Syntax

    create table amazon(title varchar(2000), text varchar(8000), price varchar(20), userId varchar(50), helpfulness_count int, helfpfulness_score int, summary varchar(4000), score int, time int, profileName varchar(50), productId varchar(50));

    copy amazon
    from 'http://bacon.urbanhogfarm.com/amazon.json'
    region 'us-west-2';

* [Redshift Data Loading Tools](https://aws.amazon.com/redshift/partners/)

# [Graph Databases](https://neo4j.com/developer/graph-db-vs-rdbms/)

    - Neo4J - Live transactional graph database. You would back your website with this
        - Competitors
          - MongoDB
          - Oracle
          - Postgres
        - Applications / benefits
          - Easier to query complex, graph oriented data
          - Amazon's recommendation engine works on Neo4J
          - Walmart runs their website on Neo4J so that you don't get the product you just bought into the recommendations
          - Fraud detection runs on Neo4J
          - Insurance industry is moving to graph databases so that you can look for network oriented patterns in the data.
          - These kinds of questions used to be the sorts of things you put into a data warehouse. Now, with a Graph Database you can write the query directly against the system you have.
          - [Hosted Neo4J](http://www.graphenedb.com)
          - [Equipmentshare](https://equipmentshare.com) They track every single thing they do. They are looking for geolocation. Working on the internet of things. What are use cases that are interesting
          - What about networks of words
          - Social networks / Twitter data for example
          - Genetics
          - [Hosted Neo4j](http://www.graphenedb.com)
          - [Neo4J Query language is Cypher](https://neo4j.com/developer/cypher-query-language/)
    - GraphX is Graph Processing for Spark.  You would put it on something that does analytics like Cassandra
    - MongoDB got a lot of crap lately, and they grew quickly because the product was easy to build things and apps.
    - [GraphX versus Neo4J](http://stackoverflow.com/questions/28609125/neo4j-or-graphx-giraph-what-to-choose)
    - [FireBase](https://firebase.google.com) They are a huge competitor for MongoDB ... this is not really a graph database.
