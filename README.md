#Nector
========
A thin C# client wrapper for Apache Cassandra  thrift interface.

This is a mashup between Hector (https://github.com/hector-client/hector) and the CassandraService class of (https://github.com/usergrid) 

The best C# driver for Cassandra thrift interface is Aquiles (https://aquiles.codeplex.com/). 
It has a lot of features (like connection pooling, endpoint manager) which is currently lacking in Nector. (Hopefully will be added in future :)). 
However if you are like me  and hate dealing with delegates then Nector will be a nice compromise for you.

## Motivation
This is the first step of a journey where I'm hoping to port Usergrid (https://github.com/usergrid) which is a pretty awesome BaaS (Backend as a Service) to .Net (or at least die trying).
Releasing this code as a reusable component so that somebody else will be able to make use of it as well and not be frustrated with working with the Thrift interface.

## Why Thrift and not CQL?
Thrift is incredibly powerful than CQL in some aspects. Especially when it comes to each Cassandra Row having its own schema. This is a very useful feature when it comes to storing dynamic and semi static data.      


##Getting Started
Open the solution using Visual Studio 2010  (or above)
Do a quick build it will restore some nuget packages
Make sure you have Cassandra installed and running
Run the DemoConsole app or the XUnit tests to see sample usage



##Sample usage
Make sure you have added a reference to Nector and imported it through a using statement ( ex: using Nector.Core;)
```csharp
            string colValue1 = "sampleValue1";
            string colName1 = "sampleName1";

            Dictionary<object, object> map = new Dictionary<object, object>();
            map.Add(colName1, colValue1);
           
            //Row key
            string key = "sampleKey";
            string colFamilyName = "cf";
 
            using (Client client = new Client())
            {
                client.dropIfExistsAndCreateKeyspace(keyspace);
                ColumnFamily cf = new ColumnFamily();
                cf.Name = colFamilyName;
                client.createColumnFamily(keyspace, cf);
                
				//Lets insert all the columns into the given row in one go
                client.setColumns(keyspace, cf, Utility.ToByteArray(key), map);
                
				//Get the column
                NColumn<string,string> columnResult = client.getColumn<string,string>(keyspace, cf, key, colName1);
                
				client.dropKeyspace(keyspace);
            }
  ```
Take a look at DemoConsoleApp and Nector.Core.Tests for more usage
