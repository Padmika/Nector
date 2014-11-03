#Achilles
========

A thin C# client wrapper for Apache Cassandra  thrift interface.

This is a mashup between Hector (https://github.com/hector-client/hector) and the CassandraService class of (https://github.com/usergrid) 

The best C# driver for Cassandra thrift interface is Aquiles (https://aquiles.codeplex.com/). 
It has a lot of features (like connection pooling, endpoint manager) which is currently lacking in Achilles. (Hopefully will be added in future :)). 
However if you are like me  and hate dealing with delegates then Achilles will be a nice compromise for you.

##Motivation,
This is the first step of a journey where I'm hoping to port Usergrid (https://github.com/usergrid) which is a pretty awesome BaaS (Backend as a Service) to .Net (or at least die trying).
Releasing this code as a reusable component so that somebody else will be able to make use of it as well and not be frustrated with working with the Thrift interface.

## Why Thrift and not CQL?
Thrift is incredibly powerful than CQL in some aspects. Especially when it comes to each Cassandra Row having its own schema. This is a very useful feature when it comes to storing dynamic and semi static data.      
