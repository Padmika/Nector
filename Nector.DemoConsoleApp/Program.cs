using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Nector.Core;

namespace Nector.DemoConsoleApp
{
    class Program
    {
        static void Main(string[] args)
        {
          
            string keyspace = "ConsoleExample";

            /* Some columns the first and second will be string columns (Both Name and Value) 
             * The third will be an int column (Name and Value)
            */
            string colValue1 = "sampleValue1";
            string colName1 = "sampleName1";
            string colValue2 = "sampleValue2";
            string colName2 = "sampleName2";
            int colValue3 = 33;
            int colName3 =  3;
            DateTime colValue4 = DateTime.UtcNow;
            DateTime colName4 = DateTime.UtcNow.AddDays(20.0d);
            // Using a dictionary to store the columns
            Dictionary<object, object> map = new Dictionary<object, object>();
            map.Add(colName1, colValue1);
            map.Add(colName2, colValue2);
            map.Add(colName3, colValue3);
            map.Add(colName4, colValue4);
            
            //We will use this to query the string columns later
            string[] colNames = new string[] { colName1, colName2};
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

                //Get the columns given in the colNames array
                ColumnSlice<string, string> stringColumnResult = client.getColumns<string, string>(keyspace, cf, key, colNames);

                //Get the int column
                NColumn<int,int> intColumnResult = client.getColumn<int, int>(keyspace, cf, key, colName3);
                Console.WriteLine(intColumnResult.Name);
                Console.WriteLine(intColumnResult.Value);
                //Get the DateTime column
                NColumn<DateTime, DateTime> dateTimeColumnResult = client.getColumn<DateTime, DateTime>(keyspace, cf, key, colName4);
                Console.WriteLine(dateTimeColumnResult.Name);
                Console.WriteLine(dateTimeColumnResult.Value);

                client.dropKeyspace(keyspace);
                Console.ReadLine();
            }
        }
    }
}
