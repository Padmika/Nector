using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;

namespace Achilles.Core.Tests
{
    public class ClientTest
    {
        [Fact]
        public void ShouldCreateKeyspace()
        {
            //Arrange
            string keyspace = "TestExample";

            using (Client client = new Client())
            {
                //Act
                client.dropIfExistsAndCreateKeyspace(keyspace);
                bool result = client.keySpaceExists(keyspace);

                //Assert
                Assert.True(result);
            }
        }  

        [Fact]
        public void ShouldCreateColumnFamily()
        {
            //Arrange
            string keyspace = "TestExample";

            using (Client client = new Client())
            {
                client.dropIfExistsAndCreateKeyspace(keyspace);
                ColumnFamily cf = new ColumnFamily();
                cf.Name = "cf";

                //Act
                bool beforeResult = client.cfExists(keyspace, "cf");
                client.createColumnFamily(keyspace, cf);
                bool afterResult = client.cfExists(keyspace, "cf");

                //Assert
                Assert.False(beforeResult);
                Assert.True(afterResult);
            }
        }

        [Fact]
        public void ShouldSetColumn() 
        {
            //Arrange
            string keyspace = "TestExample";
            string colValue = "sampleValue";
            string colName = "sampleName";
            string key = "sampleKey";
            string colFamilyName = "cf"; 

            using (Client client = new Client())
            {
                client.dropIfExistsAndCreateKeyspace(keyspace);
                ColumnFamily cf = new ColumnFamily();
                cf.Name = colFamilyName;
                client.createColumnFamily(keyspace, cf);
                
                //Act
                client.setColumn(keyspace, cf, key, colName,colValue);
                NColumn<string, string> result = client.getColumn<string,string>(keyspace, cf,key,colName);

                //Assert
                Assert.Equal<string>(colValue, result.Value);
            }
        }

        [Fact]
        public void ShouldSetColumns()
        {
            //Arrange
            string keyspace = "TestExample";
            string colValue1 = "sampleValue1";
            string colName1 = "sampleName1";
            string colValue2 = "sampleValue2";
            string colName2 = "sampleName2";
            Dictionary<object,object> map = new Dictionary<object,object>();
            map.Add(colName1,colValue1);
            map.Add(colName2,colValue2);
            string[] colNames = new string[] {colName1,colName2 };
            string key = "sampleKey";
            string colFamilyName = "cf";

            using (Client client = new Client())
            {
                client.dropIfExistsAndCreateKeyspace(keyspace);
                ColumnFamily cf = new ColumnFamily();
                cf.Name = colFamilyName;
                client.createColumnFamily(keyspace, cf);

                //Act
                client.setColumns(keyspace, cf, Utility.ToByteArray(key), map);
                var cols = client.getAllColumns<string, string>(keyspace, cf, key);
                ColumnSlice<string,string> results = client.getColumns<string, string>(keyspace, cf, key, colNames);

                //Assert
                foreach (var item in map)
                {
                    Assert.NotNull(results.getColumnByName((string)item.Key));
                }
            }
        }

        [Fact]
        public void ShouldGetRows()
        {
            //Arrange
            string keyspace = "TestExample";
            string colValue = "sampleValue";
            string colName = "sampleName";
            string colFamilyName = "cf";
            string[] keys = new string[]{"key1","key2"};

            using (Client client = new Client())
            {
                client.dropIfExistsAndCreateKeyspace(keyspace);
                ColumnFamily cf = new ColumnFamily();
                cf.Name = colFamilyName;
                client.createColumnFamily(keyspace, cf);
                client.setColumn(keyspace, cf, keys[0], colName, colValue);
                client.setColumn(keyspace, cf, keys[1], colName, colValue);

                //Act
                Rows<string, string,string> results = client.getRows<string, string, string>(keyspace, cf, keys);

                //Assert
                Assert.Equal(keys.Length, results.rowsList.Count);
            }
        }

        [Fact]
        public void ShouldGetRowsForGivenColumnNames()
        {
            //Arrange
            string keyspace = "TestExample";
            string colValue1 = "sampleValue1";
            string colName1 = "sampleName1";
            string colValue2 = "sampleValue2";
            string colName2 = "sampleName2";
            string[] colNames = new string[] {colName1,colName2 };
            string colFamilyName = "cf";
            string[] keys = new string[] { "key1", "key2" };

            using (Client client = new Client())
            {
                client.dropIfExistsAndCreateKeyspace(keyspace);
                ColumnFamily cf = new ColumnFamily();
                cf.Name = colFamilyName;
                client.createColumnFamily(keyspace, cf);
                client.setColumn(keyspace, cf, keys[0], colName1, colValue1);
                client.setColumn(keyspace, cf, keys[1], colName2, colValue2);

                //Act
                Rows<string, string, string> results = client.getRows<string, string, string>(keyspace, cf, keys,colNames);

                //Assert
                Assert.Equal(keys.Length, results.rowsList.Count);
            }
        }

        [Fact]
        public void ShouldGetRowKeySet()
        {
            //Arrange
            string keyspace = "TestExample";
            string colValue = "sampleValue";
            string colName = "sampleName";
            string colFamilyName = "cf";
            string[] keys = new string[] { "key1", "key2" };

            using (Client client = new Client())
            {
                client.dropIfExistsAndCreateKeyspace(keyspace);
                ColumnFamily cf = new ColumnFamily();
                cf.Name = colFamilyName;
                client.createColumnFamily(keyspace, cf);
                client.setColumn(keyspace, cf, keys[0], colName, colValue);
                client.setColumn(keyspace, cf, keys[1], colName, colValue);

                //Act
                HashSet<string> results = client.getRowKeySet<string>(keyspace, cf);

                //Assert
                foreach(var key in keys){
                    Assert.True(results.Contains(key));
                }
            }
        }

        [Fact]
        public void ShouldMultiGetColumns()
        {
            //Arrange
            string keyspace = "TestExample";
            string colValue = "sampleValue";
            string colName = "sampleName";
            string colFamilyName = "cf";
            List<object> keys = new List<object>();
            keys.Add("key1");
            keys.Add("key2");

            using (Client client = new Client())
            {
                client.dropIfExistsAndCreateKeyspace(keyspace);
                ColumnFamily cf = new ColumnFamily();
                cf.Name = colFamilyName;
                client.createColumnFamily(keyspace, cf);
                client.setColumn(keyspace, cf, keys[0], colName, colValue);
                client.setColumn(keyspace, cf, keys[1], colName, colValue);

                //Act
                Dictionary<byte[], List<NColumn<byte[], byte[]>>> results = client.multiGetColumns(keyspace, cf, keys, colName, colName, 100, false);

                //Assert
                Assert.Equal(keys.Count, results.Count);
            }
        }

        [Fact]
        public void ShouldDeleteColumn()
        {
            //Arrange
            string keyspace = "TestExample";
            string colValue = "sampleValue";
            string colName = "sampleName";
            string key = "sampleKey";
            string colFamilyName = "cf";

            using (Client client = new Client())
            {
                client.dropIfExistsAndCreateKeyspace(keyspace);
                ColumnFamily cf = new ColumnFamily();
                cf.Name = colFamilyName;
                client.createColumnFamily(keyspace, cf);

                //Act
                client.setColumn(keyspace, cf, key, colName, colValue);
                bool beforeResult = client.columnExists(keyspace, cf, key, colName);
                client.deleteColumn(keyspace, cf, key, colName);
                bool afterResult = client.columnExists(keyspace, cf, key, colName);

                //Assert
                Assert.True(beforeResult);
                Assert.False(afterResult);
            }
        }
    }
}
