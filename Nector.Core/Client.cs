using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Thrift.Transport;
using Thrift.Protocol;
using CassandraClient = Apache.Cassandra.Cassandra.Client;
using Apache.Cassandra;
using System.Collections;
using System.Collections.ObjectModel;
using System.IO;
using System.Threading;



namespace Nector.Core
{
    public class Client :IDisposable
    {
        CassandraClient client = null;
        TTransport transport = null;
        
        ConsistencyLevel consistencyLevel = ConsistencyLevel.ONE;

        public ThriftConsistencyLevel ThriftConsistencyLevel
        {
            set
            {
                switch (value)
                {
                    case ThriftConsistencyLevel.ALL:
                        consistencyLevel = ConsistencyLevel.ALL;
                        break;
                    case ThriftConsistencyLevel.ANY:
                        consistencyLevel = ConsistencyLevel.ANY;
                        break;
                    case ThriftConsistencyLevel.EACH_QUORUM:
                        consistencyLevel = ConsistencyLevel.EACH_QUORUM;
                        break;
                    case ThriftConsistencyLevel.LOCAL_QUORUM:
                        consistencyLevel = ConsistencyLevel.LOCAL_QUORUM;
                        break;
                    case ThriftConsistencyLevel.QUORUM:
                        consistencyLevel = ConsistencyLevel.QUORUM;
                        break;
                    case ThriftConsistencyLevel.THREE:
                        consistencyLevel = ConsistencyLevel.THREE;
                        break;
                    case ThriftConsistencyLevel.ONE:
                        consistencyLevel = ConsistencyLevel.ONE;
                        break;
                    case ThriftConsistencyLevel.TWO:
                        consistencyLevel = ConsistencyLevel.TWO;
                        break;
                    default:
                        break;
                }
            }
        }

        public static readonly int ALL_COUNT = 100000;

        /// <summary>
        /// Creates a client with default values of "localhost" and port 9160
        /// </summary>
        public Client() : this("localhost", 9160)
        {    
        }


        public Client(string host,int port)
        {     
            TSocket socket = null;
            socket = new TSocket(host,port);
            transport = new TFramedTransport(socket);
            TProtocol protocol = new TBinaryProtocol(transport);
            client = new CassandraClient(protocol);
            transport.Open();
        }

        public void login(string keyspace , string username, string password) 
        {
            client.set_keyspace(keyspace);
            
            AuthenticationRequest authrequst =new AuthenticationRequest();
            Dictionary<string,string> credentials = new Dictionary<string,string>();
            credentials.Add(username,password);
            authrequst.Credentials = credentials;
            try
            {
                client.login(authrequst);
            }
            catch (AuthenticationException e)
            {
                //To-do handle thirift auth exception with our own auth exception
                throw e;
            }
            
        }

        /// <summary>
        /// Returns the Keyspace with given name 
        /// </summary>
        /// <param name="keyspace"></param>
        /// <returns></returns>
        public Keyspace getKeyspace(String keyspace)
        {
            KsDef ks = null;
            try
            {
               ks = client.describe_keyspace(keyspace);
            }
            catch (InvalidRequestException e)
            {
                
                
            }

            return ThriftUtility.ToKeyspaceFromKsDef(ks);   
        }

        public List<Keyspace> getKeyspaces()
        {
            List<KsDef> ks = null;
            try
            {
                ks = client.describe_keyspaces();
            }
            catch (InvalidRequestException e)
            {


            }

            List<Keyspace> keyspaces = ThriftUtility.ToKeyspaceListFromKsfDefList(ks);
            return keyspaces;
        }

        /// <summary>
        /// Creates a column family in the keyspace. If it doesn't exist, it will be created, then the call will sleep
        /// until all nodes have acknowledged the schema change
        /// </summary>
        /// <param name="keyspace"></param>
        /// <param name="cf"></param>
        public void createColumnFamily(String keyspace, ColumnFamily cf)
        {
            if (cf.Keyspace == null) cf.Keyspace = keyspace;
            client.set_keyspace(keyspace);
            if (getKeyspace(keyspace) == null) throw new InvalidRequestException();
            //add the cf
            if (!cfExists(keyspace, cf.Name))
            {
                //default read repair chance to 0.1
                cf.ReadRepairChance = 0.1d ;
                client.system_add_column_family(ThriftUtility.ToCfDefFromColumnFamily(cf));
            }
        }


        /// <summary>
        /// Create the column families in the list
        /// </summary>
        /// <param name="keyspace"></param>
        /// <param name="cflist"></param>
        public void createColumnFamilies(String keyspace, List<ColumnFamily> cflist)
        {
            foreach (ColumnFamily cf in cflist)
            {
                createColumnFamily(keyspace, cf);
            }
        }


        /// <summary>
        /// Check if the keyspace exsts
        /// </summary>
        /// <param name="keyspace"></param>
        /// <returns></returns>
        public Boolean keySpaceExists(String keyspace)
        {
            KsDef ksDef = null;
            try
            {
                ksDef = client.describe_keyspace(keyspace);
            }
            catch (NotFoundException)
            {

                return false;
            }

            return ksDef != null;
        }


        /// <summary>
        /// Create the keyspace
        /// </summary>
        /// <param name="keyspace"></param>
        public void createKeySpace(String keyspace)
        {

            KsDef keyspaceDefinition = new KsDef();
            keyspaceDefinition.Name = keyspace;
            keyspaceDefinition.Replication_factor = 1;
            keyspaceDefinition.Strategy_class = "org.apache.cassandra.locator.SimpleStrategy";
            keyspaceDefinition.Strategy_options = new Dictionary<string, string>();
            keyspaceDefinition.Strategy_options.Add("replication_factor", "1");
            keyspaceDefinition.Cf_defs = new List<CfDef>();

            client.system_add_keyspace(keyspaceDefinition);
            waitForCreation(keyspace);

        }

        
        public void dropKeyspace(String keyspace) 
        {
            client.system_drop_keyspace(keyspace);
        }

        public void dropIfExistsAndCreateKeyspace(String keyspace)
        {
            if (keySpaceExists(keyspace))
            {
                dropKeyspace(keyspace);
            }
            createKeySpace(keyspace);
        }


        /// <summary>
        ///  Wait until all nodes agree on the same schema version
        /// </summary>
        /// <param name="keyspace"></param>
        private void waitForCreation(String keyspace)
        {

            while (true)
            {
                Dictionary<String, List<String>> versions = client.describe_schema_versions();
                // only 1 version, return
                if (versions != null && versions.Count == 1)
                {
                    return;
                }
                // sleep and try again
                try
                {
                    Thread.Sleep(100);
                }
                catch (ThreadInterruptedException e)
                {
                }
            }
        }


        /// <summary>
        /// Return true if the column family exists
        /// </summary>
        /// <param name="keyspace"></param>
        /// <param name="cfName"></param>
        /// <returns></returns>
        public Boolean cfExists(String keyspace, String cfName)
        {
            KsDef ksDef = client.describe_keyspace(keyspace);

            if (ksDef == null)
            {
                return false;
            }

            foreach (CfDef cf in ksDef.Cf_defs)
            {
                if (cfName.Equals(cf.Name))
                {
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// Generic method to get the columns. 
        /// </summary>
        /// <typeparam name="N">Type of column name</typeparam>
        /// <typeparam name="V">Type of column value</typeparam>
        /// <param name="keyspace"></param>
        /// <param name="columnFamily"></param>
        /// <param name="key">Row key</param>
        /// <returns>List of NColumns of N,V types</returns>
        public List<NColumn<N, V>> getAllColumns<N, V>(string keyspace, Object columnFamily, Object key)
        {
            client.set_keyspace(keyspace);
            byte[] binaryKey = key.ToByteArray();
            ColumnParent cp = new ColumnParent() { Column_family = columnFamily.ToString() };
            SlicePredicate sp = new SlicePredicate();
            sp.Slice_range = new SliceRange();
            sp.Slice_range.Start = new byte[0];
            sp.Slice_range.Finish = new byte[0];
            sp.Slice_range.Reversed = false;
            sp.Slice_range.Count = ALL_COUNT;
            var result = ThriftUtility.ToNColumnList<N, V>(client.get_slice(binaryKey, cp, sp, consistencyLevel));
            return result;
        }




        public SortedSet<String> getAllColumnNames(string keyspace, Object columnFamily, Object key)
        {
            List<NColumn<string, byte[]>> columns = getAllColumns<string, byte[]>(keyspace, columnFamily, key);
            SortedSet<String> set = new SortedSet<String>();
            foreach (NColumn<string, byte[]> column in columns)
            {
                set.Add(column.Name);
            }
            return set;
        }


       /// <summary>
       /// Returns the columns in the range from the start column to the finish column for a given row key
       /// </summary>
       /// <param name="keyspace"></param>
       /// <param name="columnFamily"></param>
       /// <param name="key"></param>
        /// <param name="start">start column name</param>
        /// <param name="finish">end column name</param>
       /// <param name="count"></param>
       /// <param name="reversed"></param>
       /// <returns></returns>
        public List<NColumn<byte, byte>> getColumns(string keyspace, Object columnFamily, Object key, Object start,
                                                                 Object finish, int count, Boolean reversed)
        {
            client.set_keyspace(keyspace);

            byte[] binaryKey = key.ToByteArray();
            ColumnParent cp = new ColumnParent() { Column_family = columnFamily.ToString() };
            SlicePredicate sp = new SlicePredicate();
            sp.Slice_range = new SliceRange();
            sp.Slice_range.Start = start.ToByteArray();
            sp.Slice_range.Finish = finish.ToByteArray();
            sp.Slice_range.Reversed = reversed;
            sp.Slice_range.Count = count;
            return ThriftUtility.ToNColumnList<byte, byte>(client.get_slice(binaryKey, cp, sp, consistencyLevel));
        }

        /// <summary>
        /// Returns a dictionary of NColumns for a given set of row keys from start colmn to finish column 
        /// </summary>
        /// <param name="keyspace"></param>
        /// <param name="columnFamily"></param>
        /// <param name="keys"></param>
        /// <param name="start">start column name</param>
        /// <param name="finish">end column name</param>
        /// <param name="count"></param>
        /// <param name="reversed"></param>
        /// <returns></returns>
        public Dictionary<byte[], List<NColumn<byte[], byte[]>>> multiGetColumns(string keyspace, Object columnFamily,
                                                                                       List<object> keys, Object start,
                                                                                       Object finish, int count,
                                                                                       bool reversed)
        {
            client.set_keyspace(keyspace);
            List<byte[]> binaryKeys = keys.ToByteArrayListFromCollection<object>();
            SlicePredicate sp = new SlicePredicate();
            sp.Slice_range = new SliceRange();
            sp.Slice_range.Start = start.ToByteArray();
            sp.Slice_range.Finish = finish.ToByteArray();
            sp.Slice_range.Reversed = reversed;
            sp.Slice_range.Count = count;
            var results = ThriftUtility.ToByteDictionaryFromColumnDictionary(client.multiget_slice(binaryKeys,
                new ColumnParent() { Column_family = columnFamily.ToString() }, sp, consistencyLevel));
            return results;
        }


        
        public Rows<K, N, V> getRows<K, N, V>(string keyspace, Object columnFamily, ICollection<K> keys)
        {

            client.set_keyspace(keyspace);
            List<byte[]> binaryKeys = keys.ToByteArrayListFromCollection<K>();
            ColumnParent cp = new ColumnParent() { Column_family = columnFamily.ToString() };
            SlicePredicate sp = new SlicePredicate();
            sp.Slice_range = new SliceRange();
            sp.Slice_range.Start = new byte[0];
            sp.Slice_range.Finish = new byte[0];
            sp.Slice_range.Reversed = false;
            sp.Slice_range.Count = ALL_COUNT;
            Rows<K, N, V> results = ThriftUtility.ToRowsFromSliceQuery<K, N, V>(client.multiget_slice(binaryKeys,
                new ColumnParent() { Column_family = columnFamily.ToString() }, sp, consistencyLevel));
            return results;
        }


 
        public List<NColumn<N, V>> getColumns<N, V>(string keyspace, Object columnFamily, Object key, HashSet<String> columnNames)
        {
            client.set_keyspace(keyspace);
            SlicePredicate sp = new SlicePredicate();
            sp.Column_names = columnNames.ToByteArrayListFromCollection<string>();
            var results = ThriftUtility.ToNColumnList<N, V>(client.get_slice(key.ToByteArray(),
                new ColumnParent() { Column_family = columnFamily.ToString() }, sp, consistencyLevel));
            return results;
        }


        
        public Rows<K, N, V> getRows<K, N, V>(string keyspace, Object columnFamily, ICollection<K> keys,
                                                ICollection<String> columnNames)
        {

            client.set_keyspace(keyspace);
            List<byte[]> binaryKeys = keys.ToByteArrayListFromCollection<K>();
            SlicePredicate sp = new SlicePredicate();
            sp.Column_names = columnNames.ToByteArrayListFromCollection<string>();
            Rows<K, N, V> results = ThriftUtility.ToRowsFromSliceQuery<K, N, V>(client.multiget_slice(binaryKeys,
               new ColumnParent() { Column_family = columnFamily.ToString() }, sp, consistencyLevel));
            return results;
        }


       
        public NColumn<N, V> getColumn<N, V>(string keyspace, Object columnFamily, Object key, N columnName)
        {

            client.set_keyspace(keyspace);
            byte[] binaryKey = key.ToByteArray();
            ColumnPath cp = new ColumnPath();
            cp.Column_family = columnFamily.ToString();
            cp.Column = columnName.ToByteArray();
            var result = ThriftUtility.ToNColumn<N, V>(client.get(binaryKey, cp, consistencyLevel).Column);
            return result;
        }


        public ColumnSlice<N, V> getColumns<N, V>(string keyspace, Object columnFamily, Object key, N[] columnNames)
        {
            client.set_keyspace(keyspace);
            byte[] binaryKey = key.ToByteArray();
            ColumnParent cp = new ColumnParent() { Column_family = columnFamily.ToString() };
            SlicePredicate sp = new SlicePredicate();
            sp.Column_names = columnNames.ToByteArrayListFromCollection<N>();
            var result = ThriftUtility.ToNColumnList<N, V>(client.get_slice(binaryKey, cp, sp, consistencyLevel));
            ColumnSlice<N, V> cslice = new ColumnSlice<N, V>();
            cslice.Columns = result;
            return cslice;
        }


        public NColumn<String, byte[]> getColumn(string keyspace, Object columnFamily, Object key, String column)
        {
            return getColumn(keyspace, columnFamily, key, column);
        }


        public void setColumn(string keyspace, Object columnFamily, Object key, Object columnName, Object columnValue)
        {
            this.setColumn(keyspace, columnFamily, key, columnName, columnValue, 0);
        }


        public void setColumn(string keyspace, Object columnFamily, Object key, Object columnName, Object columnValue,
                               int ttl)
        {
            client.set_keyspace(keyspace);
            byte[] binaryKey = key.ToByteArray();
            ColumnParent cp = new ColumnParent() { Column_family = columnFamily.ToString() };
            Column column = new Column();
            column.Name = columnName.ToByteArray();
            column.Value = columnValue.ToByteArray();
            column.Timestamp = createTimestamp();
            if (ttl != 0)
            {
                column.Ttl = ttl;
            }
            client.insert(binaryKey, cp, column, consistencyLevel);
        }


       
        public void setColumns(string keyspace, Object columnFamily, byte[] key, IDictionary<object, object> map)
        {
            this.setColumns(keyspace, columnFamily, key, map, 0);
        }


        public void setColumns(string keyspace, Object columnFamily, byte[] key, IDictionary<object, object> map, int ttl)
        {
            client.set_keyspace(keyspace);
            long timestamp = createTimestamp();

            Dictionary<byte[], Dictionary<string, List<Mutation>>> mutation_map = new Dictionary<byte[], 
                Dictionary<string, List<Mutation>>>();
            Dictionary<string, List<Mutation>> columnFamilyKey = new Dictionary<string, List<Mutation>>();
            List<Mutation> mutationList = new List<Mutation>();
            foreach (Object name in map.Keys)
            {
                Object value = null;
                map.TryGetValue(name, out value);
                if (value != null)
                {
                    Mutation mutation = new Mutation();
                    Column column = new Column();
                    column.Name = name.ToByteArray();
                    column.Value = value.ToByteArray();
                    column.Timestamp = createTimestamp();
                    if (ttl != 0)
                    {
                        column.Ttl = ttl;
                    }
                    mutation.Column_or_supercolumn = new ColumnOrSuperColumn();
                    mutation.Column_or_supercolumn.Column = column;
                    mutationList.Add(mutation);
                }
            }
            columnFamilyKey.Add(columnFamily.ToString(), mutationList);
            mutation_map.Add(key, columnFamilyKey);
            client.batch_mutate(mutation_map, consistencyLevel);
        }


        
        public long createTimestamp()
        {
            return Convert.ToInt64((DateTime.UtcNow.Subtract(new DateTime(1970, 1, 1))).TotalSeconds);
        }


        
        public void deleteColumn(string keyspace, Object columnFamily, Object key, Object columnName)
        {
            client.set_keyspace(keyspace);
            Mutation mutation = new Mutation();
            SlicePredicate sp = new SlicePredicate();
            sp.Column_names = (new[] { columnName }).ToByteArrayListFromCollection<object>();
            Deletion deletion = new Deletion();
            deletion.Predicate = sp;
            //timestamp should be optional but its not
            deletion.Timestamp = createTimestamp();
            mutation.Deletion = deletion;

            Dictionary<byte[], Dictionary<string, List<Mutation>>> mutation_map = new Dictionary<byte[], Dictionary<string, List<Mutation>>>();
            Dictionary<string, List<Mutation>> columnFamilyKey = new Dictionary<string, List<Mutation>>();
            List<Mutation> mutationList = new List<Mutation>();

            mutationList.Add(mutation);
            columnFamilyKey.Add(columnFamily.ToString(), mutationList);
            mutation_map.Add(key.ToByteArray(), columnFamilyKey);
            client.batch_mutate(mutation_map, consistencyLevel);
        }

        //This is an incedibly expensive operation to check for the existence of a column
        public bool columnExists(string keyspace, Object columnFamily, Object key, Object columnName) 
        {
            try
            {
                getColumn<string,byte[]>(keyspace, columnFamily, key, columnName.ToString());
            }
            catch (NotFoundException)
            {

                return false;
            }

            return true;
        }


        /**
         * Gets the row keys.
         */
        public HashSet<K> getRowKeySet<K>(string keyspace, Object columnFamily)
        {
            List<K> rows = getRowKeyList<K>(keyspace, columnFamily);
            HashSet<K> results = new HashSet<K>();
            foreach (var row in rows)
            {
                results.Add(row);
            }
            return results;
        }


        
        public List<K> getRowKeyList<K>(string keyspace, Object columnFamily)
        {
            client.set_keyspace(keyspace);
            KeyRange range = new KeyRange();
            range.Count = ALL_COUNT;
            range.Start_key = new byte[0];
            range.End_key = new byte[0];

            SlicePredicate sp = new SlicePredicate();
            sp.Slice_range = new SliceRange();
            sp.Slice_range.Start = new byte[0];
            sp.Slice_range.Finish = new byte[0];
            sp.Slice_range.Reversed = false;
            sp.Slice_range.Count = ALL_COUNT;
            ColumnParent cp = new ColumnParent() { Column_family = columnFamily.ToString() };
            List<KeySlice> rows = client.get_range_slices(cp, sp, range, consistencyLevel);

            List<K> list = new List<K>();
            foreach (var row in rows)
            {
                list.Add((K)row.Key.ToObjectFromByteArray(typeof(K)));
            }
            return list;
        }


        
        public void deleteRow(string keyspace, Object columnFamily, Object key)
        {
            client.set_keyspace(keyspace);
            ColumnPath columnPath = new ColumnPath();
            columnPath.Column_family = columnFamily.ToString();
            client.remove(key.ToByteArray(), columnPath, createTimestamp(), consistencyLevel);
        }

        public void destroy()
        {
            if (transport != null)
            {
                if (transport.IsOpen) transport.Close();
            }
            if (client != null)
            {
                if (client.InputProtocol.Transport.IsOpen) client.InputProtocol.Transport.Close();
                if (client.OutputProtocol.Transport.IsOpen) client.OutputProtocol.Transport.Close();
            }
            client = null;
            transport = null;
        }


        public void Dispose()
        {
            if (transport != null)
            {
                if (transport.IsOpen) transport.Close();
            }
            if (client != null)
            {
                if (client.InputProtocol.Transport.IsOpen) client.InputProtocol.Transport.Close();
                if (client.OutputProtocol.Transport.IsOpen) client.OutputProtocol.Transport.Close();
            }
            client = null;
            transport = null;
        }
    }
}
