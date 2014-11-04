using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;

namespace Nector.Core
{
    public class ThriftUtility
    {
        #region StaticHelpereMethods

        public static NColumn<N, V> ToNColumn<N, V>(Column column)
        {
            NColumn<N, V> ncolumn = new NColumn<N, V>();
            ncolumn.Name = (N)Utility.ToObjectFromByteArray(column.Name, typeof(N));
            ncolumn.Value = (V)Utility.ToObjectFromByteArray(column.Value, typeof(V));
            ncolumn.ValueBytes = column.Value;
            ncolumn.NameBytes = column.Name;
            return ncolumn;
        }

        public static List<NColumn<N, V>> ToNColumnList<N, V>(List<ColumnOrSuperColumn> columns)
        {
            List<NColumn<N, V>> results = new List<NColumn<N, V>>();

            foreach (var column in columns)
            {
                results.Add(ToNColumn<N, V>(column.Column));
            }
            return results;
        }

        public static Dictionary<byte[], List<NColumn<byte[], byte[]>>> ToByteDictionaryFromColumnDictionary(Dictionary<byte[], List<ColumnOrSuperColumn>> inputdictionary)
        {
            Dictionary<byte[], List<NColumn<byte[], byte[]>>> results = new Dictionary<byte[], List<NColumn<byte[], byte[]>>>();
            foreach (KeyValuePair<byte[], List<ColumnOrSuperColumn>> entry in inputdictionary)
            {
                results.Add(entry.Key, ToNColumnList<byte[], byte[]>(entry.Value));
            }

            return results;
        }

        public static Rows<K, N, V> ToRowsFromSliceQuery<K, N, V>(Dictionary<byte[], List<ColumnOrSuperColumn>> results)
        {
            Rows<K, N, V> rows = new Rows<K, N, V>();
            rows.rowsList = new List<Row<K, N, V>>();

            foreach (KeyValuePair<byte[], List<ColumnOrSuperColumn>> entry in results)
            {
                ColumnSlice<N, V> cs = new ColumnSlice<N, V>();
                cs.Columns = ToNColumnList<N, V>(entry.Value);

                Row<K, N, V> row = new Row<K, N, V>();
                row.ColumnSlice = cs;
                row.Key = (K)Utility.ToObjectFromByteArray(entry.Key, typeof(K));

                rows.rowsList.Add(row);
            }
            return rows;
        }

        public static Keyspace ToKeyspaceFromKsDef(KsDef ksdef) 
        {
            Keyspace ks = new Keyspace();
            ks.Name = ksdef.Name;
            ks.StrategyClass = ksdef.Strategy_class;
            ks.CfDefs = ToColumnFamilyListFromCfDefList(ksdef.Cf_defs);
            return ks;
        }

        public static List<Keyspace> ToKeyspaceListFromKsfDefList(List<KsDef> ksdefs)
        {
            List<Keyspace> ksList = new List<Keyspace>();
            foreach (var item in ksdefs)
            {
                ksList.Add(ToKeyspaceFromKsDef(item));
            }

            return ksList;
        }

        public static ColumnFamily ToColumnFamilyFromCfDef(CfDef cfdef)
        {
            ColumnFamily cf = new ColumnFamily();
            cf.Name = cfdef.Name;
            cf.Keyspace = cfdef.Keyspace;
            return cf;
        }

        public static CfDef ToCfDefFromColumnFamily(ColumnFamily cf)
        {
            CfDef cfdef = new CfDef();
            cfdef.Name = cf.Name;
            cfdef.Keyspace = cf.Keyspace;
            return cfdef;
        }

        public static List<ColumnFamily> ToColumnFamilyListFromCfDefList(List<CfDef> cfdefs) 
        {
            List<ColumnFamily> colFamilyList = new List<ColumnFamily>();
            foreach (var item in cfdefs)
            {
                colFamilyList.Add(ToColumnFamilyFromCfDef(item));   
            }

            return colFamilyList;
        }

        public static List<CfDef> ToCfDefListFromColumnFamilyList(List<ColumnFamily> cfdefs)
        {
            List<CfDef> colFamilyList = new List<CfDef>();
            foreach (var item in cfdefs)
            {
                colFamilyList.Add(ToCfDefFromColumnFamily(item));
            }

            return colFamilyList;
        }



        #endregion
    }
}
