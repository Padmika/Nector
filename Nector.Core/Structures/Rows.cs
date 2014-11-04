using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nector.Core
{
    public class Rows<K, N, V> 
    {
        public List<Row<K, N, V>> rowsList { get; set;}
        public Row<K, N, V> getByKey(K key) 
        {
            return rowsList.Where(r => Utility.ToByteArray(r.Key) == Utility.ToByteArray(key)).Single();
        }
        public int getCount() 
        {
            return rowsList.Count;
        }
    }

    public class Row<K, N, V>
    {

        public K Key{get; set;}

        public ColumnSlice<N, V> ColumnSlice;

    }

    public class ColumnSlice<N, V>
    {
        public List<NColumn<N, V>> Columns { get; set; }

        public NColumn<N, V> getColumnByName(N columnName)
        {
            return Columns.Where(c => Comparer<N>.Equals(c.Name, columnName)).SingleOrDefault();
        }

    }


}
