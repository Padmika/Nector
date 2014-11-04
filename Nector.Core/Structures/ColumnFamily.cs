using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nector.Core
{
    public class ColumnFamily
    {
        public string Keyspace {get; set;}
        public string Name {get; set;}
        /*public string ColumnType {get; set;}
        public string ComparatorType {get; set;}
        public string SubcomparatorType {get; set;}
        public string Comment {get; set;}
        public double RowCacheSize {get; set;}
        public double KeyCacheSize {get; set;}*/
        public double ReadRepairChance {get; set;}
        /*public List<ColumnDefinition> ColumnMetadata {get; set;}
        public int GcGraceSeconds {get; set;}
        public string DefaultValidationClass {get; set;}
        public int Id {get; set;}
        public int MinCompactionThreshold {get; set;}
        public int MaxCompactionThreshold {get; set;}
        public int RowCacheSavePeriodInSeconds {get; set;}
        public int KeyCacheSavePeriodInSeconds {get; set;}
        public bool ReplicateOnWrite {get; set;}
        public double MergeShardsChance {get; set;}
        public string KeyValidationClass {get; set;}
        public string RowCacheProvider {get; set;}
        public byte[] KeyAlias {get; set;}
        public string CompactionStrategy {get; set;}
        public Dictionary<string, string> CompactionStrategyOptions {get; set;}
        public int RowCacheKeysToSave {get; set;}
        public Dictionary<string, string> CompressionOptions {get; set;}*/

        public override string ToString()
        {
            return Name;
        }
    }
}
