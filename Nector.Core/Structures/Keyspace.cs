using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nector.Core
{
    public class Keyspace
    {
        public string Name {get; set;}
        public string StrategyClass { get; set; }
        public Dictionary<string, string> strategy_options { get; set; }
        public List<ColumnFamily> CfDefs { get; set; }
        bool DurableWrites;

    }
}
