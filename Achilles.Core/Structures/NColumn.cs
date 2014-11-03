using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Achilles.Core
{
    public class NColumn<N, V>
    {
        public N Name {get; set;}
        public V Value { get; set;}
        public byte[] ValueBytes { get; set; }
        public byte[] NameBytes { get; set; }
    }

}
