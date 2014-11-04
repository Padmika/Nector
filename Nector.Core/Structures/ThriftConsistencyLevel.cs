using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nector.Core
{
    public enum ThriftConsistencyLevel
    {
        ONE = 1,
        QUORUM = 2,
        LOCAL_QUORUM = 3,
        EACH_QUORUM = 4,
        ALL = 5,
        ANY = 6,
        TWO = 7,
        THREE = 8
    }
}
