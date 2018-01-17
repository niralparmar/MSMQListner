using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MSMQListner
{
    public sealed class EmailMessage
    {
        public string from = string.Empty;
        public string to = string.Empty;
        public string sub =string.Empty;
        public string body =string.Empty;
        public string displayName = string.Empty;
        public int attempt = 0;
    }
}
