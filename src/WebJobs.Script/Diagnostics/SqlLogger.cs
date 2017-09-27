using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.WebJobs.Script.Extensions;
using System.IO;
using System.Text.RegularExpressions;

namespace Microsoft.Azure.WebJobs.Script.Diagnostics
{
    // This class shouldn't be in this project. Ok for now, just getting started.
    public class SqlLogger : ILogger
    {
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        {
            IEnumerable<KeyValuePair<string, object>> properties = state as IEnumerable<KeyValuePair<string, object>>;
            string formattedMessage = formatter?.Invoke(state, exception);

            Console.WriteLine("##: " + formattedMessage);
        }

        public bool IsEnabled(LogLevel logLevel)
        {
            return true;
        }

        public IDisposable BeginScope<TState>(TState state) => DictionaryLoggerScope.Push(state);
    }
}