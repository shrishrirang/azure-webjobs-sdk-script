// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Timers;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.Script.Config;

namespace Microsoft.Azure.WebJobs.Script.Diagnostics
{
    // Abstract class that buffers the traces and flushes them periodically
    // Methods of this class are thread safe.
    // Subclass this class to provide custom buffered tracing implementation. Example: Trace to sql, trace to file, etc
    public abstract class BufferedTraceWriter : TraceWriter, IDisposable
    {
        private const int LogFlushIntervalMs = 1000;

        private readonly object _syncLock = new object();
        private readonly Timer _flushTimer;
        private readonly bool _isSystemTracesEnabled;

        private ConcurrentQueue<TraceMessage> _logBuffer = new ConcurrentQueue<TraceMessage>();

        protected BufferedTraceWriter(TraceLevel level, bool isSystemTracesEnabled = true) : base(level)
        {
            _isSystemTracesEnabled = isSystemTracesEnabled;

            try
            {
                _flushTimer = new Timer(LogFlushIntervalMs);

                // start a timer to flush accumulated logs in batches
                _flushTimer.AutoReset = true;
                _flushTimer.Elapsed += OnFlushLogs;
                _flushTimer.Start();
            }
            catch
            {
                // Clean up, if the constructor throws
                if (_flushTimer != null)
                {
                    _flushTimer.Dispose();
                }

                throw;
            }
        }

        public override void Trace(TraceEvent traceEvent)
        {
            if (traceEvent == null)
            {
                throw new ArgumentNullException(nameof(traceEvent));
            }

            object value;
            if (!_isSystemTracesEnabled &&
                traceEvent.Properties.TryGetValue(ScriptConstants.TracePropertyIsSystemTraceKey, out value)
                && value is bool && (bool)value)
            {
                // we don't want to write system traces to the user trace files
                return;
            }

            if (Level < traceEvent.Level)
            {
                return;
            }

            AppendLine(traceEvent.Message);

            if (traceEvent.Exception != null)
            {
                if (traceEvent.Exception is FunctionInvocationException ||
                    traceEvent.Exception is AggregateException)
                {
                    // we want to minimize the stack traces for function invocation
                    // failures, so we drill into the very inner exception, which will
                    // be the script error
                    Exception actualException = traceEvent.Exception;
                    while (actualException.InnerException != null)
                    {
                        actualException = actualException.InnerException;
                    }
                    AppendLine(actualException.Message);
                }
                else
                {
                    AppendLine(traceEvent.Exception.ToFormattedString());
                }
            }
        }

        public override void Flush()
        {
            if (_logBuffer.Count == 0)
            {
                return;
            }

            ConcurrentQueue<TraceMessage> currentBuffer;
            lock (_syncLock)
            {
                // Snapshot the current set of buffered logs
                // and set a new queue. This ensures that any new
                // logs are written to the new buffer.
                // We do this snapshot in a lock since Flush might be
                // called by multiple threads concurrently, and we need
                // to ensure we only log each log once.
                currentBuffer = _logBuffer;
                _logBuffer = new ConcurrentQueue<TraceMessage>();
            }

            if (currentBuffer.Count == 0)
            {
                return;
            }

            // Flush the trace messages
            lock (_syncLock)
            {
                this.FlushAsync(currentBuffer).Wait();
            }
        }

        // Flush the traces
        protected abstract Task FlushAsync(IEnumerable<TraceMessage> traceMessages);

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                _flushTimer.Dispose();

                // ensure any remaining logs are flushed
                Flush();
            }
        }

        protected virtual void AppendLine(string line)
        {
            if (line == null)
            {
                return;
            }

            // add the line to the current buffer batch, which is flushed on a timer
            _logBuffer.Enqueue(new TraceMessage
            {
                Time = DateTime.UtcNow,
                Message = line.Trim()
            });
        }

        private void OnFlushLogs(object sender, ElapsedEventArgs e)
        {
            Flush();
        }
    }

    public class TraceMessage
    {
        public DateTime Time { get; set; }

        public string Message { get; set; }
    }
}
