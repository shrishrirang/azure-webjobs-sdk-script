// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Diagnostics;
using System.IO;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.Script.Config;
using Microsoft.Azure.WebJobs.Script.Diagnostics;

namespace Microsoft.Azure.WebJobs.Script
{
    public sealed class FunctionTraceWriterFactory : ITraceWriterFactory
    {
        private readonly string _functionName;
        private readonly ScriptHostConfiguration _scriptHostConfig;

        public FunctionTraceWriterFactory(string functionName, ScriptHostConfiguration scriptHostConfig)
        {
            _functionName = functionName;
            _scriptHostConfig = scriptHostConfig;
        }

        public TraceWriter Create()
        {
            SqlTraceWriter sqlTraceWriter = null;
            FileTraceWriter fileTraceWriter = null;

            try
            {
                // TODO: This needs to be fixed. Temporarily unblocking standalone scenario
                if (ScriptHost.IsStandaloneMode())
                {
                    sqlTraceWriter = new SqlTraceWriter(AmbientConnectionStringProvider.Instance.GetConnectionString("SqlTracer"),
                                              ScriptSettingsManager.Instance.GetSetting(EnvironmentSettingNames.AzureWebsiteInstanceId),
                                              ScriptSettingsManager.Instance.GetSetting(EnvironmentSettingNames.AzureWebsiteName),
                                              _functionName,
                                              _scriptHostConfig.HostConfig.Tracing.ConsoleLevel);

                    fileTraceWriter = GetFileTraceWriter();

                    return new CompositeTraceWriter(new TraceWriter[] { sqlTraceWriter, fileTraceWriter });
                }

                if (_scriptHostConfig.FileLoggingMode != FileLoggingMode.Never)
                {
                    fileTraceWriter = GetFileTraceWriter();
                }

                return NullTraceWriter.Instance;
            }
            catch
            {
                if (sqlTraceWriter != null)
                {
                    sqlTraceWriter.Dispose();
                }

                if (sqlTraceWriter != null)
                {
                    fileTraceWriter.Dispose();
                }

                throw;
            }
        }

        private FileTraceWriter GetFileTraceWriter()
        {
            TraceLevel functionTraceLevel = _scriptHostConfig.HostConfig.Tracing.ConsoleLevel;
            string logFilePath = Path.Combine(_scriptHostConfig.RootLogPath, "Function", _functionName);
            return new FileTraceWriter(logFilePath, functionTraceLevel);
        }
    }
}
