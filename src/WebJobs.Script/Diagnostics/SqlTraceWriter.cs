﻿// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Azure.WebJobs.Script.Diagnostics
{
    /**
     * Sql based implementation of TraceWriter.
     *
     * Expected table definition:
     *
     * CREATE TABLE [functions].[logs]
     * (
     * [id] [int] IDENTITY(1,1) PRIMARY KEY,
     * [timestamp] [datetime2](7) NOT NULL,
     * [app_name] [nvarchar](max) NOT NULL,
     * [function_name] [nvarchar](max), -- NULL is OK
     * [message] [nvarchar](max) NOT NULL
     * )
     *
     */
    public class SqlTraceWriter : BufferedTraceWriter
    {
        public const string ConnectionStringName = "SqlTracer";

        private readonly string _appName;
        private readonly string _connectionString;
        private readonly string _functionName;

        public SqlTraceWriter(string connectionString, string appName, string functionName, TraceLevel level) : base(level)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                throw new ArgumentNullException(nameof(connectionString));
            }

            if (string.IsNullOrWhiteSpace(appName))
            {
                throw new ArgumentNullException(nameof(appName));
            }

            this._connectionString = connectionString;
            this._appName = appName;
            this._functionName = functionName;
        }

        public SqlTraceWriter(string connectionString, string appName, string functionName, TraceLevel level, bool isSystemLoggingEnabled) : base(level, isSystemLoggingEnabled)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                throw new ArgumentNullException(nameof(connectionString));
            }

            if (string.IsNullOrWhiteSpace(appName))
            {
                throw new ArgumentNullException(nameof(appName));
            }

            this._connectionString = connectionString;
            this._appName = appName;
            this._functionName = functionName;
        }

        protected async override Task FlushAsync(IEnumerable<TraceMessage> traceMessages)
        {
            var insertStatement =
                "INSERT INTO functions.logs (timestamp, app_name, function_name, message) values(@Timestamp, @AppName, @FunctionName, @Message)";

            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                await connection.OpenAsync();

                using (SqlCommand command = new SqlCommand(insertStatement, connection))
                {
                    command.Parameters.Add("@Timestamp", SqlDbType.DateTime2);
                    command.Parameters.Add("@AppName", SqlDbType.NVarChar).Value = _appName;
                    command.Parameters.Add("@FunctionName", SqlDbType.NVarChar).Value = _functionName ?? (object)DBNull.Value;
                    command.Parameters.Add("@Message", SqlDbType.NVarChar);

                    foreach (var traceMessage in traceMessages)
                    {
                        command.Parameters["@Timestamp"].Value = traceMessage.Time;
                        command.Parameters["@Message"].Value = traceMessage.Message;

                        await command.ExecuteNonQueryAsync();
                    }
                }
            }
        }
    }
}
