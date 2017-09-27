using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs.Host;

namespace Microsoft.Azure.WebJobs.Script.Diagnostics
{
    /**
     * Sql based implementation of ILogger.
     *
     * Expected table definition:
     *
     * CREATE TABLE [function].[logs]
     * (
     * [Id] [int] IDENTITY(1,1) PRIMARY KEY,
     * [Timestamp] [datetime2](7) NOT NULL,
     * [AppName] [nvarchar](max) NOT NULL,
     * [FunctionName] [nvarchar](max), -- NULL is OK
     * [Message] [nvarchar](max) NOT NULL
     * )
     *
     */
    public class SqlLogger : BufferedLogger
    {
        public const string ConnectionStringName = "SqlTracer";

        public SqlLogger(string category)
            : base(category)
        {
        }

        protected async override Task FlushAsync(IEnumerable<TraceMessage> traceMessages)
        {
            var insertStatement =
                "INSERT INTO [function].[Logs] ([Timestamp], [ServerName], [AppName], [FunctionName], [TraceLevel], [Message]) values(@Timestamp, @ServerName, @AppName, @FunctionName, @TraceLevel, @Message)";

             var conenctionString = AmbientConnectionStringProvider.Instance.GetConnectionString(ConnectionStringName);

            using (SqlConnection connection = new SqlConnection(conenctionString))
            {
                await connection.OpenAsync();

                using (SqlCommand command = new SqlCommand(insertStatement, connection))
                {
                    command.Parameters.Add("@Timestamp", SqlDbType.DateTime2);
                    command.Parameters.Add("@ServerName", SqlDbType.NVarChar).Value = "FIXME: server name";
                    command.Parameters.Add("@TraceLevel", SqlDbType.Int).Value = 100;
                    command.Parameters.Add("@AppName", SqlDbType.NVarChar).Value = "FIXME: app name";
                    command.Parameters.Add("@FunctionName", SqlDbType.NVarChar).Value = "FIXME: function name" ?? (object)DBNull.Value;
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
