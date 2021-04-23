using System;
using System.Collections.Generic;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Options;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace AspNetCore.Kafka.Client
{
    internal abstract class KafkaClient : IKafkaClient
    {
        public ILogger Logger { get; }
        
        public abstract IEnumerable<IMessageInterceptor> Interceptors { get; }

        protected readonly KafkaOptions Options;
        
        private readonly IHostEnvironment _environment;

        private static readonly Dictionary<SyslogLevel, LogLevel> LogMap = new()
        {
            {SyslogLevel.Emergency, LogLevel.Critical},
            {SyslogLevel.Alert, LogLevel.Critical},
            {SyslogLevel.Critical, LogLevel.Critical},
            {SyslogLevel.Error, LogLevel.Error},
            {SyslogLevel.Warning, LogLevel.Warning},
            {SyslogLevel.Notice, LogLevel.Information},
            {SyslogLevel.Info, LogLevel.Information},
            {SyslogLevel.Debug, LogLevel.Debug},
        };
        
        protected KafkaClient(ILogger logger, KafkaOptions options, IHostEnvironment environment)
        {
            Logger = logger;
            Options = options;
            _environment = environment;
        }

        protected void LogHandler(IClient client, LogMessage message)
        {
            using var _ = Logger.BeginScope(new {ClientName = client.Name});
            
            Logger.Log(LogMap[message.Level], $"{message.Facility}: {message.Message}");
        }
        
        protected string ExpandTemplate(string x) =>
            x?.Replace("{env}", _environment.EnvironmentName?.ToUpper() ?? Environment.MachineName);
    }
}