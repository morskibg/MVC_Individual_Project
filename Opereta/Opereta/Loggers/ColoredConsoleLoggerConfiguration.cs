namespace Opereta.Loggers
{
    using System;
    using Microsoft.Extensions.Logging;

    public class ColoredConsoleLoggerConfiguration
    {
        public LogLevel LogLevel { get; set; } = LogLevel.Warning;
        public int EventId { get; set; } = 0;
        public ConsoleColor Color { get; set; } = ConsoleColor.Yellow;
        public string Message { get; set; } 
    }
}
