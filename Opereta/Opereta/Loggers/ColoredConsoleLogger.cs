namespace Opereta.Loggers
{
    using System;
    using Microsoft.Extensions.Logging;

    public class ColoredConsoleLogger : ILogger
    {
        private readonly string _name;
        private readonly ColoredConsoleLoggerConfiguration _config;

        public ColoredConsoleLogger(string name, ColoredConsoleLoggerConfiguration config)
        {
            this._name = name;
            this._config = config;
        }

        public IDisposable BeginScope<TState>(TState state)
        {
            return null;
        }

        public bool IsEnabled(LogLevel logLevel)
        {
            return logLevel == this._config.LogLevel;
        }

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        {
            if (!this.IsEnabled(logLevel))
            {
                return;
            }

            if (this._config.EventId == 0 || this._config.EventId == eventId.Id)
            {
                var color = Console.ForegroundColor;
                Console.ForegroundColor = this._config.Color;
                var text = this._config.Message ?? formatter(state, exception);
                Console.WriteLine($"{logLevel.ToString()} - {eventId.Id} - {this._name} : {text}");
                Console.ForegroundColor = color;
            }
        }
    }
}
