using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Windows;
using System.Threading;

namespace LogWriter
{
    public class LogWriter
    {
        private readonly string sName;
        private static readonly Lock _lock = new Lock(); // Verwenden von System.Threading.Lock
        private readonly string logFilePath;
        private readonly string csvFilePath;

        public LogWriter(string logMessage,string stempName,int sourceID,bool bolLogTime = true)
        {
            sName = stempName;
            logFilePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory,"Log",$"Logging_{sName}.log");
            csvFilePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory,"Log",$"Logging_{sName}.csv");
            LogWrite(logMessage,sourceID,bolLogTime);
        }

        public LogWriter(string logMessage,int sourceID,bool bolLogTime = true)
        {
            sName = DateTime.Now.ToString("yyyy-MM-dd HH-mm");
            logFilePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory,"Log",$"Logging_{sName}.log´");
            csvFilePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory,"Log",$"Logging_{sName}.csv");
            LogWrite(logMessage,sourceID,bolLogTime);
        }

        public void LogWrite(string logMessage,int threadId,int sourceID,bool bolLogTime = true)
        {
            lock (_lock)
            {
                using (var txtWriter = new StreamWriter(logFilePath,true))
                {
                    Log(logMessage,txtWriter,threadId,sourceID,bolLogTime);
                }

                using (var csvWriter = new StreamWriter(csvFilePath,true))
                {
                    LogToCsv(logMessage,csvWriter,threadId,sourceID,bolLogTime);
                }
            }
        }

        public void LogToCSVInitial()
        {
            lock (_lock)
            {
                using (var csvWriter = new StreamWriter(csvFilePath,true))
                {
                    try
                    {
                        var logEntry = new StringBuilder();
                        logEntry.Append(";ThreadID;SourceID;LogMessage");
                        csvWriter.WriteLine(logEntry.ToString());
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[ERROR]: in LogToCsv - {ex.Message}");
                    }
                }
            }
        }
        public void LogWrite(string logMessage,int threadId,bool bolLogTime = true)
        {
            lock (_lock)
            {
                using (var txtWriter = new StreamWriter(logFilePath,true))
                {
                    Log(logMessage,txtWriter,threadId,-1,bolLogTime);
                }

                using (var csvWriter = new StreamWriter(csvFilePath,true))
                {
                    LogToCsv(logMessage,csvWriter,threadId,-1,bolLogTime);
                }
            }
        }
        public string GiveFileName()
        {
            return logFilePath;
        }

        public string GiveCsvFileName()
        {
            return csvFilePath;
        }

        private void Log(string logMessage,TextWriter txtWriter,int threadId,int sourceID,bool bolLogTime)
        {
            try
            {
                var logEntry = new StringBuilder();
                if (bolLogTime)
                {
                    logEntry.Append($"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} ");
                }
                logEntry.Append($"[Thread:{threadId}] [SourceID:{sourceID}] {logMessage}");
                txtWriter.WriteLine(logEntry.ToString());
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR]: in Log - {ex.Message}");
            }
        }

        private void LogToCsv(string logMessage,StreamWriter csvWriter,int threadId,int sourceID,bool bolLogTime)
        {
            try
            {
                var logEntry = new StringBuilder();
                if (bolLogTime)
                {
                    logEntry.Append($"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff};");
                }
                logEntry.Append($"{threadId};{sourceID};{logMessage}");
                csvWriter.WriteLine(logEntry.ToString());
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR]: in LogToCsv - {ex.Message}");
            }
        }

        internal void LogWrite(object p)
        {
            if (p == null)
            {
                throw new ArgumentNullException(nameof(p), "Das übergebene Objekt darf nicht null sein.");
            }

            string logMessage = p?.ToString() ?? string.Empty; // Sicherstellen, dass logMessage nicht null ist
            LogWrite(logMessage, Thread.CurrentThread.ManagedThreadId, -1);
        }
    }
}

