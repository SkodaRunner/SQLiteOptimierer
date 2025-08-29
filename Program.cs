using Microsoft.Data.Sqlite;
using SQLitePCL;
using System;
using System.IO;
using System.Linq;
using System.Text;
using IniRW;

/// <summary>
/// Hauptprogramm zur Analyse und Bereinigung von Duplikaten in einer SQLite-Datenbank
/// </summary>
class Program
{
    // Da Logger ein Instanzfeld ist, muss es static sein, damit es in static Methoden verwendet werden kann.
    static string sDateTimeLogger = DateTime.Now.ToString("yyyy-MM-dd HH-mm");
    static LogWriter.LogWriter Logger = new LogWriter.LogWriter("", sDateTimeLogger, -1);
    static IniFile MyIni = new IniFile();

    private static string ValidatePath(string path)
    {
        try
        {
            if (!string.IsNullOrEmpty(path))
                return path;

            const string defaultPath = @"EPG.sqlite";
            MyIni.Write("Path",defaultPath);
            return defaultPath;
        }
        catch (Exception ex)
        {
            LogError("ValidatePath",ex,Thread.CurrentThread.ManagedThreadId);
            return @"EPG.sqlite";
        }
    }
    static void Main()
    {
        string dbPath;
        try
        {
            dbPath = ValidatePath(MyIni.Read("Path"));
        }
        catch (Exception ex)
        {
            const string defaultPath = @"EPG.sqlite";
            LogWarn("[WARN] Path reset to default.");
            MyIni.Write("Path",defaultPath);
            LogError("Main",ex,Thread.CurrentThread.ManagedThreadId);
            return;
        }

        using var connection = new SqliteConnection($"Data Source={dbPath}");
        connection.Open();

        LogInfo("Verbindung erfolgreich hergestellt.");

        // Schema in Markdown exportieren
        string schemaMarkdown = GetDatabaseSchemaMarkdown(connection);
        File.WriteAllText("schema.md",schemaMarkdown,Encoding.UTF8);

        LogInfo("Schema wurde in 'schema.md' exportiert.");
        LookForDoubles(connection);
    }

    /// <summary>
    /// Hauptfunktion zur Duplikatsuche und -bereinigung in der Datenbank.
    /// Prüft Templates-Tabelle auf Duplikate und Überschneidungen mit GeneralDonts.
    /// </summary>
    /// <param name="connection">Aktive Datenbankverbindung</param>
    /// 
    private static void LogWarn(string message)
    {
        string warnMessage = $"{GetNow()} [WARN] {message}";
        Console.WriteLine(warnMessage);
        Logger.LogWrite(warnMessage,Thread.CurrentThread.ManagedThreadId);
    }

    // Ändere die LogInfo-Methode zu static, damit sie im static Kontext von Main aufgerufen werden kann.
    private static void LogInfo(string message)
    {
        string warnMessage = $"{GetNow()} [INFO] {message}";
        Console.WriteLine(warnMessage);
        Logger.LogWrite(warnMessage,Thread.CurrentThread.ManagedThreadId);
    }

    // Ändere auch GetNow zu static, da es von LogInfo verwendet wird.
    private static string GetNow()
    {
        return DateTime.Now.ToString("HH:mm:ss") + ": ";
    }

    private static void LogError(string methodName,Exception ex,int thread)
    {
        string errorMessage = $"{GetNow()} [ERROR]: in {methodName}: {ex.Message} ({ex.InnerException})";
        Console.WriteLine(errorMessage);
        Logger.LogWrite(errorMessage,Thread.CurrentThread.ManagedThreadId);
    }
    static void LookForDoubles(SqliteConnection connection)
    {
        string outputFile = "duplicates.txt";
        using var writer = new StreamWriter(outputFile, false);
        try
        {
            // Phase 1: Duplikate in Templates-Feldern finden und bereinigen
            using (var cmd = new SqliteCommand(
                @"SELECT CASE WHEN Name IS NOT NULL AND Name <> '' THEN Name ELSE Template END AS DisplayName,
                    Donts, TemplateID, descDont, NotChannel FROM Templates",connection))
            using (var reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    string displayName = reader.GetString(0);
                    string feldDonts = reader.IsDBNull(1) ? "" : reader.GetString(1);
                    string feldDescDonts = reader.IsDBNull(3) ? "" : reader.GetString(3);
                    string feldNotChannel = reader.IsDBNull(4) ? "" : reader.GetString(4);
                    string id = reader.GetInt32(2).ToString();

                    // Prüfe jedes Feld auf interne Duplikate
                    ProcessFieldDuplicates(displayName,feldDonts,"Donts",id,connection,writer);
                    ProcessFieldDuplicates(displayName,feldDescDonts,"descDont",id,connection,writer);
                    ProcessFieldDuplicates(displayName,feldNotChannel,"NotChannel",id,connection,writer);
                }
            }

            // Phase 2: Daten aus GeneralDonts laden
            var generalDontsChannel = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var generalDontsTemplate = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            // Channel laden
            using (var cmd = new SqliteCommand("SELECT Channel FROM GeneralDonts",connection))
            using (var reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    if (!reader.IsDBNull(0))
                    {
                        string val = reader.GetString(0).Trim();
                        if (!string.IsNullOrEmpty(val))
                            generalDontsChannel.Add(val);
                    }
                }
            }

            // Template laden
            using (var cmd = new SqliteCommand("SELECT Template FROM GeneralDonts",connection))
            using (var reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    if (!reader.IsDBNull(0))
                    {
                        string val = reader.GetString(0).Trim();
                        if (!string.IsNullOrEmpty(val))
                            generalDontsTemplate.Add(val);
                    }
                }
            }

            LogInfo($"GeneralDonts Channel geladen: {generalDontsChannel.Count} Werte");
            LogInfo($"GeneralDonts Template geladen: {generalDontsTemplate.Count} Werte\n");


            // Phase 3: Überschneidungen prüfen
            using (var cmd = new SqliteCommand(
                @"SELECT CASE WHEN Name IS NOT NULL AND Name <> '' THEN Name ELSE Template END AS DisplayName,
                    NotChannel, descDont, Donts, TemplateID FROM Templates",connection))
            using (var reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    string displayName = reader.GetString(0);
                    string notChannel = reader.IsDBNull(1) ? "" : reader.GetString(1);
                    string descDont = reader.IsDBNull(2) ? "" : reader.GetString(2);
                    string donts = reader.IsDBNull(3) ? "" : reader.GetString(3);
                    string id = reader.GetInt32(4).ToString();

                    // Prüfe NotChannel gegen GeneralDonts.Channel
                    CheckAndRemoveOverlaps(displayName,notChannel,"NotChannel",id,generalDontsChannel,"Channel",connection,writer);
                    // Prüfe descDont gegen GeneralDonts.Template
                    CheckAndRemoveOverlaps(displayName,descDont,"descDont",id,generalDontsTemplate,"Template",connection,writer);
                    // Prüfe Donts gegen GeneralDonts.Template
                    CheckAndRemoveOverlaps(displayName,donts,"Donts",id,generalDontsTemplate,"Template",connection,writer);
                }
            }

            LogInfo($"Fertig ✅ Ergebnisse in '{outputFile}' gespeichert.");

            // Abschließende Datenbankoptimierung
            using (var cmd = new SqliteCommand("VACUUM",connection)) cmd.ExecuteNonQuery();
            using (var cmd = new SqliteCommand("PRAGMA optimize",connection)) cmd.ExecuteNonQuery();
        }
        catch (Exception ex)
        {
            string message = $"Fehler {ex.Message} ({ex.InnerException})";
            LogError(message,ex,Thread.CurrentThread.ManagedThreadId);
        }
    }

    /// <summary>
    /// Prüft ein einzelnes Feld auf Duplikate und bereinigt diese.
    /// </summary>
    /// <param name="displayName">Anzeigename des Datensatzes</param>
    /// <param name="fieldValue">Zu prüfender Feldwert (;-getrennte Liste)</param>
    /// <param name="fieldName">Name des Datenbankfeldes</param>
    /// <param name="id">TemplateID des Datensatzes</param>
    /// <param name="connection">Aktive Datenbankverbindung</param>
    /// <param name="writer">StreamWriter für die Protokollierung</param>
    private static void ProcessFieldDuplicates(string displayName,string fieldValue,string fieldName,string id,
        SqliteConnection connection,StreamWriter writer)
    {
        // Aufteilen des Feldwerts in einzelne Einträge und Suche nach Duplikaten
        var items = fieldValue.Split(';', StringSplitOptions.RemoveEmptyEntries);
        var duplicates = items.GroupBy(x => x.Trim())
                            .Where(g => g.Count() > 1)
                            .Select(g => g.Key)
                            .ToArray();

        // Gefundene Duplikate protokollieren
        if (duplicates.Any())
        {
            string message = $"Datensatz {displayName} hat Dubletten in {fieldName}: {string.Join(", ", duplicates)}";
            LogWarn(message);
        }

        // Bereinigung: Trimmen und Duplikate entfernen
        var cleanedItems = items.Select(x => x.Trim())
                               .Distinct(StringComparer.OrdinalIgnoreCase)
                               .ToArray();
        string cleanedString = string.Join(";", cleanedItems);

        // Nur updaten wenn sich der Wert geändert hat
        if (cleanedString != fieldValue)
        {
            using var updateCmd = new SqliteCommand($"UPDATE Templates SET {fieldName} = @val WHERE TemplateID = @id", connection);
            updateCmd.Parameters.AddWithValue("@val",cleanedString);
            updateCmd.Parameters.AddWithValue("@id",id);
            updateCmd.ExecuteNonQuery();

            string message = $"Datensatz {displayName} {fieldName} bereinigt: \nEntfernte Dubletten: [{string.Join(", ", duplicates)}]\n" +
                           $"vorher: \"{fieldValue}\"\nhinterher \"{cleanedString}\"\n";
            LogWarn(message);

        }
    }

    /// <summary>
    /// Prüft auf Überschneidungen zwischen einem Template-Feld und GeneralDonts.Channel
    /// und entfernt diese Überschneidungen.
    /// </summary>
    /// <param name="displayName">Anzeigename des Datensatzes</param>
    /// <param name="fieldValue">Zu prüfender Feldwert (;-getrennte Liste)</param>
    /// <param name="fieldName">Name des Datenbankfeldes</param>
    /// <param name="id">TemplateID des Datensatzes</param>
    /// <param name="generalDontsChannel">Set mit allen Channel-Werten aus GeneralDonts</param>
    /// <param name="connection">Aktive Datenbankverbindung</param>
    /// <param name="writer">StreamWriter für die Protokollierung</param>
    private static void CheckAndRemoveOverlaps(string displayName,string fieldValue,string fieldName,string id,
        HashSet<string> generalDontsValues,string generalDontsField,SqliteConnection connection,StreamWriter writer)
    {
        // Aufteilen und Trimmen der Einträge
        var items = fieldValue.Split(';', StringSplitOptions.RemoveEmptyEntries)
                            .Select(x => x.Trim());

        // Überschneidungen mit GeneralDonts finden
        var overlaps = items.Where(x => generalDontsValues.Contains(x))
                           .Distinct(StringComparer.OrdinalIgnoreCase)
                           .ToArray();

        // Gefundene Überschneidungen protokollieren
        if (overlaps.Any())
        {
            string message = $"Template {displayName} hat Überschneidungen: {string.Join(", ", overlaps)} zwischen Templates-{fieldName} und General-Donts-{generalDontsField}\n";
            LogWarn(message);
        }

        // Bereinigung: Überschneidungen entfernen
        var cleanedItems = items.Where(x => !generalDontsValues.Contains(x))
                               .Distinct(StringComparer.OrdinalIgnoreCase)
                               .ToArray();
        string cleanedString = string.Join(";", cleanedItems);

        // Nur updaten wenn sich der Wert geändert hat
        if (cleanedString != fieldValue)
        {
            using var updateCmd = new SqliteCommand($"UPDATE Templates SET {fieldName} = @val WHERE TemplateID = @id", connection);
            updateCmd.Parameters.AddWithValue("@val",cleanedString);
            updateCmd.Parameters.AddWithValue("@id",id);
            updateCmd.ExecuteNonQuery();

            string message = $"Template {displayName}: GeneralDonts.{generalDontsField} entfernt aus {fieldName}.\nVorher: '{fieldValue}'\nNachher: '{cleanedString}'\n";
            LogWarn(message);
        }
    }

    /// <summary>
    /// Exportiert das Datenbankschema in Markdown-Format.
    /// Listet alle Tabellen und deren Spalten mit Datentypen auf.
    /// </summary>
    /// <param name="connection">Aktive Datenbankverbindung</param>
    /// <returns>Markdown-formatierte Beschreibung des Datenbankschemas</returns>
    static string GetDatabaseSchemaMarkdown(SqliteConnection connection)
    {
        var sb = new StringBuilder();
        sb.AppendLine("# Datenbankschema\n");

        // Alle Tabellen auflisten
        using var listTables = new SqliteCommand("SELECT name FROM sqlite_master WHERE type='table'", connection);
        using var reader = listTables.ExecuteReader();

        while (reader.Read())
        {
            string tableName = reader.GetString(0);
            sb.AppendLine($"## Tabelle: {tableName}\n");
            sb.AppendLine("| Spalte | Typ |");
            sb.AppendLine("|--------|-----|");

            // Spalteninformationen für jede Tabelle abrufen
            using var pragma = new SqliteCommand($"PRAGMA table_info({tableName})", connection);
            using var colReader = pragma.ExecuteReader();

            while (colReader.Read())
            {
                string colName = colReader["name"].ToString()!;
                string colType = colReader["type"].ToString()!;
                sb.AppendLine($"| {colName} | {colType} |");
            }

            sb.AppendLine();
        }

        return sb.ToString();
    }
}