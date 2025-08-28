using Microsoft.Data.Sqlite;
using Microsoft.VisualBasic;
using System;
using System.Data.Common;
using System.Data.SQLite;
using System.IO;
using System.Text;

class Program
{
    static void Main()
    {
        string dbPath = "EPG.sqlite";
        using var connection = new SqliteConnection($"Data Source={dbPath}");
        connection.Open();

        Console.WriteLine("Verbindung erfolgreich hergestellt.\n");

        string schemaMarkdown = GetDatabaseSchemaMarkdown(connection);
        File.WriteAllText("schema.md",schemaMarkdown,Encoding.UTF8);

        Console.WriteLine("Schema wurde in 'schema.md' exportiert.\n");
        LookForDoubles(connection);
    }

    static void LookForDoubles(Microsoft.Data.Sqlite.SqliteConnection connection)
    {
       
        using var cmd = new Microsoft.Data.Sqlite.SqliteCommand("SELECT CASE WHEN Name IS NOT NULL AND Name <> '' THEN Name ELSE Template END AS DisplayName, Donts,TemplateID FROM Templates;", connection);
        using var reader = cmd.ExecuteReader();
        string outputFile = "duplicates.txt";
        using var writer = new StreamWriter(outputFile, false);

        while (reader.Read())
        {
            string displayName = reader.GetString(0);
            string feld = reader.IsDBNull(1) ? "" : reader.GetString(1);
            string id = reader.GetInt32(2).ToString();

            var items = feld.Split(';', StringSplitOptions.RemoveEmptyEntries);
            var duplicates = items.GroupBy(x => x.Trim())
                  .Where(g => g.Count() > 1)
                  .Select(g => g.Key);

            if (duplicates.Any())
            {
                string message =$"Datensatz {displayName} hat Dubletten: {string.Join(", ",duplicates)}";
                Console.WriteLine(message);
                writer.WriteLine(message);
            }

            // Liste splitten, trimmen, Duplikate entfernen und sortieren
            var cleaned = feld.Split(';', StringSplitOptions.RemoveEmptyEntries)
                              .Select(x => x.Trim())
                              .Distinct(StringComparer.OrdinalIgnoreCase) // case-insensitive
                              //.OrderBy(x => x)                            // optional sortieren
                              .ToArray();

            string cleanedString = string.Join(";", cleaned);

            // Nur updaten, wenn sich was geändert hat
            if (cleanedString != feld)
            {
                using var updateCmd = new Microsoft.Data.Sqlite.SqliteCommand(
                    $"UPDATE Templates SET Donts = @val WHERE TemplateID = @id;", connection);
                updateCmd.Parameters.AddWithValue("@val",cleanedString);
                updateCmd.Parameters.AddWithValue("@id",id);
                updateCmd.ExecuteNonQuery();

                string message =$"Datensatz {id} bereinigt: {cleanedString}";
                Console.WriteLine(message);
                writer.WriteLine(message);
            }

        }

        // 1. Alle GeneralDonts in ein HashSet laden (schnelle Nachschlage-Struktur)
        var generalDonts = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        using (var cmd2 = new SqliteCommand("SELECT Template FROM GeneralDonts;",connection))
        using (var reader2 = cmd2.ExecuteReader())
        {
            while (reader2.Read())
            {
                if (!reader2.IsDBNull(0))
                {
                    string val = reader2.GetString(0).Trim();
                    if (!string.IsNullOrEmpty(val))
                        generalDonts.Add(val);
                }
            }
        }

        Console.WriteLine($"GeneralDonts geladen: {generalDonts.Count} Werte\n");
        writer.WriteLine($"GeneralDonts geladen: {generalDonts.Count} Werte\n");

        // 2. Templates.Donts prüfen
        using var selectCmd = new SqliteCommand("SELECT CASE WHEN Name IS NOT NULL AND Name <> '' THEN Name ELSE Template END AS DisplayName, Donts FROM Templates;", connection);
        using var tReader = selectCmd.ExecuteReader();

        while (tReader.Read())
        {
            string id = tReader.GetString(0);
            string donts = tReader.IsDBNull(1) ? "" : tReader.GetString(1);

            var items = donts.Split(';', StringSplitOptions.RemoveEmptyEntries)
                             .Select(x => x.Trim());

            // Schnittmenge finden
            var duplicates = items.Where(x => generalDonts.Contains(x)).ToList();

            if (duplicates.Any())
            {
                string message = $"Template {id} hat Überschneidungen: {string.Join(", ", duplicates)}";
                Console.WriteLine(message);
                writer.WriteLine(message);
            }
        }

        Console.WriteLine($"\nFertig ✅ Ergebnisse in '{outputFile}' gespeichert.");

        //connection.Open();
        //using var updateCmd2 = new Microsoft.Data.Sqlite.SqliteCommand("VACUUM");
        //updateCmd2.ExecuteNonQuery();

        //using var updateCmd3 = new Microsoft.Data.Sqlite.SqliteCommand("PRAGMA optimize");
        //updateCmd3.ExecuteNonQuery();
    }

    static string GetDatabaseSchemaMarkdown(SqliteConnection connection)
    {
        var sb = new StringBuilder();
        sb.AppendLine("# Datenbankschema\n");

        using var listTables = new SqliteCommand("SELECT name FROM sqlite_master WHERE type='table';", connection);
        using var reader = listTables.ExecuteReader();

        while (reader.Read())
        {
            string tableName = reader.GetString(0);
            sb.AppendLine($"## Tabelle: {tableName}\n");
            sb.AppendLine("| Spalte | Typ |");
            sb.AppendLine("|--------|-----|");

            using var pragma = new SqliteCommand($"PRAGMA table_info({tableName});", connection);
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