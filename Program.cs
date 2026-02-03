using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace CSVImport;

internal static class Program
{
    private const int DefaultBatchSize = 5000;

    private static async Task<int> Main(string[] args)
    {
        PrintIntro();

        var csvPath = Prompt("CSV file path", "C:\\data\\input.csv");
        if (string.IsNullOrWhiteSpace(csvPath) || !File.Exists(csvPath))
        {
            Console.WriteLine("Error: CSV file not found.");
            return 1;
        }

        var connectionSettings = TryReadConnectionSettings("appsettings.json");
        var serverName = PromptWithDefault("SQL Server name", "localhost", connectionSettings?.Server);
        var databaseName = PromptWithDefault("Database name", "MyDb", connectionSettings?.Database);
        var userName = PromptWithDefault("SQL username", "sa", connectionSettings?.User);
        var password = PromptPassword("SQL password", connectionSettings?.Password);

        if (string.IsNullOrWhiteSpace(serverName) || string.IsNullOrWhiteSpace(databaseName) ||
            string.IsNullOrWhiteSpace(userName) || string.IsNullOrWhiteSpace(password))
        {
            Console.WriteLine("Error: Server, database, username, and password are required.");
            return 1;
        }

        var connectionString = BuildConnectionString(serverName, databaseName, userName, password);

        var targetTableName = Prompt("Target table name (letters/numbers/underscore only)", "ImportedData");
        if (!SqlTableManager.IsValidSimpleIdentifier(targetTableName))
        {
            Console.WriteLine("Error: Invalid table name. Use letters, numbers, and underscore only.");
            return 1;
        }

        var columnsInput = Prompt("Comma-separated list of columns to extract", "FirstName,LastName,Email");
        var requestedColumns = ParseColumnList(columnsInput);
        if (requestedColumns.Count == 0)
        {
            Console.WriteLine("Error: At least one column name is required.");
            return 1;
        }

        try
        {
            var csvImporter = new CsvImporter(csvPath);
            var headers = csvImporter.ReadHeaders();
            Console.WriteLine("Detected headers:");
            Console.WriteLine(string.Join(", ", headers));

            var selection = csvImporter.ResolveSelectedColumns(requestedColumns);
            Console.WriteLine("Selected columns:");
            Console.WriteLine(string.Join(", ", selection.SelectedHeaders));

            var batchSize = DefaultBatchSize;
            var tableManager = new SqlTableManager();
            var bulkInserter = new BulkInserter();

            var stopwatch = Stopwatch.StartNew();
            long totalInserted = 0;

            totalInserted = await RetryPolicy.ExecuteAsync(async () =>
            {
                await using var connection = new SqlConnection(connectionString);
                await RetryPolicy.ExecuteAsync(async () =>
                {
                    await connection.OpenAsync();
                });

                await using var transaction = connection.BeginTransaction();
                try
                {
                    await tableManager.EnsureTableAsync(connection, transaction, targetTableName,
                        selection.SelectedHeaders);

                    using var reader = csvImporter.CreateDataReader(selection.SelectedIndices,
                        selection.SelectedHeaders);

                    long inserted = await bulkInserter.BulkInsertAsync(connection, transaction, targetTableName,
                        selection.SelectedHeaders, reader, batchSize, progress =>
                        {
                            Console.WriteLine(
                                $"Rows copied: {progress.RowsCopied:n0} | Elapsed: {progress.Elapsed:hh\\:mm\\:ss}");
                        });

                    await transaction.CommitAsync();
                    return inserted;
                }
                catch
                {
                    await transaction.RollbackAsync();
                    throw;
                }
            });

            stopwatch.Stop();
            Console.WriteLine($"Import complete. Rows inserted: {totalInserted:n0}.");
            Console.WriteLine($"Total duration: {stopwatch.Elapsed:hh\\:mm\\:ss}.");

            return 0;
        }
        catch (CsvImporterException ex)
        {
            Console.WriteLine($"CSV error: {ex.Message}");
            return 1;
        }
        catch (SqlException ex)
        {
            Console.WriteLine($"SQL error: {ex.Message}");
            return 1;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Unexpected error: {ex.Message}");
            return 1;
        }
    }

    private static void PrintIntro()
    {
        Console.WriteLine("CSV to SQL Server Importer (.NET 8)");
        Console.WriteLine("- CSV first row is treated as headers");
        Console.WriteLine("- Select a subset of columns to import");
        Console.WriteLine("- Table created if missing (NVARCHAR(MAX) + Id + ImportedAtUtc)");
        Console.WriteLine();
        Console.WriteLine("Connection string guidance (recommended settings):");
        Console.WriteLine("  - Connect Timeout=30 (or higher)");
        Console.WriteLine("  - Encrypt=True; TrustServerCertificate=True (for dev/local)");
        Console.WriteLine("  - Use appropriate authentication for your environment");
        Console.WriteLine();
        Console.WriteLine("Example CSV path: C:\\data\\input.csv");
        Console.WriteLine("Example columns: FirstName,LastName,Email");
        Console.WriteLine();
    }

    private static string Prompt(string label, string example)
    {
        Console.Write($"{label} (e.g., {example}): ");
        return Console.ReadLine()?.Trim() ?? string.Empty;
    }

    private static string PromptWithDefault(string label, string example, string? defaultValue)
    {
        if (!string.IsNullOrWhiteSpace(defaultValue))
        {
            Console.Write($"{label} (default: {defaultValue}): ");
            var input = Console.ReadLine()?.Trim();
            return string.IsNullOrWhiteSpace(input) ? defaultValue : input;
        }

        return Prompt(label, example);
    }

    private static string PromptPassword(string label, string? defaultValue)
    {
        if (!string.IsNullOrWhiteSpace(defaultValue))
        {
            Console.Write($"{label} (press Enter to use stored value): ");
            var input = Console.ReadLine();
            return string.IsNullOrWhiteSpace(input) ? defaultValue : input.Trim();
        }

        Console.Write($"{label}: ");
        return Console.ReadLine()?.Trim() ?? string.Empty;
    }

    private static List<string> ParseColumnList(string input)
    {
        var results = new List<string>();
        if (string.IsNullOrWhiteSpace(input))
        {
            return results;
        }

        var parts = input.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        foreach (var part in parts)
        {
            var trimmed = part.Trim();
            if (!string.IsNullOrWhiteSpace(trimmed))
            {
                results.Add(trimmed);
            }
        }

        return results;
    }

    private static ConnectionSettings? TryReadConnectionSettings(string path)
    {
        if (!File.Exists(path))
        {
            return null;
        }

        try
        {
            var json = File.ReadAllText(path);
            using var doc = JsonDocument.Parse(json);
            if (doc.RootElement.TryGetProperty("SqlConnection", out var section))
            {
                return new ConnectionSettings
                {
                    Server = section.GetProperty("Server").GetString(),
                    Database = section.GetProperty("Database").GetString(),
                    User = section.GetProperty("User").GetString(),
                    Password = section.GetProperty("Password").GetString()
                };
            }
        }
        catch
        {
            return null;
        }

        return null;
    }

    private static string BuildConnectionString(string server, string database, string user, string password)
    {
        var builder = new SqlConnectionStringBuilder
        {
            DataSource = server,
            InitialCatalog = database,
            UserID = user,
            Password = password,
            Encrypt = true,
            TrustServerCertificate = true,
            ConnectTimeout = 30
        };

        return builder.ConnectionString;
    }

    private sealed class ConnectionSettings
    {
        public string? Server { get; init; }
        public string? Database { get; init; }
        public string? User { get; init; }
        public string? Password { get; init; }
    }
}
