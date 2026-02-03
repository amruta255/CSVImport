using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace CSVImport;

public sealed class SqlTableManager
{
    private const string DefaultSchema = "dbo";
    private static readonly Regex SimpleIdentifierRegex = new("^[A-Za-z0-9_]+$", RegexOptions.Compiled);

    public static bool IsValidSimpleIdentifier(string? name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            return false;
        }

        return SimpleIdentifierRegex.IsMatch(name);
    }

    public async Task EnsureTableAsync(SqlConnection connection, SqlTransaction transaction,
        string tableName, IReadOnlyList<string> selectedColumns)
    {
        if (!IsValidSimpleIdentifier(tableName))
        {
            throw new InvalidOperationException("Invalid table name.");
        }

        var tableExists = await TableExistsAsync(connection, transaction, tableName);
        if (!tableExists)
        {
            await CreateTableAsync(connection, transaction, tableName, selectedColumns);
        }
        else
        {
            await EnsureColumnsAsync(connection, transaction, tableName, selectedColumns);
        }
    }

    private static async Task<bool> TableExistsAsync(SqlConnection connection, SqlTransaction transaction,
        string tableName)
    {
        const string sql = @"
SELECT 1
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = @schema AND t.name = @table;";

        await using var command = new SqlCommand(sql, connection, transaction)
        {
            CommandTimeout = 30
        };
        command.Parameters.AddWithValue("@schema", DefaultSchema);
        command.Parameters.AddWithValue("@table", tableName);

        var result = await command.ExecuteScalarAsync();
        return result != null;
    }

    private static async Task CreateTableAsync(SqlConnection connection, SqlTransaction transaction,
        string tableName, IReadOnlyList<string> selectedColumns)
    {
        var escapedTable = EscapeIdentifier(tableName);
        var columnDefinitions = new List<string>
        {
            "[Id] INT IDENTITY(1,1) PRIMARY KEY"
        };

        foreach (var column in selectedColumns)
        {
            var escapedColumn = EscapeIdentifier(column);
            columnDefinitions.Add($"{escapedColumn} NVARCHAR(MAX) NULL");
        }

        var constraintName = EscapeIdentifier($"DF_{tableName}_ImportedAtUtc");
        columnDefinitions.Add($"[ImportedAtUtc] DATETIME2 NOT NULL CONSTRAINT {constraintName} DEFAULT SYSUTCDATETIME()");

        var sql = $"CREATE TABLE {WrapSchema(DefaultSchema, escapedTable)} (\n    {string.Join(",\n    ", columnDefinitions)}\n);";

        await using var command = new SqlCommand(sql, connection, transaction)
        {
            CommandTimeout = 0
        };

        await command.ExecuteNonQueryAsync();
    }

    private static async Task EnsureColumnsAsync(SqlConnection connection, SqlTransaction transaction,
        string tableName, IReadOnlyList<string> selectedColumns)
    {
        var existingColumns = await GetExistingColumnsAsync(connection, transaction, tableName);
        var additions = new List<string>();

        foreach (var column in selectedColumns)
        {
            if (!existingColumns.Contains(column))
            {
                additions.Add($"{EscapeIdentifier(column)} NVARCHAR(MAX) NULL");
            }
        }

        if (!existingColumns.Contains("ImportedAtUtc"))
        {
            var constraintName = EscapeIdentifier($"DF_{tableName}_ImportedAtUtc");
            additions.Add($"[ImportedAtUtc] DATETIME2 NOT NULL CONSTRAINT {constraintName} DEFAULT SYSUTCDATETIME()");
        }

        if (additions.Count == 0)
        {
            return;
        }

        var sql = $"ALTER TABLE {WrapSchema(DefaultSchema, EscapeIdentifier(tableName))} ADD {string.Join(", ", additions)};";
        await using var command = new SqlCommand(sql, connection, transaction)
        {
            CommandTimeout = 0
        };

        await command.ExecuteNonQueryAsync();
    }

    private static async Task<HashSet<string>> GetExistingColumnsAsync(SqlConnection connection,
        SqlTransaction transaction, string tableName)
    {
        const string sql = @"
SELECT c.name
FROM sys.columns c
JOIN sys.tables t ON c.object_id = t.object_id
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = @schema AND t.name = @table;";

        await using var command = new SqlCommand(sql, connection, transaction)
        {
            CommandTimeout = 30
        };
        command.Parameters.AddWithValue("@schema", DefaultSchema);
        command.Parameters.AddWithValue("@table", tableName);

        var columns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            columns.Add(reader.GetString(0));
        }

        return columns;
    }

    private static string EscapeIdentifier(string name)
    {
        var escaped = name.Replace("]", "]]", StringComparison.Ordinal);
        return $"[{escaped}]";
    }

    private static string WrapSchema(string schema, string escapedTableName)
    {
        return $"[{schema}].{escapedTableName}";
    }
}
