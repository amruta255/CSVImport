using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace CSVImport;

public sealed class BulkInserter
{
    public async Task<long> BulkInsertAsync(SqlConnection connection, SqlTransaction transaction,
        string tableName, IReadOnlyList<string> columnNames, System.Data.IDataReader reader,
        int batchSize, Action<ImportProgress>? progressCallback)
    {
        var escapedTable = EscapeIdentifier(tableName);
        var destination = $"[dbo].{escapedTable}";
        var stopwatch = Stopwatch.StartNew();

        var countingReader = new CountingDataReader(reader);

        using var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.TableLock, transaction)
        {
            DestinationTableName = destination,
            BatchSize = batchSize,
            BulkCopyTimeout = 0,
            EnableStreaming = true,
            NotifyAfter = batchSize
        };

        foreach (var column in columnNames)
        {
            bulkCopy.ColumnMappings.Add(column, column);
        }

        bulkCopy.SqlRowsCopied += (_, args) =>
        {
            progressCallback?.Invoke(new ImportProgress(args.RowsCopied, stopwatch.Elapsed));
        };

        await bulkCopy.WriteToServerAsync(countingReader);
        stopwatch.Stop();

        return countingReader.RowsRead;
    }

    private static string EscapeIdentifier(string name)
    {
        var escaped = name.Replace("]", "]]", StringComparison.Ordinal);
        return $"[{escaped}]";
    }
}

public readonly record struct ImportProgress(long RowsCopied, TimeSpan Elapsed);

internal sealed class CountingDataReader : System.Data.IDataReader
{
    private readonly System.Data.IDataReader _inner;
    public long RowsRead { get; private set; }

    public CountingDataReader(System.Data.IDataReader inner)
    {
        _inner = inner;
    }

    public bool Read()
    {
        var result = _inner.Read();
        if (result)
        {
            RowsRead++;
        }

        return result;
    }

    public int FieldCount => _inner.FieldCount;
    public object GetValue(int i) => _inner.GetValue(i);
    public string GetName(int i) => _inner.GetName(i);
    public int GetOrdinal(string name) => _inner.GetOrdinal(name);
    public void Dispose() => _inner.Dispose();
    public bool IsClosed => _inner.IsClosed;
    public int RecordsAffected => _inner.RecordsAffected;
    public void Close() => _inner.Close();
    public bool NextResult() => _inner.NextResult();
    public int Depth => _inner.Depth;
    public bool GetBoolean(int i) => _inner.GetBoolean(i);
    public byte GetByte(int i) => _inner.GetByte(i);
    public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length) =>
        _inner.GetBytes(i, fieldOffset, buffer, bufferoffset, length);
    public char GetChar(int i) => _inner.GetChar(i);
    public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length) =>
        _inner.GetChars(i, fieldoffset, buffer, bufferoffset, length);
    public System.Data.IDataReader GetData(int i) => _inner.GetData(i);
    public string GetDataTypeName(int i) => _inner.GetDataTypeName(i);
    public DateTime GetDateTime(int i) => _inner.GetDateTime(i);
    public decimal GetDecimal(int i) => _inner.GetDecimal(i);
    public double GetDouble(int i) => _inner.GetDouble(i);
    public Type GetFieldType(int i) => _inner.GetFieldType(i);
    public float GetFloat(int i) => _inner.GetFloat(i);
    public Guid GetGuid(int i) => _inner.GetGuid(i);
    public short GetInt16(int i) => _inner.GetInt16(i);
    public int GetInt32(int i) => _inner.GetInt32(i);
    public long GetInt64(int i) => _inner.GetInt64(i);
    public string GetString(int i) => _inner.GetString(i);
    public int GetValues(object[] values) => _inner.GetValues(values);
    public bool IsDBNull(int i) => _inner.IsDBNull(i);
    public object this[int i] => _inner[i];
    public object this[string name] => _inner[name];
    public System.Data.DataTable GetSchemaTable() => _inner.GetSchemaTable() ?? new System.Data.DataTable();
}
