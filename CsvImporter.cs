using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace CSVImport;

public sealed class CsvImporter
{
    private readonly string _filePath;

    public CsvImporter(string filePath)
    {
        _filePath = filePath ?? throw new ArgumentNullException(nameof(filePath));
    }

    public IReadOnlyList<string> ReadHeaders()
    {
        using var reader = new StreamReader(_filePath);
        var parser = new CsvParser(reader);
        var row = parser.ReadRow();
        if (row == null)
        {
            throw new CsvImporterException("CSV file is empty.");
        }

        var headers = row.Select((h, index) =>
        {
            var trimmed = (h ?? string.Empty).Trim();
            if (index == 0)
            {
                trimmed = trimmed.TrimStart('\uFEFF');
            }

            return trimmed;
        }).ToList();

        if (headers.Count == 0 || headers.All(string.IsNullOrWhiteSpace))
        {
            throw new CsvImporterException("CSV header row is empty.");
        }

        if (headers.Any(string.IsNullOrWhiteSpace))
        {
            throw new CsvImporterException("CSV header row contains empty column names.");
        }

        var duplicate = headers
            .GroupBy(h => h, StringComparer.OrdinalIgnoreCase)
            .FirstOrDefault(g => g.Count() > 1);

        if (duplicate != null)
        {
            throw new CsvImporterException(
                $"Duplicate header name detected: '{duplicate.Key}'. Please fix the CSV header row.");
        }

        return headers;
    }

    public ColumnSelection ResolveSelectedColumns(IReadOnlyList<string> requestedColumns)
    {
        var headers = ReadHeaders();
        var headerLookup = headers
            .Select((header, index) => new { header, index })
            .ToDictionary(x => x.header, x => x.index, StringComparer.OrdinalIgnoreCase);

        var selectedIndices = new List<int>();
        var selectedHeaders = new List<string>();

        foreach (var requested in requestedColumns)
        {
            var trimmed = requested.Trim();
            if (!headerLookup.TryGetValue(trimmed, out var index))
            {
                var available = string.Join(", ", headers);
                throw new CsvImporterException(
                    $"Requested column '{requested}' not found. Available columns: {available}");
            }

            if (selectedIndices.Contains(index))
            {
                continue;
            }

            selectedIndices.Add(index);
            selectedHeaders.Add(headers[index]);
        }

        if (selectedIndices.Count == 0)
        {
            throw new CsvImporterException("No valid columns were selected.");
        }

        return new ColumnSelection(selectedIndices, selectedHeaders);
    }

    public CsvDataReader CreateDataReader(IReadOnlyList<int> selectedIndices, IReadOnlyList<string> selectedHeaders)
    {
        return new CsvDataReader(_filePath, selectedIndices, selectedHeaders);
    }
}

public sealed class ColumnSelection
{
    public ColumnSelection(IReadOnlyList<int> selectedIndices, IReadOnlyList<string> selectedHeaders)
    {
        SelectedIndices = selectedIndices;
        SelectedHeaders = selectedHeaders;
    }

    public IReadOnlyList<int> SelectedIndices { get; }
    public IReadOnlyList<string> SelectedHeaders { get; }
}

public sealed class CsvImporterException : Exception
{
    public CsvImporterException(string message) : base(message)
    {
    }
}

internal sealed class CsvParser
{
    private readonly TextReader _reader;

    public CsvParser(TextReader reader)
    {
        _reader = reader;
    }

    public string[]? ReadRow()
    {
        var record = new List<string>();
        var field = new System.Text.StringBuilder();
        var inQuotes = false;
        var anyChar = false;

        while (true)
        {
            var next = _reader.Read();
            if (next == -1)
            {
                if (inQuotes)
                {
                    throw new CsvImporterException("CSV ended inside a quoted field.");
                }

                if (!anyChar && record.Count == 0 && field.Length == 0)
                {
                    return null;
                }

                record.Add(field.ToString());
                return record.ToArray();
            }

            anyChar = true;
            var ch = (char)next;

            if (inQuotes)
            {
                if (ch == '"')
                {
                    var peek = _reader.Peek();
                    if (peek == '"')
                    {
                        _reader.Read();
                        field.Append('"');
                    }
                    else
                    {
                        inQuotes = false;
                    }
                }
                else
                {
                    field.Append(ch);
                }

                continue;
            }

            if (ch == '"')
            {
                inQuotes = true;
            }
            else if (ch == ',')
            {
                record.Add(field.ToString());
                field.Clear();
            }
            else if (ch == '\r')
            {
                var peek = _reader.Peek();
                if (peek == '\n')
                {
                    _reader.Read();
                }

                record.Add(field.ToString());
                return record.ToArray();
            }
            else if (ch == '\n')
            {
                record.Add(field.ToString());
                return record.ToArray();
            }
            else
            {
                field.Append(ch);
            }
        }
    }
}

public sealed class CsvDataReader : System.Data.IDataReader
{
    private readonly StreamReader _reader;
    private readonly CsvParser _parser;
    private readonly IReadOnlyList<int> _selectedIndices;
    private readonly IReadOnlyList<string> _selectedHeaders;
    private string[]? _currentRow;
    private bool _isClosed;

    public CsvDataReader(string filePath, IReadOnlyList<int> selectedIndices, IReadOnlyList<string> selectedHeaders)
    {
        _reader = new StreamReader(filePath);
        _parser = new CsvParser(_reader);
        _selectedIndices = selectedIndices;
        _selectedHeaders = selectedHeaders;

        _parser.ReadRow(); // Skip header row
    }

    public bool Read()
    {
        _currentRow = _parser.ReadRow();
        return _currentRow != null;
    }

    public int FieldCount => _selectedIndices.Count;

    public object GetValue(int i)
    {
        if (_currentRow == null)
        {
            throw new InvalidOperationException("No current row.");
        }

        var index = _selectedIndices[i];
        if (index < 0 || index >= _currentRow.Length)
        {
            return DBNull.Value;
        }

        var value = _currentRow[index];
        return string.IsNullOrEmpty(value) ? DBNull.Value : value;
    }

    public string GetName(int i) => _selectedHeaders[i];

    public int GetOrdinal(string name)
    {
        for (var i = 0; i < _selectedHeaders.Count; i++)
        {
            if (string.Equals(_selectedHeaders[i], name, StringComparison.OrdinalIgnoreCase))
            {
                return i;
            }
        }

        return -1;
    }

    public void Dispose()
    {
        if (_isClosed)
        {
            return;
        }

        _reader.Dispose();
        _isClosed = true;
    }

    public bool IsClosed => _isClosed;
    public int RecordsAffected => -1;
    public void Close() => Dispose();
    public bool NextResult() => false;
    public int Depth => 0;

    public bool GetBoolean(int i) => Convert.ToBoolean(GetValue(i));
    public byte GetByte(int i) => Convert.ToByte(GetValue(i));
    public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length) =>
        throw new NotSupportedException();
    public char GetChar(int i) => Convert.ToChar(GetValue(i));
    public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length) =>
        throw new NotSupportedException();
    public System.Data.IDataReader GetData(int i) => throw new NotSupportedException();
    public string GetDataTypeName(int i) => "nvarchar";
    public DateTime GetDateTime(int i) => Convert.ToDateTime(GetValue(i));
    public decimal GetDecimal(int i) => Convert.ToDecimal(GetValue(i));
    public double GetDouble(int i) => Convert.ToDouble(GetValue(i));
    public Type GetFieldType(int i) => typeof(string);
    public float GetFloat(int i) => Convert.ToSingle(GetValue(i));
    public Guid GetGuid(int i) => Guid.Parse(GetString(i));
    public short GetInt16(int i) => Convert.ToInt16(GetValue(i));
    public int GetInt32(int i) => Convert.ToInt32(GetValue(i));
    public long GetInt64(int i) => Convert.ToInt64(GetValue(i));
    public string GetString(int i) => GetValue(i)?.ToString() ?? string.Empty;
    public int GetValues(object[] values)
    {
        var count = Math.Min(values.Length, FieldCount);
        for (var i = 0; i < count; i++)
        {
            values[i] = GetValue(i);
        }

        return count;
    }
    public bool IsDBNull(int i) => GetValue(i) == DBNull.Value;

    public object this[int i] => GetValue(i);
    public object this[string name] => GetValue(GetOrdinal(name));
    public System.Data.DataTable GetSchemaTable() => throw new NotSupportedException();
}
