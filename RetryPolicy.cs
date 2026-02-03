using System;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace CSVImport;

public static class RetryPolicy
{
    public static async Task ExecuteAsync(Func<Task> action, int maxRetries = 3)
    {
        await ExecuteAsync(async () =>
        {
            await action();
            return true;
        }, maxRetries);
    }

    public static async Task<T> ExecuteAsync<T>(Func<Task<T>> action, int maxRetries = 3)
    {
        var attempt = 0;
        var delay = TimeSpan.FromSeconds(1);
        var jitter = new Random();

        while (true)
        {
            try
            {
                return await action();
            }
            catch (Exception ex) when (IsTransient(ex) && attempt < maxRetries)
            {
                attempt++;
                var jitterMs = jitter.Next(0, 250);
                var wait = delay + TimeSpan.FromMilliseconds(jitterMs);
                await Task.Delay(wait);
                delay = TimeSpan.FromSeconds(Math.Min(delay.TotalSeconds * 2, 30));
            }
        }
    }

    private static bool IsTransient(Exception ex)
    {
        if (ex is SqlException sqlException)
        {
            foreach (SqlError error in sqlException.Errors)
            {
                if (IsTransientErrorNumber(error.Number))
                {
                    return true;
                }
            }
        }

        if (ex is TimeoutException)
        {
            return true;
        }

        return false;
    }

    private static bool IsTransientErrorNumber(int number)
    {
        return number is -2 or 1205 or 4060 or 40197 or 40501 or 40613 or 10928 or 10929 or 49918 or 49919 or 49920;
    }
}
