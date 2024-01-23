using Microsoft.Data.SqlClient;

namespace RootNine.Persistence
{
    /// <summary>
    /// Datenbankverbindung zum lokalen SQL-Server.
    /// </summary>
    public class LocalDbConnectionFactory : IDisposable
    {
        private const string _localDbName = "MSSQLLocalDB";
        private readonly string _databaseName;
        private readonly string _databaseFile;
        private readonly string _connectionString;

        public LocalDbConnectionFactory()
        {
            _databaseName = "test_" + Guid.NewGuid().ToString("N");
            _databaseFile = Path.Combine(Path.GetTempPath(), _databaseName + ".mdf");
            _connectionString = $@"Data Source=(localdb)\{_localDbName};Initial Catalog={_databaseName};Integrated Security=True";

            CreateDatabase();
        }

        public void Dispose()
        {
            DropDatabase();
        }

        public SqlConnection CreateSqlConnection()
        {
            var result = new SqlConnection(_connectionString);
            return result;
        }

        public string ConnectionString => _connectionString;

        private void CreateDatabase()
        {
            string connectionString = $@"Data Source=(localdb)\{_localDbName};Initial Catalog=master;Integrated Security=True";
            using (var connection = new SqlConnection(connectionString))
            {
                SqlCommand cmd = connection.CreateCommand();
                cmd.CommandText = $"CREATE DATABASE {_databaseName} ON (NAME = N'{_databaseName}', FILENAME = '{_databaseFile}')";

                connection.Open();
                cmd.ExecuteNonQuery();

                cmd = connection.CreateCommand();
                cmd.CommandText = $"ALTER DATABASE {_databaseName} SET READ_COMMITTED_SNAPSHOT ON WITH ROLLBACK IMMEDIATE";
                cmd.ExecuteNonQuery();
                cmd = connection.CreateCommand();
                cmd.CommandText = $"ALTER DATABASE {_databaseName} SET ALLOW_SNAPSHOT_ISOLATION ON";
                cmd.ExecuteNonQuery();
            }
        }

        private void DoWithRetry(Action a, int retries)
        {
            var random = new Random();
            int tryCount = 0;
            while (true)
            {
                try
                {
                    a();
                    return;
                }
                catch
                {
                    if (tryCount >= retries)
                        throw;
                    tryCount++;
                    Thread.Sleep(random.Next(300));
                }
            }
        }

        private void DropDatabase()
        {
            try
            {
                DropDatabase(_databaseName, true);
            }
            catch
            {
                // Es ist kein Problem, wenn die Test-DB nicht aufgeräumt werden kann. Da diese im Temp-Verzeichnis liegt wird Windows diese DB nach einem Neustart hoffentlich irgendwann aufräumen.
            }
        }

        public void DropDatabase(string databaseName, bool autoRollback)
        {
            var connectionString = $@"Data Source=(localdb)\{_localDbName};Initial Catalog=master;Integrated Security=True";
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();

                var cmd = connection.CreateCommand();
                cmd.CommandText = $"ALTER DATABASE [{databaseName}] SET OFFLINE";
                if (autoRollback)
                    cmd.CommandText += " WITH ROLLBACK IMMEDIATE";
                else
                    cmd.CommandText += " WITH NO_WAIT";
                cmd.ExecuteNonQuery();


                cmd = connection.CreateCommand();
                // TryDetachDatabase(databaseName);
                cmd.CommandText = $"DROP DATABASE {databaseName}";
                cmd.ExecuteNonQuery();
            }
            File.Delete(_databaseFile);
        }
    }
}
