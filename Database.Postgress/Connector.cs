using System;
using System.Collections.Generic;
using System.Data;
using Npgsql;

namespace Database.Postgress
{
    public class Connector
    {
        // property
        public int TimeOut { get; set; }
        public string ConnectionString { get; set; }
        // Constructor
        public Connector()
        {
            this.TimeOut = 0;
        }
        public Connector(string connectonString)
        {
            this.TimeOut = 0;
            this.ConnectionString = connectonString;
        }
        // Method
        public int GetInt(string Sql)
        {
            int oINT = 0;
            using (var connection = new NpgsqlConnection(this.ConnectionString)) {
                connection.Open();
                NpgsqlCommand command = new NpgsqlCommand(Sql, connection);
                if (TimeOut != 0) command.CommandTimeout = TimeOut;
                oINT = Convert.ToInt32(command.ExecuteScalar());
                connection.Close();
                connection.Dispose();
                command.Dispose();
            }
            return oINT;
        }
        public string GetString(string Sql)
        {
            string strVal = "";
            using (var connection = new NpgsqlConnection(this.ConnectionString)) {
                connection.Open();
                NpgsqlCommand command = new NpgsqlCommand(Sql, connection);
                if (TimeOut != 0 || !string.IsNullOrEmpty(TimeOut.ToString())) command.CommandTimeout = TimeOut;
                strVal = Convert.ToString(command.ExecuteScalar());
                connection.Close();
            }
            return strVal;
        }
        public DataTable GetDataTable(string Sql)
        {
            DataSet oDS = new DataSet();
            try {
                using (var connection = new NpgsqlConnection(this.ConnectionString)) {
                    connection.Open();
                    NpgsqlCommand command = new NpgsqlCommand(Sql, connection);
                    if (TimeOut != 0 || !string.IsNullOrEmpty(TimeOut.ToString())) command.CommandTimeout = TimeOut;
                    NpgsqlDataAdapter dataAdapter = new NpgsqlDataAdapter(command);
                    dataAdapter.Fill(oDS);
                    command.Dispose();
                    dataAdapter.Dispose();
                    connection.Close();
                }
                return oDS.Tables[0];
            } catch (Exception ex) {
                throw ex;
            }
        }
        public DataSet GetDataSet(string Sql)
        {
            DataSet oDS = new DataSet();
            try {
                using (var connection = new NpgsqlConnection(this.ConnectionString)) {
                    connection.Open();
                    NpgsqlCommand command = new NpgsqlCommand(Sql, connection);
                    if (TimeOut != 0 || !string.IsNullOrEmpty(TimeOut.ToString())) command.CommandTimeout = TimeOut;
                    NpgsqlDataAdapter dataAdapter = new NpgsqlDataAdapter(command);
                    dataAdapter.Fill(oDS);
                    command.Dispose();
                    dataAdapter.Dispose();
                    connection.Close();
                }
                return oDS;
            } catch (Exception ex) {
                throw ex;
            }
        }
        public bool Exucute(string Sql)
        {
            using (NpgsqlConnection connection = new NpgsqlConnection(this.ConnectionString)) {
                connection.Open();
                NpgsqlCommand command = connection.CreateCommand();
                NpgsqlTransaction transaction;
                if (TimeOut != 0) command.CommandTimeout = TimeOut;
                transaction = connection.BeginTransaction();
                command.Connection = connection;
                command.Transaction = transaction;

                try {
                    command.CommandText = Sql.ToString();
                    command.ExecuteNonQuery();
                    transaction.Commit();
                    connection.Close();
                    transaction.Dispose();
                    command.Dispose();
                } catch (Exception ex) {
                    transaction.Rollback();
                    connection.Close();
                    transaction.Dispose();
                    command.Dispose();
                    throw ex;
                }
            }
            return true;
        }
        public bool Exucute(List<string> Sql)
        {
            bool IsCommit = false;
            using (NpgsqlConnection connection = new NpgsqlConnection(this.ConnectionString)) {
                NpgsqlTransaction transaction;
                NpgsqlCommand command = connection.CreateCommand();
                if (TimeOut != 0) command.CommandTimeout = TimeOut;
                transaction = connection.BeginTransaction();
                connection.Open();
                command.Connection = connection;
                command.Transaction = transaction;
                foreach (string sql in Sql) {
                    try {
                        command.CommandText = Sql.ToString();
                        command.ExecuteNonQuery();
                        IsCommit = true;
                    } catch (Exception ex) {
                        IsCommit = false;
                        transaction.Rollback();
                        throw ex;
                    }
                }
                transaction.Commit();
                transaction.Dispose();
                connection.Close();
                command.Dispose();
            }

            return IsCommit;
        }
        public bool Exucute(NpgsqlCommand NpgsqlCommand)
        {
            using (NpgsqlConnection connection = new NpgsqlConnection(this.ConnectionString)) {
                connection.Open();
                NpgsqlCommand = connection.CreateCommand();
                NpgsqlTransaction transaction;
                if (TimeOut != 0) NpgsqlCommand.CommandTimeout = TimeOut;
                transaction = connection.BeginTransaction();
                NpgsqlCommand.Connection = connection;
                NpgsqlCommand.Transaction = transaction;
                try {
                    NpgsqlCommand.ExecuteNonQuery();
                    transaction.Commit();
                    connection.Close();
                    transaction.Dispose();
                    NpgsqlCommand.Dispose();
                } catch (Exception ex) {
                    transaction.Rollback();
                    connection.Close();
                    transaction.Dispose();
                    NpgsqlCommand.Dispose();
                    throw ex;
                }
            }
            return true;
        }
        public bool Exucute(List<NpgsqlCommand> NpgsqlCommand)
        {
            using (NpgsqlConnection connection = new NpgsqlConnection(this.ConnectionString)) {
                try {
                    connection.Open();
                    foreach (NpgsqlCommand oCM in NpgsqlCommand) {
                        if (TimeOut != 0) oCM.CommandTimeout = TimeOut;
                        oCM.Connection = connection;
                        oCM.ExecuteNonQuery();
                        oCM.Dispose();
                    }
                    connection.Close();
                } catch (Exception ex) {
                    throw ex;
                }
            }
            return true;
        }

        public void SetConnectionString(string server, int port, string schema, string username, string password)
        {
            this.ConnectionString = string.Format(@"Server={0};Port={1};Database={2};User Id={3};Password={4}", server, port, schema, username, password);
        }
    }
}
