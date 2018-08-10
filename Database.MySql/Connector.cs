using System;
using System.Collections.Generic;
using System.Data;
using MySql.Data.MySqlClient;

namespace Database.MySql
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
            using (var connection = new MySqlConnection(this.ConnectionString)) {
                connection.Open();
                MySqlCommand command = new MySqlCommand(Sql, connection);
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
            using (var connection = new MySqlConnection(this.ConnectionString)) {
                connection.Open();
                MySqlCommand command = new MySqlCommand(Sql, connection);
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
                using (var connection = new MySqlConnection(this.ConnectionString)) {
                    connection.Open();
                    MySqlCommand command = new MySqlCommand(Sql, connection);
                    if (TimeOut != 0 || !string.IsNullOrEmpty(TimeOut.ToString())) command.CommandTimeout = TimeOut;
                    MySqlDataAdapter dataAdapter = new MySqlDataAdapter(command);
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
                using (var connection = new MySqlConnection(this.ConnectionString)) {
                    connection.Open();
                    MySqlCommand command = new MySqlCommand(Sql, connection);
                    if (TimeOut != 0 || !string.IsNullOrEmpty(TimeOut.ToString())) command.CommandTimeout = TimeOut;
                    MySqlDataAdapter dataAdapter = new MySqlDataAdapter(command);
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
            using (MySqlConnection connection = new MySqlConnection(this.ConnectionString)) {
                connection.Open();
                MySqlCommand command = connection.CreateCommand();
                MySqlTransaction transaction;
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
            using (MySqlConnection connection = new MySqlConnection(this.ConnectionString)) {
                MySqlTransaction transaction;
                MySqlCommand command = connection.CreateCommand();
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
        public bool Exucute(MySqlCommand MySqlCommand)
        {
            using (MySqlConnection connection = new MySqlConnection(this.ConnectionString)) {
                connection.Open();
                MySqlCommand = connection.CreateCommand();
                MySqlTransaction transaction;
                if (TimeOut != 0) MySqlCommand.CommandTimeout = TimeOut;
                transaction = connection.BeginTransaction();
                MySqlCommand.Connection = connection;
                MySqlCommand.Transaction = transaction;
                try {
                    MySqlCommand.ExecuteNonQuery();
                    transaction.Commit();
                    connection.Close();
                    transaction.Dispose();
                    MySqlCommand.Dispose();
                } catch (Exception ex) {
                    transaction.Rollback();
                    connection.Close();
                    transaction.Dispose();
                    MySqlCommand.Dispose();
                    throw ex;
                }
            }
            return true;
        }
        public bool Exucute(List<MySqlCommand> MySqlCommand)
        {
            using (MySqlConnection connection = new MySqlConnection(this.ConnectionString)) {
                try {
                    connection.Open();
                    foreach (MySqlCommand oCM in MySqlCommand) {
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
            this.ConnectionString = string.Format(@"Datasource={0};Port={1};Database={2};uid={3};pwd={4};", server, port, schema, username, password);
        }
        public void SetConnectionString(string server, string schema, string username, string password)
        {
            this.ConnectionString = string.Format(@"Datasource={0};Database={1};uid={2};pwd={3};", server, schema, username, password);
        }
    }
}
