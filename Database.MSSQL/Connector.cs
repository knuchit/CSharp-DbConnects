using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Data;

namespace Database.MSSql
{
    public class Connector
    {
        // property
        public int TimeOut { get; set; }
        public string ConnectionString { get; set; }
        // Constructor
        public Connector(string connectonString)
        {
            this.TimeOut = 0;
            this.ConnectionString = connectonString;
        }
        // Method
        public int GetInt(string Sql)
        {
            int oINT = 0;
            using (var connection = new SqlConnection(this.ConnectionString)) {
                connection.Open();
                SqlCommand command = new SqlCommand(Sql, connection);
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
            using (var connection = new SqlConnection(this.ConnectionString)) {
                connection.Open();
                SqlCommand command = new SqlCommand(Sql, connection);
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
                using (var connection = new SqlConnection(this.ConnectionString)) {
                    connection.Open();
                    SqlCommand command = new SqlCommand(Sql, connection);
                    if (TimeOut != 0 || !string.IsNullOrEmpty(TimeOut.ToString())) command.CommandTimeout = TimeOut;
                    SqlDataAdapter dataAdapter = new SqlDataAdapter(command);
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
                using (var connection = new SqlConnection(this.ConnectionString)) {
                    connection.Open();
                    SqlCommand command = new SqlCommand(Sql, connection);
                    if (TimeOut != 0 || !string.IsNullOrEmpty(TimeOut.ToString())) command.CommandTimeout = TimeOut;
                    SqlDataAdapter dataAdapter = new SqlDataAdapter(command);
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
            using (var connection = new SqlConnection(this.ConnectionString)) {
                connection.Open();
                SqlCommand command = connection.CreateCommand();
                SqlTransaction transaction;
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
            using (var connection = new SqlConnection(this.ConnectionString)) {
                SqlTransaction transaction;
                SqlCommand command = connection.CreateCommand();
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
        public bool Exucute(SqlCommand SqlCommand)
        {
            using (SqlConnection connection = new SqlConnection(this.ConnectionString)) {
                connection.Open();
                SqlCommand = connection.CreateCommand();
                SqlTransaction transaction;
                if (TimeOut != 0) SqlCommand.CommandTimeout = TimeOut;
                transaction = connection.BeginTransaction();
                SqlCommand.Connection = connection;
                SqlCommand.Transaction = transaction;
                try {
                    SqlCommand.ExecuteNonQuery();
                    transaction.Commit();
                    connection.Close();
                    transaction.Dispose();
                    SqlCommand.Dispose();
                } catch (Exception ex) {
                    transaction.Rollback();
                    connection.Close();
                    transaction.Dispose();
                    SqlCommand.Dispose();
                    throw ex;
                }
            }
            return true;
        }
        public bool Exucute(List<SqlCommand> SqlCommand)
        {
            using (var connection = new SqlConnection(this.ConnectionString)) {
                try {
                    connection.Open();
                    foreach (SqlCommand oCM in SqlCommand) {
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
    }
}
