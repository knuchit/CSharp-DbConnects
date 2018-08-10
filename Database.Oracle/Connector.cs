using Oracle.DataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace Database.Oracle
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
            using (var connection = new OracleConnection(this.ConnectionString)) {
                connection.Open();
                OracleCommand command = new OracleCommand(Sql, connection);
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
            using (var connection = new OracleConnection(this.ConnectionString)) {
                connection.Open();
                OracleCommand command = new OracleCommand(Sql, connection);
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
                using (var connection = new OracleConnection(this.ConnectionString)) {
                    connection.Open();
                    OracleCommand command = new OracleCommand(Sql, connection);
                    if (TimeOut != 0 || !string.IsNullOrEmpty(TimeOut.ToString())) command.CommandTimeout = TimeOut;
                    OracleDataAdapter dataAdapter = new OracleDataAdapter(command);
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
                using (var connection = new OracleConnection(this.ConnectionString)) {
                    connection.Open();
                    OracleCommand command = new OracleCommand(Sql, connection);
                    if (TimeOut != 0 || !string.IsNullOrEmpty(TimeOut.ToString())) command.CommandTimeout = TimeOut;
                    OracleDataAdapter dataAdapter = new OracleDataAdapter(command);
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
            using (OracleConnection connection = new OracleConnection(this.ConnectionString)) {
                connection.Open();
                OracleCommand command = connection.CreateCommand();
                OracleTransaction transaction;
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
            using (OracleConnection connection = new OracleConnection(this.ConnectionString)) {
                OracleTransaction transaction;
                OracleCommand command = connection.CreateCommand();
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
        public bool Exucute(OracleCommand oracleCommand)
        {
            using (OracleConnection connection = new OracleConnection(this.ConnectionString)) {
                connection.Open();
                oracleCommand = connection.CreateCommand();
                OracleTransaction transaction;
                if (TimeOut != 0) oracleCommand.CommandTimeout = TimeOut;
                transaction = connection.BeginTransaction();
                oracleCommand.Connection = connection;
                oracleCommand.Transaction = transaction;
                try {
                    oracleCommand.ExecuteNonQuery();
                    transaction.Commit();
                    connection.Close();
                    transaction.Dispose();
                    oracleCommand.Dispose();
                } catch (Exception ex) {
                    transaction.Rollback();
                    connection.Close();
                    transaction.Dispose();
                    oracleCommand.Dispose();
                    throw ex;
                }
            }
            return true;
        }
        public bool Exucute(List<OracleCommand> oracleCommand)
        {
            using (OracleConnection connection = new OracleConnection(this.ConnectionString)) {
                try {
                    connection.Open();
                    foreach (OracleCommand oCM in oracleCommand) {
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
            this.ConnectionString = string.Format(@"Data Source=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={0})(PORT={1}))(CONNECT_DATA=(SERVER=default)(SID={2})));Password={3};User ID={4}", server, port, schema, password, username);
        }
        public void SetConnectionString(string tnsName, string username, string password)
        {
            this.ConnectionString = string.Format(@"Data Source={0};User ID={1};Password={2};", tnsName, username, password);
        }
    }
}