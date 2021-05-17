﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace DatabaseHelper
{
    public class Database<TConnection>: IDisposable where TConnection : DbConnection, new()
    {
        private readonly string _connectionString;
        private readonly bool _useSingleTone;
        private bool _disposed;
        private DbConnection _connection;
        private DbTransaction _transaction;

        public Database(string connectionString, bool useSingleTone = false)
        {
            _connectionString = connectionString;
            _useSingleTone = useSingleTone;
            _disposed = false;
        }
        
        public DbConnection GetConnection()
        {
            CheckIfDisposed();

            if (_connection == null || !_useSingleTone)
            {
                _connection = new TConnection();
                _connection.ConnectionString = _connectionString;
            }

            return _connection;
        }
        
        public DbCommand GetCommand(string commandText, CommandType commandType, params DbParameter[] parameters)
        {
            CheckIfDisposed();

            DbCommand command = GetConnection().CreateCommand();
            command.CommandText = commandText;
            command.CommandType = commandType;
            if (parameters != null)
            {
                foreach (var parameter in parameters)
                {
                    DbParameter databaseParameter = command.CreateParameter();
                    Rewrite(parameter, databaseParameter);
                    command.Parameters.Add(databaseParameter);
                }
            }
            return command;

            void Rewrite(DbParameter from, DbParameter to)
            {
                to.ParameterName = from.ParameterName;
                to.DbType = from.DbType;
                to.Value = from.Value;
                to.Direction = from.Direction;
                to.Size = from.Size;
                to.IsNullable = from.IsNullable;
            }
        }
        
        public DbCommand GetCommand(string commandText, params DbParameter[] parameters)
        {
            return GetCommand(commandText, CommandType.Text, parameters);
        }

        public int ExecuteNonQuery(string commandText, CommandType commandType, params DbParameter[] parameters)
        {
            CheckIfDisposed();

            DbCommand command = GetCommand(commandText, commandType, parameters);

            try
            {
                if(command.Connection.State == ConnectionState.Closed)
                    command.Connection.Open();

                if (TransactionIsOn())
                    command.Transaction = _transaction;

                return command.ExecuteNonQuery();
            }
            finally
            {
                if (!_useSingleTone)
                    command.Connection.Close();
            }
        }

        public int ExecuteNonQuery(string commandText, params DbParameter[] parameters)
        {
            return ExecuteNonQuery(commandText, CommandType.Text, parameters);
        }

        public object ExecuteScalar(string commandText, CommandType commandType, params DbParameter[] parameters)
        {
            CheckIfDisposed();

            DbCommand command = GetCommand(commandText, commandType, parameters);

            try
            {
                if (command.Connection.State == ConnectionState.Closed)
                    command.Connection.Open();

                if (TransactionIsOn())
                    command.Transaction = _transaction;

                return command.ExecuteScalar();
            }
            finally
            {
                if (!_useSingleTone)
                    command.Connection.Close();
            }
        }

        public object ExecuteScalar(string commandText, params DbParameter[] parameters)
        {
            return ExecuteScalar(commandText, CommandType.Text, parameters);
        }
        
        public DbDataReader ExecuteReader(string commandText, CommandType commandType, params DbParameter[] parameters)
        {
            CheckIfDisposed();

            DbCommand command = GetCommand(commandText, commandType, parameters);

            if(command.Connection.State == ConnectionState.Closed)
                command.Connection.Open();

            if (TransactionIsOn())
                command.Transaction = _transaction;

            return command.ExecuteReader();
        }

        public DbDataReader ExecuteReader(string commandText, params DbParameter[] parameters)
        {
            return ExecuteReader(commandText, CommandType.Text, parameters);
        }
        
        public DbTransaction BeginTransaction()
        {
            CheckIfDisposed();
            CheckIfSingleTone();

            if (_transaction == null || _transaction.Connection != null)
            {
                GetConnection().Open();
                _transaction = _connection.BeginTransaction();
            }

            return _transaction;
        }

        public void CommitTransaction()
        {
            CheckIfDisposed();
            CheckIfSingleTone();

            if (_transaction.Connection == null)
                throw new Exception("Transaction is not valid");

            _transaction.Commit();
        }

        public void RollbackTransaction()
        {
            CheckIfDisposed();
            CheckIfSingleTone();

            if (_transaction.Connection == null)
                throw new Exception("Transaction is not valid");

            _transaction.Rollback();
        }

        public DbParameter[] GetParameters(IDictionary<string, object> parameters)
        {
            DbParameter[] sqlParameters = new DbParameter[parameters.Count];
            int index = 0;

            foreach (var pair in parameters)
            {
                DbParameter parameter = GetConnection().CreateCommand().CreateParameter();
                parameter.ParameterName = pair.Key;
                parameter.Value = pair.Value;
                sqlParameters[index++] = parameter;
            }
            
            return sqlParameters;
        }

        public bool TransactionIsOn()
        {
            if (_transaction != null && _transaction.Connection != null)
                return true;

            return false;
        }
        
        private void CheckIfDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException("Object is disposed.");
        }

        private void CheckIfSingleTone()
        {
            if (!_useSingleTone)
                throw new Exception("Transaction is only supported in SingleToneMode");
        }
        
        public void Dispose()
        {
            _connection?.Close();
            _transaction?.Dispose();
            _connection?.Dispose();
            _disposed = true;
        }
    }
}