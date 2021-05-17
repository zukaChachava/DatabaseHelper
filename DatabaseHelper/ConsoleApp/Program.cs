using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using DatabaseHelper;
using System.Data.SqlClient;
using System.IO;

namespace ConsoleApp
{
    class Program
    {
        static void Main(string[] args)
        {
            StreamReader reader = new StreamReader("/home/zura/Desktop/pass.txt");
            string connectionString = reader.ReadLine();
            Database<SqlConnection> database = new Database<SqlConnection>(connectionString, true);
            DbParameter[] parameters = database.GetParameters(new Dictionary<string, object>()
            {
                {"Username", "Jorako"},
                {"Password", "jora123"},
                {"Id", 7}
            });
            
            DbParameter[] parameters2 = database.GetParameters(new Dictionary<string, object>()
            {
                {"Username", "Zurako"},
                {"Password", "zura123"},
                {"Id", 1}
            });
            
            DbParameter[] parameters3 = database.GetParameters(new Dictionary<string, object>()
            {
                {"Username", "Ermaliko"},
                {"Password", "Ermalo123"},
                {"Id", 6}
            });

            database.BeginTransaction();
            database.ExecuteScalar("InsertUser_SP", CommandType.StoredProcedure, parameters);
            database.ExecuteScalar("InsertUser_SP", CommandType.StoredProcedure, parameters2);
            int id = (int)database.ExecuteScalar("InsertUser_SP", CommandType.StoredProcedure, parameters3);
            database.CommitTransaction();
            database.GetConnection().Close();
            Console.WriteLine(id);
        }
    }
}