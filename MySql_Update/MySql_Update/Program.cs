using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using MySql.Data.MySqlClient;

namespace MySql_Update
{
    class Program
    {
        /*
         * MySql_Update v 1.1
         *The purpose of this is to provide an easy command line tool to update a mysql table based on a reference csv file. This tool could be made
         * simple to call if a number of assumptions are made (ie. hard coded connection strings). But to be flexible must allow for the power to provide parameters. So
         * this requires the use of 9 parameters:
         * 1. path/name of connection file. File that contains specs needed for connecting. mainly URL/server name, userid, password, and database to use.
         * 2. Name of table that needs a column updated.
         * 3. Column name to match on in the table
         * 4. Column name to update in the table
         * 5. path/name of source CSV file. File that contains the source data for update
         * 6. Field # (integer) to match on in source file
         * 7. Field # (integer) to update from in source file
         * 8. delimiter for source file
         * 9. (optional) encapsulate characters
         * 10. (optional only if item 9 provided) escape characters
         * 
         * An example of calling this script in usage assumes 1) either you are in the directory of the program or the program directory is in the environment path, 2) you have
         * an connection file that contains the valid information for the connection. But otherwise, a command line call like 
         * 'MySql_Update "C:\Path\To\Connection.txt" table_name id_column "C:\Path\To\sourcefile.csv" 1 2 , "'
         * Connection file should have 4 lines: URL/ip address of DB server, Userid, Password, and DB name, These will be parsed in this
         * order for the connection string.
         * 
         * It should also be noted that this provides a varchar type. This is an assumption but this can be changed by editing lines 109-110 to change the mysql type.
         */
        static void Main(string[] args)
        {
            //Next block gives feedback if parameters are not entered.
            if (args.Length < 8)
            {
                System.Console.WriteLine("You must enter all parameters when calling this program:");
                System.Console.WriteLine("\t1. Path/name to file containing connection info.");
                System.Console.WriteLine("\t2. Name of table to use.");
                System.Console.WriteLine("\t3. Column name to match on.");
                System.Console.WriteLine("\t4. Column name to update.");
                System.Console.WriteLine("\t5. Path/name to csv/txt source file.");
                System.Console.WriteLine("\t6. Field # (int) in source file to match on.");
                System.Console.WriteLine("\t7. Field # (int) in source file to update from.");
                System.Console.WriteLine("\t8. Delimiter for source file.");
                System.Console.WriteLine("\t(Optional) 9. Enclosing character");
                System.Console.WriteLine("\t(Optional if #9 provided) 10. Escape character.");
                System.Console.WriteLine("Arguments must be in order, separated by space, if argument contains a space enclose with quotes.");
                System.Console.WriteLine();
                System.Console.WriteLine("Connection file should contain 4 lines that give in order: URL/IP of server, Userid, Password, and DB name.");
                System.Console.WriteLine("No action taken. Press any key to continue.");
                System.Console.ReadKey();
                System.Environment.Exit(1);
            }

            //assign all mandatory arguments
            string connectFile = args[0];
            string tableName = args[1];
            string columnMatch = args[2];
            string columnUpdate = args[3];
            string sourceFile = args[4];
            int sourceMatch = Convert.ToInt16(args[5]);
            int sourceUpdate = Convert.ToInt16(args[6]);
            string sourceDelimiter = args[7];
            
            //Next block takes in the arguments in the connection file to create connection string.
            string finalConnectString = null;
            try
            {
                string connectText = System.IO.File.ReadAllText(connectFile.Replace("\"",""));
                System.Console.WriteLine("Connection file found.");

                string[] connectFileLines = System.IO.File.ReadAllLines(connectFile.Replace("\"", ""));
                if (!(connectFileLines.Length < 4)) {
                    finalConnectString = "Server=" + connectFileLines[0] + ";Uid=" + connectFileLines[1] + ";Pwd=" + connectFileLines[2] + ";Database=" + connectFileLines[3] + ";";
                    Console.WriteLine("Connection String: " + finalConnectString);
                }
                else
                {
                    System.Console.WriteLine("Not enough arguments provided in file. Make sure your file has 4 seperate lines giving URL/IP of server, Userid, Password (can be left blank), and Databasename.");
                }
            }
            catch(IOException e) 
            {
                Console.WriteLine(e.Message);
            }

            //Connect to DB
            using(MySqlConnection dbConnection = new MySqlConnection(finalConnectString))
            {
                int totalUpdated = 0;
                try
                {
                    //connect to source file
                    dbConnection.Open();
                    try
                    {
                        //initialize command parameters
                        MySqlCommand updateQuery = dbConnection.CreateCommand();
                        updateQuery.CommandText = "UPDATE "+tableName+" SET "+columnUpdate+" = @updateSource WHERE "+columnMatch+" = @matchSource";
                        updateQuery.Parameters.Add("@updateSource", MySqlDbType.VarChar);
                        updateQuery.Parameters.Add("@matchSource", MySqlDbType.VarChar);



                        //read through csv source file to construct update query
                        foreach (string line in File.ReadLines(sourceFile))
                        {
                            string cleanLine = line;
                            
                            //cleanup string if escape,encapsulate characters used
                            if (args.Length > 8)
                            {
                                cleanLine = cleanLine.Replace(args[8], "");
                            }
                            if (args.Length > 9)
                            {
                                cleanLine = cleanLine.Replace(args[9], "");
                            }

                            string[] sourceColumns = cleanLine.Split(Convert.ToChar(sourceDelimiter));
                            
                            if (sourceColumns.Length < Math.Max(sourceMatch, sourceUpdate))
                            {
                                System.Console.WriteLine("Problem encountered: Index out of bounds for source match or update index given. (There are not enough columns in source file!)");
                                break;
                            }
                            else
                            {
                                updateQuery.Parameters["@updateSource"].Value = sourceColumns[sourceUpdate - 1];
                                updateQuery.Parameters["@matchSource"].Value = sourceColumns[sourceMatch - 1];

                                totalUpdated += updateQuery.ExecuteNonQuery();
                            }
                        }

                        System.Console.WriteLine("Update completed. {0} lines updated.",totalUpdated);

                    }
                    catch(IOException e)
                    {
                        System.Console.WriteLine(e.Message);
                    }
                }
                catch(MySqlException e)
                {
                    System.Console.WriteLine(e.Message);
                }
                
            }

        }
    }
}
