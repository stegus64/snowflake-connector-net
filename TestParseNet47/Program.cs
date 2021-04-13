#define NET46 

using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Client;


namespace TestParseNet47
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine($"Frequency={Stopwatch.Frequency} IsHighResolution={Stopwatch.IsHighResolution}"); 
#if NET46
            log4net.GlobalContext.Properties["framework"] = "net46";
            log4net.Config.XmlConfigurator.Configure();

#else
            log4net.GlobalContext.Properties["framework"] = "netcoreapp2.0";
            var logRepository = log4net.LogManager.GetRepository(Assembly.GetEntryAssembly());
            log4net.Config.XmlConfigurator.Configure(logRepository, new FileInfo("App.config"));
#endif

            for (int i = 0; i < 1; i++)
            {
                RunTest();
            }

        }

        private static void RunTest()
        {
            System.Diagnostics.Stopwatch sw;
            int rowcount = 0;

            var conn = new SnowflakeDbConnection();
            conn.ConnectionString = "ACCOUNT=cgi_partner;DB=SWEDEN_STEGUS_DEV1;HOST=cgi_partner.eu-west-1.snowflakecomputing.com;WAREHOUSE=sweden_wh;ROLE=SWEDEN_DARWIN_DEV;USER=gustafssons;PASSWORD=Brec67vek";
            conn.Open();

            using (IDbCommand command = conn.CreateCommand())
            {
                command.CommandText = "select * from SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.LINEITEM limit 100000";
                var dr = command.ExecuteReader();
                sw = System.Diagnostics.Stopwatch.StartNew();
                while (dr.Read())
                {
                    rowcount++;
                    for (int i = 0; i < dr.FieldCount; i++)
                    {
                        dr.GetValue(i);
                    }
                }
                sw.Stop();
                Console.WriteLine("rowcount = {0} Time = {1}", rowcount, sw.Elapsed);
            }
            conn.Close();

        }

        private static void RunTest2()
        {
            System.Diagnostics.Stopwatch sw;
            int rowcount = 0;

            var conn = new SnowflakeDbConnection();
            conn.ConnectionString = "ACCOUNT=cgi_partner;DB=SWEDEN_STEGUS_DEV1;HOST=cgi_partner.eu-west-1.snowflakecomputing.com;WAREHOUSE=sweden_wh;ROLE=SWEDEN_DARWIN_DEV;USER=gustafssons;PASSWORD=Brec67vek";
            conn.Open();

            using (IDbCommand command = conn.CreateCommand())
            {
                command.CommandText = "select * from SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.LINEITEM limit 1000";
                var dr = command.ExecuteReader();
                dr.Read();
                // Read all values once to make sure the chunk is properly downloaded
                // and all one-time initializations has been done
                for (int i = 0; i < dr.FieldCount; i++)
                {
                    dr.GetValue(i);
                }
                dr.Read();
                Thread.Sleep(1000);
                sw = System.Diagnostics.Stopwatch.StartNew();
                const int ITERATIONS = 10000000;
                int rows = ITERATIONS;
                while (rows-- > 0)
                {
                    rowcount++;
                    dr.GetValue(0);
                }
                sw.Stop();
                Console.WriteLine("rowcount = {0} Time = {1} ns/call", rowcount, sw.Elapsed.TotalMilliseconds / ITERATIONS * 1000000);
            }
            conn.Close();

        }
    }
}
