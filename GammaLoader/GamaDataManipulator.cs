using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Text;
using System.Web.Helpers;
using DxHelpersLib;
using PluginInteractionLib;

namespace GamaLoader
{
    class GamaDataManipulator :DataManipulator
    {
        private string savePath = "";
        public GamaDataManipulator(SqlConnection con, Logger log) : base(con, log)
        {

            ExeConfigurationFileMap configMap = new ExeConfigurationFileMap();
            configMap.ExeConfigFilename = Path.Combine(Path.GetDirectoryName(Assembly.GetEntryAssembly().Location),
                                                       "lanta.sqlconfig.dll.config");
            Configuration config = ConfigurationManager.OpenMappedExeConfiguration(configMap,
                                                                                   ConfigurationUserLevel.None);
            savePath = config.AppSettings.Settings["GammaPath"].Value;
            savePath += DateTime.Now.ToString("ddMMyyyyHHmm") + @"\\";
            if (!Directory.Exists(savePath))
            {
                Directory.CreateDirectory(savePath);
            }
        }

        private const string insertItinerary = @"INSERT INTO [dbo].[##temp_Gama_Itinerary]
           ([cruise_id]
           ,[townID]
           ,[town]
           ,[sts]
           ,[ets])
     VALUES
           (@cruise_id
           ,@townID
           ,@town
           ,@sts
           ,@ets)";
        private const string insertCost = @"INSERT INTO [dbo].[##temp_Gama_Costs]
           ([CruiseId]
           ,[CabinName]
           ,[places]
           ,[extra]
           ,[categoryName]
           ,[inCabin]
           ,[std0]
           ,[std3]
           ,[child0]
           ,[child3]
           ,[extra0]
           ,[extra3]
           ,[std2]
           ,[child2]
           ,[extra2]
           ,[extrachild0]
           ,[extrachild2]
           ,[extrachild3])
     VALUES
           (@CruiseId
           ,@CabinName
           ,@places
           ,@extra
           ,@categoryName
           ,@inCabin
           ,@std0
           ,@std3
           ,@child0
           ,@child3
           ,@extra0
           ,@extra3
           ,@std2
           ,@child2
           ,@extra2
           ,@extrachild0
           ,@extrachild2
           ,@extrachild3)";    
        private const string insertCity = @"INSERT INTO [dbo].[Gama_City]
           ([id]
           ,[name])
     VALUES
           (@id
           ,@name)";
        private const string insertCruise = @"INSERT INTO [dbo].[##temp_Gama_Cruises]
           ([id]
           ,[sts]
           ,[stownid]
           ,[fts]
           ,[ftownid]
           ,[way]
           ,[shipID])
     VALUES
           (@id
           ,@sts
           ,@stownid
           ,@fts
           ,@ftownid
           ,@way
           ,@shipID)";
        private const string insertShip = @"INSERT INTO [dbo].[Gama_Ships]
           ([id]
           ,[name])
                VALUES
           (@id
           ,@name)";
        public override string GetShipsData()
        {
            _logFile.WriteLine("Гама : Начало загрузки кораблей");
            WebClient client = new WebClient();
            byte[] data = client.DownloadData("http://gama-nn.ru/ru/dba/navs/");
            Stream stream = new MemoryStream(data);
            DataSet ds = new DataSet();
            ds.ReadXml(stream);
            
            DataTable ships = ds.Tables["navigation"];
            ClearTable("Gama_Ships");
            foreach (DataRow row in ships.Rows)
            {
               int id= int.Parse(row.Field<string>("ship_iid"));
               string name = row.Field<string>("ship_name");
                using (SqlCommand com = new SqlCommand(insertShip,_connection))
                {
                    com.Parameters.AddWithValue("@id", id);
                    com.Parameters.AddWithValue("@name", name);
                    com.ExecuteNonQuery();
                }
            }
            _logFile.WriteLine("Гама : Корабли загружены");
            return "";
        }
        void ClearTable(string tablename)
        {
            using (SqlCommand com = new SqlCommand("delete from "+tablename,_connection))
            {
                com.ExecuteNonQuery();
            }
        }
        public override string GetDecksData()
        {
            throw new NotImplementedException();
        }

        public override string GetCabinsData()
        {
            throw new NotImplementedException();
        }

        public override string GetItineraryData()
        {
            _logFile.WriteLine("Гама : Начало загрузки маршрутов");
            DataTable dt = new DataTable();
            using (SqlDataAdapter adapter = new SqlDataAdapter("select id from Gama_Cruises", _connection))
            {
                adapter.Fill(dt);
            }
            using (SqlCommand com = new SqlCommand(@"CREATE TABLE [dbo].[##temp_Gama_Itinerary](
	[cruise_id] [int] NULL,
	[townID] [int] NULL,
	[town] [varchar](100) NULL,
	[sts] [datetime] NULL,
	[ets] [datetime] NULL
) ", _connection))
            {
                com.ExecuteNonQuery();
            }
            //ClearTable("Gama_Itinerary");
            foreach (DataRow row in dt.Rows)
            {
                int CruiseId = row.Field<int>("id");
                DataSet ds = new DataSet();
                ds.ReadXml(string.Format(savePath+"cruise{0}.xml",CruiseId));
                DataTable point = ds.Tables["point"];
                foreach (DataRow dataRow in point.Rows)
                {
                    string town = dataRow.Field<string>("town_name");
                    int townId = int.Parse(dataRow.Field<string>("town_iid"));
                    DateTime sts = DateTime.Parse(dataRow.Field<string>("STS")),
                             ets = DateTime.Parse(dataRow.Field<string>("ETS"));
                    using (SqlCommand com = new SqlCommand(insertItinerary,_connection))
                    {
                        com.Parameters.AddWithValue("@cruise_id", CruiseId);
                        com.Parameters.AddWithValue("@townID", townId);
                        com.Parameters.AddWithValue("@town", town);
                        com.Parameters.AddWithValue("@sts", sts);
                        com.Parameters.AddWithValue("@ets", ets);
                        com.ExecuteNonQuery();
                    }
                }

            }
            ClearTable("Gama_Itinerary");
            using (SqlCommand com = new SqlCommand(@"insert into Gama_Itinerary select * from ##temp_Gama_Itinerary drop table ##temp_Gama_Itinerary ", _connection))
            {
                com.ExecuteNonQuery();
            }
            _logFile.WriteLine("Гама : Маршруты загружены");
            return "";
        }

        void GetCruiseData()
        {
            _logFile.WriteLine("Гама : Начало загрузки списка круизов");
            WebClient  client = new WebClient();
            byte[] data = client.DownloadData("http://gama-nn.ru/ru/dba/navs/");
            Stream stream = new MemoryStream(data);
            DataSet ds = new DataSet();
            ds.ReadXml(stream);
            BinaryWriter bw = new BinaryWriter(File.Create(savePath + "cruises.xml"));
            bw.Write(data);
            bw.Close();
            DataTable cruises = ds.Tables["way"],ships=ds.Tables["navigation"],ways = ds.Tables["ways"];
            
            //ClearTable("Gama_Cruises");
            using (SqlCommand com = new SqlCommand(@"CREATE TABLE [dbo].[##temp_Gama_Cruises](
	[id] [int] NULL,
	[sts] [datetime] NULL,
	[stownid] [int] NULL,
	[fts] [datetime] NULL,
	[ftownid] [nchar](10) NULL,
	[way] [varchar](300) NULL,
	[shipID] [int] NULL
) ", _connection))
            {
                com.ExecuteNonQuery();
            }
            foreach (DataRow row in cruises.Rows)
            {
                int id = int.Parse(row.Field<string>("iid")), stownid = int.Parse(row.Field<string>("STownid")),ftownid = int.Parse(row.Field<string>("FtownId"));
                DateTime sts = DateTime.Parse(row.Field<string>("STS")),fts=DateTime.Parse(row.Field<string>("FTS"));
                DataRow[] wayRow = ways.Select("ways_id='" + row.Field<int>("ways_id").ToString() + "'");
                int navId = wayRow[0].Field<int>("navigation_id");
                DataRow[] shipRow = ships.Select("navigation_Id=" + navId.ToString());
                int shipId = int.Parse(shipRow[0].Field<string>("ship_iid"));
                string way = row.Field<string>("Way");
                using (SqlCommand com= new SqlCommand(insertCruise,_connection))
                {
                    com.Parameters.AddWithValue("@id", id);
                    com.Parameters.AddWithValue("@sts", sts);
                    com.Parameters.AddWithValue("@stownid", stownid);
                    com.Parameters.AddWithValue("@fts", fts);
                    com.Parameters.AddWithValue("@ftownid", ftownid);
                    com.Parameters.AddWithValue("@way", way);
                    com.Parameters.AddWithValue("@shipID", shipId);
                    com.ExecuteNonQuery();
                }

            }
            ClearTable("Gama_Cruises");
            using (SqlCommand com = new SqlCommand(@"insert into Gama_Cruises select * from ##temp_Gama_Cruises drop table ##temp_Gama_Cruises", _connection))
            {
                com.ExecuteNonQuery();
            }
            _logFile.WriteLine("Гама : Список круизов загружен");
        }
        void GetCityData()
        {
            _logFile.WriteLine("Гама : Начало загрузки городов");
            WebClient client = new WebClient();
            byte[] data = client.DownloadData("http://gama-nn.ru/ru/dba/towns/");
            Stream stream = new MemoryStream(data);
            DataSet ds = new DataSet();
            ds.ReadXml(stream);
            DataTable city = ds.Tables["town"];
            ClearTable("Gama_City");
            foreach (DataRow row in city.Rows)
            {
                int id = int.Parse(row.Field<string>("iid"));
                string name = row.Field<string>("name");
                using (SqlCommand com = new SqlCommand(insertCity,_connection))
                {
                    com.Parameters.AddWithValue("@id", id);
                    com.Parameters.AddWithValue("@name", name);
                    com.ExecuteNonQuery();
                }
            }
            _logFile.WriteLine("Гама : Города загружены");
        }

        private void GetPriceData()
        {
            _logFile.WriteLine("Гама : Начало загрузки цен");
            DataTable dt = new DataTable();
            using (SqlDataAdapter adapter = new SqlDataAdapter("select id from Gama_Cruises", _connection))
            {
                adapter.Fill(dt);
            }

            using (SqlCommand com = new SqlCommand(@"CREATE TABLE [dbo].[##temp_Gama_Costs](
	            [CruiseId] [int] NULL,
	            [CabinName] [varchar](15) NULL,
	[places] [int] NULL,
	[extra] [int] NULL,
	[categoryName] [varchar](30) NULL,
	[inCabin] [int] NULL,
	[std0] [int] NULL,
	[std3] [int] NULL,
	[child0] [int] NULL,
	[child3] [int] NULL,
	[extra0] [int] NULL,
	[extra3] [int] NULL,
    [std2]   [int] null,
    [child2] [int] null,
    [extra2] [int] null,
    [extrachild0] [int] null,
    [extrachild2] [int] null,
    [extrachild3] [int] null
) ", _connection))
            {
                com.ExecuteNonQuery();
            }
            //ClearTable("Gama_Costs");
            foreach (DataRow row in dt.Rows)
            {
                int CruiseId = row.Field<int>("id");
                DataSet ds = new DataSet();
                ds.ReadXml(string.Format(savePath + "cruise{0}.xml", CruiseId));
                DataTable cost = ds.Tables["cost"], cabin = ds.Tables["cabin"];



                if (cost != null && cabin != null)
                {
                    if (!cost.Columns.Contains("std0"))
                    {
                        cost.Columns.Add(new DataColumn("std0", typeof (int)));
                    }
                    if (!cost.Columns.Contains("std2"))
                    {
                        cost.Columns.Add(new DataColumn("std2", typeof(int)));
                    }
                    
                    if (!cost.Columns.Contains("std3"))
                    {
                        cost.Columns.Add(new DataColumn("std3", typeof (int)));
                    }
                    if (!cost.Columns.Contains("child0"))
                    {
                        cost.Columns.Add(new DataColumn("child0", typeof (int)));
                    }
                    if (!cost.Columns.Contains("child2"))
                    {
                        cost.Columns.Add(new DataColumn("child2", typeof(int)));
                    }
                    
                    if (!cost.Columns.Contains("child3"))
                    {
                        cost.Columns.Add(new DataColumn("child3", typeof (int)));
                    }
                    if (!cost.Columns.Contains("extra0"))
                    {
                        cost.Columns.Add(new DataColumn("extra0", typeof (int)));
                    }
                    if (!cost.Columns.Contains("extra2"))
                    {
                        cost.Columns.Add(new DataColumn("extra2", typeof(int)));
                    }
                    if (!cost.Columns.Contains("extra3"))
                    {
                        cost.Columns.Add(new DataColumn("extra3", typeof (int)));
                    }



                    if (!cost.Columns.Contains("extrachild0"))
                    {
                        cost.Columns.Add(new DataColumn("extrachild0", typeof(int)));
                    }
                    if (!cost.Columns.Contains("extrachild2"))
                    {
                        cost.Columns.Add(new DataColumn("extrachild2", typeof(int)));
                    }
                    if (!cost.Columns.Contains("extrachild3"))
                    {
                        cost.Columns.Add(new DataColumn("extrachild3", typeof(int)));
                    }



                    var results = from table1 in cost.AsEnumerable()
                                  join table2 in cabin.AsEnumerable() on (int) table1["cabin_id"] equals
                                      (int) table2["cabin_id"]
                                  select new
                                      {
                                          cabinName = table2.Field<string>("name"),
                                          places = table2.Field<string>("places"),
                                          extra = table2.Field<string>("extra"),
                                          categoryId = table2.Field<string>("category_iid"),
                                          categoryName = table2.Field<string>("category_name"),
                                          inCabin = (string) table1["inCabin"],
                                          std0 = table1.Field<string>("std0"),
                                          std2 = table1.Field<string>("std2"),
                                          std3 = table1.Field<string>("std3"),
                                          child0 = table1.Field<string>("child0"),
                                          child2 = table1.Field<string>("child2"),
                                          child3 = table1.Field<string>("child3"),
                                          extra0 = table1.Field<string>("extra0"),
                                          extra2 = table1.Field<string>("extra2"),
                                          extra3 = table1.Field<string>("extra3"),
                                          extrachild0 = table1.Field<string>("extrachild0"),
                                          extrachild2 = table1.Field<string>("extrachild2"),
                                          extrachild3 = table1.Field<string>("extrachild3")
                                      };
                    foreach (var result in results)
                    {


                        using (SqlCommand com = new SqlCommand(insertCost, _connection))
                        {
                            com.Parameters.AddWithValue("@CruiseId", CruiseId);
                            com.Parameters.AddWithValue("@CabinName", result.cabinName);
                            com.Parameters.AddWithValue("@places", int.Parse(result.places));
                            com.Parameters.AddWithValue("@extra", int.Parse(result.extra));
                            com.Parameters.AddWithValue("@categoryName", result.categoryName);
                            com.Parameters.AddWithValue("@inCabin", int.Parse(result.inCabin));
                            if (result.std0 == null)
                            {
                                com.Parameters.AddWithValue("@std0", DBNull.Value);
                            }
                            else
                            {
                                com.Parameters.AddWithValue("@std0", result.std0);
                            }
                            if (result.std2 == null)
                            {
                                com.Parameters.AddWithValue("@std2", DBNull.Value);
                            }
                            else
                            {
                                com.Parameters.AddWithValue("@std2", result.std2);
                            }
                            if (result.std3 == null)
                            {
                                com.Parameters.AddWithValue("@std3", DBNull.Value);
                            }
                            else
                            {
                                com.Parameters.AddWithValue("@std3", result.std3);
                            }


                            if (result.child0 == null)
                            {
                                com.Parameters.AddWithValue("@child0", DBNull.Value);
                            }
                            else
                            {
                                com.Parameters.AddWithValue("@child0", result.child0);
                            }
                            if (result.child2 == null)
                            {
                                com.Parameters.AddWithValue("@child2", DBNull.Value);
                            }
                            else
                            {
                                com.Parameters.AddWithValue("@child2", result.child2);
                            }
                            if (result.child3 == null)
                            {
                                com.Parameters.AddWithValue("@child3", DBNull.Value);
                            }
                            else
                            {
                                com.Parameters.AddWithValue("@child3", result.child3);
                            }

                            if (result.extra0 == null)
                            {
                                com.Parameters.AddWithValue("@extra0", DBNull.Value);
                            }
                            else
                            {
                                com.Parameters.AddWithValue("@extra0", result.extra0);
                            }
                            if (result.extra2 == null)
                            {
                                com.Parameters.AddWithValue("@extra2", DBNull.Value);
                            }
                            else
                            {
                                com.Parameters.AddWithValue("@extra2", result.extra2);
                            }
                            if (result.extra3 == null)
                            {
                                com.Parameters.AddWithValue("@extra3", DBNull.Value);
                            }
                            else
                            {
                                com.Parameters.AddWithValue("@extra3", result.extra3);
                            }





                            if (result.extrachild0 == null)
                            {
                                com.Parameters.AddWithValue("@extrachild0", DBNull.Value);
                            }
                            else
                            {
                                com.Parameters.AddWithValue("@extrachild0", result.extrachild0);
                            }
                            if (result.extrachild2 == null)
                            {
                                com.Parameters.AddWithValue("@extrachild2", DBNull.Value);
                            }
                            else
                            {
                                com.Parameters.AddWithValue("@extrachild2", result.extrachild2);
                            }
                            if (result.extrachild3 == null)
                            {
                                com.Parameters.AddWithValue("@extrachild3", DBNull.Value);
                            }
                            else
                            {
                                com.Parameters.AddWithValue("@extrachild3", result.extrachild3);
                            }

                            com.ExecuteNonQuery();

                        }
                    }

                }

            }
            ClearTable("Gama_Costs");
            using (SqlCommand com = new SqlCommand(@"insert into Gama_Costs select * from ##temp_Gama_Costs drop table ##temp_Gama_Costs ",_connection))
            {
                com.ExecuteNonQuery();
            }
            _logFile.WriteLine("Гама : Цены загружены");
        }

        void GetExcursins()
        {
             _logFile.WriteLine("Гама : Начало загрузки экскурсий");
            DataTable dt = new DataTable();
            using (SqlDataAdapter adapter = new SqlDataAdapter("select id from Gama_Cruises", _connection))
            {
                adapter.Fill(dt);
            }
            ClearTable("Gama_Itinerary");
            foreach (DataRow row in dt.Rows)
            {
                int CruiseId = row.Field<int>("id");
                DataSet ds = new DataSet();
                ds.ReadXml(string.Format(savePath + "cruise{0}.xml", CruiseId));
                DataTable items = ds.Tables["items"],
                          groups = ds.Tables["group"],
                          excursions = ds.Tables["excursions"],
                          item = ds.Tables["item"];
                if (items != null && item != null && groups != null && excursions != null)
                {
                    var results = from table1 in item.AsEnumerable()
                                  join table2 in items.AsEnumerable() on (int)table1["items_id"] equals (int)table2["items_id"]
                                  join table3 in groups.AsEnumerable() on (int)table2["group_id"] equals (int)table3["group_id"]
                                  join table4 in excursions.AsEnumerable() on (int)table3["excursions_id"] equals (int)table4["excursions_id"]
                                  select new
                                  {
                                      town = table1.Field<string>("town"),
                                      name = table1.Field<string>("name"),
                                      groupname = table3.Field<string>("name"),
                                      costStd = table3.Field<string>("costStd"),
                                      costChild = table3.Field<string>("costChild")
                                  };
                }
            }
            _logFile.WriteLine("Гама : Экскурсии загружены");
        }
        
        void SaveDataCruises()
        {
            _logFile.WriteLine("Гама : Начало кэширования крузов");
            DataTable dt = new DataTable();
            using (SqlDataAdapter adapter = new SqlDataAdapter("select id from Gama_Cruises", _connection))
            {
                adapter.Fill(dt);
            }
             foreach (DataRow row in dt.Rows)
            {
                int CruiseId = row.Field<int>("id");
                WebClient client = new WebClient();
                byte[] data = client.DownloadData(string.Format("http://gama-nn.ru/ru/dba/way/{0}/",CruiseId));
                BinaryWriter bw = new BinaryWriter(File.Create(savePath + "cruise"+CruiseId.ToString()+".xml"));
                bw.Write(data);
                bw.Close();
             }
             _logFile.WriteLine("Гама : Кэширование круизов закончено");
        }
        void SumbitChanges()
        {
            _logFile.WriteLine("Гама : Применение изменений в базе");
            using (SqlCommand com = new SqlCommand("gama",_connection))
            {
                com.CommandType =CommandType.StoredProcedure;
                com.ExecuteNonQuery();
            }
            _logFile.WriteLine("Гама : Примение изменений в базе законченно");
        }
        public override void GetData()
        {
            _logFile.WriteLine("Гама : Начало загрузки данных");
            GetCityData();
            GetShipsData();
            GetCruiseData();
            SaveDataCruises();
            GetPriceData();
            GetItineraryData();
            SumbitChanges();
            _logFile.WriteLine("Гама : Загрузка данных закончена");
        }
    }
}
