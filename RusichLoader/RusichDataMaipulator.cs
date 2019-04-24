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
using DxHelpersLib;
using PluginInteractionLib;

namespace RusichLoader
{

    
         
    
    class RusichDataMaipulator :DataManipulator 
    {
        
        private string apiUrl = "";
        private string savePath = "";
        #region SqlTempCreate
        private const string createTempCabins = @"CREATE TABLE [dbo].[##Temp_Rusich_Cabins](
	[id] [int] NULL,
	[ship_id] [int] NULL,
	[deck_id] [int] NULL,
	[number] [varchar](40) NULL,
	[classOfCabin] [int] NULL
)";
        private const string createTempCabinsFree = @"CREATE TABLE [dbo].[##Temp_Rusich_CabinsFree](
	[cabin_id] [int] NULL,
	[cruise_id] [int] NULL,
	[free] [int] NULL
)";
        private const string createTempCruises = @"CREATE TABLE [dbo].[##Temp_Rusich_Cruises](
	[id] [int] NULL,
	[name] [varchar](1000) NULL,
	[showname] [varchar](300) NULL,
	[ship_id] [int] NULL,
	[portOfDeparture] [varchar](50) NULL,
	[departureDate] [datetime] NULL,
	[portOfArrival] [varchar](50) NULL,
	[arrivalDate] [datetime] NULL,
	[duration] [int] NULL,
	[plase4ChildWithoutPlace] [float] NULL
)";
        private const string createTempClassOfCabins = @"CREATE TABLE [dbo].[##Temp_Rusich_ClassOfCabins](
	[id] [int] NULL,
	[ship_id] [int] NULL,
	[deck_id] [int] NULL,
	[places] [int] NULL,
	[priceForDay] [int] NULL,
	[description] [varchar](100) NULL
)";
        private const string createTempDecks = @"CREATE TABLE [dbo].[##Temp_Rusich_deck](
	[id] [int] NULL,
	[Ship_id] [int] NULL,
	[name] [varchar](50) NULL
)";
        private const string createTempPrices = @"CREATE TABLE [dbo].[##Temp_Rusich_Prices](
	[cruise_id] [int] NULL,
	[classOfCabin_id] [int] NULL,
	[costOfPlace] [float] NULL,
	[IsFull] [int] NULL,
    [priceForDay] int null
)";
        private const string createTempSips = @"CREATE TABLE [dbo].[##Temp_Rusich_Ships](
	[id] [int] NULL,
	[name] [varchar](50) NULL
)";
        #endregion
        #region SqlInsert

        private const string insertShip = @"INSERT INTO [dbo].[##Temp_Rusich_Ships]
           ([id]
           ,[name])
     VALUES
           (@id
           ,@name)";
        private const string insertCabin = @"INSERT INTO [dbo].[##Temp_Rusich_Cabins]
           ([id]
           ,[ship_id]
           ,[deck_id]
           ,[number]
           ,[classOfCabin])
     VALUES
           (@id
           ,@ship_id
           ,@deck_id
           ,@number
           ,@classOfCabin)";
        private const string insertCabinFree = @"INSERT INTO [dbo].[##Temp_Rusich_CabinsFree]
           ([cabin_id]
           ,[cruise_id]
           ,[free])
     VALUES
           (@cabin_id
           ,@cruise_id
           ,@free)";
        private const string insertCruise = @"INSERT INTO [dbo].[##Temp_Rusich_Cruises]
           ([id]
           ,[name]
           ,[showname]
           ,[ship_id]
           ,[portOfDeparture]
           ,[departureDate]
           ,[portOfArrival]
           ,[arrivalDate]
           ,[duration]
           ,[plase4ChildWithoutPlace])
     VALUES
           (@id
           ,@name
           ,@showname
           ,@ship_id
           ,@portOfDeparture
           ,@departureDate
           ,@portOfArrival
           ,@arrivalDate
           ,@duration
           ,@plase4ChildWithoutPlace)";
        private const string insertClassOfCabin = @"INSERT INTO [dbo].[##Temp_Rusich_ClassOfCabins]
           ([id]
           ,[ship_id]
           ,[deck_id]
           ,[places]
           ,[priceForDay]
           ,[description])
     VALUES
           (@id
           ,@ship_id
           ,@deck_id
           ,@places
           ,@priceForDay
           ,@description)";
        private const string insertPrice = @"INSERT INTO [dbo].[##Temp_Rusich_Prices]
           ([cruise_id]
           ,[classOfCabin_id]
           ,[costOfPlace]
           ,[IsFull]
           ,[priceForDay])
     VALUES
           (@cruise_id
           ,@classOfCabin_id
           ,@costOfPlace
           ,@IsFull
           ,@priceForDay)";
        private const string insertDeck = @"INSERT INTO [dbo].[##Temp_Rusich_deck]
           ([id]
           ,[Ship_id]
           ,[name])
     VALUES
           (@id
           ,@Ship_id
           ,@name)";

#endregion

        public RusichDataMaipulator(SqlConnection con, Logger log) : base(con, log)
        {
            

            ExeConfigurationFileMap configMap = new ExeConfigurationFileMap();
            configMap.ExeConfigFilename = Path.Combine(Path.GetDirectoryName(Assembly.GetEntryAssembly().Location),
                                                       "lanta.sqlconfig.dll.config");
            Configuration config = ConfigurationManager.OpenMappedExeConfiguration(configMap,
                                                                                   ConfigurationUserLevel.None);

            apiUrl = config.AppSettings.Settings["RusichURL"].Value;
            savePath = config.AppSettings.Settings["RusichtPath"].Value + "\\" + DateTime.Now.ToString("ddMMyyyyHHmm") + "\\";
            if (!Directory.Exists(savePath))
            {
                Directory.CreateDirectory(savePath);
            }
        }
        void ReloadTable(string tableName)
        {
            string command = string.Format(@"delete from {0}
                                             insert into {0} select * from ##Temp_{0}
                                              drop table ##Temp_{0}", tableName);
            using (SqlCommand com = new SqlCommand(command,_connection))
            {
                com.ExecuteNonQuery();
            }

        }
        public override string GetShipsData()
        {
            throw new NotImplementedException();
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
            throw new NotImplementedException();
        }
        void GetFileDate()
        {
            WebClient client = new WebClient();
            byte[] data;
            try
            {
                data = client.DownloadData(apiUrl);
            }
            catch (Exception)
            {

                return;
                throw;
            }

            Stream stream = new MemoryStream(data);

            BinaryWriter bw = new BinaryWriter(File.Create(savePath+"\\cashe.xml"));
            bw.Write(data);
            bw.Close();
            DataSet ds = new DataSet();
            ds.ReadXml(stream);
            _logFile.WriteLine("Руcич : Создание временных таблиц");
            using (SqlCommand com = new SqlCommand(createTempCabins+"\n"+createTempCabinsFree+"\n"+createTempCruises+"\n"+createTempDecks+"\n"+createTempPrices+"\n"+createTempSips+"\n"+createTempClassOfCabins,_connection))
            {
                com.ExecuteNonQuery();
            }
            _logFile.WriteLine("Руcич : Загрузка кораблей");
            DataTable ships = ds.Tables["ship"];
            foreach (DataRow row in ships.Rows)
            {
                int id = int.Parse(row.Field<string>("id"));
                string name = row.Field<string>("name");
                using (SqlCommand com = new SqlCommand(insertShip,_connection))
                {
                    com.Parameters.AddWithValue("@id", id);
                    com.Parameters.AddWithValue("@name", name);
                    com.ExecuteNonQuery();
                }

            }
            _logFile.WriteLine("Руcич : Загрузка палуб");
            DataTable decks = ds.Tables["deck"];
            foreach (DataRow row in decks.Rows)
            {
                int id=int.Parse(row.Field<string>("id"));
                int ship_id = int.Parse(row.Field<string>("ship_id"));
                string name = row.Field<string>("name");
                using (SqlCommand com = new SqlCommand(insertDeck,_connection))
                {
                    com.Parameters.AddWithValue("@id", id);
                    com.Parameters.AddWithValue("@Ship_id", ship_id);
                    com.Parameters.AddWithValue("@name", name);
                    com.ExecuteNonQuery();
                }
            }
            _logFile.WriteLine("Руcич : Загрузка категорий кают");
            DataTable classOfCabin = ds.Tables["classOfCabin"];
            foreach (DataRow row in classOfCabin.Rows)
            {
                int id = int.Parse(row.Field<string>("id"));
                int ship_id = int.Parse(row.Field<string>("ship_id"));
                int deck_id = int.Parse(row.Field<string>("deck_id"));
                int places = int.Parse(row.Field<string>("places"));
                int priceForDay = int.Parse(row.Field<string>("priceForDay"));
                string description = row.Field<string>("description");
                using (SqlCommand com = new SqlCommand(insertClassOfCabin,_connection))
                {
                    com.Parameters.AddWithValue("@id",id);
                    com.Parameters.AddWithValue("@ship_id", ship_id);
                    com.Parameters.AddWithValue("@deck_id", deck_id);
                    com.Parameters.AddWithValue("@places", places);
                    com.Parameters.AddWithValue("@priceForDay", priceForDay);
                    com.Parameters.AddWithValue("@description", description);
                    com.ExecuteNonQuery();
                }
            }
            _logFile.WriteLine("Руcич : Загрузка кают");
            DataRow[] cabins = ds.Tables["cabin"].Select("cruise_id is null");
            foreach (DataRow row in cabins)
            {
                int id = int.Parse(row.Field<string>("id"));
                int ship_id = int.Parse(row.Field<string>("ship_id"));
                int deck_id = int.Parse(row.Field<string>("deck_id"));
                string number = row.Field<string>("number");
                int classOfCabin_id = int.Parse(row.Field<string>("classOfCabin_id"));
                using (SqlCommand com = new SqlCommand(insertCabin,_connection))
                {
                    com.Parameters.AddWithValue("@id", id);
                    com.Parameters.AddWithValue("@ship_id", ship_id);
                    com.Parameters.AddWithValue("@deck_id", deck_id);
                    com.Parameters.AddWithValue("@number", number);
                    com.Parameters.AddWithValue("@classOfCabin", classOfCabin_id);
                    com.ExecuteNonQuery();
                }
            }
            _logFile.WriteLine("Руcич : Загрузка круизов");
            DataRow[] cruises = ds.Tables["cruise"].Select("freePlace_id is null");
            foreach (DataRow row in cruises)
            {
                int id = int.Parse(row.Field<string>("id"));
                string name = row.Field<string>("name");
                string showname = row.Field<string>("showname");
                int ship_id = int.Parse(row.Field<string>("ship_id"));
                string portOfDeparture = row.Field<string>("portOfDeparture");
                string portOfArrival = row.Field<string>("portofArrival");
                DateTime departureDate = DateTime.Parse(row.Field<string>("departureDate"));
                DateTime ArrivalDate = DateTime.Parse(row.Field<string>("ArrivalDate"));
                int duration = int.Parse(row.Field<string>("duration"));
                double? place4ChildWithoutPlace = null;

                if (row.Field<string>("place4ChildWithoutPlace") == null || row.Field<string>("place4ChildWithoutPlace") == string.Empty)
                {
                    place4ChildWithoutPlace = null;
                }
                else
                {
                    place4ChildWithoutPlace =
                        double.Parse(row.Field<string>("place4ChildWithoutPlace").Replace(".", ","));
                }
                using (SqlCommand com = new SqlCommand(insertCruise,_connection))
                {
                    com.Parameters.AddWithValue("@id", id);
                    com.Parameters.AddWithValue("@ship_id", ship_id);
                    com.Parameters.AddWithValue("@name", name);
                    com.Parameters.AddWithValue("@showname", showname);
                    com.Parameters.AddWithValue("@portOfDeparture", portOfDeparture);
                    com.Parameters.AddWithValue("@departureDate", departureDate);
                    com.Parameters.AddWithValue("@portOfArrival", portOfArrival);
                    com.Parameters.AddWithValue("@arrivalDate", ArrivalDate);
                    com.Parameters.AddWithValue("@duration", duration);
                    com.Parameters.AddWithValue("@plase4ChildWithoutPlace", place4ChildWithoutPlace==null?(object)DBNull.Value:(object)place4ChildWithoutPlace);
                    com.ExecuteNonQuery();
                }
                
            }
            _logFile.WriteLine("Руcич : Загрузка цен");
            DataTable prices = ds.Tables["price"];
            foreach (DataRow row in prices.Rows)
            {
                int cruise_id = int.Parse(row.Field<string>("cruise_id"));
                int classOfCabin_id = int.Parse(row.Field<string>("classOfCabin_id"));
                double costOfPlace = double.Parse(row.Field<string>("costOfPlace").Replace(".", ","));
                int full = int.Parse(row.Field<string>("full"));
                int priceForDay = int.Parse(row.Field<string>("priceForDay"));
                using (SqlCommand com = new SqlCommand(insertPrice,_connection))
                {
                    com.Parameters.AddWithValue("@cruise_id", cruise_id);
                    com.Parameters.AddWithValue("@classOfCabin_id", classOfCabin_id);
                    com.Parameters.AddWithValue("@costOfPlace", costOfPlace);
                    com.Parameters.AddWithValue("@IsFull", full);
                    com.Parameters.AddWithValue("@priceForDay", priceForDay);
                    com.ExecuteNonQuery();
                }
            }
            _logFile.WriteLine("Руcич : Загрузка свободных кают");
            DataRow[] cabinsFree = ds.Tables["cabin"].Select("cruise_id is not null");
            foreach (DataRow row in cabinsFree)
            {
                int cabin_id = int.Parse(row.Field<string>("id"));
                int free = int.Parse(row.Field<string>("free"));
                DataRow[] cruses = ds.Tables["cruise"].Select("cruise_id = "+ row.Field<int>("cruise_id"));
                int id_cruise = int.Parse(cruses[0].Field<string>("id"));
                using (SqlCommand com = new SqlCommand(insertCabinFree,_connection))
                {
                    com.Parameters.AddWithValue("@cabin_id", cabin_id);
                    com.Parameters.AddWithValue("@cruise_id", id_cruise);
                    com.Parameters.AddWithValue("@free", free);
                    com.ExecuteNonQuery();
                }
               
            }
            _logFile.WriteLine("Руcич : Перегрузка из временных таблиц в боковые");
            ReloadTable("Rusich_Cabins");
            ReloadTable("Rusich_CabinsFree");
            ReloadTable("Rusich_ClassOfCabins");
            ReloadTable("Rusich_Cruises");
            ReloadTable("Rusich_deck");
            ReloadTable("Rusich_Prices");
            ReloadTable("Rusich_Ships");
            //string str = "";
            //foreach (DataTable table in ds.Tables)
            //{
            //    str += "\n\n\n\n" + table.TableName + "::";
            //    foreach (DataColumn column in table.Columns)
            //    {
            //        str += "\n" + column.ColumnName + " : " + column.DataType;
            //    }
            //}
            //Console.WriteLine(str);
        }

        void SumbitChages()
        {
            _logFile.WriteLine("Руcич : Применение изменений в базе");
            using (SqlCommand com = new SqlCommand("Rusich", _connection))
            {
                com.CommandType=CommandType.StoredProcedure;
                com.ExecuteNonQuery();
            }
        }
        public override void GetData()
        {
            try
            {
                _logFile.WriteLine("Руcич : Начало загрузки");
                GetFileDate();
                SumbitChages();
                _logFile.WriteLine("Русич : Загрузка окончена");
            }
            catch (Exception ex)
            {

                _logFile.WriteLine("Русич : Произошла ошибка " + ex.Message + " StackTrace: " + ex.StackTrace);
                new System.Net.Mail.SmtpClient("mail.mcruises.ru").Send("errorreport@mcruises.ru",
                                                                                     "tech_error@mcruises.ru", "Русич",
                                                                                    string.Format("Русич : Произошла ошибка " + ex.Message + " StackTrace: " + ex.StackTrace));
                throw;
            }
      
        }
    }
}
