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
using Newtonsoft.Json;
using PluginInteractionLib;

namespace InfoflotLoader
{
    class InfoFlotDataManipulator :DataManipulator
    {
        private const string insExcur = @"INSERT INTO [dbo].[Infoflot_additional_excursions]
           ([name]
           ,[description]
           ,[price]
           ,[currency]
           ,[id_Step])
     VALUES
           (@name
           ,@desc
           ,@price
           ,@cur
           ,@idStep)";
        
        
        private const string insertCabin = @"INSERT INTO [dbo].[InfoFlot_Cabins]
           ([ShipId]
           ,[CabinNomber]
           ,[CabinType]
           ,[GeneralPlase]
           ,[GeneralUppPlase]
           ,[DopPlase]
           ,[DopUppPlase]
           ,[CountPlase])
     VALUES
           (@ShipId
           ,@CabinNomber
           ,@CabinType
           ,@GeneralPlase
           ,@GeneralUppPlase
           ,@DopPlase
           ,@DopUppPlase
           ,@CountPlase)";
        private const string insertCabinStatus = @"INSERT INTO [dbo].[InfoFlot_CabinsStatus]
           ([id]
           ,[cruise_id]
           ,[ship_id]
           ,[name]
           ,[deck]
           ,[type]
           ,[price]
           ,[separate]
           ,[status]
           ,[gender])
     VALUES
           (@id
           ,@cruise_id
           ,@ship_id
           ,@name
           ,@deck
           ,@type
           ,@price
           ,@separate
           ,@status
           ,@gender)";
        private const string insertPlace = @"INSERT INTO [dbo].[InfoFlot_placseCabinStatus]
           ([id]
           ,[id_CabinStatus]
           ,[name]
           ,[type]
           ,[position]
           ,[status])
     VALUES
           (@id
           ,@id_CabinStatus
           ,@name
           ,@type
           ,@position
           ,@status)";
        private const string insertItinaryStep = @"INSERT INTO [dbo].[InfoFlot_Itinary]
           ([id]
           ,[city]
           ,[date_start]
           ,[time_start]
           ,[date_end]
           ,[time_end]
           ,[description]
           ,[cruise_id]
           ,[ship_id])
     VALUES
           (@id
           ,@city
           ,@date_start
           ,@time_start
           ,@date_end
           ,@time_end
           ,@description
           ,@cruise_id
           ,@ship_id)";
        private const string insertPrice = @"INSERT INTO [dbo].[InfoFlot_Prices]
           ([price_id]
           ,[cruise_id]
           ,[name]
           ,[price]
           ,[places_total]
           ,[places_free]
           ,[price_eur]
           ,[price_usd])
     VALUES
           (@price_id
           ,@cruise_id
           ,@name
           ,@price
           ,@places_total
           ,@places_free
           ,@price_eur
           ,@price_usd)";
        private const string insertCruise = @"INSERT INTO [dbo].[InfoFlot_Cruises]
           ([id]
           ,[name]
           ,[date_start]
           ,[date_end]
           ,[nights]
           ,[days]
           ,[cyties]
           ,[route]
           ,[ship_id]
           ,[weekend]
           ,[surchage_meal_rub]
           ,[surcharge_excursions_rub])
     VALUES
           (@id
           ,@name
           ,@date_start
           ,@date_end
           ,@nights
           ,@days
           ,@cyties
           ,@route
           ,@ship_id
           ,@weekend
           ,@surchage_meal_rub
           ,@surcharge_excursions_rub)";
        
        
        
        private  string apikey = "b2c156534863412ef70404fba3005cc53e683758";
        private  string urlBegin = "http://api.infoflot.com/JSON/";
        private SqlConnection _connection;
        private Logger _log;
        public InfoFlotDataManipulator(SqlConnection con, Logger log) : base(con, log)
        {
            _connection = con;
            _log = log;
            ExeConfigurationFileMap configMap = new ExeConfigurationFileMap();
            configMap.ExeConfigFilename = Path.Combine(Path.GetDirectoryName(Assembly.GetEntryAssembly().Location),
                                                       "lanta.sqlconfig.dll.config");
            Configuration config = ConfigurationManager.OpenMappedExeConfiguration(configMap,
                                                                                   ConfigurationUserLevel.None);
            apikey = config.AppSettings.Settings["InfoFlotapikey"].Value;
            urlBegin = config.AppSettings.Settings["InfoFlotURL"].Value;
        }
        
        public override string GetShipsData()
        {
            string urlEnd = "/Ships/";
            WebClient client = new WebClient();
            byte[] data = client.DownloadData(urlBegin + apikey + urlEnd);
            // Console.WriteLine(str);
            // Console.ReadKey();

            Stream stream = new MemoryStream(data);
            StreamReader reader = new StreamReader(stream);
            string str =
            reader.ReadToEnd();

            var decode = Json.Decode<Dictionary<string, dynamic>>(str);

           // Console.WriteLine(decode);
            List<ship> ships = new List<ship>();
            foreach (var s in decode)
            {
                //Console.WriteLine(s);
                string shipName = decode[s.Key] as string;
                ships.Add(new ship(Int32.Parse(s.Key), shipName));
                //Console.WriteLine(shipName);
            }
            foreach (ship ship in ships)
            {
                using (SqlCommand com = new SqlCommand(@"--declare @p1 int
--declare @p2 varchar(150)
--set @p1 = 221
--set @p2 = 'Alemannia'


If not Exists(select top 1 id from dbo.InfoFlot_Ships where  id = @p1 and name=@p2 )
begin
insert into dbo.InfoFlot_Ships(id,name) values (@p1,@p2)
end",_connection))
                {
                    com.Parameters.AddWithValue("@p1", ship.id);
                    com.Parameters.AddWithValue("@p2", ship.name);
                    com.ExecuteNonQuery();
                }
            }
            return "Инфофлот : Загружено " + ships.Count.ToString() + " кораблей";
        }

        public override string GetDecksData()
        {
            throw new NotImplementedException();
        }

        public override string GetCabinsData()
        {
            _logFile.WriteLine("Инфофлот : Начало загрузки кают");
            string urlEnd = "/Cabins/";
            WebClient client = new WebClient();
            
            //byte[] data = client.DownloadData("HTTP://api.infoflot.com/JSON/b2c156534863412ef70404fba3005cc53e683758/ShipsSpecial/38/");
            // Console.WriteLine(str);
            // Console.ReadKey();
            DataTable dt = new DataTable();
            using (SqlCommand com = new SqlCommand(@"select [id] from  InfoFlot_Ships where enable=1 delete from  InfoFlot_Cabins", _connection))
            {
                SqlDataAdapter adapter = new SqlDataAdapter(com);
                adapter.Fill(dt);
            }

            foreach (DataRow row in dt.Rows)
            {
                int ship_id = row.Field<int>("id");
                byte[] data = client.DownloadData(urlBegin + apikey + urlEnd+ship_id.ToString());
                
                Stream stream = new MemoryStream(data);
                StreamReader reader = new StreamReader(stream);
                string str =reader.ReadToEnd();
                var decode = Json.Decode<Dictionary<string, Cabin>>(str);
               // Console.WriteLine(decode);
                foreach (KeyValuePair<string, Cabin> keyValuePair in decode)
                {
                    Cabin selectCabin = keyValuePair.Value;
                    int plaseCount = selectCabin.places.Count();
                    int gen=0, dop=0, genupp=0, dopupp=0;
                    foreach (Plase place in selectCabin.places)
                    {
                        if (place.type == 0)
                        {
                            if (place.position == 0)
                            {
                                gen++;
                            }
                            else
                            {
                                genupp++;
                            }
                        }
                        else if (place.type == 1)
                        {
                            if (place.position == 0)
                            {
                                dop++;
                            }
                            else
                            {
                                dopupp++;
                            }
                        }
                    }                  
                    using (SqlCommand com = new SqlCommand(insertCabin,_connection) )
                    {
                        com.Parameters.AddWithValue("@ShipId", ship_id);
                        com.Parameters.AddWithValue("@CabinNomber", selectCabin.name);
                        com.Parameters.AddWithValue("@CabinType", selectCabin.type);
                        com.Parameters.AddWithValue("@GeneralPlase", gen);
                        com.Parameters.AddWithValue("@GeneralUppPlase", genupp);
                        com.Parameters.AddWithValue("@DopPlase", dop);
                        com.Parameters.AddWithValue("@DopUppPlase", dopupp);
                        com.Parameters.AddWithValue("@CountPlase", plaseCount);

                        string msg = "CABINS: ";
                        msg = msg + ship_id.ToString();
                        msg = msg + ", " + selectCabin.name;
                        msg = msg + ", " + selectCabin.type;
                        msg = msg + ", " + gen.ToString();
                        msg = msg + ", " + genupp.ToString();
                        msg = msg + ", " + dop.ToString();
                        msg = msg + ", " + dopupp.ToString();
                        msg = msg + ", " + plaseCount.ToString();
                        //_logFile.WriteLine(msg);

                        try
                        {
                            com.ExecuteNonQuery();
                        }
                        catch (System.Exception ex)
                        {
                            _logFile.WriteLine("GetCabinsData ERROR: " + ex.Message);
                        }
                    }
                }

            }

            return "Инфофлот : Категории кают загружены";

        }
        public string GetCitys()
        {
            string urlEnd = "/Cities/";
            WebClient client = new WebClient();
            byte[] data = client.DownloadData(urlBegin + apikey + urlEnd);
            // Console.WriteLine(str);
            // Console.ReadKey();

            Stream stream = new MemoryStream(data);
            StreamReader reader = new StreamReader(stream);
            string str =
            reader.ReadToEnd();

            var decode = Json.Decode<Dictionary<string, dynamic>>(str);

            // Console.WriteLine(decode);
            
            foreach (var s in decode)
            {
                //Console.WriteLine(s);
                string cytiName = decode[s.Key] as string;
                using (SqlCommand com = new SqlCommand(@"--declare @p1 int
--declare @p2 varchar(150)
--set @p1 = 221
--set @p2 = 'Alemannia'


If not Exists(select top 1 id from dbo.InfoFlot_citys where  id = @p1 and name=@p2 )
begin
insert into dbo.InfoFlot_citys(id,name) values (@p1,@p2)
end", _connection))
                {
                    com.Parameters.AddWithValue("@p1", int.Parse(s.Key));
                    com.Parameters.AddWithValue("@p2", cytiName);
                    com.ExecuteNonQuery();
                }
                //Console.WriteLine(shipName);
            }
  
            return "Инфофлот : Загружены города";
        }
        public string GetCruisesData()
        {
            string urlEnd = "/Tours/";
            WebClient client = new WebClient();
            
             DataTable dt = new DataTable();
             using (SqlCommand com = new SqlCommand(@"select [id] from InfoFlot_Ships where  [enable] =1  ", _connection))
            {
                SqlDataAdapter adapter = new SqlDataAdapter(com);
                adapter.Fill(dt);
            }
             using (SqlCommand com = new SqlCommand(@"delete from InfoFlot_Prices delete from InfoFlot_Cruises", _connection))
             {
                 com.ExecuteNonQuery();
             }
            foreach (DataRow row in dt.Rows)
            {
                int ship_id =row.Field<int>("id");
                string url = urlBegin + apikey + urlEnd + ship_id.ToString();
                bool bURL = false;
                byte[] data = client.DownloadData(url);
                Stream stream = new MemoryStream(data);
                StreamReader reader = new StreamReader(stream);
                string str =
                    reader.ReadToEnd();
                if(str=="[]"){continue;}
                var decode = Json.Decode<Dictionary<string,Cruise>>(str);
                
                foreach (var cruise in decode)
                {
                    Cruise selectedCruise = decode[cruise.Key];
                    //Console.WriteLine(Json.Encode(selectedCruise));
                    //BinaryWriter bw = new BinaryWriter(File.Create("c:\\\\test\\"+selectedCruise.name+".txt"));
                    //bw.Write(Json.Encode(selectedCruise));
                    //bw.Close();
                    using (SqlCommand com = new SqlCommand(insertCruise,_connection))
                    {
                        com.Parameters.AddWithValue("@id", int.Parse(cruise.Key));
                        com.Parameters.AddWithValue("@name", selectedCruise.name);
                        com.Parameters.AddWithValue("@date_start", DateTime.Parse(selectedCruise.date_start));
                        com.Parameters.AddWithValue("@date_end", DateTime.Parse(selectedCruise.date_end));
                        com.Parameters.AddWithValue("@nights", int.Parse(selectedCruise.nights));
                        com.Parameters.AddWithValue("@days", int.Parse(selectedCruise.days));
                        com.Parameters.AddWithValue("@cyties", selectedCruise.cities);
                        com.Parameters.AddWithValue("@route", selectedCruise.route);
                        com.Parameters.AddWithValue("@ship_id", ship_id);
                        com.Parameters.AddWithValue("@weekend", selectedCruise.weekend); 
                        if (selectedCruise.surchage_meal_rub == null)
                        {
                            com.Parameters.AddWithValue("@surchage_meal_rub", DBNull.Value);
                        }
                        else
                        {
                            com.Parameters.AddWithValue("@surchage_meal_rub", float.Parse(selectedCruise.surchage_meal_rub));
                        }

                        if (selectedCruise.surcharge_excursions_rub == null || selectedCruise.surcharge_excursions_rub == string.Empty)
                        {
                            com.Parameters.AddWithValue("@surcharge_excursions_rub", DBNull.Value);
                        }
                        else
                        {
                            com.Parameters.AddWithValue("@surcharge_excursions_rub", float.Parse(selectedCruise.surcharge_excursions_rub));
                        }
                        com.ExecuteNonQuery();
                    }
                    foreach (var cruisePrice in selectedCruise.prices)
                    {

                        CruisePrice selectedPrice = selectedCruise.prices[cruisePrice.Key];
                        using (SqlCommand com = new SqlCommand(insertPrice,_connection))
                        {
                            com.Parameters.AddWithValue("@price_id", int.Parse(cruisePrice.Key));
                            com.Parameters.AddWithValue("@cruise_id", int.Parse(cruise.Key));
                            com.Parameters.AddWithValue("@name", selectedPrice.name);
                            com.Parameters.AddWithValue("@price", double.Parse(selectedPrice.price));
                            com.Parameters.AddWithValue("@price_eur", double.Parse(selectedPrice.price_eur));
                            com.Parameters.AddWithValue("@price_usd", double.Parse(selectedPrice.price_usd));
                            if (selectedPrice.places_total == null)
                            {
                                com.Parameters.AddWithValue("@places_total", DBNull.Value);
                            }
                            else
                            {
                                com.Parameters.AddWithValue("@places_total",int.Parse(selectedPrice.places_total));
                            }
                            if (selectedPrice.places_free == null)
                            {
                                com.Parameters.AddWithValue("@places_free", DBNull.Value); 
                            }
                            else
                            {
                               com.Parameters.AddWithValue("@places_free", int.Parse(selectedPrice.places_free)); 
                            }

                            string msg = "PRICES: ";
                            msg = msg + cruisePrice.Key;
                            msg = msg + ", " + cruise.Key;
                            msg = msg + ", " + selectedPrice.name;
                            msg = msg + ", " + selectedPrice.price;
                            msg = msg + ", " + selectedPrice.price_eur;
                            msg = msg + ", " + selectedPrice.price_usd;
                            msg = msg + ", " + selectedPrice.places_total;
                            msg = msg + ", " + selectedPrice.places_free;
                            //if (cruise.Key == "303118")
                            //{
                            //    if (!bURL)
                            //    {
                            //        bURL = true;
                            //        _logFile.WriteLine(url);
                            //    }
                            //    _logFile.WriteLine(msg);
                            //}

                            try
                            {
                                com.ExecuteNonQuery();
                            }
                            catch (System.Exception ex)
                            {
                                _logFile.WriteLine("GetCruisesData ERROR: " + ex.Message);
                            }

                        }
                        
                        //Console.WriteLine(selrctedPrice.name + selrctedPrice.price);
                    }
                }
               // Console.WriteLine(decode);
            }
            return "Инфофлот : Круизы загружены";
        }
        public override string GetItineraryData()
        {
            string urlEnd = "/Excursions/";
            WebClient client = new WebClient();

            DataTable dt = new DataTable();
            using (SqlCommand com = new SqlCommand(@"select [id],ship_id from InfoFlot_Cruises ", _connection))
            {
                SqlDataAdapter adapter = new SqlDataAdapter(com);
                adapter.Fill(dt);
            }
            using (SqlCommand com = new SqlCommand(@"delete from InfoFlot_Itinary delete from Infoflot_additional_excursions ", _connection))
            {
                com.ExecuteNonQuery();
            }
            foreach (DataRow row in dt.Rows)
            {
                int ship_id=row.Field<int>("ship_id"),cruise_id = row.Field<int>("id");
                byte[] data = client.DownloadData(urlBegin + apikey + urlEnd + ship_id.ToString()+"/"+cruise_id.ToString());
                Stream stream = new MemoryStream(data);
                StreamReader reader = new StreamReader(stream);
               // string str =reader.ReadToEnd();
                //if (str == "[]")
                //{
                //    continue;
                //}
                Dictionary<string, ItinaryStep> decode = new Dictionary<string, ItinaryStep>();
                try
                {
                    Newtonsoft.Json.JsonSerializer serializer= new JsonSerializer();
                    decode = serializer.Deserialize<Dictionary<string, ItinaryStep>>(new JsonTextReader(reader) );
                        //Json.Decode<Dictionary<string, ItinaryStep>>(str);

                }
                catch (Exception)
                {
                    
                    Console.WriteLine("Не загружен " + cruise_id.ToString());
                }

                foreach (var itin in decode)
                {
                    ItinaryStep selectedStep = decode[itin.Key];
                    using (SqlCommand com = new SqlCommand(insertItinaryStep, _connection))
                    {
                        com.Parameters.AddWithValue("@id", int.Parse(itin.Key));
                        com.Parameters.AddWithValue("@city", selectedStep.city);
                        com.Parameters.AddWithValue("@date_start", DateTime.Parse(selectedStep.date_start));
                        com.Parameters.AddWithValue("@time_start", selectedStep.time_start);
                        string date_end = selectedStep.date_end;
                        if (date_end == "false") date_end = selectedStep.date_start;
                        string time_end = selectedStep.time_end;
                        if (time_end == "false") time_end = selectedStep.time_start;
                        try
                        {
                            com.Parameters.AddWithValue("@date_end", DateTime.Parse(date_end));
                            com.Parameters.AddWithValue("@time_end", time_end);
                        }
                        catch (System.Exception ex)
                        {
                            string str = ex.Message;
                        }
                        com.Parameters.AddWithValue("@description", selectedStep.description);
                        com.Parameters.AddWithValue("@cruise_id", cruise_id);
                        com.Parameters.AddWithValue("@ship_id", ship_id);
                        com.ExecuteNonQuery();
                    }
                    foreach (Excursions additionalExcursion in selectedStep.additional_excursions)
                    {
                        using (SqlCommand com = new SqlCommand (insExcur,_connection))
                        {
                            com.Parameters.AddWithValue("@name", additionalExcursion.name);
                            com.Parameters.AddWithValue("@desc", additionalExcursion.description);
                            com.Parameters.AddWithValue("@price", float.Parse(additionalExcursion.price));
                            com.Parameters.AddWithValue("@cur", additionalExcursion.currency);
                            com.Parameters.AddWithValue("@idStep", int.Parse(itin.Key));
                            com.ExecuteNonQuery();
                        }
                    }
                }
                // Console.WriteLine(decode);
            }
            return "Инфофлот : Маршруты загружены";
        }
        public string GetStatusCabin()
        {
            string urlEnd = "/CabinsStatus/";
            WebClient client = new WebClient();

            DataTable dt = new DataTable();
            using (SqlCommand com = new SqlCommand(@"select [id],ship_id from InfoFlot_Cruises", _connection))
            {
                SqlDataAdapter adapter = new SqlDataAdapter(com);
                adapter.Fill(dt);
            }
            using (SqlCommand com = new SqlCommand(@"delete from InfoFlot_placseCabinStatus delete from InfoFlot_CabinsStatus", _connection))
            {
                com.ExecuteNonQuery();
            }
            foreach (DataRow row in dt.Rows)
            {
                int ship_id = row.Field<int>("ship_id"), cruise_id = row.Field<int>("id");
                string url = urlBegin + apikey + urlEnd + ship_id.ToString() + "/" + cruise_id.ToString();
                byte[] data = client.DownloadData(url);
                Stream stream = new MemoryStream(data);
                StreamReader reader = new StreamReader(stream);

                string str = "" + reader.ReadToEnd();
                if (str.Length > 10)
                {
                    try
                    {
                        var decode = Json.Decode<Dictionary<string, CabinStatus>>(str);

                        foreach (var cabinStatus in decode)
                        {
                            CabinStatus selectedCabin = decode[cabinStatus.Key];
                            using (SqlCommand com = new SqlCommand(insertCabinStatus, _connection))
                            {
                                com.Parameters.AddWithValue("@id", int.Parse(cabinStatus.Key));
                                com.Parameters.AddWithValue("@cruise_id", cruise_id);
                                com.Parameters.AddWithValue("@ship_id", ship_id);
                                com.Parameters.AddWithValue("@name", selectedCabin.name);
                                com.Parameters.AddWithValue("@deck", selectedCabin.deck);
                                com.Parameters.AddWithValue("@type", selectedCabin.type);
                                com.Parameters.AddWithValue("@price", Double.Parse(selectedCabin.price));
                                com.Parameters.AddWithValue("@separate", selectedCabin.separate);
                                com.Parameters.AddWithValue("@status", selectedCabin.status);
                                com.Parameters.AddWithValue("@gender", selectedCabin.gender);
                                com.ExecuteNonQuery();
                            }
                            foreach (var cabinPlace in selectedCabin.places)
                            {

                                Place selectedPlace = selectedCabin.places[cabinPlace.Key];
                                using (SqlCommand com = new SqlCommand(insertPlace, _connection))
                                {
                                    com.Parameters.AddWithValue("@id", cabinPlace.Key);
                                    com.Parameters.AddWithValue("@id_CabinStatus", int.Parse(cabinStatus.Key));
                                    com.Parameters.AddWithValue("@name", selectedPlace.name);
                                    com.Parameters.AddWithValue("@type", selectedPlace.type);
                                    com.Parameters.AddWithValue("@position", selectedPlace.position);
                                    com.Parameters.AddWithValue("@status", selectedPlace.status);
                                    com.ExecuteNonQuery();
                                }
                            }
                        }
                    }
                    catch (System.Exception ex)
                    {
                        _logFile.WriteLine("Инфофлот : Ошибка чтения данных в GetStatusCabin " + ex.Message);
                    }
                }
                else
                {
                    _logFile.WriteLine("Инфофлот : Получены пустые данные в GetStatusCabin ship_id=" + ship_id.ToString() + " cruise_id=" + cruise_id.ToString() + " >> " + str + " URL: " + url);
                }
            }
            return "Инфофлот : Статусы кают загружены";
        } 
        public void SumbitChanges()
        {
            _logFile.WriteLine("Инфофлот : Применение изменений в базе");
            SqlCommand com = new SqlCommand("infoFlot",_connection);
            com.CommandType= CommandType.StoredProcedure;
            com.ExecuteNonQuery();
            _logFile.WriteLine("Инфофлот : Применение изменений в базе закончено");
        }
        public override void GetData()
        {
            try
            {

                _logFile.WriteLine("Инфофлот : Начало загрузки");
                _logFile.WriteLine(GetShipsData());
                _logFile.WriteLine(GetCitys());
                _logFile.WriteLine(GetCabinsData());
                _logFile.WriteLine(GetCruisesData());
                _logFile.WriteLine(GetItineraryData());
                _logFile.WriteLine(GetStatusCabin());
                SumbitChanges();
                _logFile.WriteLine("Инфофлот : Конец загрузки");
            }
            catch (Exception ex)
            {

                
                _logFile.WriteLine("Инфофлот : Произошла ошибка в загрузке данных : "+ex.Message+"  stackTrace : "+ex.StackTrace);
                //new System.Net.Mail.SmtpClient("mail.mcruises.ru").Send("errorreport@mcruises.ru",
                //                                                                   "tech_error@mcruises.ru", "Инфофлот",
                //                                                                   string.Format("Инфофлот : Произошла ошибка " + ex.InnerException + " StackTrace : " + ex.StackTrace));
                throw;

            }
           
        }
    }
}
