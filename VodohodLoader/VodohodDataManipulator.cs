using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Text;
using System.Web.Helpers;
using DxHelpersLib;
using Newtonsoft.Json;
using PluginInteractionLib;

namespace VodohodLoader
{

    class VodohodDataManipulator : DataManipulator
    {
        private DateTimeFormatInfo dateformat = new DateTimeFormatInfo();
        private const string insertRoom = @"INSERT INTO [dbo].[Vodohod_Room_availability]
           ([cruise_id]
           ,[rt_name]
           ,[rp_name]
           ,[cabinnomber]
           ,[deck_name])
     VALUES
           (@cruise_id
           ,@rt_name
           ,@rp_name
           ,@cabinnomber
           ,@deck_name)";
        private const string insertPrice = @"INSERT INTO [dbo].[Vodohod_Prices]
           ([id_cruise]
           ,[tariff_name]
           ,[deck_name]
           ,[rt_name]
           ,[rp_name]
           ,[price_value])
     VALUES
           (@id_cruise
           ,@tariff_name
           ,@deck_name
           ,@rt_name
           ,@rp_name
           ,@price_value)";
        private const string insertItinerary = @"INSERT INTO [dbo].[Vodohod_itinerary]
           ([id_cruise]
           ,[day]
           ,[time_start]
           ,[time_stop]
           ,[excursion]
           ,[port])
     VALUES
           (@id_cruise
           ,@day
           ,@time_start
           ,@time_stop
           ,@excursion
           ,@port)";
        private const string insertShip = @"INSERT INTO [dbo].[Vodohod_Ships]
           ([id]
           ,[name]
           ,[code]
           ,[image]
           ,[decks]
           ,[decks_pre]
           ,[description])
     VALUES
           (@id
           ,@name
           ,@code
           ,@image
           ,@decks
           ,@decks_pre
           ,@description)";
        private const string insertCruise = @"INSERT INTO [dbo].[Vodohod_Cruises]
           ([id_cruise]
           ,[motorship_id]
           ,[name]
           ,[days]
           ,[date_start]
           ,[date_stop]
           ,[availability_count]
           ,[directions])
     VALUES
           (@id_cruise
           ,@motorship_id
           ,@name
           ,@days
           ,@date_start
           ,@date_stop
           ,@availability_count
           ,@directions)";
        private string apikey = "fvWvnjkacSvoWSmcz";
        private string urlBegin = "http://cruises.vodohod.com/agency/";
        public VodohodDataManipulator(SqlConnection con, Logger log) : base(con, log)
        {
            ExeConfigurationFileMap configMap = new ExeConfigurationFileMap();
            configMap.ExeConfigFilename = Path.Combine(Path.GetDirectoryName(Assembly.GetEntryAssembly().Location),
                                                       "lanta.sqlconfig.dll.config");
            Configuration config = ConfigurationManager.OpenMappedExeConfiguration(configMap,
                                                                                   ConfigurationUserLevel.None);
            apikey = config.AppSettings.Settings["VodohodApiKey"].Value;
            urlBegin = config.AppSettings.Settings["VodohodURL"].Value;
            dateformat.FullDateTimePattern = "YYYY-MM-DD";
        }

        public override string GetShipsData()
        {
            _logFile.WriteLine("Водоход : Начало загрузки лайнеров");
            WebRequest request = HttpWebRequest.Create(urlBegin + "json-motorships.htm?pauth="+apikey);
            WebResponse response = request.GetResponse();

            Stream stream = response.GetResponseStream();
            StreamReader sr = new StreamReader(stream);
            //string shipsJson = sr.ReadToEnd();
            //Console.WriteLine(shipsJson);
          
            
            //var rezult = Json.Decode<Dictionary<string, Ship>>(shipsJson);
            Newtonsoft.Json.JsonSerializer jsser = new JsonSerializer();
            var rezult = jsser.Deserialize<Dictionary<string, Ship>>(new JsonTextReader(sr));
            using (SqlCommand com = new SqlCommand(@"delete from [Vodohod_Ships]",_connection))
            {
                com.ExecuteNonQuery();
            }
            foreach (KeyValuePair<string, Ship> ship in rezult)
            {
                Ship selectedShip = ship.Value;
                using (SqlCommand com = new SqlCommand(insertShip,_connection))
                {
                    com.Parameters.AddWithValue("@id", ship.Key);
                    com.Parameters.AddWithValue("@name", selectedShip.name);
                    com.Parameters.AddWithValue("@code", selectedShip.code);
                    com.Parameters.AddWithValue("@image", selectedShip.image);
                    com.Parameters.AddWithValue("@decks", selectedShip.decks);
                    com.Parameters.AddWithValue("@decks_pre", selectedShip.decks_pre);
                    com.Parameters.AddWithValue("@description", selectedShip.description);
                    com.ExecuteNonQuery();
                }
            }
            _logFile.WriteLine("Водоход : Лайнеры загружены");
            return "Водоход : Загружено "+rezult.Count + " лайнеров";

        }

        public override string GetDecksData()
        {
            throw new NotImplementedException();
        }

        public override string GetCabinsData()
        {
            throw new NotImplementedException();
        }

        void GetCruiseData()
        {
            _logFile.WriteLine("Водоход : Начало загрузки круизов");
            WebRequest request = HttpWebRequest.Create(urlBegin + "json-cruises.htm?pauth=" + apikey);
            WebResponse response = request.GetResponse();

            Stream stream = response.GetResponseStream();
            StreamReader sr = new StreamReader(stream);
            string cruisesJson = sr.ReadToEnd();
            //Console.WriteLine(shipsJson);
            var rezult = Json.Decode<Dictionary<string, Cruise>>(cruisesJson);
            using (SqlCommand com = new SqlCommand(@"delete from [Vodohod_Cruises]", _connection))
            {
                com.ExecuteNonQuery();
            }
            foreach (KeyValuePair<string, Cruise> cruise in rezult)
            {
                Cruise selectedCruise = cruise.Value;
                using (SqlCommand com = new SqlCommand(insertCruise, _connection))
                {
                    com.Parameters.AddWithValue("@id_cruise", cruise.Key);
                    com.Parameters.AddWithValue("@motorship_id", selectedCruise.motorship_id);
                    com.Parameters.AddWithValue("@name", selectedCruise.name);
                    com.Parameters.AddWithValue("@days", int.Parse(selectedCruise.days));
                    com.Parameters.AddWithValue("@date_start", DateTime.Parse(selectedCruise.date_start,dateformat));
                    com.Parameters.AddWithValue("@date_stop", DateTime.Parse(selectedCruise.date_stop, dateformat));
                    com.Parameters.AddWithValue("@availability_count", int.Parse(selectedCruise.availability_count));
                    string directions = null;
                    foreach (string direction in selectedCruise.directions)
                    {
                        if (directions == null)
                        {
                            directions = direction;
                        }
                        else
                        {
                            directions += "|" + direction;
                        }
                    }
                    if (directions == null)
                    {
                        com.Parameters.AddWithValue("@directions", DBNull.Value);
                    }
                    else
                    {
                        com.Parameters.AddWithValue("@directions", directions);
                    }
                    
                    com.ExecuteNonQuery();
                }
            }
            _logFile.WriteLine("Водоход : Круизы загружены");
        }
        public override string GetItineraryData()
        {
            _logFile.WriteLine("Водоход : Начало загрузки маршрутов");
            DataTable dt = new DataTable();
            using (SqlDataAdapter adapter = new SqlDataAdapter(@"select id_cruise from [Vodohod_Cruises] ", _connection))
            {
                adapter.Fill(dt);
            }
            using (SqlCommand com = new SqlCommand(@"delete from [Vodohod_itinerary]",_connection))
            {
                com.ExecuteNonQuery();
            }
            foreach (DataRow row in dt.Rows)
            {
                string id_cruise = row.Field<string>("id_cruise");
                WebRequest request = HttpWebRequest.Create(urlBegin + "json-days.htm?pauth=" + apikey + "&cruise="+id_cruise);
                WebResponse response = request.GetResponse();

                Stream stream = response.GetResponseStream();
                StreamReader sr = new StreamReader(stream);
                string itinJson = sr.ReadToEnd();
                //Console.WriteLine(shipsJson);
                var rezult = Json.Decode<ItineraryStep[]>(itinJson);
                foreach (ItineraryStep itineraryStep in rezult)
                {
                    using (SqlCommand com = new SqlCommand(insertItinerary,_connection))
                    {
                        com.Parameters.AddWithValue("@id_cruise",id_cruise);
                        com.Parameters.AddWithValue("@day", int.Parse(itineraryStep.day));
                        com.Parameters.AddWithValue("@time_start", itineraryStep.time_start);
                        com.Parameters.AddWithValue("@time_stop", itineraryStep.time_stop);
                        com.Parameters.AddWithValue("@excursion", itineraryStep.excursion);
                        com.Parameters.AddWithValue("@port", itineraryStep.port);
                        com.ExecuteNonQuery();

                    }
                }
            }
            _logFile.WriteLine("Водоход : Маршруты загружены");
            return "";
        }
        void GetPriceData()
        {
            _logFile.WriteLine("Водоход : Начало загрузки цен");
            DataTable dt = new DataTable();
            using (SqlDataAdapter adapter = new SqlDataAdapter(@"select id_cruise from [Vodohod_Cruises] ", _connection))
            {
                adapter.Fill(dt);
            }
            using (SqlCommand com = new SqlCommand(@"delete from [Vodohod_Prices] delete from [Vodohod_Room_availability]", _connection))
            {
                com.ExecuteNonQuery();
            }
            foreach (DataRow row in dt.Rows)
            {
                string id_cruise = row.Field<string>("id_cruise");
                WebRequest request = HttpWebRequest.Create(urlBegin + "json-prices.htm?pauth=" + apikey + "&cruise=" + id_cruise);
                WebResponse response = request.GetResponse();

                Stream stream = response.GetResponseStream();
                StreamReader sr = new StreamReader(stream);
                string priceJson = sr.ReadToEnd();
                //Console.WriteLine(shipsJson);
                var rezult = Json.Decode<PriceList>(priceJson);
                //Console.WriteLine(rezult);
                //Цены
                foreach (Tariff tariff in rezult.tariffs)
                {
                    string selectedTarriffName = tariff.tariff_name;
                    foreach (Price price in tariff.prices)
                    {
                        using (SqlCommand com = new SqlCommand(insertPrice,_connection))
                        {
                            com.Parameters.AddWithValue("@id_cruise", id_cruise);
                            com.Parameters.AddWithValue("@tariff_name", selectedTarriffName);
                            com.Parameters.AddWithValue("@deck_name", price.deck_name);
                            com.Parameters.AddWithValue("@rt_name", price.rt_name);
                            com.Parameters.AddWithValue("@rp_name", price.rp_name);
                            try
                            {
                                if (string.IsNullOrEmpty(price.price_value) || price.price_value == "—")
                                {

                                    com.Parameters.AddWithValue("@price_value", DBNull.Value);
                                }
                                else
                                {
                                    com.Parameters.AddWithValue("@price_value",
                                                                double.Parse(price.price_value.Replace(".", ",")));
                                }

                            }
                            catch (Exception)
                            {

                                com.Parameters.AddWithValue("@price_value", DBNull.Value);
                            }


                            com.ExecuteNonQuery();
                        }
                    }
                }
                
                
                //Доступность кают
                Dictionary<string, object> room_avdi = rezult.room_availability as Dictionary<string, object>;
                object[] room_avarr = rezult.room_availability as object[];

                if (room_avdi != null)
                {
                    foreach (KeyValuePair<string, object> keyValuePair in room_avdi)
                    {
                        string deck = rezult.tariffs[0].prices[int.Parse(keyValuePair.Key)].deck_name;
                        string rt_name = rezult.tariffs[0].prices[int.Parse(keyValuePair.Key)].rt_name;
                        string rp_name = rezult.tariffs[0].prices[int.Parse(keyValuePair.Key)].rp_name;
                        object[] cabins = keyValuePair.Value as object[];
                        foreach (object cabin in cabins)
                        {
                            string cab = cabin as string;
                            using (SqlCommand com = new SqlCommand(insertRoom,_connection))
                            {
                                com.Parameters.AddWithValue("@cruise_id", id_cruise);
                                com.Parameters.AddWithValue("@rt_name", rt_name);
                                com.Parameters.AddWithValue("@rp_name", rp_name);
                                com.Parameters.AddWithValue("@cabinnomber", cab);
                                com.Parameters.AddWithValue("@deck_name", deck);
                                com.ExecuteNonQuery();

                            }

                        }
                    }
                }
                if (room_avarr != null)
                {
                    for (int j=0; j < room_avarr.Length;j++ )
                    {
                        string deck = rezult.tariffs[0].prices[j].deck_name;
                        string rt_name = rezult.tariffs[0].prices[j].rt_name;
                        string rp_name = rezult.tariffs[0].prices[j].rp_name;
                        object[] cabins = room_avarr[j] as object[];
                        foreach (object cabin in cabins)
                        {
                            string cab = cabin as string;
                            using (SqlCommand com = new SqlCommand(insertRoom, _connection))
                            {
                                com.Parameters.AddWithValue("@cruise_id", id_cruise);
                                com.Parameters.AddWithValue("@rt_name", rt_name);
                                com.Parameters.AddWithValue("@rp_name", rp_name);
                                com.Parameters.AddWithValue("@cabinnomber", cab);
                                com.Parameters.AddWithValue("@deck_name", deck);
                                com.ExecuteNonQuery();

                            }
                        }
                    }
                }

            }
            _logFile.WriteLine("Водоход : Цены загружены");
        }

        void SumbitChanges()
        {
            _logFile.WriteLine("Водоход : Применение изменений в базе");
            using (SqlCommand com = new SqlCommand("dbo.vodohod",_connection))
            {
                com.CommandType = CommandType.StoredProcedure;
                com.ExecuteNonQuery();
            }
            _logFile.WriteLine("Водоход : Применение измененний в базе закончено");
        }
        public override void GetData()
        {
            _logFile.WriteLine("Водоход : Начало загрузки");
            GetShipsData();
            GetCruiseData();
            GetItineraryData();
            GetPriceData();
            SumbitChanges();
            _logFile.WriteLine("Водоход : Загрузка окончена");
        }
    }
}
