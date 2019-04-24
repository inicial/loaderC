using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;


using System.Globalization;
using System.IO;

using System.Net;
using System.Reflection;
//using System.Web.Helpers;
using System.Web.Helpers;
using DxHelpersLib;
using Newtonsoft.Json;
using PluginInteractionLib;

namespace MosTurFlotLoader
{
    class MosTurFlotDataManipulator :DataManipulator
    {
        private SqlConnection _connection;
        DateTimeFormatInfo format = new DateTimeFormatInfo();
        private string apiUSERHASH = "6e232ab6d0d86649381030eb915eee012e9fb6cc";
        private string apiUrl = "http://booking.mosturflot.ru/api?";
        private string savePath = "";
        private string createTemp = @"CREATE TABLE [dbo].[##Temp_MosTurFlot_CategoryTariffs](
	[tourid] [int] NULL,
	[categoryid] [int] NULL,
	[tariffid] [int] NULL,
	[tariffname] [varchar](100) NULL,
	[tariffminprice] [float] NULL,
	[tariffpassgty] [varchar](10) NULL
)
CREATE TABLE [dbo].[##Temp_MosTurFlot_Cruises](
	[tourid] [int] NULL,
	[shipid] [int] NULL,
	[shipname] [varchar](100) NULL,
	[shipown] [int] NULL,
	[tourstart] [datetime] NULL,
	[tourfinish] [datetime] NULL,
	[tourroute] [varchar](300) NULL,
	[tourdays] [int] NULL,
	[tourholiday] [int] NULL,
	[touronline] [int] NULL,
	[tourminprice] [float] NULL,
	[tourcabinstotal] [int] NULL,
	[tourcabinsbusy] [int] NULL,
	[tourcabinsfree] [int] NULL,
	[tourdiscount] [int] NULL,
	[tourdiscountext] [datetime] NULL
)
CREATE TABLE [dbo].[##Temp_MosTurFlot_Meals](
	[tourid] [int] NULL,
	[categoryid] [int] NULL,
	[tariffid] [int] NULL,
	[mealid] [int] NULL,
	[mealname] [varchar](100) NULL,
	[mainprice] [float] NULL,
	[upperprice] [float] NULL,
	[advprice] [float] NULL
)
CREATE TABLE [dbo].[##Temp_MosTurFlot_TourTariffs](
	[tourid] [int] NULL,
	[categoryid] [int] NULL,
	[categoryname] [varchar](50) NULL,
	[categoryminprice] [varchar](50) NULL,
	[categorynote] [varchar](50) NULL,
	[tariffminprice] [float] NULL
)";
        private string insertCabin = @"INSERT INTO [dbo].[MosTurFlot_Cabins]
           ([ShipId]
           ,[cabinnumber]
           ,[cabincategoryname]
           ,[cabinclass]
           ,[cabinmainpass]
           ,[cabinupperpass]
           ,[cabinadvpass]
           ,[cabinmaxpass])
     VALUES
           (@ShipId
           ,@cabinnumber
           ,@cabincategoryname
           ,@cabinclass
           ,@cabinmainpass
           ,@cabinupperpass
           ,@cabinadvpass
           ,@cabinmaxpass)";
        private const string insertCabinStatus =@"INSERT INTO [dbo].[MosTurFlot_availability_cabins]
           ([cruiseId]
           ,[cabinId]
           ,[cabinNomber]
           ,[categoryId]
           ,[categoryName]
           ,[cabinStatus]
           ,[cabinStatusName])
     VALUES
           (@cruiseId
           ,@cabinId
           ,@cabinNomber
           ,@categoryId
           ,@categoryName
           ,@cabinStatus
           ,@cabinStatusName)";
        private const string insertExcu = @"INSERT INTO [dbo].[MosTurFlot_excursions]
           ([tourid]
           ,[cityid]
           ,[arrival]
           ,[desc]
           ,[type]
           ,[typename]
           ,[departure]
           ,[date])
     VALUES
           (@tourid
           ,@cityid
           ,@arrival
           ,@desc
           ,@type
           ,@typename
           ,@departure
           ,@date)";

        private const string clearCruises = "DELETE FROM [dbo].[MosTurFlot_CategoryTariffs] " +
                                            "insert into MosTurFlot_CategoryTariffs select * from ##Temp_MosTurFlot_CategoryTariffs " +
                                            "drop table ##Temp_MosTurFlot_CategoryTariffs " +
                                            "DELETE FROM [dbo].[MosTurFlot_Cruises] " +
                                            "insert into MosTurFlot_Cruises select * from ##Temp_MosTurFlot_Cruises " +
                                            "drop table ##Temp_MosTurFlot_Cruises " +
                                            "DELETE FROM [dbo].[MosTurFlot_Meals] " +
                                            "insert into MosTurFlot_Meals select * from ##Temp_MosTurFlot_Meals " +
                                            "drop table ##Temp_MosTurFlot_Meals " +
                                            "DELETE FROM [dbo].[MosTurFlot_TourTariffs] " +
                                            "insert into [MosTurFlot_TourTariffs] select * from ##Temp_MosTurFlot_TourTariffs " +
                                            "drop table ##Temp_MosTurFlot_TourTariffs ";
        private const string insertCategoryTariff = @"INSERT INTO [dbo].[##Temp_MosTurFlot_CategoryTariffs]
           ([tourid]
           ,[categoryid]
           ,[tariffid]
           ,[tariffname]
           ,[tariffminprice]
           ,[tariffpassgty])
     VALUES
           (@tourid
           ,@categoryid
           ,@tariffid
           ,@tariffname
           ,@tariffminprice
           ,@tariffpassgty)";
        private const string insertTourTariff = @"INSERT INTO [dbo].[##Temp_MosTurFlot_TourTariffs]
           ([tourid]
           ,[categoryid]
           ,[categoryname]
           ,[categoryminprice]
           ,[categorynote]
           ,[tariffminprice])
     VALUES
           (@tourid
           ,@categoryid
           ,@categoryname
           ,@categoryminprice
           ,@categorynote
           ,@tariffminprice)";
        private const string insertRoutDetail = @"INSERT INTO [dbo].[MosTurFlot_RouteDetail]
           ([tourid]
           ,[pointname]
           ,[cityid]
           ,[cityname]
           ,[arrival]
           ,[departure]
           ,[note]
           ,[date])
     VALUES
           (@tourid
           ,@pointname
           ,@cityid
           ,@cityname
           ,@arrival
           ,@departure
           ,@note
           ,@date)";
        private const string insertCruise = @"INSERT INTO [dbo].[##Temp_MosTurFlot_Cruises]
           ([tourid]
           ,[shipid]
           ,[shipname]
           ,[shipown]
           ,[tourstart]
           ,[tourfinish]
           ,[tourroute]
           ,[tourdays]
           ,[tourholiday]
           ,[touronline]
           ,[tourminprice]
           ,[tourcabinstotal]
           ,[tourcabinsbusy]
           ,[tourcabinsfree]
           ,[tourdiscount]
           ,[tourdiscountext])
     VALUES
           (@tourid
           ,@shipid
           ,@shipname
           ,@shipown
           ,@tourstart
           ,@tourfinish
           ,@tourroute
           ,@tourdays
           ,@tourholiday
           ,@touronline
           ,@tourminprice
           ,@tourcabinstotal
           ,@tourcabinsbusy
           ,@tourcabinsfree
           ,@tourdiscount
           ,@tourdiscountext)";
        private const string insertMeal = @"INSERT INTO [dbo].[##Temp_MosTurFlot_Meals]
           ([tourid]
           ,[categoryid]
           ,[tariffid]
           ,[mealid]
           ,[mealname]
           ,[mainprice]
           ,[upperprice]
           ,[advprice])
     VALUES
           (@tourid
           ,@categoryid
           ,@tariffid
           ,@mealid
           ,@mealname
           ,@mainprice
           ,@upperprice
           ,@advprice)";
        private const string insertShip= @"if not exists(select * from  MosTurFlot_Ships where id=@id and name = @name)
begin
INSERT INTO [dbo].[MosTurFlot_Ships]
           ([id]
           ,[name])
     VALUES
           (@id
           ,@name)
end ";
        public MosTurFlotDataManipulator(SqlConnection con, Logger log) : base(con, log)
        {
            _connection = con;  
            format.DateSeparator = "yyyy-MM-ddTHH:mm:ss";
            ExeConfigurationFileMap configMap = new ExeConfigurationFileMap();
            configMap.ExeConfigFilename = Path.Combine(Path.GetDirectoryName(Assembly.GetEntryAssembly().Location),
                                                       "lanta.sqlconfig.dll.config");
            Configuration config = ConfigurationManager.OpenMappedExeConfiguration(configMap,
                                                                                   ConfigurationUserLevel.None);
            apiUSERHASH = config.AppSettings.Settings["MosTurFlotapiUSERHASH"].Value;
            apiUrl = config.AppSettings.Settings["MosTurFlotURL"].Value;
            savePath = config.AppSettings.Settings["MosTurFlotPath"].Value+"\\"+DateTime.Now.ToString("ddMMyyyyHHmm")+"\\";
            if (!Directory.Exists(savePath))
            {
                Directory.CreateDirectory(savePath);
            }
        }

        public override string GetShipsData()
        {
            string fullUrl = apiUrl + "userhash=" + apiUSERHASH + "&" + "section=rivercruises&request=ships&own=false";
            WebClient client = new WebClient();
            byte[] data = client.DownloadData(fullUrl);
            Stream stream = new MemoryStream(data);           
            DataSet ds = new DataSet();
            ds.ReadXml(stream);
            DataTable items = ds.Tables["item"];
            //using (SqlCommand com = new SqlCommand("delete from MosTurFlot_Ships",_connection))
            //{
            //    com.ExecuteNonQuery();
            //}
            foreach (DataRow row in items.Rows)
            {
                using (SqlCommand com = new SqlCommand(insertShip,_connection))
                {
                    com.Parameters.AddWithValue("@id", ParseInt(row.Field<string>("shipid")));
                    com.Parameters.AddWithValue("@name", row.Field<string>("shipname"));
                    com.ExecuteNonQuery();

                }
            }
            return "МосТурФлот : Лайнеры успешно загружены";
        }

        private void SelectCruises(DateTime dateFrom, DateTime dateTo)
        {
            string fullUrl = apiUrl + "userhash=" + apiUSERHASH + "&format=json&section=rivercruises&request=tours&tariffs=true&routedetail=true&loading=true&own=false" + "&datefrom=" + dateFrom.ToString("yyyy-MM-ddTHH:mm:ss") + "&dateto=" + dateTo.ToString("yyyy-MM-ddTHH:mm:ss");
            WebClient client = new WebClient();
            byte[] data = client.DownloadData(fullUrl);
            Stream stream = new MemoryStream(data);
            StreamReader reader = new StreamReader(stream);
           // string strData = reader.ReadToEnd();
            BinaryWriter bw = new BinaryWriter(File.Create(savePath + "tours" + dateFrom.ToString("ddMMyyyyHHmm") + "_" + dateTo.ToString("ddMMyyyyHHmm")+".json"));
            bw.Write(data);
            bw.Close();
           // ds.ReadXml(stream);
          //  if(strData.IndexOf("\"answer\":[]")>=0)
            //    return;

           // var ddf = Json.Decode(strData);
          //  Debug.WriteLine(ddf);
           JsonSerializer seri = new JsonSerializer();
           
            HelperClass decode =  seri.Deserialize<HelperClass>(new JsonTextReader(reader)) ;
            foreach (KeyValuePair<string, Cruise> cruise in decode.answer)
            {
                Cruise selectedCruise = cruise.Value as Cruise;
                using (SqlCommand com = new SqlCommand(insertCruise, _connection))
                {
                    com.Parameters.AddWithValue("@tourid", ParseInt(selectedCruise.tourid));
                    com.Parameters.AddWithValue("@shipid", ParseInt(selectedCruise.shipid));
                    com.Parameters.AddWithValue("@shipname", selectedCruise.shipname);
                    com.Parameters.AddWithValue("@shipown", ParseInt(selectedCruise.shipown));
                    com.Parameters.AddWithValue("@tourstart", Convert.ToDateTime(selectedCruise.tourstart, format));
                    com.Parameters.AddWithValue("@tourfinish", Convert.ToDateTime(selectedCruise.tourfinish, format));
                    com.Parameters.AddWithValue("@tourroute", selectedCruise.tourroute.Substring(0, 300));
                    com.Parameters.AddWithValue("@tourdays", ParseInt(selectedCruise.tourdays));
                    com.Parameters.AddWithValue("@tourholiday", ParseInt(selectedCruise.tourholiday));
                    com.Parameters.AddWithValue("@touronline", ParseInt(selectedCruise.touronline));
                    com.Parameters.AddWithValue("@tourminprice", double.Parse(selectedCruise.tourminprice));
                    com.Parameters.AddWithValue("@tourcabinstotal", ParseInt(selectedCruise.tourcabinstotal));
                    com.Parameters.AddWithValue("@tourcabinsbusy", ParseInt(selectedCruise.tourcabinsbusy));
                    com.Parameters.AddWithValue("@tourcabinsfree", ParseInt(selectedCruise.tourcabinsfree));
                    if (IsFalse(selectedCruise.tourdiscountext))
                    {
                        com.Parameters.AddWithValue("@tourdiscount", 0);
                    }
                    else
                    {
                        com.Parameters.AddWithValue("@tourdiscount", ParseInt(selectedCruise.tourdiscount));
                    }
                    if (IsFalse(selectedCruise.tourdiscountext) || selectedCruise.tourdiscountext == "" || selectedCruise.tourdiscountext == "null" || selectedCruise.tourdiscountext == null)
                    {
                        com.Parameters.AddWithValue("@tourdiscountext", DBNull.Value);
                    }
                    else
                    {
                        com.Parameters.AddWithValue("@tourdiscountext", Convert.ToDateTime(selectedCruise.tourdiscountext,format));
                    }
                    

                    com.ExecuteNonQuery();

                }

                //foreach (RouteDetail routeDetail in selectedCruise.tourroutedetail)
                //{
                //    using (SqlCommand com = new SqlCommand(insertRoutDetail, _connection))
                //    {
                //        com.Parameters.AddWithValue("@tourid", ParseInt(selectedCruise.tourid));
                //        com.Parameters.AddWithValue("@pointname", routeDetail.pointname);
                //        com.Parameters.AddWithValue("@cityid", ParseInt(routeDetail.cityid));
                //        com.Parameters.AddWithValue("@cityname", routeDetail.cityname);
                //        if (routeDetail.arrival != "False")
                //        {
                //            com.Parameters.AddWithValue("@arrival", Convert.ToDateTime(routeDetail.arrival, format));
                //        }
                //        else
                //        {
                //            com.Parameters.AddWithValue("@arrival", DBNull.Value);
                //            com.Parameters["@arrival"].DbType = DbType.DateTime;
                //        }
                //        if (routeDetail.departure != "False")
                //        {
                //            com.Parameters.AddWithValue("@departure", Convert.ToDateTime(routeDetail.departure, format));
                //        }
                //        else
                //        {
                //            com.Parameters.AddWithValue("@departure", DBNull.Value);
                //            com.Parameters["@departure"].DbType = DbType.DateTime;
                //        }

                //        com.ExecuteNonQuery();

                //    }
                //}
                foreach (KeyValuePair<string, TourTariff> tarrif in selectedCruise.tourtariffs)
                {
                    TourTariff selectedTarrif = tarrif.Value as TourTariff;
                    using (SqlCommand com = new SqlCommand(insertTourTariff, _connection))
                    {
                        com.Parameters.AddWithValue("@tourid", ParseInt(selectedCruise.tourid));
                        com.Parameters.AddWithValue("@categoryid", ParseInt(selectedTarrif.categoryid));
                        com.Parameters.AddWithValue("@categoryname", selectedTarrif.categoryname);
                        if (IsFalse(selectedTarrif.categoryminprice) )
                        {
                            com.Parameters.AddWithValue("@categoryminprice", DBNull.Value);
                        }
                        else
                        {
                            com.Parameters.AddWithValue("@categoryminprice", selectedTarrif.categoryminprice);
                        }
                        if (IsFalse(selectedTarrif.categorynote)|| selectedTarrif.categorynote == null)
                        {
                            com.Parameters.AddWithValue("@categorynote", DBNull.Value);
                        }
                        else
                        {
                            com.Parameters.AddWithValue("@categorynote", selectedTarrif.categorynote);
                        }

                        if (selectedTarrif.tariffminprice == null)
                        {
                            com.Parameters.AddWithValue("@tariffminprice", DBNull.Value);
                        }
                        else
                        {
                            com.Parameters.AddWithValue("@tariffminprice", double.Parse(selectedTarrif.tariffminprice));
                        }

                        com.ExecuteNonQuery();

                    }
                    foreach (KeyValuePair<string, CategoryTariffs> categorytariff in selectedTarrif.categorytariffs)
                    {
                        CategoryTariffs selectedcategorytariff = categorytariff.Value as CategoryTariffs;
                        using (SqlCommand com = new SqlCommand(insertCategoryTariff, _connection))
                        {
                            com.Parameters.AddWithValue("@tourid", ParseInt(selectedCruise.tourid));
                            com.Parameters.AddWithValue("@categoryid", ParseInt(selectedTarrif.categoryid));
                            com.Parameters.AddWithValue("@tariffid", ParseInt(selectedcategorytariff.tariffid));
                            com.Parameters.AddWithValue("@tariffname", selectedcategorytariff.tariffname);
                            if(selectedcategorytariff.tariffpassqty==null)
                            {
                                com.Parameters.AddWithValue("@tariffpassgty", DBNull.Value);
                            }
                            else
                            {
                                com.Parameters.AddWithValue("@tariffpassgty", selectedcategorytariff.tariffpassqty);
                            }
                            try
                            {
                                com.Parameters.AddWithValue("@tariffminprice",
                            double.Parse(selectedcategorytariff.tariffminprice));
                            }
                            catch (Exception)
                            {

                                com.Parameters.AddWithValue("@tariffminprice",DBNull.Value);
                            }

                            com.ExecuteNonQuery();

                        }
                        Dictionary<string, Meal> meals = selectedcategorytariff.meals;


                       // Messages.Information(selectedcategorytariff.meals.GetType().ToString());
                        
                        //if (selectedcategorytariff.meals.GetType() == typeof(Dictionary<string, dynamic>))
                        //{

                        //    var mealsTmp = selectedcategorytariff.meals as Dictionary<string, object>;
                        //    foreach (KeyValuePair<string, object> keyValuePair in mealsTmp)
                        //    {
                        //        Dictionary<string, object> tmp = keyValuePair.Value as Dictionary<string, object>;
                        //        Meal mealTmp = new Meal();
                        //        mealTmp.advprice = tmp["advprice"].ToString();
                        //        mealTmp.mainprice = tmp["mainprice"].ToString();
                        //        mealTmp.mealid = tmp["mealid"].ToString();
                        //        mealTmp.mealname = tmp["mealname"].ToString();
                        //        mealTmp.upperprice = tmp["upperprice"].ToString();
                        //        meals.Add(keyValuePair.Key, mealTmp);

                        //        //Console.WriteLine(keyValuePair.Value);
                        //    }

                        //}
                        //else if (selectedcategorytariff.meals.GetType() == typeof (dynamic[]))
                        //{
                        //    int i = 0;
                        //    foreach (var meal in selectedcategorytariff.meals)
                        //    {
                        //        Dictionary<string, object> tmp = meal as Dictionary<string, object>;
                        //        Meal mealTmp = new Meal();
                        //        mealTmp.advprice = tmp["advprice"].ToString();
                        //        mealTmp.mainprice = tmp["mainprice"].ToString();
                        //        mealTmp.mealid = tmp["mealid"].ToString();
                        //        mealTmp.mealname = tmp["mealname"].ToString();
                        //        mealTmp.upperprice = tmp["upperprice"].ToString();
                        //        meals.Add(i.ToString(), mealTmp);
                        //        i++;
                        //    }
                        //}
                        //else
                        //{
                        //    throw new Exception("Meals not found");
                        //}

                        // Dictionary<string,Meal> meals = new Dictionary<string, Meal>();


                        foreach (KeyValuePair<string, Meal> meal in meals)
                        {
                            Meal selectedMeal = meal.Value;
                           // Console.WriteLine(Json.Encode(selectedMeal));
                            using (SqlCommand com = new SqlCommand(insertMeal, _connection))
                            {
                                com.Parameters.AddWithValue("@tourid", ParseInt(selectedCruise.tourid));
                                com.Parameters.AddWithValue("@categoryid", ParseInt(selectedTarrif.categoryid));
                                com.Parameters.AddWithValue("@tariffid", ParseInt(selectedcategorytariff.tariffid));
                                com.Parameters.AddWithValue("@mealid", ParseInt(selectedMeal.mealid));
                                com.Parameters.AddWithValue("@mealname", selectedMeal.mealname);
                                if (IsFalse(selectedMeal.mainprice))
                                {
                                    com.Parameters.AddWithValue("@mainprice", DBNull.Value);
                                }
                                else
                                {
                                    com.Parameters.AddWithValue("@mainprice", double.Parse(selectedMeal.mainprice));
                                }
                                if (IsFalse(selectedMeal.upperprice ))
                                {
                                    com.Parameters.AddWithValue("@upperprice", DBNull.Value);
                                }
                                else
                                {
                                    com.Parameters.AddWithValue("@upperprice", double.Parse(selectedMeal.upperprice));
                                }
                                if (IsFalse(selectedMeal.advprice ))
                                {
                                    com.Parameters.AddWithValue("@advprice", DBNull.Value);
                                }
                                else
                                {
                                    com.Parameters.AddWithValue("@advprice", double.Parse(selectedMeal.advprice));
                                }
                                com.ExecuteNonQuery();

                            }

                        }

                    }
                }
            }
        }
        object ParseInt(string value)
        {
            if (IsFalse(value) )
            {
                return DBNull.Value;
            }
            else
            {
                try
                {
                    return Int32.Parse(value);
                }
                catch (Exception)
                {

                    return DBNull.Value;
                }
            }
        }
        void GetAvailabilityData()
        {
            _logFile.WriteLine("МосТурФлот : Загрузка доступности кают");
            DataTable  dt = new DataTable();
            using (SqlDataAdapter adapter = new SqlDataAdapter(@"select tourid from MosTurFlot_Cruises", _connection))
            {
                adapter.Fill(dt);
            }

            using (SqlCommand com = new SqlCommand(@"delete from [MosTurFlot_availability_cabins]",_connection))
            {
                com.ExecuteNonQuery();
            }
            foreach (DataRow row in dt.Rows)
            {
                int cruiseId = row.Field<int>("tourid");
               string fullUrl = apiUrl + "userhash=" + apiUSERHASH + "&format=xml&section=rivercruises&request=tour&tourid="+ cruiseId.ToString()+"&loading=true";
                WebClient client = new WebClient();
                byte[] data ={0,0};
                try
                {
                    data = client.DownloadData(fullUrl);
                }
                catch (Exception)
                {
                   // _logFile.WriteLine("МосТурФлот : Произошла ошибка " + ex.InnerException + " StackTrace : " + ex.StackTrace);
                    new System.Net.Mail.SmtpClient("mail.mcruises.ru").Send("errorreport@mcruises.ru",
                                                                                        "tech_error@mcruises.ru", "Мостурлот",
                                                                                        string.Format("МосТурФлот : Не найден круиз " + cruiseId.ToString()));
                    continue;
                }

                Stream stream = new MemoryStream(data);
                DataSet ds = new DataSet();
                ds.ReadXml(stream);
                DataTable cabinsStatus = ds.Tables["item"];
                if(cabinsStatus==null)continue;
                foreach (DataRow dataRow in cabinsStatus.Rows)
                {
                    using (SqlCommand com = new SqlCommand(insertCabinStatus, _connection))
                    {
                        // FIXME Реализовать проверку по другому, а то это немного грязно
                        bool load = true;
                        try
                        {
                            string id = dataRow.Field<string>("cabincategoryid");
                            if (String.IsNullOrEmpty(dataRow.Field<string>("cabincategoryname")))
                            {
                                load = false;
                            }
                        } catch (Exception){
                            load = false;
                        }
                        if (load)
                        {
                            com.Parameters.AddWithValue("@cruiseId", cruiseId);
                            com.Parameters.AddWithValue("@cabinId", ParseInt(dataRow.Field<string>("cabinid")));
                            com.Parameters.AddWithValue("@cabinNomber", ParseInt(dataRow.Field<string>("cabinnumber")));
                            com.Parameters.AddWithValue("@categoryId", ParseInt(dataRow.Field<string>("cabincategoryid")));
                            com.Parameters.AddWithValue("@categoryName", dataRow.Field<string>("cabincategoryname"));
                            com.Parameters.AddWithValue("@cabinStatus", ParseInt(dataRow.Field<string>("cabinstatus")));
                            com.Parameters.AddWithValue("@cabinStatusName", dataRow.Field<string>("cabinstatusname"));
                            com.ExecuteNonQuery();
                        }
                    }
                }

            }
            _logFile.WriteLine("МосТурФлот : Загрузка доступности кают закончена");
        }


        public string GetCruiseData()
        {
            using (SqlCommand com = new SqlCommand(createTemp,_connection))
            {
                com.ExecuteNonQuery();
            }
            DateTime dateFrom, dateTo;
            dateFrom = DateTime.Now.Date.AddDays(-1);
            dateTo = dateFrom.AddDays(365).Date.AddDays(-1);
            SelectCruises(dateFrom, dateTo);
            dateFrom = dateTo.AddSeconds(1);
            dateTo = dateFrom.AddDays(365).AddDays(-1);
            SelectCruises(dateFrom, dateTo);
            using (SqlCommand com = new SqlCommand(clearCruises, _connection))
            {
                com.ExecuteNonQuery();
            }
            return "МосТурФлот : Круизы получены";
        }
        public override string GetDecksData()
        {
            throw new NotImplementedException();
        }

        public override string GetCabinsData()
        {
            _logFile.WriteLine("МосТурФлот : Загрузка кают");
            DataTable dt = new DataTable();
            using (SqlDataAdapter adapter = new SqlDataAdapter(@"select id from [MosTurFlot_Ships]", _connection))
            {
                adapter.Fill(dt);
            }

            using (SqlCommand com = new SqlCommand(@"delete from [MosTurFlot_Cabins]", _connection))
            {
                com.ExecuteNonQuery();
            }
            foreach (DataRow row in dt.Rows)
            {
                int idShip = row.Field<int>("id");
                string fullUrl = apiUrl + "userhash=" + apiUSERHASH + "&format=Json&section=rivercruises&request=ship&shipid=" + idShip.ToString() + "&cabins=true";
                WebClient client = new WebClient();
                byte[] data = client.DownloadData(fullUrl);
                Stream stream = new MemoryStream(data);
                StreamReader sr = new StreamReader(stream);
                string shipJson = sr.ReadToEnd();
                ShipRequest rezult;
                try
                {
                     rezult = Json.Decode<ShipRequest>(shipJson);
                }
                catch (Exception)
                {
                   // new System.Net.Mail.SmtpClient("mail.mcruises.ru").Send("errorreport@mcruises.ru",
                     //                                                                    "tech_error@mcruises.ru", "Мостурлот",
                       //string.Format("МосТурФлот : Не найден корабль " + idShip.ToString())); 
                    _logFile.WriteLine("МосТурФлот : Не найден корабль " + idShip.ToString());
                   continue;
                }
                //var rezult = Json.Decode<ShipRequest>(shipJson);
                Ship ship = rezult.answer;
                foreach (var cabin in ship.shipcabins)
                {
                    Cabin selectedCabin = cabin.Value;
                    using (SqlCommand com = new SqlCommand(insertCabin,_connection))
                    {
                        com.Parameters.AddWithValue("@ShipId", idShip);
                        com.Parameters.AddWithValue("@cabinnumber", ParseInt(selectedCabin.cabinnumber));
                        if (!string.IsNullOrEmpty(selectedCabin.cabincategoryname))
                        {
                            com.Parameters.AddWithValue("@cabincategoryname", selectedCabin.cabincategoryname);
                        }
                        else
                        {
                          com.Parameters.AddWithValue("@cabincategoryname", "");
                        }
                    if (selectedCabin.cabinclass != null)
                        {
                            com.Parameters.AddWithValue("@cabinclass", selectedCabin.cabinclass);
                        }
                        else
                        {
                            com.Parameters.AddWithValue("@cabinclass", DBNull.Value);
                        }
                        com.Parameters.AddWithValue("@cabinmainpass", ParseInt(selectedCabin.cabinmainpass));
                        com.Parameters.AddWithValue("@cabinupperpass", ParseInt(selectedCabin.cabinupperpass));
                        com.Parameters.AddWithValue("@cabinadvpass", ParseInt(selectedCabin.cabinadvpass));
                        com.Parameters.AddWithValue("@cabinmaxpass", ParseInt(selectedCabin.cabinmaxpass));
                        
                        com.ExecuteNonQuery();
                    }
                }
                //DataSet ds = new DataSet();
                //ds.ReadXml(stream);
                //DataTable cabins = ds.Tables[0];
            }
            _logFile.WriteLine("МосТурФлот : Загрузка кают закончена");
            return "";
        }

        public override string GetItineraryData()
        {
            _logFile.WriteLine("МосТурФлот : Загрузка маршрутов");
            DataTable dt = new DataTable();
            using (SqlDataAdapter adapter = new SqlDataAdapter(@"select tourid from MosTurFlot_Cruises", _connection))
            {
                adapter.Fill(dt);
            }

            using (SqlCommand com = new SqlCommand(@"delete from [MosTurFlot_RouteDetail]  delete from [MosTurFlot_excursions]", _connection))
            {
                com.ExecuteNonQuery();
            }
            foreach (DataRow row in dt.Rows)
            {
                int cruiseId = row.Field<int>("tourid");
                string fullUrl = apiUrl + "userhash=" + apiUSERHASH + "&format=json&section=rivercruises&request=tour&tourid=" + cruiseId.ToString() + "&routedetail=true";
              //  _logFile.WriteLine("МосТурФлот : " + fullUrl);
                WebClient client = new WebClient();
                byte[] data;
                try
                {
                    data = client.DownloadData(fullUrl);
                }
                catch (Exception)
                {
                    
                    continue;
                }
                
                Stream stream = new MemoryStream(data);
                StreamReader reader = new StreamReader(stream);
                string strData = reader.ReadToEnd();
                if (strData.IndexOf("\"answer\":[]") >= 0) return"";
                var decode = Json.Decode<RoutDetailRequest>(strData);

                foreach (var routeDetails in decode.answer.tourroutedetail)
                {
                    RouteDetail routeDetail = routeDetails.Value;
                    if (routeDetail.cityname == null){continue;}
                    using (SqlCommand com = new SqlCommand(insertRoutDetail, _connection))
                    {
                        com.Parameters.AddWithValue("@tourid", cruiseId);
                        com.Parameters.AddWithValue("@pointname", routeDetail.pointname);
                        com.Parameters.AddWithValue("@cityid", ParseInt(routeDetail.cityid));
                        com.Parameters.AddWithValue("@cityname", routeDetail.cityname);
                        com.Parameters.AddWithValue("@note", routeDetail.note);
                        if (!IsFalse(routeDetail.date))
                        {
                            com.Parameters.AddWithValue("@date", Convert.ToDateTime(routeDetail.date, format));
                        }
                        else
                        {
                            com.Parameters.AddWithValue("@date", DBNull.Value);
                            com.Parameters["@date"].DbType = DbType.DateTime;
                        }
                        if (!IsFalse(routeDetail.arrival))
                        {
                            com.Parameters.AddWithValue("@arrival", Convert.ToDateTime(routeDetail.arrival, format));
                        }
                        else
                        {
                            com.Parameters.AddWithValue("@arrival", DBNull.Value);
                            com.Parameters["@arrival"].DbType = DbType.DateTime;
                        }
                        if (!IsFalse(routeDetail.departure ))
                        {
                            com.Parameters.AddWithValue("@departure", Convert.ToDateTime(routeDetail.departure, format));
                        }
                        else
                        {
                            com.Parameters.AddWithValue("@departure", DBNull.Value);
                            com.Parameters["@departure"].DbType = DbType.DateTime;
                        }

                        com.ExecuteNonQuery();

                    }
                    Dictionary<string, Excursion> exursions = routeDetail.excursions as Dictionary<string, Excursion>;
                    if (exursions != null)
                    {
                        foreach (var excursion in exursions)
                        {
                            Excursion exurs = excursion.Value;
                            string desc = exurs.desc;
                            int? type =(int?)ParseInt(exurs.type);
                            string typename = exurs.typename;
                            using (SqlCommand com = new SqlCommand(insertExcu,_connection))
                            {
                                com.Parameters.AddWithValue("@tourid",cruiseId);
                                com.Parameters.AddWithValue("@cityid", routeDetail.cityid);
                                if (!IsFalse(routeDetail.arrival ))
                                {
                                    com.Parameters.AddWithValue("@arrival", Convert.ToDateTime(routeDetail.arrival, format));
                                }
                                else
                                {
                                    com.Parameters.AddWithValue("@arrival", DBNull.Value);
                                    com.Parameters["@arrival"].DbType = DbType.DateTime;
                                }
                                if (!IsFalse(routeDetail.date ))
                                {
                                    com.Parameters.AddWithValue("@date", Convert.ToDateTime(routeDetail.date, format));
                                }
                                else
                                {
                                    com.Parameters.AddWithValue("@date", DBNull.Value);
                                    com.Parameters["@date"].DbType = DbType.DateTime;
                                }
                                if (!IsFalse(routeDetail.departure ))
                                {
                                    com.Parameters.AddWithValue("@departure", Convert.ToDateTime(routeDetail.departure, format));
                                }
                                else
                                {
                                    com.Parameters.AddWithValue("@departure", DBNull.Value);
                                    com.Parameters["@departure"].DbType = DbType.DateTime;
                                }
                                
                                com.Parameters.AddWithValue("@desc", desc);
                                if (type == null)
                                {
                                    com.Parameters.AddWithValue("@type", DBNull.Value);
                                }
                                else
                                {
                                    com.Parameters.AddWithValue("@type", type);
                                }
                                
                                com.Parameters.AddWithValue("@typename", typename);
                                com.ExecuteNonQuery();
                            }

                        }
                    }
                }
                //DataSet ds = new DataSet();
                //ds.ReadXml(stream);

                //DataTable cabinsStatus = ds.Tables["item"];
                //foreach (DataRow dataRow in cabinsStatus.Rows)
                //{
                //    using (SqlCommand com = new SqlCommand(insertCabinStatus, _connection))
                //    {
                //        //com.Parameters.AddWithValue("@cruiseId", cruiseId);
                //        //com.Parameters.AddWithValue("@cabinId", ParseInt(dataRow.Field<string>("cabinid")));
                //        //com.Parameters.AddWithValue("@cabinNomber", ParseInt(dataRow.Field<string>("cabinnumber")));
                //        //com.Parameters.AddWithValue("@categoryId", ParseInt(dataRow.Field<string>("cabincategoryid")));
                //        //com.Parameters.AddWithValue("@categoryName", dataRow.Field<string>("cabincategoryname"));
                //        //com.Parameters.AddWithValue("@cabinStatus", ParseInt(dataRow.Field<string>("cabinstatus")));
                //        //com.Parameters.AddWithValue("@cabinStatusName", dataRow.Field<string>("cabinstatusname"));
                //        //com.ExecuteNonQuery();

                //    }
                //}

            }
            _logFile.WriteLine("МосТурФлот : Загрузка маршрутов закончена");
            return "";
        }
        void SumbitChanges()
        {
            _logFile.WriteLine("МосТурФлот : Применение изменений в базе");
            using (SqlCommand com = new SqlCommand("MosTurFlot",_connection))
            {
                com.CommandType= CommandType.StoredProcedure;
                com.ExecuteNonQuery();
            }
            _logFile.WriteLine("МосТурФлот : Применение изменений в базе закончено");
        }
        private bool IsFalse(string a)
        {
            if (a == null) return true;
            if (a.ToLower().Trim() == "false")
            {
                return true;
            }
            else
            {
                return false;
            }
        }
       
        public override void GetData()
        {
            try
            {
                _logFile.WriteLine("МосТурФлот : Начало загрузки");
                _logFile.WriteLine(GetShipsData());
                _logFile.WriteLine(GetCruiseData());
                GetAvailabilityData();
                GetCabinsData();
                GetItineraryData();
                SumbitChanges();
                _logFile.WriteLine("МосТурФлот : Загрузка окончена");
            }
            catch (Exception ex)
            {

                _logFile.WriteLine("МосТурФлот : Произошла ошибка " + ex.InnerException + " StackTrace : " + ex.StackTrace);
                new System.Net.Mail.SmtpClient("mail.mcruises.ru").Send("errorreport@mcruises.ru",
                                                                                    "tech_error@mcruises.ru", "Мостурлот",
                                                                                    string.Format("МосТурФлот : Произошла ошибка " + ex.InnerException + " StackTrace : " + ex.StackTrace));
                throw;
            }

        }
    }
}