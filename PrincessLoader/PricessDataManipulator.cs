using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Net;
using System.Threading;
using System.Web;
using System.Windows.Forms;
using System.Data.SqlTypes;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Xml;
using DxHelpersLib;
using PluginInteractionLib;

namespace PrincessLoader
{
    class PricessDataManipulator:DataManipulator
    {
       // private string _path = "d:\\Princess";
        private string _file = null;
        //private XmlNode[] _cruises = null; 
        private string _cruises = null, _days = null, _price = null, _startdays = null;
        public PricessDataManipulator(SqlConnection con, Logger log) : base(con, log)
        {
            ExeConfigurationFileMap configMap = new ExeConfigurationFileMap();
            configMap.ExeConfigFilename = Path.Combine(Path.GetDirectoryName(Assembly.GetEntryAssembly().Location),
                                                       "lanta.sqlconfig.dll.config");
            Configuration config = ConfigurationManager.OpenMappedExeConfiguration(configMap,
                                                                                   ConfigurationUserLevel.None);
           
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
            _logFile.WriteLine("Breez : Загрузка маршрутов");
            string insertItinerary = @"INSERT INTO [dbo].[Breez_Itinerary]
           ([cruise_id]
           ,[day_number]
           ,[location]
           ,[arrival]
           ,[departure])
     VALUES
           (@cruise_id
           ,@day_number
           ,@location
           ,@arrival
           ,@departure)
";
            if (_days == null)
            {
                GetFile();
                parseFile();
            }
            _logFile.WriteLine("Breez : Разбор по записям");
            DataSet daysDS = new DataSet();
            MemoryStream stream = new MemoryStream();
            StreamWriter writer = new StreamWriter(stream);
            writer.Write(_days);
            writer.Flush();
            stream.Position = 0;
            int selday=0;
            daysDS.ReadXml(stream);
            DataTable cruisesdays = daysDS.Tables["data"];
            List<DayItinerary> daysList = new List<DayItinerary>();
            DayItinerary cr = null;
            foreach (DataRow day in cruisesdays.Rows)
            {
                if ((cr == null) || (day.Field<int>("CruiseDay_Id") != selday))
                {
                    cr = new DayItinerary();
                    selday = day.Field<int>("CruiseDay_Id");
                }
                switch (day.Field<string>("Type"))
                {
                    case "Cruise_id":
                        cr.cruise_id = Convert.ToInt32(day.Field<string>("Data_Text").Trim());
                        break;
                    case "Day_Number":
                        cr.day_number =Convert.ToInt32( day.Field<string>("Data_Text").Trim());
                        break;
                    case "Location":
                        cr.location = day.Field<string>("Data_Text").Trim();
                        break;
                    case "Arrival":
                        cr.arrival = day.Field<string>("Data_Text").Trim();
                        break;
                    case "Departure":
                        cr.depature = day.Field<string>("Data_Text").Trim();
                        break;
                    
                    default:
                        Console.WriteLine(day.Field<string>("Type").Trim());
                        break;
                }
                if (daysList.IndexOf(cr) < 0)
                {
                    daysList.Add(cr);
                }
            }
            _logFile.WriteLine("Breez : Очистка в базе данных");
            using (SqlCommand com = new SqlCommand("delete from Breez_Itinerary", _connection))
            {
                com.ExecuteNonQuery();
            }
            _logFile.WriteLine("Breez : Загрузка в базу данных");
            foreach (DayItinerary cruise1 in daysList)
            {
                using (SqlCommand com = new SqlCommand(insertItinerary, _connection))
                {
                    com.Parameters.AddWithValue("@cruise_id", cruise1.cruise_id);
                    com.Parameters.AddWithValue("@day_number", cruise1.day_number);
                    com.Parameters.AddWithValue("@location", cruise1.location);
                    com.Parameters.AddWithValue("@arrival", cruise1.arrival);
                    com.Parameters.AddWithValue("@departure", cruise1.depature);
                    com.ExecuteNonQuery();
                }
            }
            _logFile.WriteLine("Breez : Загрузка маршрутов окончена");
            return "";
        }

        void GetFile()
        {
            ////Получение файла от бриза
            _logFile.WriteLine("Breez : Получение файла от Breez");
            ExeConfigurationFileMap configMap = new ExeConfigurationFileMap();
            configMap.ExeConfigFilename = Path.Combine(Path.GetDirectoryName(Assembly.GetEntryAssembly().Location),
                                                       "lanta.sqlconfig.dll.config");
            Configuration config = ConfigurationManager.OpenMappedExeConfiguration(configMap,
                                                                                   ConfigurationUserLevel.None);
            string url = config.AppSettings.Settings["url"].Value;
            string token= config.AppSettings.Settings["token"].Value;    
            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(url);
            request.Headers.Add("Authorization", "Token token="+token);
            HttpWebResponse response = (HttpWebResponse)request.GetResponse();
            string ss = new StreamReader(response.GetResponseStream(), Encoding.UTF8).ReadToEnd();
            _file = ss;

        }
        void GetPrice()
        {
            _logFile.WriteLine("Breez : Загрузка цен");
            string insertPrice = @"INSERT INTO [dbo].[Breez_price]
           ([cruise_start_id]
           ,[name]
           ,[category]
           ,[price]
           ,[currensy])
     VALUES
           (@cruise_start_id
           ,@name
           ,@category
           ,@price
           ,@currensy)";
           
            if (_price == null)
            {
                GetFile();
                parseFile();
            }
            _logFile.WriteLine("Breez : Разбор по записям");
            DataSet priceDS = new DataSet();
            MemoryStream stream = new MemoryStream();
            StreamWriter writer = new StreamWriter(stream);
            writer.Write(_price);
            writer.Flush();
            stream.Position = 0;
            int selday = 0;
            priceDS.ReadXml(stream);
            DataTable priceTable = priceDS.Tables["data"];
            List<Price> daysList = new List<Price>();
            Price cr = null;
            foreach (DataRow pric in priceTable.Rows)
            {
                if ((cr == null) || (pric.Field<int>("Prices_Id") != selday))
                {
                    cr = new Price();
                    selday = pric.Field<int>("Prices_Id");
                }
                switch (pric.Field<string>("Type"))
                {
                    case "Cruise_start_id":
                        cr.cruise_stert_id = Convert.ToInt32(pric.Field<string>("Data_Text").Trim());
                        break;
                    case "Name":
                        cr.name = pric.Field<string>("Data_Text").Trim();
                        int positions = pric.Field<string>("Data_Text").Trim().IndexOf("категория") + "категория".Length, kolSim = pric.Field<string>("Data_Text").Trim().IndexOf(")") - pric.Field<string>("Data_Text").Trim().IndexOf("категория") - "категория".Length;
                        if (kolSim < 3)
                        {
                            kolSim = 3;
                        } ;
                        //cr.category = pric.Field<string>("Data_Text").Trim().Substring(positions,kolSim).Trim();
                        string data_text = pric.Field<string>("Data_Text").Trim();
                        if (!string.IsNullOrEmpty(data_text))
                            if (data_text.Length >= positions + kolSim)
                                data_text = data_text.Substring(positions, kolSim).Trim();
                        cr.category = data_text;
                        if (cr.category.Length > 3)
                        {
                            cr.category = "??";
                        }
                        break;
                    case "Price":
                        cr.price =decimal.Parse(pric.Field<string>("Data_Text").Trim().Replace('.',','));
                        break;
                    case "Currency":
                        cr.currensy = pric.Field<string>("Data_Text").Trim();
                        break;
                  

                    default:
                        Console.WriteLine(pric.Field<string>("Type").Trim());
                        break;
                }
                if (daysList.IndexOf(cr) < 0)
                {
                    daysList.Add(cr);
                }
            }
            _logFile.WriteLine("Breez : Очистка в базе данных");
            using (SqlCommand com = new SqlCommand("delete from Breez_price", _connection))
            {
                com.ExecuteNonQuery();
            }
            _logFile.WriteLine("Breez : Загрузка в базу данных");
            foreach (Price cruise1 in daysList)
            {
                using (SqlCommand com = new SqlCommand(insertPrice, _connection))
                {
                    com.Parameters.AddWithValue("@cruise_start_id", cruise1.cruise_stert_id);
                    com.Parameters.AddWithValue("@name", cruise1.name);
                    com.Parameters.AddWithValue("@category", cruise1.category);
                    com.Parameters.AddWithValue("@price", cruise1.price);
                    com.Parameters.AddWithValue("@currensy", cruise1.currensy);
                    com.ExecuteNonQuery();
                }
            }

            _logFile.WriteLine("Breez : Загрузка цен закончена");
        }
        void ShumbitChanges()
        {
            _logFile.WriteLine("Breez : Применение изменений в базе");
            using (SqlCommand com = new SqlCommand("breez",_connection))
            {
                com.CommandType = CommandType.StoredProcedure;
                com.CommandTimeout = 1200;
                com.ExecuteNonQuery();
            }
            _logFile.WriteLine("Breez : Изменения применены");
        }
        void GetStartDays()
        {
            _logFile.WriteLine("Breez : Загрузка дней выхода");
            string insertStartDay = @"INSERT INTO [dbo].[Breez_start_days]
           ([cruise_id]
           ,[cruise_start_id]
           ,[datestr]
           ,[date_start]
           ,[curency]
           ,[price]
           ,[comment]
           ,[port_fee]
           ,[tax]
           ,[group_price]
           ,[guide])
     VALUES
           (@cruise_id
           ,@cruise_start_id
           ,@datestr
           ,@date_start
           ,@curency
           ,@price
           ,@comment
           ,@port_fee
           ,@tax
           ,@group_price
           ,@guide)";
            if (_startdays == null)
            {
                GetFile();
                parseFile();
            }
            _logFile.WriteLine("Breez : Разбор по записям");
            DataSet startDaysDS = new DataSet();
            MemoryStream stream = new MemoryStream();
            StreamWriter writer = new StreamWriter(stream);
            writer.Write(_startdays);
            writer.Flush();
            stream.Position = 0;
            int selday = 0;
            startDaysDS.ReadXml(stream);
            DataTable startDayTable = startDaysDS.Tables["data"];
            List<CruiseStartDay> daysList = new List<CruiseStartDay>();
            CruiseStartDay cr = null;
            foreach (DataRow pric in startDayTable.Rows)
            {
                if ((cr == null) || (pric.Field<int>("CruiseStartDay_Id") != selday))
                {
                    cr = new CruiseStartDay();
                    selday = pric.Field<int>("CruiseStartDay_Id");
                }
                switch (pric.Field<string>("Type"))
                {
                    case "Cruise_start_id":
                        cr.cruise_start_id = Convert.ToInt32(pric.Field<string>("Data_Text").Trim());
                        break;
                    case "Cruise_id":
                        cr.cruise_id = Convert.ToInt32(pric.Field<string>("Data_Text").Trim());
                        break;
                    case "Price":
                        try
                        {
                            cr.price = decimal.Parse(pric.Field<string>("Data_Text").Trim().Replace('.', ','));
                        }
                        catch (Exception)
                        {
                            cr.price = 0;
                        }
                        
                        break;
                    case "Port_Fee":
                        try
                        {
                            cr.port_fee = decimal.Parse(pric.Field<string>("Data_Text").Trim().Replace('.', ','));
                        }
                        catch (Exception)
                        {
                            cr.port_fee = 0;
                        }
                        break;
                    case "Tax":
                        try
                        {
                            cr.tax = decimal.Parse(pric.Field<string>("Data_Text").Trim().Replace('.', ','));
                        }
                        catch (Exception)
                        {
                            cr.tax = 0;
                        }
                        break;
                    case "Currency":
                        cr.curency = pric.Field<string>("Data_Text").Trim();
                        break;
                    case "Day":
                        cr.datestr = pric.Field<string>("Data_Text").Trim();
                        cr.date = DateTime.Parse(pric.Field<string>("Data_Text"));
                        break;
                    case "Comment":
                        cr.comment = pric.Field<string>("Data_Text").Trim();
                        break;
                    case "Group":
                        try
                        {
                            cr.group = bool.Parse(pric.Field<string>("Data_Text").Trim());
                        }
                        catch (Exception)
                        {

                            cr.group = false;
                        }
                        
                        break;
                    case "Guide":
                        try
                        {
                            cr.Guide = bool.Parse(pric.Field<string>("Data_Text").Trim());
                        }
                        catch (Exception)
                        {

                            cr.Guide = false;
                        }
                        
                        break;


                    

                    default:
                        Console.WriteLine(pric.Field<string>("Type").Trim());
                        break;
                }
                if (daysList.IndexOf(cr) < 0)
                {
                    daysList.Add(cr);
                }
            }
            _logFile.WriteLine("Breez : Очистка в базе данных");
            using (SqlCommand com = new SqlCommand("delete from Breez_start_days", _connection))
            {
                com.ExecuteNonQuery();
            }
            _logFile.WriteLine("Breez : Заг" +
                               "рузка дней в базу данных");
            foreach (CruiseStartDay cruise1 in daysList)
            {
                using (SqlCommand com = new SqlCommand(insertStartDay, _connection))
                {
                    com.Parameters.AddWithValue("@cruise_id", cruise1.cruise_id);
                    com.Parameters.AddWithValue("@cruise_start_id", cruise1.cruise_start_id);
                    com.Parameters.AddWithValue("@datestr", cruise1.datestr);
                    com.Parameters.AddWithValue("@date_start", cruise1.date);
                    com.Parameters.AddWithValue("@curency", cruise1.curency);
                    com.Parameters.AddWithValue("@price", cruise1.price);
                    com.Parameters.AddWithValue("@comment", cruise1.comment);
                    com.Parameters.AddWithValue("@port_fee", cruise1.port_fee);
                    com.Parameters.AddWithValue("@tax", cruise1.tax);
                    com.Parameters.AddWithValue("@group_price", cruise1.group);
                    com.Parameters.AddWithValue("@guide", cruise1.Guide);
                    com.ExecuteNonQuery();
                }
            }
            _logFile.WriteLine("Breez : Загрузка закончена");
        
        }
        
        void GetCruises()
        {
            _logFile.WriteLine("Breez : Загрузка круизов");
            string insertCruise = @"INSERT INTO [dbo].[Breez_Cruises]
           ([id_cruise]
           ,[name]
           ,[region]
           ,[ship]
           ,[sotd_to]
           ,[hit_sales]
           ,[drop_price]
           ,[last_stateroom]
           ,[newship]
           ,[category]
           ,[information])
     VALUES
           (@id_cruise
           ,@name
           ,@region
           ,@ship
           ,@sotd_to
           ,@hit_sales
           ,@drop_price
           ,@last_stateroom
           ,@newship
           ,@category
           ,@information)";
            if (_cruises == null)
            {
                GetFile();
                parseFile();
            }
            _logFile.WriteLine("Breez : Разбор по записям");
            DataSet cruisesDS = new DataSet();
            MemoryStream stream = new MemoryStream();
            StreamWriter writer = new StreamWriter(stream);
            writer.Write(_cruises);
            writer.Flush();
            stream.Position = 0;
            int selCruise=0;
            cruisesDS.ReadXml(stream);
            DataTable cruises = cruisesDS.Tables["data"];
            List<Cruise> cruiseList = new List<Cruise>();
            Cruise cr = null;
            foreach (DataRow cruise in cruises.Rows)
            {
                if ((cr == null) || (cruise.Field<int>("Cruise_Id") != selCruise))
                {
                    cr = new Cruise();
                    selCruise = cruise.Field<int>("Cruise_Id");
                }
                switch (cruise.Field<string>("Type"))
                {
                    case "Cruise_id":
                        cr.id_cruise = Convert.ToInt32(cruise.Field<string>("Data_Text").Trim());
                        break;
                    case "Name":
                        cr.name = cruise.Field<string>("Data_Text").Trim();
                        break;
                    case "Region":
                        cr.region = cruise.Field<string>("Data_Text").Trim();
                        break;
                    case "Ship":
                        cr.ship = cruise.Field<string>("Data_Text").Trim();
                        break;
                    case"Category":
                        cr.category = cruise.Field<string>("Data_Text").Trim();
                        break;
                    case "Information":
                        cr.information = cruise.Field<string>("Data_Text").Trim();
                        break;
                    case "NewShip":
                        try
                        {
                            cr.newship = bool.Parse(cruise.Field<string>("Data_Text").Trim());
                        }
                        catch (Exception)
                        {

                            cr.newship = false;
                        }
                        
                        break;
                    case "Hit_Sales":
                        try
                        {
                            cr.hit_sales = bool.Parse(cruise.Field<string>("Data_Text").Trim());
                        }
                        catch (Exception)
                        {

                            cr.hit_sales = false;
                        }
                        
                        break;
                    case "Last_Stateroom":
                        try
                        {
                            cr.last_stateroom = bool.Parse(cruise.Field<string>("Data_Text").Trim());
                        }
                        catch (Exception)
                        {

                            cr.last_stateroom = false;
                        }
                        cr.last_stateroom = bool.Parse(cruise.Field<string>("Data_Text").Trim());
                        break;
                    case "Price_Drop":
                        try
                        {
                            cr.drop_price = bool.Parse(cruise.Field<string>("Data_Text").Trim());
                        }
                        catch (Exception)
                        {

                            cr.drop_price = false;
                        }
                        cr.drop_price = bool.Parse(cruise.Field<string>("Data_Text").Trim());
                        break;
                    case "Sold_To":
                        try
                        {
                            cr.sold_to = bool.Parse(cruise.Field<string>("Data_Text").Trim());
                        }
                        catch (Exception)
                        {

                            cr.sold_to = false;
                        }
                       
                        break;
                    default:
                        Console.WriteLine(cruise.Field<string>("Type").Trim());
                        break;
                }
                if (cruiseList.IndexOf(cr) < 0)
                {
                    cruiseList.Add(cr);
                }
                
            }
            //Загрузка круизов в базу данных
            _logFile.WriteLine("Breez : Очистка в базе данных");
            using (SqlCommand com = new SqlCommand("delete from Breez_Cruises", _connection))
            {
                com.ExecuteNonQuery();
            }
            _logFile.WriteLine("Breez : Загрузка в базу данных");
            foreach (Cruise cruise1 in cruiseList)
            {
                using (SqlCommand com = new SqlCommand(insertCruise, _connection))
                {
                    com.Parameters.AddWithValue("@id_cruise", cruise1.id_cruise);
                    com.Parameters.AddWithValue("@name", cruise1.name);
                    com.Parameters.AddWithValue("@region", cruise1.region);
                    com.Parameters.AddWithValue("@ship", cruise1.ship);
                    com.Parameters.AddWithValue("@sotd_to", cruise1.sold_to);
                    com.Parameters.AddWithValue("@hit_sales", cruise1.hit_sales);
                    com.Parameters.AddWithValue("@drop_price", cruise1.drop_price);
                    com.Parameters.AddWithValue("@last_stateroom", cruise1.last_stateroom);
                    com.Parameters.AddWithValue("@newship", cruise1.newship);
                    com.Parameters.AddWithValue("@category", cruise1.category);
                    com.Parameters.AddWithValue("@information", cruise1.information);
                    com.ExecuteNonQuery();
                }
            }
            _logFile.WriteLine("Breez : Загрузка круизов окончена");
        }
        void parseFile()
        {
            //Разбор файла
         
          //  StreamReader partnerXml = new StreamReader(_path + _filename);
            _logFile.WriteLine("Breez : Разбор XML Breez");
            string st = null;
            if (_file != null)
            {
                 st = _file;
            }
            else
            {
                GetFile();
            }
            string cruises = "<?xml version=\"1.0\"?>" + Convert.ToChar(13) + "<Cruises>" + Convert.ToChar(13);
            string days = "<?xml version=\"1.0\"?>" + Convert.ToChar(13) + "<CruiseDays>" + Convert.ToChar(13);
            string price = "<?xml version=\"1.0\"?>" + Convert.ToChar(13) + "<Pricess>" + Convert.ToChar(13);
            string startDays = "<?xml version=\"1.0\"?>" + Convert.ToChar(13) + "<CruiseStartDays>" + Convert.ToChar(13);
            //Круизы
            int i = 0;
            while (st.IndexOf("<Cruise>")>=0)
            {


                cruises += st.Substring(st.IndexOf("<Cruise>"),
                                        st.LastIndexOf("</Cruise>") - st.IndexOf("<Cruise>") + "</Cruise>".Length) + Convert.ToChar(13);
                st =st.Remove(st.IndexOf("<Cruise>"),
                          st.LastIndexOf("</Cruise>") - st.IndexOf("<Cruise>") + "</Cruise>".Length);
                
#if DEBUG
                Console.WriteLine(i.ToString()+"Круиз");
                i++;
                if (i == 10) break;
#endif
            }
            cruises += "</Cruises>";
           //Маршруты
            i = 0;
            while (st.IndexOf("<CruiseDay>") >= 0)
            {


                days += st.Substring(st.IndexOf("<CruiseDay>"),
                                        st.LastIndexOf("</CruiseDay>") - st.IndexOf("<CruiseDay>") + "</CruiseDay>".Length) + Convert.ToChar(13);
                st = st.Remove(st.IndexOf("<CruiseDay>"),
                          st.LastIndexOf("</CruiseDay>") - st.IndexOf("<CruiseDay>") + "</CruiseDay>".Length);
                
#if DEBUG
                Console.WriteLine(i.ToString() + "День");
                i++;
                if (i == 10) break;
#endif
            }
            days += "</CruiseDays>";
            //Цены
            i = 0;
            while (st.IndexOf("<Prices>") >= 0)
            {


                price += st.Substring(st.IndexOf("<Prices>"),
                                        st.LastIndexOf("</Prices>") - st.IndexOf("<Prices>") + "</Prices>".Length) + Convert.ToChar(13);
                st = st.Remove(st.IndexOf("<Prices>"),
                          st.LastIndexOf("</Prices>") - st.IndexOf("<Prices>") + "</Prices>".Length);
                
#if DEBUG
                Console.WriteLine(i.ToString() + "Цена");
                i++;
                if(i==10) break;
#endif
            }
            price += "</Pricess>";
            //Порты выхода
            i = 0;
            while (st.IndexOf("<CruiseStartDay>") >= 0)
            {


                startDays += st.Substring(st.IndexOf("<CruiseStartDay>"),
                                        st.LastIndexOf("</CruiseStartDay>") - st.IndexOf("<CruiseStartDay>") + "</CruiseStartDay>".Length) + Convert.ToChar(13);
                st = st.Remove(st.IndexOf("<CruiseStartDay>"),
                          st.LastIndexOf("</CruiseStartDay>") - st.IndexOf("<CruiseStartDay>") + "</CruiseStartDay>".Length);
               
#if DEBUG 
                Console.WriteLine(i.ToString() + "Порт");
                i++;
                if (i == 10) break;
#endif
            }
            startDays += "</CruiseStartDays>";

            _cruises = cruises;
            _price = price;
            _startdays = startDays;
            _days = days;

            _logFile.WriteLine("Breez : Разбор XML закончен");
          // Console.WriteLine(st);
        }
   

        public override void GetData()
        {
            try
            {
                _logFile.WriteLine("Breez : Загрузка Breez");
                GetFile();
                parseFile();
                GetPrice();
                GetCruises();
                GetStartDays();
                GetItineraryData();
                ShumbitChanges();
                _logFile.WriteLine("Breez : Загрузка Breez окончена");
            }
            catch (Exception ex)
            {

                _logFile.WriteLine("Breez : Произошла ошибка "+ex.Message+Convert.ToChar(13)+ex.StackTrace);
                throw;
            }
      
        }
    }
}
