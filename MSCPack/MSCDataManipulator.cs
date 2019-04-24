using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Xml;
using System.Xml.Linq;
using DxHelpersLib;
using PluginInteractionLib;

namespace PackMSCPlugin
{
    public class MSCDataManipulator : DataManipulator
    {
        private const string GETAWAY_SHIPS_URL = "http://gate.gocruise.ru/xml/ships";
        private const string GETAWAY_DECKS_URL = "http://gate.gocruise.ru/xml/decks";
        private const string GETAWAY_CABINTYPES_URL = "http://gate.gocruise.ru/xml/cabintypes";
        private const string GETAWAY_CABINS_URL = "http://gate.gocruise.ru/xml/cabins";

        private const string GETAWAY_CRUISES_URL = "http://gate.gocruise.ru/xml/cruises";
        private const string GETAWAY_DATES_URL = "http://gate.gocruise.ru/xml/dates";
        
        private const string GETAWAY_ITINERARYS_URL = "http://gate.gocruise.ru/xml/itinerary";
        private const string GETAWAY_PRICES_URL = "http://gate.gocruise.ru/xml/prices";
        private const string GETAWAY_SURCHARGES_URL = "http://gate.gocruise.ru/xml/surcharges";
        private const string GETAWAY_EXTRACHARGE_URL = "http://gate.gocruise.ru/xml/extracharge";
        private const string GETAWAY_EXCURSIONS_URL = "http://gate.gocruise.ru/xml/excursions";
        private const string GETAWAY_EXTRAPACKS_URL = "http://gate.gocruise.ru/xml/packets";

        public MSCDataManipulator(SqlConnection con, Logger log) : base(con, log) { }
        private static XmlNodeList GetXmlNodes(string url, string section)
        {
            XmlDocument xDoc = new XmlDocument();
            XmlTextReader xmlTextReader = new XmlTextReader(url);
            xDoc.Load(xmlTextReader);
            var xmlElement = xDoc["root"][section];
            return xmlElement.GetElementsByTagName("item");
        }
        private static SqlDataAdapter GetSqlAdapter(SqlCommand command)
        {

            SqlDataAdapter adapter = new SqlDataAdapter(command);
            SqlCommandBuilder commandBuilder = new SqlCommandBuilder(adapter);
            adapter.DeleteCommand = commandBuilder.GetDeleteCommand();
            adapter.InsertCommand = commandBuilder.GetInsertCommand();
            adapter.UpdateCommand = commandBuilder.GetUpdateCommand();
            return adapter;
        }
        public override string GetShipsData()
        {
            XmlNodeList items = GetXmlNodes(GETAWAY_SHIPS_URL, "ships");
            if (_connection.State == ConnectionState.Closed) _connection.Open();
            SqlDataAdapter adapter =
                GetSqlAdapter(
                    new SqlCommand(
                        @"SELECT [S_ID],[S_COMPANY],[S_TITLE],[S_CATEGORY],[S_ORDERING],[S_TTH],[S_DESCRIPTION]
                                                                            FROM [dbo].[ships_pac_msc] ",
                        _connection));

            DataTable tbShips = new DataTable("ships");
            adapter.Fill(tbShips);

            foreach (XmlNode node in items)
            {

                int id = Convert.ToInt32(node["id"].InnerText);
                string company = node["company"] == null ? string.Empty : node["company"].InnerText;
                string title = node["title"] == null ? string.Empty : node["title"].InnerText;
                string category = node["category"] == null ? string.Empty : node["category"].InnerText;
                int ordering = Convert.ToInt32(node["ordering"] == null ? (-1).ToString() : node["ordering"].InnerText);
                string tth = node["tth"] == null ? string.Empty : node["tth"].InnerText;
                string description = node["description"] == null ? string.Empty : node["description"].InnerText;


                var rows = tbShips.Select(string.Format("S_ID={0}", id));
                DataRow dataRow = rows.Length == 0 ? tbShips.NewRow() : rows[0];
                dataRow["S_ID"] = id;
                dataRow["S_COMPANY"] = company;
                dataRow["S_TITLE"] = title;
                dataRow["S_CATEGORY"] = category == string.Empty ? DBNull.Value : (object)category;
                dataRow["S_ORDERING"] = ordering == -1 ? DBNull.Value : (object)ordering;
                dataRow["S_TTH"] = tth;
                dataRow["S_DESCRIPTION"] = description;
                if (rows.Length == 0) { tbShips.Rows.InsertAt(dataRow, 0); }
            }

            adapter.Update(tbShips);
            return items.Count == 0 ? "\nЛайнеры отсутствуют\n" : string.Format("\nMSC Лайнеры получены: {0}\n", items.Count);
        }
        public override string GetDecksData()
        {
            XmlNodeList items = GetXmlNodes(GETAWAY_DECKS_URL, "decks");

            SqlDataAdapter adapter =
                GetSqlAdapter(
                    new SqlCommand(
                        @"SELECT [D_ID],[S_ID],[D_ORDERING],[D_TITLE],[D_DESCRIPTION],[D_SCHEME]
                            FROM [total_services].[dbo].[decks_pac_msc] ",
                        _connection));

            DataTable tbDecks = new DataTable("decks");
            adapter.Fill(tbDecks);
            foreach (XmlNode node in items)
            {
                int id = Convert.ToInt32(node["id"].InnerText);
                object shipId = Convert.ToInt32(node["ship_id"] == null ? DBNull.Value : (object)node["ship_id"].InnerText);
                object ordering = Convert.ToInt32(node["ordering"] == null ? DBNull.Value : (object)node["ordering"].InnerText);
                object title = node["title"] == null ? DBNull.Value : (object)node["title"].InnerText;
                object description = node["description"] == null ? DBNull.Value : (object)node["description"].InnerText;
                object scheme = node["scheme"] == null ? DBNull.Value : (object)node["scheme"].InnerText;

                var rows = tbDecks.Select(string.Format("D_ID={0}", id));
                DataRow dataRow = rows.Length == 0 ? tbDecks.NewRow() : rows[0];
                dataRow["D_ID"] = id;
                dataRow["S_ID"] = shipId;
                dataRow["D_ORDERING"] = ordering;
                dataRow["D_TITLE"] = title;
                dataRow["D_DESCRIPTION"] = description;
                dataRow["D_SCHEME"] = scheme;
                if (rows.Length == 0) { tbDecks.Rows.InsertAt(dataRow, 0); }

            }
            adapter.Update(tbDecks);

            return items.Count == 0 ? "\nПалубы отсутствуют\n" : string.Format("\nMSC Палубы получены: {0}\n", items.Count); ;
        }
        public override string GetCabinsData()
        {
            XmlNodeList items = GetXmlNodes(GETAWAY_CABINS_URL, "cabins");
            SqlDataAdapter adapter =
               GetSqlAdapter(
                   new SqlCommand(
                       @"SELECT [ID],[C_ID],[CT_ID],[C_CATEGORY],[C_EXTRATITLE],[C_ORDERING],[D_ID]
                            FROM [dbo].[cabins_pac_msc] ",
                       _connection));

            DataTable tbCabins = new DataTable("cabins");
            adapter.Fill(tbCabins);

            foreach (XmlNode node in items)
            {
                if (node["id"] == null) continue;
                int id = Convert.ToInt32(node["id"].InnerText);
                int typeId = Convert.ToInt32(node["type_id"].InnerText);
                object category = node["category"] == null ? DBNull.Value : (object)node["category"].InnerText;
                object extraTitle = node["extra_title"] == null ? DBNull.Value : (object)node["extra_title"].InnerText;
                object ordering = Convert.ToInt32(node["ordering"] == null ? DBNull.Value : (object)node["ordering"].InnerText);
                object deck = DBNull.Value;

                if (node["decks"] == null)
                {
                    var rows = tbCabins.Select(string.Format("C_ID={0} and D_ID is null", id));
                    DataRow dataRow = rows.Length == 0 ? tbCabins.NewRow() : rows[0];
                    dataRow["C_ID"] = id;
                    dataRow["CT_ID"] = typeId;
                    dataRow["C_CATEGORY"] = category;
                    dataRow["C_EXTRATITLE"] = extraTitle;
                    dataRow["C_ORDERING"] = ordering;
                    dataRow["D_ID"] = deck;
                    if (rows.Length == 0) { tbCabins.Rows.InsertAt(dataRow, 0); }
                }
                else
                {
                    foreach (XmlNode dnode in node["decks"].GetElementsByTagName("item"))
                    {
                        deck = Convert.ToInt32(dnode.InnerText);
                        var rows = tbCabins.Select(string.Format("C_ID={0} and D_ID={1}", id, deck));
                        DataRow dataRow = rows.Length == 0 ? tbCabins.NewRow() : rows[0];
                        dataRow["C_ID"] = id;
                        dataRow["CT_ID"] = typeId;
                        dataRow["C_CATEGORY"] = category;
                        dataRow["C_EXTRATITLE"] = extraTitle;
                        dataRow["C_ORDERING"] = ordering;
                        dataRow["D_ID"] = deck;
                        if (rows.Length == 0) { tbCabins.Rows.InsertAt(dataRow, 0); }
                    }
                }

            }
            adapter.Update(tbCabins);
            var cabinTypes = GetCabinTypesData();
            return (items.Count == 0 ? "\nКаюты отсутствуют\n" : string.Format("\nMSC Каюты получены: {0}\n", items.Count + "\n")) + cabinTypes; ;
        }
        public override string GetItineraryData()
        {
            return GetItinerary();
        }

        private string GetCabinTypesData()
        {
            XmlNodeList items = GetXmlNodes(GETAWAY_CABINTYPES_URL, "cabintypes");


            SqlDataAdapter adapter =
               GetSqlAdapter(
                   new SqlCommand(
                       @"SELECT [CT_ID],[S_ID],[CT_TITLE],[CT_DESCRIPTION],[CT_PHOTO],[CT_SCHEME]
                            FROM [dbo].[cabintypes_pac_msc] ",
                       _connection));

            DataTable tbCabinTypes = new DataTable("cabintypes");
            adapter.Fill(tbCabinTypes);
            foreach (XmlNode node in items)
            {
                int id = Convert.ToInt32(node["id"].InnerText);
                object shipId = Convert.ToInt32(node["ship_id"] == null ? DBNull.Value : (object)node["ship_id"].InnerText);
                object title = node["title"] == null ? DBNull.Value : (object)node["title"].InnerText;
                object description = node["description"] == null ? DBNull.Value : (object)node["description"].InnerText;
                object photo = node["photo"] == null ? DBNull.Value : (object)node["photo"].InnerText;
                object scheme = node["scheme"] == null ? DBNull.Value : (object)node["scheme"].InnerText;


                var rows = tbCabinTypes.Select(string.Format("CT_ID={0}", id));
                DataRow dataRow = rows.Length == 0 ? tbCabinTypes.NewRow() : rows[0];
                dataRow["CT_ID"] = id;
                dataRow["S_ID"] = shipId;
                dataRow["CT_TITLE"] = title;
                dataRow["CT_DESCRIPTION"] = description;
                dataRow["CT_PHOTO"] = photo;
                dataRow["CT_SCHEME"] = scheme;
                if (rows.Length == 0) { tbCabinTypes.Rows.InsertAt(dataRow, 0); }

            }
            adapter.Update(tbCabinTypes);

            return items.Count == 0 ? "\nТипы кают отсутствуют\n\n" : string.Format("\nMSC Типы кают получены: {0}\n\n", items.Count);
        }

        public override void GetData()
        {
            try
            {
                _logFile.WriteLine("\nPAC_MSC:Начало загрузки данных по MSC");
                //GetShipsData();
                //GetCabinsData();
                //GetDecksData();



                GetCruisesInfo();
                GetItinerary();
                GetDates();
                GetPrices();
                //GetSurcharges();
                GetExtracharge();
                GetExcursions();
                GetExtraPacks();
                _logFile.WriteLine("PAC_MSC:Данные по MSC получены \n");
            }
            catch (Exception ex)
            {
                _logFile.WriteLine(string.Format("PAC_MSC:Ошибка во время получения данных: \nException:{0} \nInnerException:{1} \nStackTrace:{2}", ex.Message, ex.InnerException, ex.StackTrace));
                throw;
            }

        }
#if SPOTEST
        public void GetCruisesInfoSPOTEST()
        {
            GetExcursions();
            _logFile.WriteLine("PAC_MSC:Получение Информации по круизам");
            DataSet DS = new DataSet();
            DS.ReadXml(GETAWAY_CRUISES_URL);
            GetCruises(DS);
            _logFile.WriteLine("PAC_MSC:Круизы получены");
            GetSPOCruises(DS);
            _logFile.WriteLine("PAC_MSC:Спецпредложения получены");
            GetCruiseNotes(DS);
            _logFile.WriteLine("PAC_MSC:Описания круизов получены");
        }
#endif
        private void GetCruisesInfo()
        {
            try
            {
                using (SqlCommand com = new SqlCommand(@"Delete from [cruises_pac_msc] Delete from [cruises_spo_pac_msc]  Delete from [cruises_notes_pac_msc] ", _connection))
                {
                    com.ExecuteNonQuery();
                }
                _logFile.WriteLine("PAC_MSC:Получение Информации по круизам");
                DataSet DS = new DataSet();
                DS.ReadXml(GETAWAY_CRUISES_URL);
                GetCruises(DS);
                _logFile.WriteLine("PAC_MSC:Круизы получены");
                GetSPOCruises(DS);
                _logFile.WriteLine("PAC_MSC:Спецпредложения получены");
                GetCruiseNotes(DS);
                _logFile.WriteLine("PAC_MSC:Описания круизов получены");
            }
            catch (Exception ex)
            {
                _logFile.WriteLine(string.Format("PAC_MSC:Ошибка во время получения данных: \nException:{0} \nStackTrace:{1}", ex.Message, ex.StackTrace));
            }
        }
        private void GetCruises(DataSet DS)
        {
            var tbItems = DS.Tables["item"];
            SqlDataAdapter adapter = GetSqlAdapter(new SqlCommand(@"select * from cruises_pac_msc", _connection));
            var tbCruises = new DataTable();
            adapter.Fill(tbCruises);
            foreach (DataRow row in tbItems.Rows)
            {
                var cRows = tbCruises.Select(string.Format("C_ID={0}", row["id"].ToString()));
                var crdataRow = cRows.Length == 0 ? tbCruises.NewRow() : cRows[0];
                crdataRow["C_ID"] = GetValue<int>(row["id"]);
                crdataRow["SHIP_ID"] = GetValue<int>(row["ship_id"]);
                crdataRow["C_NUMBER"] = GetValue<string>(row["number"]);
                crdataRow["C_NIGHTS"] = GetValue<int>(row["nights"]);
                crdataRow["C_DAY_START"] = GetValue<int>(row["day_start"]);
                crdataRow["C_DAY_END"] = GetValue<int>(row["day_end"]);
                crdataRow["C_TITLE"] = GetValue<string>(row["title"]);
                //crdataRow["C_DESCRIPTION"] = GetValue<string>(row["description"]);
                crdataRow["C_MAP"] = GetValue<string>(row["map"]);
                if (cRows.Length == 0) { tbCruises.Rows.InsertAt(crdataRow, 0); }
            }
            adapter.Update(tbCruises);
        }
        private void GetCruiseNotes(DataSet DS)
        {
            var dstbNote = DS.Tables["notes"];
            if (dstbNote == null) return;
            using (SqlDataAdapter adapter = GetSqlAdapter(new SqlCommand(@"select * from cruises_notes_pac_msc", _connection)))
            {
                var tbNotes = new DataTable();
                adapter.Fill(tbNotes);
                foreach (DataRow row in dstbNote.Rows)
                {
                    var cruiseid = DS.Tables["item"].Select(string.Format("item_Id={0}", row["item_Id"]))[0]["id"];
                    var cRows = tbNotes.Select(string.Format("C_ID={0}", cruiseid));
                    var nRow = cRows.Length == 0 ? tbNotes.NewRow() : cRows[0];
                    nRow["C_ID"] = GetValue<int>(cruiseid);
                    nRow["N_ITINERARY"] = GetValue<string>(row["itinerary"]);
                    //nRow["N_PRICE"] = GetValue<string>(row["prices"]);
                    if (cRows.Length == 0)
                    {
                        tbNotes.Rows.InsertAt(nRow, 0);
                    }
                }
                adapter.Update(tbNotes);
            }

        }
        private void GetSPOCruises(DataSet DS)
        {
            try
            {
                var dstbSpo = DS.Tables["spo"];
                if (dstbSpo == null) return;
                using (
                    SqlDataAdapter adapter =
                        GetSqlAdapter(new SqlCommand(@"select * from cruises_spo_pac_msc", _connection)))
                {

                    var tbSPO = new DataTable();
                    adapter.Fill(tbSPO);
                    foreach (DataRow row in dstbSpo.Rows)
                    {
                        var cruiseid = DS.Tables["item"].Select(string.Format("item_Id={0}", row["item_Id"]))[0]["id"];
                        var sRows = tbSPO.Select(string.Format("C_ID={0}", cruiseid));
                        var spoRow = sRows.Length == 0 ? tbSPO.NewRow() : sRows[0];
                        spoRow["C_ID"] = GetValue<int>(cruiseid);
                        spoRow["S_BASE_CRUISE_ID"] = GetValue<int>(row["base_cruise_id"]);
                        spoRow["S_NUMBER"] = GetValue<string>(row["number"]);
                        spoRow["S_TITLE"] = GetValue<string>(row["title"]);
                        spoRow["S_FROM"] = GetValue<DateTime>(row["from"]);
                        spoRow["S_TO"] = GetValue<DateTime>(row["to"]);
                        if (sRows.Length == 0)
                        {
                            tbSPO.Rows.InsertAt(spoRow, 0);
                        }
                    }
                    adapter.Update(tbSPO);
                }
            }
            catch (Exception ex)
            {
                _logFile.WriteLine(string.Format("PAC_MSC:Ошибка во время получения данных: \nException:{0}  \nStackTrace:{1}", ex.Message, ex.StackTrace));
            }
        }
        private void GetDates()
        {
            try
            {
                _logFile.WriteLine("PAC_MSC:Получение информации по датам");
                DataSet DS = new DataSet();
                DS.ReadXml(GETAWAY_DATES_URL);

                using (SqlCommand com = new SqlCommand(@"Delete from [cruises_dates_pac_msc]", _connection))
                {
                    com.ExecuteNonQuery();
                }
                var tbItem = DS.Tables["item"];
                var tbCruises = DS.Tables["cruise"];
                var tbDates = new DataTable();

                var adapter = GetSqlAdapter(new SqlCommand(@"select * from cruises_dates_pac_msc", _connection));
                adapter.Fill(tbDates);
                SqlDataAdapter ad =
                    GetSqlAdapter(new SqlCommand(@"select * from cruises_pac_msc", _connection));
                var tbTempCruises = new DataTable();
                ad.Fill(tbTempCruises);
                foreach (DataRow row in tbItem.Rows)
                {
                    var cruiseId =
                        GetValue<int>(tbCruises.Select(string.Format("cruise_Id={0}", row["cruise_Id"]))[0]["id"]);
                  
                   
                    var dRows =
                        tbDates.Select(string.Format("D_DATE = '{0}' and C_ID = {1}", GetValue<DateTime>(row["date"]),
                                                     cruiseId));
                    var dRow = dRows.Length == 0 ? tbDates.NewRow() : dRows[0];
                    dRow["D_DATE"] = GetValue<DateTime>(row["date"]);
                    dRow["D_RUS"] = GetValue<string>(row["rus"]);
                    dRow["D_SEASON"] = GetValue<string>(row["season"]);
                    dRow["D_CITY"] = GetValue<string>(row["city"]);
                    dRow["C_ID"] = cruiseId;
                    if (dRows.Length == 0)
                    {
                        tbDates.Rows.InsertAt(dRow, 0);
                    }
                }
                ad.Update(tbTempCruises);
                adapter.Update(tbDates);
                _logFile.WriteLine("PAC_MSC:Даты круизов получены");
            }
            catch (Exception ex)
            {
                _logFile.WriteLine(string.Format("PAC_MSC:Ошибка во время получения данных: \nException:{0}  \nStackTrace:{1}", ex.Message, ex.StackTrace));
                
            }
        }
        private string GetItinerary()
        {
            try
            {
                _logFile.WriteLine("PAC_MSC:Получение информации по маршрутам");
                DataSet DS = new DataSet();
                DS.ReadXml(GETAWAY_ITINERARYS_URL);
                using (SqlCommand com = new SqlCommand(@"Delete from [cruises_itinerary_pac_msc]", _connection))
                {
                    com.ExecuteNonQuery();
                }
                var tbItem = DS.Tables["item"];
                var tbCruises = DS.Tables["cruise"];
                var tbItinerary = new DataTable();

                var adapter = GetSqlAdapter(new SqlCommand(@"select * from cruises_itinerary_pac_msc", _connection));
                adapter.Fill(tbItinerary);

                foreach (DataRow row in tbItem.Rows)
                {
                    var cruiseId =
                        GetValue<int>(tbCruises.Select(string.Format("cruise_Id={0}", row["cruise_Id"]))[0]["id"]); 
                    //SqlDataAdapter ad =
                    //    GetSqlAdapter(new SqlCommand(@"select * from cruises_pac_msc where C_ID = " + cruiseId,
                    //                                 _connection));

                    //var tbtemp = new DataTable();
                    //ad.Fill(tbtemp);
                    //if (tbtemp.Rows.Count < 1)
                    //{
                    //    var r = tbtemp.NewRow();
                    //    r["C_ID"] = cruiseId;
                    //    tbtemp.Rows.InsertAt(r, 0);
                    //    ad.Update(tbtemp);
                    //}
                    var iRows =
                        tbItinerary.Select(string.Format("I_DAY = {0} and C_ID = {1}", GetValue<int>(row["day"]),
                                                         cruiseId));
                    var iRow = iRows.Length == 0 ? tbItinerary.NewRow() : iRows[0];
                    iRow["I_CITY"] = GetValue<string>(row["city"]);
                    iRow["I_COUNTRY"] = GetValue<string>(row["country"]);
                    iRow["I_DAY"] = GetValue<int>(row["day"]);
                    iRow["I_ARRIVAL"] = GetValue<string>(row["arrival"]);
                    iRow["I_DEPARTURE"] = GetValue<string>(row["departure"]);
                    iRow["I_BOARDING"] = GetValue<string>(row["boarding"]);
                    iRow["I_ORDERING"] = GetValue<int>(row["ordering"]);
                    //iRow["I_NOTE"] = GetValue<string>(row["note"]);
                    iRow["C_ID"] = cruiseId;
                    if (iRows.Length == 0)
                    {
                        tbItinerary.Rows.InsertAt(iRow, 0);
                    }
                }
                adapter.Update(tbItinerary);
                _logFile.WriteLine("PAC_MSC:Информация по маршрутам получена");
                return tbItinerary.Rows.Count == 0
                           ? "MSC маршруты отсутствуют\n"
                           : string.Format("MSC маршруты получены: {0}", tbItinerary.Rows.Count);
            }
            catch (Exception ex)
            {
                _logFile.WriteLine(string.Format("PAC_MSC:Ошибка во время получения данных: \nException:{0}  \nStackTrace:{1}", ex.Message, ex.StackTrace));
                return string.Format("PAC_MSC:Ошибка во время получения данных: \nException:{0}  \nStackTrace:{1}",
                                     ex.Message, ex.StackTrace);
            }
        }
        private void GetPrices()
        {
            try
            {
                _logFile.WriteLine("PAC_MSC:Получение информации по ценам");
                DataSet DS = new DataSet();
                DS.ReadXml(GETAWAY_PRICES_URL);
                using (SqlCommand com = new SqlCommand("delete from cruises_prices_pac_msc",_connection))
                {
                    com.ExecuteNonQuery();
                }
                var tbItem = DS.Tables["item"];
                var tbCruises = DS.Tables["cruise"];
                var tbDates = DS.Tables["date"];
                if (tbItem == null || tbCruises == null || tbDates == null)
                {
                    _logFile.WriteLine("PAC_MSC:Информация по ценам отсутствует");
                    return;
                }
                var tbPrices = new DataTable();
                //SqlDataAdapter adapter =
                //    GetSqlAdapter(new SqlCommand(@"select D_ID,D_DATE,C_ID from cruises_dates_pac_msc", _connection));
                //var tbDateID = new DataTable();
                //adapter.Fill(tbDateID);
                var adapter = GetSqlAdapter(new SqlCommand(@"select * from cruises_prices_pac_msc", _connection));
                adapter.Fill(tbPrices);

                foreach (DataRow row in tbItem.Rows)
                {

                    var date =
                        GetValue<DateTime>(
                            tbDates.Select(string.Format("date_Id = {0}", GetValue<int>(row["date_Id"])))[0]["value"]);
                    var cruiseId =
                        GetValue<int>(
                            tbDates.Select(string.Format("date_Id = {0}", GetValue<int>(row["date_Id"])))[0]["cruise_Id"
                                ]);
                    var cruise = GetValue<int>(tbCruises.Select(string.Format("cruise_id ={0}", cruiseId))[0]["id"]);
                    
                   
                    var pRows =
                        tbPrices.Select(string.Format("CAB_ID = {0} and C_ID = {1} and C_Date='{2}'", GetValue<int>(row["cabin_id"]),
                                                      cruise,date));
                    var pRow = pRows.Length == 0 ? tbPrices.NewRow() : pRows[0];

                    pRow["CAB_ID"] = GetValue<int>(row["cabin_id"]);
                    pRow["CAB_CATEGORY"] = GetValue<string>(row["cabin_cat"]);
                    pRow["ad1"] = GetValue<decimal>(row["ad1"]);
                    pRow["ad2"] = GetValue<decimal>(row["ad2"]);
                    pRow["ad3"] = GetValue<decimal>(row["ad3"]);
                    pRow["ad4"] = GetValue<decimal>(row["ad4"]);
                    pRow["ad2_ch1"] = GetValue<decimal>(row["ad2_ch1"]);
                    pRow["ad2_ch2"] = GetValue<decimal>(row["ad2_ch2"]);
                    pRow["ad3_ch1"] = GetValue<decimal>(row["ad3_ch1"]);
                    pRow["C_ID"] = cruise;
                    pRow["C_Date"] = date; 

                    if (pRows.Length == 0) tbPrices.Rows.InsertAt(pRow, 0);
                }
                adapter.Update(tbPrices);
                _logFile.WriteLine("PAC_MSC:Информация по ценам получена");
            }
            catch (Exception ex)
            {
                _logFile.WriteLine(string.Format("PAC_MSC:Ошибка во время получения данных: \nException:{0}  \nStackTrace:{1}", ex.Message, ex.StackTrace));
            }
        }
        private void GetSurcharges()
        {
            try
            {
                _logFile.WriteLine("PAC_MSC:Получение информации по доп.платам");
                DataSet DS = new DataSet();
                DS.ReadXml(GETAWAY_SURCHARGES_URL);

                var tbItem = DS.Tables["item"];
                var tbCruises = DS.Tables["cruise"];
                var tbDates = DS.Tables["date"];
                if (tbItem == null || tbCruises == null || tbDates == null)
                {
                    _logFile.WriteLine("PAC_MSC:Информация по доп.платам отсутствует");
                    return;
                }
                var tbSurcharges = new DataTable();
                SqlDataAdapter adapter =
                    GetSqlAdapter(new SqlCommand(@"select D_ID,D_DATE,C_ID from cruises_dates_pac_msc", _connection));
                var tbDateID = new DataTable();
                adapter.Fill(tbDateID);
                adapter = GetSqlAdapter(new SqlCommand(@"select * from cruises_surcharges_pac_msc", _connection));
                adapter.Fill(tbSurcharges);
                foreach (DataRow row in tbItem.Rows)
                {

                    var date = GetValue<DateTime>(tbDates.Select(string.Format("date_Id = {0}", GetValue<int>(row["date_Id"])))[0]["value"]);
                    var cruiseId = GetValue<int>(tbDates.Select(string.Format("date_Id = {0}", GetValue<int>(row["date_Id"])))[0]["cruise_Id"]);
                    var cruise = GetValue<int>(tbCruises.Select(string.Format("cruise_id ={0}", cruiseId))[0]["id"]);
                    if (tbDateID.Select(string.Format("D_DATE = '{0}' and C_ID = {1}", date, cruise)).Length == 0) continue;

                    var dateId =
                        GetValue<int>(
                            tbDateID.Select(string.Format("D_DATE = '{0}' and C_ID = {1}", date, cruise))[0]["D_ID"]);
                    var sRows = tbSurcharges.Select(string.Format("SC_TITLE = '{0}' and D_ID = {1}", GetValue<string>(row["title"]), dateId));
                    var sRow = sRows.Length == 0 ? tbSurcharges.NewRow() : sRows[0];

                    sRow["SC_TITLE"] = GetValue<string>(row["title"]);
                    sRow["SC_VALUE"] = GetValue<int>(row["value"]);
                    sRow["SC_TYPE"] = GetValue<string>(row["type"]);
                    sRow["D_ID"] = dateId;
                    if (sRows.Length == 0) tbSurcharges.Rows.InsertAt(sRow, 0);
                }
                adapter.Update(tbSurcharges);
                _logFile.WriteLine("PAC_MSC:Информация по доплатам получена");
            }
            catch (Exception ex)
            {
                _logFile.WriteLine(string.Format("PAC_MSC:Ошибка во время получения данных: \nException:{0}  \nStackTrace:{1}", ex.Message, ex.StackTrace));
            }
        }
        private void GetExtracharge()
        {
            try
            {
                _logFile.WriteLine("PAC_MSC:Получение включеных в стоимость/дополнительных услуг");
                DataSet DS = new DataSet();
                DS.ReadXml(GETAWAY_EXTRACHARGE_URL);
                var tbItem = DS.Tables["item"];
                var tbCruises = DS.Tables["cruise"];
                if (tbItem == null || tbCruises == null )
                {
                    _logFile.WriteLine("PAC_MSC:Информация по услугам отсутствует");
                    return;
                }
                var tbExtracharge = new DataTable();
                using (SqlCommand com = new SqlCommand(@"delete from cruises_extracharge_pac_msc", _connection))
                {
                    com.ExecuteNonQuery();
                }
                SqlDataAdapter adapter = GetSqlAdapter(new SqlCommand(@"select * from cruises_extracharge_pac_msc", _connection));
                adapter.Fill(tbExtracharge);
                foreach (DataRow row in tbItem.Rows)
                {
                    object excludeCruiseId = DBNull.Value;
                    object includeCruiseId = DBNull.Value;
                    DataRow[] sRows;
                    if (row["exclude_Id"] == DBNull.Value)
                    {
                        includeCruiseId = GetValue<int>(tbCruises.Select(string.Format("cruise_id ={0}", GetValue<int>(row["include_Id"])))[0]["id"]);
                        //SqlDataAdapter ad =
                        //    GetSqlAdapter(new SqlCommand(@"select * from cruises_pac_msc where C_ID = " + includeCruiseId, _connection));

                        //var tbtemp = new DataTable();
                        //ad.Fill(tbtemp);
                        //if (tbtemp.Rows.Count < 1)
                        //{
                        //    var r = tbtemp.NewRow();
                        //    r["C_ID"] = includeCruiseId;
                        //    tbtemp.Rows.InsertAt(r, 0);
                        //    ad.Update(tbtemp);
                        //}
                        sRows = tbExtracharge.Select(string.Format("EX_TEXT = '{0}' and C_ID_EXCLUDE is NULL and C_ID_INCLUDE = {1}", GetValue<string>(row["item_Text"]), includeCruiseId));
                    }
                    else
                    {
                        excludeCruiseId = GetValue<int>(tbCruises.Select(string.Format("cruise_id ={0}", GetValue<int>(row["exclude_Id"])))[0]["id"]);
                        //SqlDataAdapter ad =
                        //    GetSqlAdapter(new SqlCommand(@"select * from cruises_pac_msc where C_ID = " + excludeCruiseId, _connection));

                        //var tbtemp = new DataTable();
                        //ad.Fill(tbtemp);
                        //if (tbtemp.Rows.Count < 1)
                        //{
                        //    var r = tbtemp.NewRow();
                        //    r["C_ID"] = excludeCruiseId;
                        //    tbtemp.Rows.InsertAt(r, 0);
                        //    ad.Update(tbtemp);
                        //}
                        sRows = tbExtracharge.Select(string.Format("EX_TEXT = '{0}' and C_ID_EXCLUDE = {1} and C_ID_INCLUDE is NULL", GetValue<string>(row["item_Text"]), excludeCruiseId));
                    }


                    var sRow = sRows.Length == 0 ? tbExtracharge.NewRow() : sRows[0];

                    sRow["EX_TEXT"] = GetValue<string>(row["item_Text"]);
                    sRow["C_ID_EXCLUDE"] = excludeCruiseId;
                    sRow["C_ID_INCLUDE"] = includeCruiseId;
                    if (sRows.Length == 0) tbExtracharge.Rows.InsertAt(sRow, 0);
                }
                adapter.Update(tbExtracharge);
                _logFile.WriteLine("PAC_MSC:Получение информации по услугам завершено");
            }
            catch (Exception ex)
            {
                _logFile.WriteLine(string.Format("PAC_MSC:Ошибка во время получения данных: \nException:{0}  \nStackTrace:{1}", ex.Message, ex.StackTrace));
            }
        }
        private void GetExcursions()
        {
            try
            {
                _logFile.WriteLine("PAC_MSC:Получение данных по экскурсиям");
                DataSet DS = new DataSet();
                DS.ReadXml(GETAWAY_EXCURSIONS_URL);
                var dstbItem = DS.Tables["item"];
                var dstbCDates = DS.Tables["cruise_dates"];
                var dstbPackets = DS.Tables["packet"];
                if (dstbItem == null || dstbPackets == null || dstbCDates == null)
                {
                    _logFile.WriteLine("PAC_MSC: Информация по экскурсиям отстуствует");
                    return;
                }
                DataTable tbPackets = new DataTable();
                var adapter = GetSqlAdapter(new SqlCommand(@"select * from excursions_packets_pac_msc", _connection));
                adapter.Fill(tbPackets);
                foreach (DataRow row in dstbPackets.Rows)
                {
                    var dRows = tbPackets.Select("ID=" + GetValue<int>(row["packet_Id"]));
                    var dRow = dRows.Length == 0 ? tbPackets.NewRow() : dRows[0];

                    dRow["ID"] = row["packet_Id"];
                    dRow["PACKET_ID"] = row["id"];
                    dRow["PACKET_BOARDING_ID"] = row["BOARDING_ID"];
                    dRow["PACKET_PRICE"] = row["price"];
                    dRow["PACKET_CHILD_PRICE"] = row["child_price"];
                    dRow["PACKET_A_PRICE"] = row["agencies_price"];
                    dRow["PACKET_AC_PRICE"] = row["agencies_child_price"];
                    dRow["PACKET_NOTE"] = row["note"];
                    dRow["C_ID"] =
                        DS.Tables["cruise"].Select("cruise_Id=" + row.Field<int>("cruise_Id"))[0].Field<string>("id");
                    if (dRows.Length == 0) tbPackets.Rows.InsertAt(dRow, 0);
                }
                adapter.Update(tbPackets);
                adapter = GetSqlAdapter(new SqlCommand(@"select * from excursions_excursionslist_pac_msc", _connection));
                var tbExcursions = new DataTable();
                adapter.Fill(tbExcursions);
                foreach (DataRow row in dstbItem.Rows)
                {
                    var packet_id =
                        DS.Tables["excurslist"].Select("excurslist_id = " + row["excurslist_Id"])[0].Field<int>(
                            "packet_Id");
                    var dRows = tbExcursions.Select("PACKET_ID=" + packet_id);

                    var dRow = dRows.Length == 0 ? tbExcursions.NewRow() : dRows[0];

                    dRow["EL_TITLE"] = row["title"];
                    dRow["EL_CITY"] = row["city"];
                    dRow["EL_LENGTH"] = row["length"];
                    dRow["EL_PRICE"] =row["price"];
                    dRow["EL_CHILD_PRICE"] = row["child_price"];
                    dRow["EL_AGENCY_PRICE"] = row["agency_price"];
                    dRow["EL_AC_PRICE"] = row["agency_child_price"];
                    dRow["EL_ORDERING"] = row["ordering"];
                    dRow["EL_DESCRIPTION"] = row["description"];
                    dRow["PACKET_ID"] = packet_id;

                    if (dRows.Length == 0) tbExcursions.Rows.InsertAt(dRow, 0);
                }
                adapter.Update(tbExcursions);

                var tbDates = new DataTable();
                adapter = GetSqlAdapter(new SqlCommand(@"select * from excursions_packets_c_date_pac_msc", _connection));
                adapter.Fill(tbDates);
                foreach (DataRow row in dstbCDates.Rows)
                {
                    var dRows =
                        tbDates.Select("DT_DATE='" + GetValue<DateTime>(row["item"]) + "' and " + "PACKET_ID=" +
                                       GetValue<int>(row["packet_Id"]));
                    var dRow = dRows.Length == 0 ? tbDates.NewRow() : dRows[0];

                    dRow["DT_DATE"] = row["item"];
                    dRow["PACKET_ID"] = row["packet_Id"];

                    if (dRows.Length == 0) tbDates.Rows.InsertAt(dRow, 0);
                }
                adapter.Update(tbDates);
                _logFile.WriteLine("PAC_MSC:Данные по экскурсиям получены");
            }
            catch (Exception ex)
            {
                _logFile.WriteLine("PAC_MSC:Формат данных был изменен, функционал временно не работоспособен");
                //_logFile.WriteLine(string.Format("PAC_MSC:Ошибка во время получения данных: \nException:{0}  \nStackTrace:{1}", ex.Message, ex.StackTrace));
            }

        }
        private void GetExtraPacks()
        {
            try
            {
                _logFile.WriteLine("PAC_MSC:Получение информации по дополнительным пакетам");
                byte[] data;
                using (WebClient webClient = new WebClient())
                    data = webClient.DownloadData(GETAWAY_EXTRAPACKS_URL);

                
                DataSet ds = new DataSet();
             // ds.ReadXml(new MemoryStream(data));
                
                
                
                
                
                
                
                
                
                
                string str = Encoding.GetEncoding("UTF-8").GetString(data);
                XDocument XDoc = XDocument.Parse(str);
                var xPackets = XDoc.Root.Elements().First(el => el.Name.LocalName == "packets");
                var tbExtraPacks = new DataTable();
                var tbIncludes = new DataTable();
                using (SqlCommand com = new SqlCommand(@"--delete from cruises_extra_pack_includes_pac_msc_old
                                                         --delete from cruises_extra_packs_pac_msc_old
                                                         --insert into cruises_extra_pack_includes_pac_msc_old select cruises_extra_pack_includes_pac_msc.*,GetDate() from cruises_extra_pack_includes_pac_msc
                                                         --insert into cruises_extra_packs_pac_msc_old select cruises_extra_packs_pac_msc.*,GetDate() from cruises_extra_packs_pac_msc  
                                                         delete from cruises_extra_pack_includes_pac_msc 
                                                         delete from cruises_extra_packs_pac_msc", _connection))
                {
                    com.ExecuteNonQuery();
                }
                var packsAdapter = GetSqlAdapter(new SqlCommand(@"select *  from cruises_extra_packs_pac_msc", _connection));
                packsAdapter.Fill(tbExtraPacks);
                tbExtraPacks.Rows.Clear();
                //var includesAdapter = GetSqlAdapter(new SqlCommand(@"select *  from cruises_extra_pack_includes_pac_msc", _connection));
                //includesAdapter.Fill(tbIncludes);
                //tbIncludes.Rows.Clear();
                List<Packet> packets = new List<Packet>(xPackets.Nodes().Count());
                
                foreach (XElement cruiseNode in xPackets.Nodes())
                {
                    foreach (XElement item in cruiseNode.Elements())
                    {
                        
                        Packet p = new Packet();
                        p.cruiseid = (int) GetValue<int>(cruiseNode.Attribute(XName.Get("id", "")).Value);
                        p.id = (int) GetValue<int>(item.Attribute(XName.Get("id", "")).Value);
                        p.title = item.Element(XName.Get("title", "http://gate.gocruise.ru/")).Value;
                        p.adult =
                            (int) GetValue<int>(item.Element(XName.Get("adult", "http://gate.gocruise.ru/")).Value);
                        p.adult_3_4 =
                            (int) GetValue<int>(item.Element(XName.Get("adult_3_4", "http://gate.gocruise.ru/")).Value);
                        p.child =
                            (int) GetValue<int>(item.Element(XName.Get("child", "http://gate.gocruise.ru/")).Value);
                        p.description = item.Element(XName.Get("description", "http://gate.gocruise.ru/")).Value;
                        p.date =
                            (DateTime)
                            GetValue<DateTime>(item.Element(XName.Get("date", "http://gate.gocruise.ru/")).Value);
                        XElement include = item.Element(XName.Get("include", "http://gate.gocruise.ru/"));
                        if (include != null)
                        {
                            List<string> incldeItems = new List<string>(include.Elements().Count());
                            incldeItems.AddRange(include.Elements().Select(includeItem => includeItem.Value));
                            p.incldeItems = incldeItems;
                        }
                    packets.Add(p);
                    }
                }
                foreach (Packet pack in packets)
                {
                    var dRows = tbExtraPacks.Select(string.Format("EP_ID={0}", pack.id));
                    var dRow = dRows.Length == 0 ? tbExtraPacks.NewRow() : dRows[0];
                    dRow["EP_ID"] = pack.id;
                    dRow["EP_TITLE"] = pack.title;
                    dRow["EP_ADULT"] = pack.adult;
                    dRow["EP_ADULT_3_4"] = pack.adult_3_4;
                    dRow["EP_CHILD"] = pack.child;
                    dRow["EP_DESCRIPTION"] = pack.description;
                    dRow["EP_DATE"] = pack.date;
                    dRow["C_ID"] = pack.cruiseid;

                    if (dRows.Length == 0) tbExtraPacks.Rows.InsertAt(dRow, 0);
                }
                packsAdapter.Update(tbExtraPacks);
                tbExtraPacks.Clear();
                packsAdapter.Fill(tbExtraPacks);
                foreach (Packet pack in packets)
                {
                    var ID = pack.id;
                    if(pack.incldeItems==null)continue;
                    foreach (string item in pack.incldeItems)
                    {
                        using (SqlCommand com = new SqlCommand(@"insert into [cruises_extra_pack_includes_pac_msc] ([EP_ID],[EPI_INCLUDE]) values (@p1,@p2) ",_connection))
                        {
                            com.Parameters.AddWithValue("@p1", ID);
                            com.Parameters.AddWithValue("@p2", item);
                            com.ExecuteNonQuery();
                        }
    


                    }
                }
                
                _logFile.WriteLine("PAC_MSC:Информация по доп.пакетам получена");
            }
            catch (Exception ex)
            {
                _logFile.WriteLine(string.Format("PAC_MSC:Ошибка во время получения данных: \nException:{0}  \nStackTrace:{1}", ex.Message, ex.StackTrace));
            }
        }

        private struct Packet
        {
            public int cruiseid;
            public int id;
            public string title;
            public int adult;
            public int adult_3_4;
            public int child;
            public string description;
            public DateTime date;
            public List<string> incldeItems;
        }
        private static object GetValue<T>(object o)
        {
            try
            {
                if (o == DBNull.Value)
                    return o;
                if (typeof(T) == typeof(string))
                    return o.ToString();
                if (typeof(T) == typeof(int))
                    return int.Parse(o.ToString());
                if (typeof(T) == typeof(decimal))
                    return decimal.Parse(o.ToString().Replace('.',','));
                if (typeof(T) == typeof(DateTime))
                    return Convert.ToDateTime(o);
                return (T)o;
            }
            catch (Exception)
            {

                throw;
            }


        }
    }
}