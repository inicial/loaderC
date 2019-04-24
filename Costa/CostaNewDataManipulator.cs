using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Reflection;
using System.Text;
using System.Xml;
using System.Xml.Schema;
using Costa.CostaSoap;
using DxHelpersLib;
using ICSharpCode.SharpZipLib.Zip;

using PluginInteractionLib;
using Costa;
using System.Xml.Serialization;

namespace CostaNewPlugin
{
  
    class Url_damp
    {

        public string _url;
        public string _file_name;
        public Url_damp(string url, string file)
        {
            _url = url;
            _file_name = file;
        }
    }
    public class CostaNewDatamanipulator : DataManipulator 
    {
        Agency costaAgency = new Agency();
        Partner costaPartner = new Partner();
        private string _path,_pointend;

        Dictionary<string, List<DataSet>> _dataGroups = new Dictionary<string, List<DataSet>>();
        string[] str =
                {
                    "Caribbean", "GreatEasternCruises", "Mediterranean",
                    "North Europe", "Pacific Asia", "Persian Gulf", "Read Sea",
                    "Round World", "South America", "Transatlantic"
                };
        public CostaNewDatamanipulator(SqlConnection con, Logger log) : base(con, log)
        {
            ExeConfigurationFileMap configMap = new ExeConfigurationFileMap();
            configMap.ExeConfigFilename = Path.Combine(Path.GetDirectoryName(Assembly.GetEntryAssembly().Location),
                "lanta.sqlconfig.dll.config");
            Configuration config = ConfigurationManager.OpenMappedExeConfiguration(configMap,
                ConfigurationUserLevel.None);
            costaAgency.Code = config.AppSettings.Settings["costaCode"].Value;
            costaPartner.Name = config.AppSettings.Settings["costaName"].Value;
            costaPartner.Password = config.AppSettings.Settings["costaPass"].Value;
            _path = config.AppSettings.Settings["costaPath"].Value;
            string str = DateTime.Now.ToString("ddMMyyyyHHmm") + "\\";
            _path += str;
        }
        
        Stream UnpackZip(byte[] zipByteArray)
        {
            Stream zipStream = new MemoryStream(zipByteArray);
            using (ZipInputStream zipInputStream = new ZipInputStream(zipStream))
            {
                ZipEntry entry = zipInputStream.GetNextEntry();
                try
                {
                    MemoryStream result = new MemoryStream();
                    int size;
                    byte[] buffer = new byte[1024];
                    do
                    {
                        size = zipInputStream.Read(buffer, 0, buffer.Length);
                        result.Write(buffer, 0, size);
                    } while (size > 0);
                    result.Position = 0;
                    return result;
                }
                catch (Exception)
                {
                    throw;
                }
            }
        }

        private ExportSoapClient getSoapClient()
        {
            ExportSoapClient _costaClient = null;
            try
            {
                _costaClient = new ExportSoapClient();
            }
            catch (Exception ec)
            {
                _logFile.WriteLine("ошибка  " + ec.Message + "   лист вызова" + ec.InnerException + "  Траса " + ec.StackTrace);
                throw;
            }
            _logFile.WriteLine("CostaNew : Подключение к Soap шлюзу прошло успешно");
            return _costaClient;
        }

        private void downloadFiles(List<Url_damp> Urls)
        {
            
           
            if (!Directory.Exists(_path))
            {
                Directory.CreateDirectory(_path);
            }
            _logFile.WriteLine("CostaNew : Выкачка файлов");
           
            WebClient webClient = new WebClient();
            Dictionary<String, Int64> firstFileSizesInBytes = new Dictionary<String, Int64>();
            
            foreach (Url_damp url in Urls)
                
            {
                DateTime before = DateTime.Now;
                String fileName = _path + url._file_name + ".zip";
                String filecosta = url._url.ToString().Replace("https", "http");
               //
                //filecosta=filecosta.r
                Int64 firstFileSizeInBytes;
                // Пробуем в течении 5 минут выкачать файл, до тех пор не появится что-то с размером больше 0
              //  System.Threading.Thread.Sleep(20000);
              //  _logFile.WriteLine("CostaNew : Задержка 20 сек ");
                _logFile.WriteLine(filecosta);
                while (DateTime.Now.Subtract(before).Minutes < 5)
                {
                   // _logFile.WriteLine("CostaNew : 1 ");
                   
                   // webClient.DownloadFile(url._url, fileName);
                    webClient.DownloadFile(filecosta, fileName);
                   // _logFile.WriteLine("CostaNew : 2 "); 
                    firstFileSizeInBytes = new FileInfo(fileName).Length;
                    if (firstFileSizeInBytes > 0)
                    {
                        break;
                    }
                    System.Threading.Thread.Sleep(1000);
                }
                firstFileSizeInBytes = new FileInfo(fileName).Length;
                firstFileSizesInBytes[fileName] = firstFileSizeInBytes;
              //  webClient.DownloadFile(url._url, fileName);
                webClient.DownloadFile(filecosta, fileName);
            }
            /* Чтобы проверить не меняются ли размеры после первого появления файла, мы через 5 минут
             * качаем его снова и сравниваем размер. Если не совпадают, пишем в лог ошибку.
             */
            System.Threading.Thread.Sleep(300000);
            foreach (Url_damp url in Urls)
            {
                String fileName = _path + url._file_name + ".zip";
                String filecosta = url._url.ToString().Replace("https", "http");
                webClient.DownloadFile(filecosta, fileName);
                Int64 lastFileSizeInBytes = new FileInfo(fileName).Length;
                if (firstFileSizesInBytes[fileName] != lastFileSizeInBytes)
                {
                    _logFile.WriteLine(String.Format(
                        "Обнаружено несовпадение размеров при контрольной выкачке файла {0}, оригинальный размер {1}, контрольный {2}",
                        fileName, firstFileSizesInBytes[fileName], lastFileSizeInBytes));
                }
            }
            _logFile.WriteLine("CostaNew : Файлы выкачены");
        }

   
        public string GetExportFare()
        {
            ExportSoapClient _costaClient = getSoapClient();
            List<Url_damp> Urls = new List<Url_damp>();
            String url_Fare_all = _costaClient.ExportFare(costaAgency, costaPartner, "");
            Urls.Add(new Url_damp(url_Fare_all, "ExportFare"));
            downloadFiles(Urls);
            byte[] fares = File.ReadAllBytes(_path + "ExportFare.zip");
            Stream farestream = UnpackZip(fares);
            DataSet faresds = new DataSet();
            faresds.ReadXml(farestream);
            SqlCommand delCom = new SqlCommand("delete from CostaExportFare", _connection);
            delCom.ExecuteNonQuery();
            DataTable destinationTable = faresds.Tables["Destination"];
            string insFare = @"INSERT INTO [dbo].[CostaExportFare] ([DestinationCode]
           ,[DestinationDisplayName]
           ,[CruiseCode]
           ,[FareCode]
           ,[FareDescription])
            VALUES
           (@DestinationCode
           ,@DestinationDisplayName
           ,@CruiseCode
           ,@FareCode
           ,@FareDescription)";
            int count = 0;
            foreach (DataRow destination in destinationTable.Rows)
            {
                foreach (DataRow cruise in destination.GetChildRows(destinationTable.ChildRelations[0])) {
                    DataRow faresRow = cruise.GetChildRows(faresds.Tables["Cruise"].ChildRelations[0])[0];
                    foreach (DataRow row in faresRow.GetChildRows(faresds.Tables["Fares"].ChildRelations[0]))
                    {
                        count++;
                        SqlCommand insertComand = new SqlCommand(insFare, _connection);
                        insertComand.Parameters.AddWithValue("@DestinationCode", destination["Code"]);
                        insertComand.Parameters.AddWithValue("@DestinationDisplayName", destination["DisplayName"]);
                        insertComand.Parameters.AddWithValue("@CruiseCode", cruise["Code"]);
                        insertComand.Parameters.AddWithValue("@FareCode", row["Code"]);
                        insertComand.Parameters.AddWithValue("@FareDescription", row["FareDescription"]);
                        insertComand.ExecuteNonQuery();
                    }
                }
            }
            if (count > 0) return String.Format("CostaNew : ExportFare получен {0} записей.", count);
            else
            {
                return "CostaNew : ExportFare записи не получены";
            }
        }

        public override string GetShipsData()
        {
            byte[] ships = File.ReadAllBytes(_path + "Ships.zip");
            Stream shipStream = UnpackZip(ships);
            DataSet shipds = new DataSet();
            shipds.ReadXml(shipStream);
            SqlCommand delCom = new SqlCommand("delete from ships_costa", _connection);
            delCom.ExecuteNonQuery();
            DataTable shipTable = shipds.Tables["Ship"];
            string insShip = @"INSERT INTO [dbo].[ships_costa] ([S_CCN_COD_NAVE]
           ,[S_DISPLAY_NAME]
           ,[S_DESCRIPTION]
           ,[S_IMG_URL]
           ,[S_URL])
            VALUES
           (@S_CCN_COD_NAVE
           ,@S_DISPLAY_NAME
           ,@S_DESCRIPTION
           ,@S_IMG_URL
           ,@S_URL)" ;
            int count = 0;
            foreach (DataRow row in shipTable.Rows)
            {
                count++;
                SqlCommand insertComand = new SqlCommand(insShip,_connection);
                insertComand.Parameters.AddWithValue("@S_CCN_COD_NAVE", row["CCN_COD_NAVE"]);
                insertComand.Parameters.AddWithValue("@S_DISPLAY_NAME", row["DisplayName"]);
                insertComand.Parameters.AddWithValue("@S_DESCRIPTION", row["Description"]);
                insertComand.Parameters.AddWithValue("@S_IMG_URL", row["ImgUrl"]);
                insertComand.Parameters.AddWithValue("@S_URL", row["Url"]);
                insertComand.ExecuteNonQuery();
            }
            if (count > 0) return String.Format("CostaNew : Получено {0} лайнеров", count);
            else
            {
                return "CostaNew : Лайнеры не получены";
            }
        }

        public override string GetDecksData()
        {
            byte[] ships = File.ReadAllBytes(_path + "Ships.zip");
            Stream shipStream = UnpackZip(ships);
            DataSet shipds = new DataSet();
            shipds.ReadXml(shipStream);
            SqlCommand delCom = new SqlCommand("Delete from dbo.decks_costa", _connection);
            delCom.ExecuteNonQuery();
            DataTable shipTable = shipds.Tables["Ship"];
            string insDeck = @"INSERT INTO [dbo].[decks_costa]
           ([D_CODE]
           ,[D_DESCRIPTION]
           ,[S_CCN_COD_NAVE])
     VALUES
           (@D_CODE
           ,@D_DESCRIPTION
           ,@S_CCN_COD_NAVE)";
            DataTable deckTable = shipds.Tables["Deck"];
            int count = 0;
            foreach (DataRow row in deckTable.Rows)
            {
                count++;
                SqlCommand insertComand = new SqlCommand(insDeck, _connection);
                insertComand.Parameters.AddWithValue("@D_CODE", row["DeckCode"]);
                insertComand.Parameters.AddWithValue("@D_DESCRIPTION", row["DeckDescription"]);
                insertComand.Parameters.AddWithValue("@S_CCN_COD_NAVE", shipTable.Select("Ship_Id=" + row["Decks_Id"].ToString())[0]["CCN_COD_NAVE"]);
                insertComand.ExecuteNonQuery();
            }
            
            if (count > 0) return String.Format("CostaNew : Получено {0} палуб", count);
            else
            {
                return "CostaNew : Палубы не получены";
            }
        }

        public override string GetCabinsData()
        {
            byte[] ships = File.ReadAllBytes(_path + "Ships.zip");
            Stream shipStream = UnpackZip(ships);
            DataSet shipds = new DataSet();
            shipds.ReadXml(shipStream);
            SqlCommand delCom = new SqlCommand("Delete from dbo.cabin_categories_costa", _connection);
            delCom.ExecuteNonQuery();
            DataTable shipTable = shipds.Tables["Ship"];
            string insCategory = @"INSERT INTO [dbo].[cabin_categories_costa]
           ([CAT_CODE]
           ,[CAT_NAME]
           ,[CAT_DESCRIPTION]
           ,[CAT_QUICK_TIME_URL]
           ,[S_CCN_COD_NAVE])
            VALUES
           (@CAT_CODE
           ,@CAT_NAME
           ,@CAT_DESCRIPTION
           ,@CAT_QUICK_TIME_URL
           ,@S_CCN_COD_NAVE)";
            DataTable categoryTable = shipds.Tables["Category"];
            int count = 0;
            foreach (DataRow row in categoryTable.Rows)
            {
                count++;
                SqlCommand insertComand = new SqlCommand(insCategory, _connection);
                insertComand.Parameters.AddWithValue("@CAT_CODE", row["CategoryCode"]);
                insertComand.Parameters.AddWithValue("@CAT_NAME", row["CategoryName"]);
                insertComand.Parameters.AddWithValue("@CAT_DESCRIPTION", row["CategoryDescription"]);
                insertComand.Parameters.AddWithValue("@CAT_QUICK_TIME_URL", row["QuickTimeUrl"]);
                insertComand.Parameters.AddWithValue("@S_CCN_COD_NAVE", shipTable.Select("Ship_Id=" + row["Categories_Id"].ToString())[0]["CCN_COD_NAVE"]);
                insertComand.ExecuteNonQuery();
            }
            if (count > 0) return String.Format("CostaNew : Получено {0} категорий кают", count);
            else
            {
                return "CostaNew : Категории кают не получены";
            }
        }

        public string GetShips()
        {
            ExportSoapClient _costaClient = getSoapClient();
            List<Url_damp> Urls = new List<Url_damp>();
            try
            {
                String url_Ships = _costaClient.ExportShipsAndCategories(costaAgency, costaPartner);
                Urls.Add(new Url_damp(url_Ships, "Ships"));
            }
            catch (Exception)
            {
                _logFile.WriteLine("CostaNew : не удалось скачать файл кораблей");
            }
            downloadFiles(Urls);
            return GetShipsData() + "\n" +
            GetDecksData() + "\n" +
            GetCabinsData();
        }

        public override string GetItineraryData()
        {
            ExportSoapClient _costaClient = getSoapClient();
            List<Url_damp> Urls = new List<Url_damp>();
            String url_iten_all = _costaClient.ExportItineraryAndSteps(costaAgency, costaPartner, "");
            Urls.Add(new Url_damp(url_iten_all, "Itenery"));
            String url_Destination = _costaClient.ExportDestination(costaAgency, costaPartner, "");
            Urls.Add(new Url_damp(url_Destination, "Destination"));
            downloadFiles(Urls);
            byte[] Distinations = File.ReadAllBytes(_path+"Destination.zip");
            Stream DistinationsStream = UnpackZip(Distinations);
            DataSet Distinationsds = new DataSet();
            Distinationsds.ReadXml(DistinationsStream);
            byte[] itener = File.ReadAllBytes(_path+"Itenery.zip");
            Stream itenerStream = UnpackZip(itener);
            DataSet itenerds = new DataSet();
            itenerds.ReadXml(itenerStream);
            int count = 0;
            DataTable ports = Distinationsds.Tables["Port"];
            SqlCommand delports = new SqlCommand("Delete from  ports_costa",_connection);
            delports.ExecuteNonQuery();
            string insport = @"INSERT INTO [dbo].[ports_costa]
           ([PORT_CODE]
           ,[PORT_DESCRIPTION])
     VALUES
           (@PORT_CODE
           ,@PORT_DESCRIPTION)";
            
            foreach (DataRow row in ports.Rows)
            {
                using (SqlCommand com = new SqlCommand(insport,_connection))
                {
                    com.Parameters.AddWithValue("@PORT_CODE", row["Code"]);
                    com.Parameters.AddWithValue("@PORT_DESCRIPTION", row["DisplayName"]);
                    com.ExecuteNonQuery();
                }
            }

            DataTable dest = itenerds.Tables["Destination"];
            SqlCommand deldest = new SqlCommand("Delete from  destinations_costa",_connection);
            deldest.ExecuteNonQuery();
            string insdest = @"INSERT INTO [dbo].[destinations_costa]
           ([DEST_CODE]
           ,[DEST_DISPLAY_NAME])
     VALUES
           (@DEST_CODE
           ,@DEST_DISPLAY_NAME)";
            foreach (DataRow row in dest.Rows)
            {
                 using (SqlCommand com = new SqlCommand(insdest,_connection))
                {
                    com.Parameters.AddWithValue("@DEST_CODE", row["Code"]);
                    com.Parameters.AddWithValue("@DEST_DISPLAY_NAME", row["DisplayName"]);
                    com.ExecuteNonQuery();
                }
            }

            DataTable itene = Distinationsds.Tables["Itinerary"];
            DataTable itinerary = itenerds.Tables["Itinerary"];
            SqlCommand delitinerary = new SqlCommand("Delete from  itinerary_costa", _connection);
            delitinerary.ExecuteNonQuery();
            string insitinerary = @"INSERT INTO [dbo].[itinerary_costa]
           ([ITI_CODE]
           ,[ITI_DISPLAY_NAME]
           ,[DEST_CODE]
           ,[ITI_URL]
           ,[ITI_NAME]
           ,[ITI_DESCRIPTION])
     VALUES
           (@ITI_CODE
           ,@ITI_DISPLAY_NAME
           ,@DEST_CODE
           ,@ITI_URL
           ,@ITI_NAME
           ,@ITI_DESCRIPTION)";
            foreach (DataRow row in itinerary.Rows)
            {
                count++;
                using (SqlCommand com = new SqlCommand(insitinerary, _connection))
                {
                    com.Parameters.AddWithValue("@ITI_CODE", row["Code"]);
                    com.Parameters.AddWithValue("@ITI_DISPLAY_NAME", row["DisplayName"]);
                    com.Parameters.AddWithValue("@DEST_CODE", dest.Select("Destination_Id=" + row["Destination_Id"].ToString())[0]["Code"]);
                    com.Parameters.AddWithValue("@ITI_URL", itene.Select("Code='" + row["Code"].ToString()+"' and Itineraries_Id=0" )[0]["Url"]);
                    com.Parameters.AddWithValue("@ITI_NAME", itene.Select("Code='" + row["Code"].ToString() + "' and Itineraries_Id=0")[0]["Name"]);
                    com.Parameters.AddWithValue("@ITI_DESCRIPTION", itene.Select("Code='" + row["Code"].ToString() + "' and Itineraries_Id=0")[0]["Description"]);
                    com.ExecuteNonQuery();
                }
            }

            DataTable itinStep = itenerds.Tables["Step"];
            SqlCommand delitinstep = new SqlCommand("Delete from  itinerary_steps_costa", _connection);
            delitinstep.ExecuteNonQuery();
            string insitinstep = @"INSERT INTO [dbo].[itinerary_steps_costa]
           ([PORT_CODE_DEPARTURE]
            ,PORT_DISC_DEP
           ,[PORT_CODE_ARRIVAL]
            ,PORT_DISC_ARR
           ,[ITIS_DEPARTURE_TIME]
           ,[ITIS_ARRIVAL_TIME]
           ,[ITIS_DEPARTURE_DAY]
           ,[ITIS_ARRIVAL_DAY]
           ,[ITI_CODE])
     VALUES
           (@PORT_CODE_DEPARTURE
            ,@PORT_DISC_DEP
           ,@PORT_CODE_ARRIVAL
            ,@PORT_DISC_ARR
           ,@ITIS_DEPARTURE_TIME
           ,@ITIS_ARRIVAL_TIME
           ,@ITIS_DEPARTURE_DAY
           ,@ITIS_ARRIVAL_DAY
           ,@ITI_CODE)";

            foreach (DataRow row in itinStep.Rows)
            {
                using (SqlCommand com = new SqlCommand(insitinstep, _connection))
                {
                    com.Parameters.AddWithValue("@PORT_CODE_DEPARTURE", row["CodeDeparturePort"]);
                    com.Parameters.AddWithValue("@PORT_DISC_DEP", row["DeparturePortDescription"]);
                    com.Parameters.AddWithValue("@PORT_CODE_ARRIVAL", row["CodeArrivelPort"]);
                    com.Parameters.AddWithValue("@PORT_DISC_ARR", row["ArrivelPortDescrption"]);
                    com.Parameters.AddWithValue("@ITIS_DEPARTURE_TIME", row["DepartureTime"]);
                    com.Parameters.AddWithValue("@ITIS_ARRIVAL_TIME", row["ArrivalTime"]);
                    com.Parameters.AddWithValue("@ITIS_DEPARTURE_DAY", row["DepartureDay"]);
                    com.Parameters.AddWithValue("@ITIS_ARRIVAL_DAY", row["ArrivalDay"]);
                    com.Parameters.AddWithValue("@ITI_CODE", itinerary.Select("Itinerary_Id=" + row["Steps_Id"].ToString())[0]["Code"]);
                    com.ExecuteNonQuery();
                }
            }

            if (count > 0)
            {
                return "CostaNew : Маршруты получены  :  " + count.ToString();

            }
            else
            {
                return "CostaNew : Маршруты не получены ";
            }
        }

        public string GetPrice(String fare)
        {
            ExportSoapClient _costaClient = getSoapClient();
            List<Url_damp> Urls = new List<Url_damp>();
            String url_Catalog = _costaClient.ExportCatalog(costaAgency, costaPartner);
            Urls.Add(new Url_damp(url_Catalog, "Catalog"));
            // Старый формат цен, который уже давно не используется.
            //String url_Price = _costaClient.ExportPrice(costaAgency, costaPartner);
            //Urls.Add(new Url_damp(url_Price, "Price"));
            ExportPriceType value = (ExportPriceType)Enum.Parse(typeof(ExportPriceType), fare);
            try
            {
                String farePrices = _costaClient.ExportPriceWithPaxBreakdown(costaAgency, costaPartner, "", ((ExportPriceType)value), MaxOccupancy.TwoPax);
                Urls.Add(new Url_damp(farePrices, "Price_all_" + fare));
            }
            catch (Exception e)
            {
                throw new Exception("CostaNew : не удалось запросить файл для тарифа " + fare, e);
            }
            downloadFiles(Urls);
            byte[] catalog = File.ReadAllBytes(_path + "Catalog.zip");
            Stream catalogStream = UnpackZip(catalog);
            DataSet catalogds = new DataSet();
            catalogds.ReadXml(catalogStream);
            int count = 0;
            string insCruise = @"INSERT INTO [dbo].[cruises_costa]
           ([C_CODE]
           ,[C_DISPLAY_NAME]
           ,[PORT_CODE_DEPARTURE]
           ,[PORT_CODE_ARRIVAL]
           ,[C_DEPARTURE_DATE]
           ,[C_ARRIVAL_DATE]
           ,[C_AVAILABILITY]
           ,[C_SELLABILITY]
           ,[C_IS_IMMIDIATE_CONFIRM]
           ,[C_FLIGHT_MANDATORY]
           ,[C_HOTTEL_MANDATORY]
           ,[ITI_CODE]
           ,[S_CCN_COD_NAVE])
     VALUES
           (@C_CODE
           ,@C_DISPLAY_NAME
           ,@PORT_CODE_DEPARTURE
           ,@PORT_CODE_ARRIVAL
           ,@C_DEPARTURE_DATE
           ,@C_ARRIVAL_DATE
           ,@C_AVAILABILITY
           ,@C_SELLABILITY
           ,@C_IS_IMMIDIATE_CONFIRM
           ,@C_FLIGHT_MANDATORY
           ,@C_HOTTEL_MANDATORY
           ,@ITI_CODE
           ,@S_CCN_COD_NAVE)";
            DataTable cruise = catalogds.Tables["Cruise"];
            DataTable iten = catalogds.Tables["Itinerary"];
            SqlCommand delCruise = new SqlCommand("Delete from cruises_costa",_connection);
            delCruise.ExecuteNonQuery();
            foreach (DataRow row in cruise.Rows)
            {
                count++;
                using (SqlCommand com =  new SqlCommand(insCruise,_connection) )
                {
                    com.Parameters.AddWithValue("@C_CODE", row["code"]);
                    com.Parameters.AddWithValue("@C_DISPLAY_NAME", row["DisplayName"]);
                    com.Parameters.AddWithValue("@PORT_CODE_DEPARTURE", row["DeparturePort"]);
                    com.Parameters.AddWithValue("@PORT_CODE_ARRIVAL", row["ArrivalPort"]);
                    com.Parameters.AddWithValue("@C_DEPARTURE_DATE", Convert.ToDateTime(row["DepartureDate"].ToString()).Date);
                    com.Parameters.AddWithValue("@C_ARRIVAL_DATE", Convert.ToDateTime(row["ArrivalDate"].ToString()).Date);
                    com.Parameters.AddWithValue("@C_AVAILABILITY", IsYes(row["Availability"].ToString()));
                    com.Parameters.AddWithValue("@C_SELLABILITY", IsYes(row["Sellability"].ToString()));
                    com.Parameters.AddWithValue("@C_IS_IMMIDIATE_CONFIRM", IsYes(row["IsImmediateConfirm"].ToString()));
                    com.Parameters.AddWithValue("@C_FLIGHT_MANDATORY", bool.Parse(row["FlightMandatory"].ToString()));
                    com.Parameters.AddWithValue("@C_HOTTEL_MANDATORY", bool.Parse(row["HotelMandatory"].ToString()));
                    com.Parameters.AddWithValue("@ITI_CODE", iten.Select("Itinerary_Id="+row["Itinerary_Id"].ToString())[0]["Code"]);
                    com.Parameters.AddWithValue("@S_CCN_COD_NAVE", row["Ship"]);
                    com.ExecuteNonQuery();
                }
            }

            SqlCommand delPrice = new SqlCommand("Delete from prices_costa_with_fares where Fare = @fare", _connection);
            delPrice.Parameters.AddWithValue("@fare", fare);
            delPrice.ExecuteNonQuery();

            String priceFilename = "Price_all_" + ((ExportPriceType)value).ToString() + ".zip";
            if (File.Exists(_path + priceFilename))
            {
                byte[] price = File.ReadAllBytes(_path + priceFilename);
                Stream priceStream = UnpackZip(price);
                DataSet priceds = new DataSet();
                priceds.ReadXml(priceStream);
                DataColumn column = new DataColumn("Fare", typeof(string));
                column.DefaultValue = ((ExportPriceType)value).ToString();
                priceds.Tables["Category"].Columns.Add(column);
                count += writePrices(priceds);
            }

            if (count > 0)
            {
                return "CostaNew : Круизы получены  :  " + count.ToString();
            }
            else
            {
                return "CostaNew : Круизы не получены ";
            }
        }

        private int writePrices(DataSet priceds)
        {
            int count = 0;
            string insPrice = @"INSERT INTO [dbo].[prices_costa_with_fares]
           ([PR_CODE]
           ,[PR_DESCRIPTION]
           ,[PR_DISCOUNT]
           ,[PR_BEST_PRICE]
           ,[PR_LIST_PRICE]
           ,[FirstAdult]
           ,[SecondAdult]
           ,[ThirdAdult]
           ,[FourthAdult]
           ,[ThirdChild]
           ,[ThirdJunior]
           ,[SingleSupplement]
           ,[PR_MANDATORY_FLIGHT]
           ,[PR_HOTEL_MANDATORY]
           ,[PR_AVAILABILITY]
           ,[C_CODE]
           ,[Fare])
     VALUES
           (@PR_CODE
           ,@PR_DESCRIPTION
           ,@PR_DISCOUNT
           ,@PR_BEST_PRICE
           ,@PR_LIST_PRICE
           ,@FirstAdult
           ,@SecondAdult
           ,@ThirdAdult
           ,@FourthAdult
           ,@ThirdChild
           ,@ThirdJunior
           ,@SingleSupplement
           ,@PR_MANDATORY_FLIGHT
           ,@PR_HOTEL_MANDATORY
           ,@PR_AVAILABILITY
           ,@C_CODE
           ,@Fare)";
            DataTable cruises = priceds.Tables["Cruise"];
            DataTable prices = priceds.Tables["Category"];
            foreach (DataRow row in prices.Rows)
            {
                count++;
                using (SqlCommand com = new SqlCommand(insPrice, _connection))
                {
                    com.Parameters.AddWithValue("@PR_CODE", row["Code"]);
                    com.Parameters.AddWithValue("@PR_DESCRIPTION", row["Description"]);
                    com.Parameters.AddWithValue("@PR_DISCOUNT", row["Discount"]);
                    com.Parameters.AddWithValue("@PR_BEST_PRICE", row["BestPrice"]);
                    com.Parameters.AddWithValue("@PR_LIST_PRICE", row["ListPrice"]);
                    com.Parameters.AddWithValue("@FirstAdult", row["FirstAdult"]);
                    com.Parameters.AddWithValue("@SecondAdult", row["SecondAdult"]);
                    com.Parameters.AddWithValue("@ThirdAdult", row["ThirdAdult"]);
                    com.Parameters.AddWithValue("@FourthAdult", row["FourthAdult"]);
                    com.Parameters.AddWithValue("@ThirdChild", row["ThirdChild"]);
                    com.Parameters.AddWithValue("@ThirdJunior", row["ThirdJunior"]);
                    com.Parameters.AddWithValue("@SingleSupplement", row["SingleSupplement"]);
                    com.Parameters.AddWithValue("@PR_MANDATORY_FLIGHT", bool.Parse(row["MandatoryFlight"].ToString()));
                    com.Parameters.AddWithValue("@PR_HOTEL_MANDATORY", bool.Parse(row["HotelMandatory"].ToString()));
                    com.Parameters.AddWithValue("@PR_AVAILABILITY", bool.Parse(row["Availability"].ToString()));
                    com.Parameters.AddWithValue("@C_CODE", cruises.Select("Cruise_Id=" + row["Categories_Id"].ToString())[0]["Code"]);
                    com.Parameters.AddWithValue("@Fare", row["Fare"]);

                    com.ExecuteNonQuery();
                }
            }
            return count;
        }

        bool IsYes(string st)
        {
            if (st == "N") return false;
            else
            {
                return true;
            }
        }

        void SetChanges()
        {
            _logFile.WriteLine("CostaNew : Применение изменений в базе");
            SqlCommand com = new SqlCommand("dbo.Costa",_connection);
            com.CommandType=CommandType.StoredProcedure;
            com.CommandTimeout = 1200;
            com.ExecuteNonQuery();
            _logFile.WriteLine("CostaNew : Применение изменений в базе закончено");
        }

        public Queue<Task> readTasksIntoQueue(String fileName)
        {
            XmlSerializer serializer = new XmlSerializer(typeof(List<Task>));
            FileStream stream = new FileStream(fileName, FileMode.Open);
            List<Task> list = (List<Task>)serializer.Deserialize(stream);
            stream.Close();
            Queue<Task> tasks = new Queue<Task>(list);
            return tasks;
        }

        public override void GetData()
        {
            String defaultQueueFileName = Path.Combine(Path.GetDirectoryName(Assembly.GetEntryAssembly().Location),
"tasksDefault.xml");
            String queueFileName = Path.Combine(Path.GetDirectoryName(Assembly.GetEntryAssembly().Location),
"tasks.xml");
            Queue<Task> tasks;
            if (File.Exists(queueFileName))
            {
                tasks = readTasksIntoQueue(queueFileName);
            }
            else
            {
                tasks = readTasksIntoQueue(defaultQueueFileName);
            }
            Task task = tasks.Dequeue();
            try
            {
                switch (task.Type)
                {
                    case "Fare":
                        GetPrice(task.Id);
                        break;
                    case "Itinerary":
                        _logFile.WriteLine(GetItineraryData());
                        break;
                    case "Ship":
                        _logFile.WriteLine(GetShips());
                        break;
                    case "ExportFare":
                        _logFile.WriteLine(GetExportFare());
                        break;
                    default:
                        break;
                }
                SetChanges();
            }
            catch (Exception e)
            {
                _logFile.WriteLine("CostaNew : закачка прервалась с ошибкой " + e.Message);
            }
            task.Date = DateTime.Now;
            tasks.Enqueue(task);
            List<Task> list = new List<Task>(tasks);
            XmlSerializer serializer =
            new XmlSerializer(list.GetType());
            StreamWriter writer = new StreamWriter(queueFileName);
            serializer.Serialize(writer, list);
            writer.Close();
        }
    }
}
