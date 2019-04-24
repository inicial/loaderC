using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Net;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using DxHelpersLib;
using PluginInteractionLib;
using StPeterLineLoader.com.stpeterline.spl_test;


//using StPeterLineLoader.StPeterLineSoap;

namespace StPeterLineLoader
{

    class stPeterLineDataManipulator:DataManipulator
    {
        private const string insertCruisePrice = @"INSERT INTO [dbo].[stPeterLine_CruisePrices]
           ([cruise_id]
           ,[CabinCategory]
           ,[2adult]
           ,[3adult]
           ,[2adult1child]
           ,[single]
           ,[curency])
     VALUES
           (@cruise_id
           ,@CabinCategory
           ,@2adult
           ,@3adult
           ,@2adult1child
           ,@single
           ,@curency)";
        private const string insertCruise = @"INSERT INTO [dbo].[stPeterLine_Cruises]
           ([id]
           ,[ship]
           ,[FromDate]
           ,[FromPort]
           ,[FromPier]
           ,[ToDate]
           ,[ToPort]
           ,[ToPier]
           ,[PakCode])
     VALUES
           (@id
           ,@ship
           ,@FromDate
           ,@FromPort
           ,@FromPier
           ,@ToDate
           ,@ToPort
           ,@ToPier
           ,@PakCode)";
        private SqlConnection _connection;
        private Logger _log;
        private string _sessionGUID = "";
        private string _resId = "";
        private MsgHeader _header;
        private string user = "AGENT1";
        private string password = "XXX";
        const CountryEnum Country = CountryEnum.RU;
        const LanguageEnum Language = LanguageEnum.en;
        
        public stPeterLineDataManipulator(SqlConnection con, Logger log) : base(con, log)
        {
            _connection = con;
            _log = log;
            ExeConfigurationFileMap configMap = new ExeConfigurationFileMap();
            configMap.ExeConfigFilename = Path.Combine(Path.GetDirectoryName(Assembly.GetEntryAssembly().Location),
                                                       "lanta.sqlconfig.dll.config");
            Configuration config = ConfigurationManager.OpenMappedExeConfiguration(configMap,
                                                                                   ConfigurationUserLevel.None);
            user = config.AppSettings.Settings["StPeterLineUser"].Value;
            password = config.AppSettings.Settings["StPeterLinePassward"].Value;
           
            
        }
        void GetPromoCodes()
        {

            DataTable dt = new DataTable();
            List<CruiseCabinPrice> priceList = new List<CruiseCabinPrice>();
            _log.WriteLine("St.PeterLine : Начало загрузки промо-кодов");
#if DEBUG
            using (SqlDataAdapter adapter = new SqlDataAdapter("SELECT distinct top 10 id from stPeterLine_Cruises", _connection))
            {
                adapter.Fill(dt);
            }
#else
            using (SqlDataAdapter adapter = new SqlDataAdapter("SELECT distinct id from stPeterLine_Cruises", _connection))
            {
                adapter.Fill(dt);
            }
#endif


            foreach (DataRow row in dt.Rows)
            {
                com.stpeterline.spl_test.VXAPIService client = new VXAPIService();


                GetAvailablePromotionsRequest promos = new GetAvailablePromotionsRequest();

                promos.CategoryCode = "B2";

                promos.PackageId = row.Field<string>("id");

                promos.MsgHeader = _header;



                var pr = client.GetAvailablePromotions(promos);
            }
            _log.WriteLine("St.PeterLine : Загрузка промо-кодов закончена");

        }

        void GetGUID()
        {


            com.stpeterline.spl_test.VXAPIService client = new VXAPIService();
            //OpenSessionRequest openSessionRequest = new OpenSessionRequest();
            //openSessionRequest.MsgHeader = new MsgHeader();
            //openSessionRequest.MsgHeader.CallerInfo = new CallerInfo();
            //openSessionRequest.MsgHeader.CallerInfo.UserInfo = new UserInfo();
            //openSessionRequest.MsgHeader.CallerInfo.UserInfo.Item = new UserInfoTravelAgent();
            //openSessionRequest.MsgHeader.CallerInfo.UserInfo.Item.Username = user;
            //openSessionRequest.MsgHeader.CallerInfo.UserInfo.Item.Password = password;
            //openSessionRequest.MsgHeader.CallerInfo.ExtSystemInfo = new ExternalSystemInfo();
            //openSessionRequest.MsgHeader.CallerInfo.ExtSystemInfo.ExternalSystemId = "PriceLoader";
            //openSessionRequest.MsgHeader.Country = Country;
            //openSessionRequest.MsgHeader.Language = Language;
            //OpenSessionResponse openSessionResponse= client.OpenSession(openSessionRequest);
           
            TravelAgentLoginRequest request = new TravelAgentLoginRequest();

            request.Username = user;
            request.Password = password;
            request.MsgHeader = new MsgHeader();
            request.MsgHeader.Country = Country;
            request.MsgHeader.Language = Language;
            TravelAgentLoginResponse response = client.TravelAgentLogin(request);
            _header = response.MsgHeader;
            _sessionGUID = response.MsgHeader.SessionGUID;



        }
        void CloseSession()
        {
            com.stpeterline.spl_test.VXAPIService client = new VXAPIService();
            CloseSessionRequest closeSession = new CloseSessionRequest();
            if (_header==null)return;
            closeSession.MsgHeader = _header;
            //closeSession.MsgHeader.Country = Country;
            //closeSession.MsgHeader.Language = Language;
            //closeSession.MsgHeader.SessionGUID = _sessionGUID;
            client.CloseSession(closeSession);
        }

        private string CreateReservation(int adult, int child)
        {
            com.stpeterline.spl_test.VXAPIService client = new VXAPIService();
            CreateReservationRequest request = new CreateReservationRequest();
            request.MsgHeader = _header;
            request.Adults = adult.ToString();
            if (child > 1)
            {
                request.Children = new AbstractCreateReservationRequestChild[child];
                foreach (AbstractCreateReservationRequestChild children in request.Children)
                {
                    children.Age = 10.ToString();
                }
            }
            request.LanguageCode = LanguageEnum.ru; 

            
            CreateReservationResponse response =  client.CreateReservation(request);
            return  response.Reservation.Id;

        }
        void GetCruise()
        {
            _log.WriteLine("St.PeterLine : Начало загрузки круизов");
            DataTable dt= new DataTable();
            DateTime dateFrom = DateTime.Now.Date.AddMonths(-1), dateTo =dateFrom.AddYears(3);
#if DEBUG
            dt.Columns.Add("Name", typeof(string));
            dt.Columns.Add("Code", typeof (string));
           // dt.Rows.Add("Tallinn", "TAL");
           // dt.Rows.Add("Helsinki", "HEL");
           // dt.Rows.Add("Stockholm", "STO");
            dt.Rows.Add("Saint Petersburg", "SPB");
           // dt.Rows.Add("Riga", "RIG");
           //dateTo = dateFrom.AddMonths(3);
#else
            using (SqlCommand com = new SqlCommand("Select name_en as Name,code from Seaports where id_crline =47",_connection))
            {
                SqlDataAdapter adapter = new SqlDataAdapter(com);
                adapter.Fill(dt);
            }
#endif
            
            using (SqlCommand com = new SqlCommand("delete from stPeterLine_Cruises",_connection))
            {
                com.ExecuteNonQuery();
            }
            while (dateTo > dateFrom)
            {
                foreach (DataRow row in dt.Rows)
                {
                    string cityCode = row["Code"].ToString();
                   //GetCruiseSailsResponseSail[] result1Adult = SeachCruise(dateFrom, dateFrom.AddMonths(1), 1, 0, cityCode);
                    GetCruiseSailsResponseSail[] result2Adult = SeachCruise(dateFrom, dateFrom.AddMonths(1), 2, 0, cityCode);
                    //GetCruiseSailsResponseSail[] result2Adult1Child = SeachCruise(dateFrom, dateFrom.AddMonths(1), 2, 1, cityCode);
                    //GetCruiseSailsResponseSail[] result3Adult = SeachCruise(dateFrom, dateFrom.AddMonths(1), 3, 0, cityCode);
                    if(result2Adult==null)
                        continue;
                    foreach (GetCruiseSailsResponseSail sail in result2Adult)
                    {
                       // Console.WriteLine(sail);
                        using (SqlCommand com = new SqlCommand(insertCruise,_connection))
                        {
                            com.Parameters.AddWithValue("@id", sail.SailPackage.Id);
                            com.Parameters.AddWithValue("@ship", sail.Ship.Value);
                            com.Parameters.AddWithValue("@FromDate", sail.From.DateTime);
                            com.Parameters.AddWithValue("@FromPort", sail.From.Port.Value);
                            com.Parameters.AddWithValue("@FromPier", sail.From.Pier.Value);
                            com.Parameters.AddWithValue("@ToDate", sail.To.DateTime);
                            com.Parameters.AddWithValue("@ToPort",  sail.To.Port.Value);
                            com.Parameters.AddWithValue("@ToPier", sail.To.Pier.Value);
                            com.Parameters.AddWithValue("@PakCode", sail.SailPackage.Code.Value);
                            //foreach (SqlParameter parameter in com.Parameters)
                            //{
                            //    parameter.DbType = DbType.String;
                            //}
                            //com.Parameters["@FromDate"].DbType = DbType.DateTime;
                            //com.Parameters["@ToDate"].DbType = DbType.DateTime;
                            com.ExecuteNonQuery();
                        }
                    }
                    //Console.WriteLine(result1Adult);
                    //Console.WriteLine(result2Adult);
                    //Console.WriteLine(result3Adult);
                    //Console.WriteLine(result2Adult1Child);
                }
                dateFrom = dateFrom.AddMonths(1);
            }
            _log.WriteLine("St.PeterLine : Загрузка крузов закончена");

        }
       
        GetCruiseSailsResponseSail[] SeachCruise(DateTime from, DateTime to, int adults, int child,string cityCode)
        {
            
            com.stpeterline.spl_test.VXAPIService client = new VXAPIService();
            GetCruiseSailsRequest request = new GetCruiseSailsRequest();
            request.MsgHeader = _header;
            request.ResId = CreateReservation(adults,child);
            request.PaymentMethod = PaymentMethodEnum.MONEY;
            request.Options = new SailSearchRequestOptions();
            request.Options.IncludePrices = true;
            request.Options.IncludeVehicles = false;
            request.Options.IncludePreviousDeparture = true;
            DateRange dateRange = new DateRange();
            dateRange.From = from;
            dateRange.To = to;
            request.DepartureCity = cityCode;
            request.DepartureDate = dateRange;
            GetCruiseSailsResponse response = client.GetCruiseSails(request);
            return response.Sails;
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

        void GetCruisePrice()
        {
            _log.WriteLine("St.PeterLine : Начало загрузки цен");
            DataTable dt = new DataTable();
            List<CruiseCabinPrice> priceList = new List<CruiseCabinPrice>();
#if DEBUG
            using (SqlDataAdapter adapter = new SqlDataAdapter("SELECT distinct top 10 id from stPeterLine_Cruises", _connection))
            {
                adapter.Fill(dt);
            }
#else
            using (SqlDataAdapter adapter = new SqlDataAdapter("SELECT distinct id from stPeterLine_Cruises",_connection))
            {
                adapter.Fill(dt);
            }
#endif

            foreach (DataRow row in dt.Rows)
            {

                string idCruise = row.Field<string>("id");
                
                com.stpeterline.spl_test.VXAPIService client = new VXAPIService();
                //GetCruisePriceListRequest request = new GetCruisePriceListRequest();
                GetAvailableTravelClassesRequest request = new GetAvailableTravelClassesRequest();
                request.MsgHeader = _header;
                request.ResId = CreateReservation(2, 0);

                request.PaymentMethod = PaymentMethodEnum.MONEY;
                request.PackageId = idCruise;

            //    request.Promo = "EARLY BOOKERS";
                request.Options = new GetAvailableTravelClassesRequestOptions();
                request.Options.AvailabilityMode =AvailabilityMode.GTY;
                request.Options.IncludePrices = true;
                request.Options.IncludeCabins = true;

                GetAvailableTravelClassesResponse response= client.GetAvailableTravelClasses(request);
                //request.ResId = CreateReservation(2, 0);
                //request.PaymentMethod = PaymentMethodEnum.MONEY;
                //request.ArrivalCity = "SPB";
                //request.DepartureCity = "SPB";
                //DateRange date = new DateRange();
                //date.From =new DateTime(2014,12,31);
                //date.To = date.From.AddMonths(1);
                //request.DepartureDate = date;
                //GetCruisePriceListResponse response= client.GetCruisePriceList(request);
                
                foreach (AvailTravelClassCategory availTravelClassCategory in response.TravelClassCategories)
                {
                    //2Adult
                    string cabinCat = availTravelClassCategory.CabinCategory.Value;
                    decimal price = availTravelClassCategory.Price.Money;
                    CruiseCabinPrice cabinPrice = priceList.Find(delegate(CruiseCabinPrice ccp)
                        {
                            return ccp.id_cruise == idCruise &&
                                   ccp.cabinCategory == cabinCat;
                        });
                    if (cabinPrice != null)
                    {
                        cabinPrice.twoAdult = price;
                    }
                    else
                    {
                        string currency = "";
                        if (response != null) currency = response.Currency.ToString();
                        try
                        {
                            priceList.Add(new CruiseCabinPrice(criseId: idCruise, cur: currency, category: cabinCat, Adult2: price));

                        }
                        catch (System.Exception ex)
                        {
                            string str = ex.Message;
                        }
                    }

                }
                request.ResId = CreateReservation(3,0);
                response = client.GetAvailableTravelClasses(request);
                foreach (AvailTravelClassCategory availTravelClassCategory in response.TravelClassCategories)
                {
                    //3Adult
                    string cabinCat = availTravelClassCategory.CabinCategory.Value;
                    decimal price = availTravelClassCategory.Price.Money;
                    CruiseCabinPrice cabinPrice = priceList.Find(delegate(CruiseCabinPrice ccp)
                    {
                        return ccp.id_cruise == idCruise &&
                               ccp.cabinCategory == cabinCat;
                    });
                    if (cabinPrice != null)
                    {
                        cabinPrice.threeAdult = price;
                    }
                    else
                    {
                        string currency = "";
                        if (response != null) currency = response.Currency.ToString();
                        priceList.Add(new CruiseCabinPrice(criseId: idCruise, cur: currency, category: cabinCat, Adult3: price));
                    }

                }
                request.ResId = CreateReservation(2, 1);
                response = client.GetAvailableTravelClasses(request);
                foreach (AvailTravelClassCategory availTravelClassCategory in response.TravelClassCategories)
                {
                    //2Adult1Child
                                        string cabinCat = availTravelClassCategory.CabinCategory.Value;
                    decimal price = availTravelClassCategory.Price.Money;
                    CruiseCabinPrice cabinPrice = priceList.Find(delegate(CruiseCabinPrice ccp)
                        {
                            return ccp.id_cruise == idCruise &&
                                   ccp.cabinCategory == cabinCat;
                        });
                    if (cabinPrice != null)
                    {
                        cabinPrice.twoAdultOneChild = price;
                    }
                    else
                    {
                        string currency = "";
                        if (response != null) currency = response.Currency.ToString();
                        priceList.Add(new CruiseCabinPrice(criseId: idCruise, cur: currency, category: cabinCat, child: price));
                    }

                }
                request.ResId = CreateReservation(1,0);
                response = client.GetAvailableTravelClasses(request);
                foreach (AvailTravelClassCategory availTravelClassCategory in response.TravelClassCategories)
                {
                    //1adult
                    string cabinCat = availTravelClassCategory.CabinCategory.Value;
                    decimal price = availTravelClassCategory.Price.Money;
                    CruiseCabinPrice cabinPrice = priceList.Find(delegate(CruiseCabinPrice ccp)
                    {
                        return ccp.id_cruise == idCruise &&
                               ccp.cabinCategory == cabinCat;
                    });
                    if (cabinPrice != null)
                    {
                        cabinPrice.single = price;
                    }
                    else
                    {
                        string currency = "";
                        if (response != null) currency = response.Currency.ToString();
                        priceList.Add(new CruiseCabinPrice(criseId: idCruise, cur: currency, category: cabinCat, oneAdult: price));
                    }

                }
                
            }
            _log.WriteLine("St.PeterLine : Загрузка цен в БД");
            using (SqlCommand com = new SqlCommand("delete from stPeterLine_CruisePrices", _connection))
            {
                com.ExecuteNonQuery();
            }
            foreach (CruiseCabinPrice cruiseCabinPrice in priceList)
            {
                using (SqlCommand com = new SqlCommand(insertCruisePrice,_connection))
                {
                    com.Parameters.AddWithValue("@cruise_id", cruiseCabinPrice.id_cruise);
                    com.Parameters.AddWithValue("@CabinCategory", cruiseCabinPrice.cabinCategory);
                    com.Parameters.AddWithValue("@2adult", cruiseCabinPrice.twoAdult);
                    com.Parameters.AddWithValue("@3adult", cruiseCabinPrice.threeAdult);
                    com.Parameters.AddWithValue("@2adult1child", cruiseCabinPrice.twoAdultOneChild);
                    com.Parameters.AddWithValue("@single", cruiseCabinPrice.single);
                    com.Parameters.AddWithValue("@curency", cruiseCabinPrice.curency);

                    try
                    {
                        com.ExecuteNonQuery();
                    }
                    catch (System.Exception ex)
                    {
                        string msg = "PRICES FOR CRUISE " + cruiseCabinPrice.id_cruise + ": ";
                        msg = msg + "CabinCategory=" + cruiseCabinPrice.cabinCategory;
                        msg = msg + ", 2adult=" + cruiseCabinPrice.twoAdult.ToString();
                        msg = msg + ", 3adult=" + cruiseCabinPrice.threeAdult.ToString();
                        msg = msg + ", 2adult1child=" + cruiseCabinPrice.twoAdultOneChild.ToString();
                        msg = msg + ", single=" + cruiseCabinPrice.single.ToString();
                        msg = msg + ", curency=" + cruiseCabinPrice.curency;
                        _log.WriteLine("GetCruisePrice ERROR: " + ex.Message + " >> " + msg);
                    }
                }
            }
            _log.WriteLine("St.PeterLine : Загрузки цен закончена");
        }
        public override string GetItineraryData()
        {
            _log.WriteLine("St.PeterLine : Начало загрузки маршрутов");

            DataTable dt = new DataTable();
            List<CruiseCabinPrice> priceList = new List<CruiseCabinPrice>();
#if DEBUG
            using (SqlDataAdapter adapter = new SqlDataAdapter("SELECT distinct top 10 id from stPeterLine_Cruises", _connection))
            {
                adapter.Fill(dt);
            }
#else
            using (SqlDataAdapter adapter = new SqlDataAdapter("SELECT distinct id from stPeterLine_Cruises",_connection))
            {
                adapter.Fill(dt);
            }
#endif
            
            
            com.stpeterline.spl_test.VXAPIService client = new VXAPIService();
            GetCitiesRequest request = new GetCitiesRequest();
            request.MsgHeader = _header;
            GetCitiesResponse response = client.GetCities(request);

            _log.WriteLine("St.PeterLine : Загрузка маршрутов окончена");
            return "";
        }

        class CruiseCabinPrice
        {
            public string id_cruise;
            public string cabinCategory;
            public string curency;
            public decimal? twoAdult = null;
            public decimal? threeAdult = null;
            public decimal? twoAdultOneChild = null;
            public decimal? single = null;
            public CruiseCabinPrice(string criseId, string category,string cur, decimal? Adult2 = null, decimal? Adult3 = null, decimal? child = null,decimal? oneAdult =null)
            {
                id_cruise = criseId;
                cabinCategory = category;
                twoAdult = Adult2;
                threeAdult = Adult3;
                twoAdultOneChild = child;
                curency = cur;
                single = oneAdult;
            }
        }
        void SumbitChanges()
        {
            _logFile.WriteLine("St.PeterLine : Применение изменений в базе ");
            using (SqlCommand com = new SqlCommand("dbo.StPeterLine",_connection))
            {
                com.CommandType = CommandType.StoredProcedure;
                com.ExecuteNonQuery();
            }
            _logFile.WriteLine("St.PeterLine : Применение изменений в базе закончено");
        }
        public override void GetData()
        {
            try
            {
                GetGUID();
                GetCruise();
                GetItineraryData();
                GetCruisePrice();
                GetPromoCodes();
                SumbitChanges();
            }
            catch (Exception Ex)
            {
                _logFile.WriteLine("St.PeterLine : Произошла ошибка " + Ex.Message + " StackTrace : " + Ex.StackTrace);
            }

            try
            {
                CloseSession();
            }
            catch (Exception Ex)
            {
                _logFile.WriteLine("St.PeterLine.CloseSession : Произошла ошибка " + Ex.Message);
                //throw;
            }
        }
    }
}
