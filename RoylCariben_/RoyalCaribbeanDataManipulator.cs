using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Globalization;
using System.IO;
using System.Net;
using System.Reflection;
using System.Windows.Forms;
using BytesRoad.Net.Ftp;
using DxHelpersLib;
using System.Configuration;
using ICSharpCode.SharpZipLib.Zip;
using PluginInteractionLib;



namespace RoyalCaribbeanPlugin
{
    public class RoyalCaribbeanDataManipulator:DataManipulator
    {   string server = "", user = "", pass = "";
        int port;
        bool passiveMode = false;
        private const string insIten = @"INSERT INTO [dbo].[Rccl_Itinerary]
           ([package]
           ,[sailDate]
           ,[activityDate]
           ,[shipCode]
           ,[Sub_reg_code]
           ,[Reg_code]
           ,[Dep_port_code]
           ,[itinerary]
           ,[Itinerary_ef_date]
           ,[sailingFlag]
           ,[portion]
           ,[locCode]
           ,[locName]
           ,[activity]
           ,[arrival]
           ,[departure]
           ,[BrandCode]
           ,[NumberOfDays])
     VALUES
           (@package
           ,@sailDate
           ,@activityDate
           ,@shipCode
           ,@Sub_reg_code
           ,@Reg_code
           ,@Dep_port_code
           ,@itinerary
           ,@Itinerary_ef_date
           ,@sailingFlag
           ,@portion
           ,@locCode
           ,@locName
           ,@activity
           ,@arrival
           ,@departure
           ,@BrandCode
           ,@NumberOfDays )";
        const string insPrice = @"INSERT INTO [dbo].[Rccl_price]
           ([Package]
           ,[Sail_date]
           ,[fare_code]
           ,[roomCategory]
           ,[priceEfective]
           ,[priceEnd]
           ,[brandCode]
           ,[shipCode]
           ,[Dep_port_code]
           ,[Sub_reg_code]
           ,[Reg_code]
           ,[Prom_class_type]
           ,[Prom_qu_eli_type]
           ,[Flag_qua]
           ,[List_Criteria]
           ,[Sup_stateroom_type]
           ,[stateroom_code_type]
           ,[triple_quad_flag]
           ,[guarantee_flag]
           ,[sailing_flag]
           ,[Pack_description]
           ,[fare_description]
           ,[con_price_flag]
           ,[Guest_1]
           ,[Guest_2]
           ,[Guest_3]
           ,[Guest_4]
           ,[Child]
           ,[Infant]
           ,[Single]
           ,[Guest_1_2_grat]
           ,[Guest_3_4_grat]
           ,[Child_grat]
           ,[Guest_1_non_com]
           ,[Guest_2_non_com]
           ,[Guest_3_non_com]
           ,[Guest_4_non_com]
           ,[Child_non_com]
           ,[Infant_non_com]
           ,[Single_non_com]
           ,[Taxes_and_fees]
           ,[access_cabin]
           ,[relase_access_cabin]
           ,[best_value_single]
           ,[best_value_double]
           ,[best_rate_single]
           ,[best_rate_double]
           ,[best_value_triple]
           ,[best_rate_triple]
           ,[best_value_quad]
           ,[best_rate_quad]
           ,[offer_type]
           ,[value_currency]
           ,[value_single]
           ,[value_guest1]
           ,[value_guest2]
           ,[value_guest3]
           ,[value_guest4]
           ,[value_child]
           ,[value_infant]
           ,[sequence_nuber])
     VALUES
           (@Package
           ,@Sail_date
           ,@fare_code
           ,@roomCategory
           ,@priceEfective
           ,@priceEnd
           ,@brandCode
           ,@shipCode
           ,@Dep_port_code
           ,@Sub_reg_code
           ,@Reg_code
           ,@Prom_class_type
           ,@Prom_qu_eli_type
           ,@Flag_qua
           ,@List_Criteria
           ,@Sup_stateroom_type
           ,@stateroom_code_type
           ,@triple_quad_flag
           ,@guarantee_flag
           ,@sailing_flag
           ,@Pack_description
           ,@fare_description
           ,@con_price_flag
           ,@Guest_1
           ,@Guest_2
           ,@Guest_3
           ,@Guest_4
           ,@Child
           ,@Infant
           ,@Single
           ,@Guest_1_2_grat
           ,@Guest_3_4_grat
           ,@Child_grat
           ,@Guest_1_non_com
           ,@Guest_2_non_com
           ,@Guest_3_non_com
           ,@Guest_4_non_com
           ,@Child_non_com
           ,@Infant_non_com
           ,@Single_non_com
           ,@Taxes_and_fees
           ,@access_cabin
           ,@relase_access_cabin
           ,@best_value_single
           ,@best_value_double
           ,@best_rate_single
           ,@best_rate_double
           ,@best_value_triple
           ,@best_rate_triple
           ,@best_value_quad
           ,@best_rate_quad
           ,@offer_type
           ,@value_currency
           ,@value_single
           ,@value_guest1
           ,@value_guest2
           ,@value_guest3
           ,@value_guest4
           ,@value_child
           ,@value_infant
           ,@sequence_nuber)";
        bool isYes(string st)
        {
            if (st=="N")
            {
                return false;
            }
            else
            {
                return true;
            }
        }
        private const int FTP_TIMEOUT = 60000;
        private FtpClient _ftpClient;
        public RoyalCaribbeanDataManipulator(SqlConnection con, Logger log) : base(con, log)
        {
            ExeConfigurationFileMap configMap = new ExeConfigurationFileMap();
            configMap.ExeConfigFilename = Path.Combine(Path.GetDirectoryName(Assembly.GetEntryAssembly().Location),
                                                       "lanta.sqlconfig.dll.config");
            Configuration config = ConfigurationManager.OpenMappedExeConfiguration(configMap,
                                                                                   ConfigurationUserLevel.None);
            server = config.AppSettings.Settings["rcFtpServer"].Value;
            user = config.AppSettings.Settings["rcFtpUser"].Value;
            pass = config.AppSettings.Settings["rcFtpPass"].Value;
            port = int.Parse(config.AppSettings.Settings["rcFtpPort"].Value);
    
            _ftpClient = GetFtpClient();
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
        private void CopyFiles()
        {
            //string itiUrl = @"ftps://" + server + ":" + port.ToString() + "/itinerary/itinerary.txt";
            //Uri itiUri = new Uri("ftp://" + server + ":" + port.ToString() + "/itinerary/itinerary.txt");
            //FtpWebRequest itinarFtpReq = (FtpWebRequest)FtpWebRequest.Create(itiUri);
            //if (!Directory.Exists(filePath))
            //{
            //    Directory.CreateDirectory(filePath);
            //}
            //FileStream ItioutputStream = new FileStream(filePath + @"\" + "itinerary.txt",FileMode.Create);
            //Stream itinftpStream = null;
            //FtpWebResponse itinresponse = null;
            //itinarFtpReq.Method = WebRequestMethods.Ftp.DownloadFile;
            //itinarFtpReq.UseBinary = true;
            //itinarFtpReq.EnableSsl = true;
            //itinarFtpReq.Credentials = new NetworkCredential(user,pass);
            //_logFile.WriteLine("RC : itinerary.txt Начало закачки");
            //itinarFtpReq.Timeout = 600000;
            //itinresponse = (FtpWebResponse)itinarFtpReq.GetResponse();
            //itinftpStream = itinresponse.GetResponseStream();
            //StreamReader sr = new StreamReader(itinftpStream);
            //string itin = sr.ReadToEnd();
           // Console.WriteLine(itin);
            //long cl = itinresponse.ContentLength;
            //int bufferSize = 4096;
            //int readCount;
            //byte[] buffer = new byte[bufferSize];
            
           // readCount = itinftpStream.Read(buffer, 0, bufferSize);
           // while (readCount > 0)
           //{
           //    ItioutputStream.Write(buffer, 0, readCount);
           //    readCount = itinftpStream.Read(buffer, 0, bufferSize);
           // }  


            //--------------------------------------------------------------------------------------
            //FtpWebRequest priceFtpReq = (FtpWebRequest)FtpWebRequest.Create(new Uri(@"ftps://" + server + ":" + port.ToString() + @"MIA_UKR_USD/MIA_UKR_USD_cruise_no_air_price.zip"));
            //FileStream priceoutputStream = new FileStream(filePath + @"\" + "MIA_UKR_USD_cruise_no_air_price.zip", FileMode.Create);
            //Stream priceftpStream = null;
            //FtpWebResponse priceresponse = null;
            //priceFtpReq.Method = WebRequestMethods.Ftp.DownloadFile;
            //priceFtpReq.UseBinary = true;
            //priceFtpReq.EnableSsl = true;
            //priceFtpReq.Credentials = new NetworkCredential(user, pass);
            //_logFile.WriteLine("RC : MIA_UKR_USD_cruise_no_air_price.zip Начало закачки");


            //priceresponse = (FtpWebResponse)priceFtpReq.GetResponse();
            //priceftpStream = priceresponse.GetResponseStream();
            // cl = priceresponse.ContentLength;
            // bufferSize = 2048;
          
            // buffer = new byte[bufferSize];

            //readCount = priceftpStream.Read(buffer, 0, bufferSize);
            //while (readCount > 0)
            //{
            //    priceoutputStream.Write(buffer, 0, readCount);
            //    readCount = priceftpStream.Read(buffer, 0, bufferSize);
            //}
        }
        public override string GetItineraryData()
        {
            try
            {

           
//            _ftpClient.Connect(FTP_TIMEOUT, server, port);
//            _logFile.WriteLine("RC: Ftp подключен");
//            _ftpClient.Login(FTP_TIMEOUT, user, pass);
//            _logFile.WriteLine("RC: Ftp вход выполнен");
//#if DEBUG
//            byte[] itenerary = _ftpClient.GetFile(60000,"itinerary/ITINERARY_Test.txt");
//#else
//            byte[] itenerary = _ftpClient.GetFile(60000, "itinerary/itinerary.txt");
//#endif
//                if (itenerary == null)
//                {
//                    _logFile.WriteLine("RC:Отсутствуют маршруты на ftp");
//                    new System.Net.Mail.SmtpClient("mail.mcruises.ru").Send("errorreport@mcruises.ru",
//                                                                                   "tech_error@mcruises.ru", "RC","RC:Отсутствуют маршруты на ftp");

//                    return "RC:Маршруты не получны";
//                }
//              
//            _ftpClient.Disconnect(FTP_TIMEOUT);
//            Stream iten = new MemoryStream(itenerary);


            //------------------------------------  

            Uri itiUri = new Uri("ftp://" + server + ":" + port.ToString() + "/itinerary/itinerary.txt");
            FtpWebRequest itinarFtpReq = (FtpWebRequest)FtpWebRequest.Create(itiUri);
            Stream itinftpStream = null;
            FtpWebResponse itinresponse = null;
            itinarFtpReq.Method = WebRequestMethods.Ftp.DownloadFile;
            itinarFtpReq.UseBinary = true;
            itinarFtpReq.EnableSsl = true;
            itinarFtpReq.Credentials = new NetworkCredential(user, pass);
            _logFile.WriteLine("RC : itinerary.txt Начало закачки");
            itinarFtpReq.Timeout = 600000;
            itinresponse = (FtpWebResponse)itinarFtpReq.GetResponse();
            itinftpStream = itinresponse.GetResponseStream();
            //--------------------------------------

            SqlCommand delCom = new SqlCommand("Delete from Rccl_Itinerary ", _connection);
            delCom.ExecuteNonQuery();   
            int cout = 0;
            using (StreamReader sr = new StreamReader(itinftpStream))
            {
                var t = sr.ReadToEnd();
                   
                    var items = t.Split(new[] {'\n'});
                for (int i = 1; i < items.Length - 1; i++)
                {


                    var s = items[i].Split(new char[] {'|'});
                    using (SqlCommand com = new SqlCommand(insIten, _connection))
                    {
                        com.Parameters.AddWithValue("@package",s[0]);
                        com.Parameters.AddWithValue("@sailDate", Convert.ToDateTime(s[1], new CultureInfo("en-US")));
                        if (s[12] == string.Empty || s[17] == string.Empty || s[2] == "01/01/0001") continue;
                        if(s[2]!=string.Empty)
                        com.Parameters.AddWithValue("@activityDate", Convert.ToDateTime(s[2], new CultureInfo("en-US")));
                        else
                        {
                            com.Parameters.AddWithValue("@activityDate", DBNull.Value);
                        }
                        com.Parameters.AddWithValue("@shipCode", s[3]);
                        com.Parameters.AddWithValue("@Sub_reg_code", s[4]);
                        com.Parameters.AddWithValue("@Reg_code", s[5]);
                        com.Parameters.AddWithValue("@Dep_port_code", s[6]);
                        com.Parameters.AddWithValue("@itinerary", s[7]);
                        if (s[8] == string.Empty)
                        {
                            com.Parameters.AddWithValue("@Itinerary_ef_date", DBNull.Value);
                        }
                        else
                        {
                            com.Parameters.AddWithValue("@Itinerary_ef_date", Convert.ToDateTime(s[8], new CultureInfo("en-US"))); 
                        }
                        
                        com.Parameters.AddWithValue("@sailingFlag", isYes(s[9]));
                        com.Parameters.AddWithValue("@portion", s[10]);
                        com.Parameters.AddWithValue("@locCode", s[11]);
                        com.Parameters.AddWithValue("@locName", s[12]);
                        com.Parameters.AddWithValue("@activity", s[13]);
                        com.Parameters.AddWithValue("@arrival", s[14]);
                        com.Parameters.AddWithValue("@departure", s[15]);
                        com.Parameters.AddWithValue("@BrandCode", s[16]);
                        com.Parameters.AddWithValue("@NumberOfDays",Convert.ToInt32( s[17]));
                        com.ExecuteNonQuery();
                    }
                }
            }
            
            _logFile.WriteLine("RC: Маршруты загружены");
            return "RC: Машруты получены : " + cout.ToString();
            
            }
            catch (Exception ex)
            {

                _logFile.WriteLine(
                   string.Format("RC: Ошибка при получении маршрутов \n Exception:{0}\nInnerException:{1}\nStackTrace:{2}",
                                 ex.Message, ex.InnerException, ex.StackTrace));
                new System.Net.Mail.SmtpClient("mail.mcruises.ru").Send("errorreport@mcruises.ru",
                                                                                   "tech_error@mcruises.ru", "RC",
                                                                                   string.Format(
                       "RC: Ошибка при получении маршрутов, причина: {0}\n",
                       ex.Message));
                throw;
                return "RC:Ошибка в получение маршрутов";

            }
            
        }

        public override void GetData()
        {
            GetPricing();
            GetItineraryData();
           // CopyFiles();
            SetChanges();

        }
        void SetChanges()
        {
            _logFile.WriteLine("RC:Применение изменений в базе данных");
            //Вызов хранимой процедура
            SqlCommand com = new SqlCommand("dbo.RCCL",_connection);
            com.CommandTimeout = 1200;
            com.CommandType = CommandType.StoredProcedure;
            com.ExecuteNonQuery();
            _logFile.WriteLine("RC:Применение изменений в базе данных закончено");
        }
        void GetPricing()
        {


            try
            {
                //_ftpClient.Connect(FTP_TIMEOUT, server, port);
                //_logFile.WriteLine("RC: Ftp подключен");
                //_ftpClient.Login(FTP_TIMEOUT, user, pass);
                //_logFile.WriteLine("RC: Ftp вход выполнен");
                //byte[] pricing = _ftpClient.GetFile(600000, "MIA_UKR_USD/MIA_UKR_USD_cruise_no_air_price.zip");
                //if (pricing == null)
                //{
                //    _logFile.WriteLine("RC:Отсутствуют цены на ftp");
                //    new System.Net.Mail.SmtpClient("mail.mcruises.ru").Send("errorreport@mcruises.ru",
                //                                                            "tech_error@mcruises.ru", "RC",
                //                                                            "RC:Отсутствуют цены на ftp");

                //    return;
                //}
                //_ftpClient.Disconnect(FTP_TIMEOUT);
                //--------------------------------------------------------------------
                //------------------------------------
//MIA_RUE_USD_cruise_no_air_price.zip Начало закачки
//EP1_RUE_EUR_cruise_no_air_price.zip Начало закачки


                //Uri PriceUri = new Uri("ftp://" + server + ":" + port.ToString() + "/MIA_UKR_USD/MIA_UKR_USD_cruise_no_air_price.zip");
                Uri PriceUri = new Uri("ftp://" + server + ":" + port.ToString() + "/MIA_RUE_USD/MIA_RUE_USD_cruise_no_air_price.zip");
                FtpWebRequest PriceFtpReq = (FtpWebRequest)FtpWebRequest.Create(PriceUri);
                Stream priceftpStream = null;
                FtpWebResponse priceresponse = null;
                PriceFtpReq.Method = WebRequestMethods.Ftp.DownloadFile;
                PriceFtpReq.UseBinary = true;
                PriceFtpReq.EnableSsl = true;
                PriceFtpReq.Credentials = new NetworkCredential(user, pass);
                _logFile.WriteLine("RC : MIA_RUE_USD_cruise_no_air_price.zip Начало закачки");
                PriceFtpReq.Timeout = 600000;
                priceresponse = (FtpWebResponse)PriceFtpReq.GetResponse();
                priceftpStream = priceresponse.GetResponseStream();
                //Stream stream = new MemoryStream();

              //  byte[] dataPrice= 
                //--------------------------------------
                //--------------------------------------------------------------------

                Stream priceRCCL = UnpackZip(priceftpStream);
                SqlCommand delCom = new SqlCommand("Delete from Rccl_price ",_connection);
                delCom.ExecuteNonQuery();
                
                using (StreamReader sr = new StreamReader(priceRCCL))
                {
                    var t = sr.ReadToEnd();
                   
                    var items = t.Split(new[] {'\n'});
                    for(int i=1 ; i<items.Length-1;i++)
                    {
                      

                        var s = items[i].Split(new char[] {'|'});
                        using (SqlCommand com = new SqlCommand(insPrice,_connection))
                        {

                            com.Parameters.AddWithValue("@Package", s[0]);
                            com.Parameters.AddWithValue("@Sail_date", Convert.ToDateTime(s[1], new CultureInfo("en-US")));
                            com.Parameters.AddWithValue("@fare_code", s[2]);
                            com.Parameters.AddWithValue("@roomCategory", s[3]);
                            com.Parameters.AddWithValue("@priceEfective",
                                                        Convert.ToDateTime(s[4] + " " + s[5], new CultureInfo("en-US")));
                            com.Parameters.AddWithValue("@PriceEnd",
                                                        Convert.ToDateTime(s[6] + " " + s[7], new CultureInfo("en-US")));
                            com.Parameters.AddWithValue("@brandCode", s[8]);
                            com.Parameters.AddWithValue("@shipCode", s[9]);
                            com.Parameters.AddWithValue("@Dep_port_code", s[10]);
                            com.Parameters.AddWithValue("@Sub_reg_code", s[11]);
                            com.Parameters.AddWithValue("@Reg_code", s[12]);
                            com.Parameters.AddWithValue("@Prom_class_type", s[13]);
                            com.Parameters.AddWithValue("@Prom_qu_eli_type", s[14]);
                            com.Parameters.AddWithValue("@Flag_qua", isYes(s[15]));
                            com.Parameters.AddWithValue("@List_Criteria", s[16]);
                            com.Parameters.AddWithValue("@Sup_stateroom_type", s[17]);
                            com.Parameters.AddWithValue("@stateroom_code_type", s[18]);
                            com.Parameters.AddWithValue("@triple_quad_flag", isYes(s[19]));
                            com.Parameters.AddWithValue("@guarantee_flag", isYes(s[20]));
                            com.Parameters.AddWithValue("@sailing_flag", isYes(s[21]));
                            com.Parameters.AddWithValue("@Pack_description", s[22]);
                            com.Parameters.AddWithValue("@fare_description", s[23]);
                            com.Parameters.AddWithValue("@con_price_flag", isYes(s[24]));
                            com.Parameters.AddWithValue("@Guest_1", Convert.ToDecimal(s[25].Replace('.', ',')));
                            com.Parameters.AddWithValue("@Guest_2", Convert.ToDecimal(s[26].Replace('.', ',')));
                            com.Parameters.AddWithValue("@Guest_3", Convert.ToDecimal(s[27].Replace('.', ',')));
                            com.Parameters.AddWithValue("@Guest_4", Convert.ToDecimal(s[28].Replace('.', ',')));
                            com.Parameters.AddWithValue("@Child", Convert.ToDecimal(s[29].Replace('.', ',')));
                            com.Parameters.AddWithValue("@Infant", Convert.ToDecimal(s[30].Replace('.', ',')));
                            com.Parameters.AddWithValue("@Single", Convert.ToDecimal(s[31].Replace('.', ',')));
                            com.Parameters.AddWithValue("@Guest_1_2_grat", Convert.ToDecimal(s[32].Replace('.', ',')));
                            com.Parameters.AddWithValue("@Guest_3_4_grat", Convert.ToDecimal(s[33].Replace('.', ',')));
                            com.Parameters.AddWithValue("@Child_grat", Convert.ToDecimal(s[34].Replace('.', ',')));
                            com.Parameters.AddWithValue("@Guest_1_non_com", Convert.ToDecimal(s[35].Replace('.', ',')));
                            com.Parameters.AddWithValue("@Guest_2_non_com", Convert.ToDecimal(s[36].Replace('.', ',')));
                            com.Parameters.AddWithValue("@Guest_3_non_com", Convert.ToDecimal(s[37].Replace('.', ',')));
                            com.Parameters.AddWithValue("@Guest_4_non_com", Convert.ToDecimal(s[38].Replace('.', ',')));
                            com.Parameters.AddWithValue("@Child_non_com", Convert.ToDecimal(s[39].Replace('.', ',')));
                            com.Parameters.AddWithValue("@Infant_non_com", Convert.ToDecimal(s[40].Replace('.', ',')));
                            com.Parameters.AddWithValue("@Single_non_com", Convert.ToDecimal(s[41].Replace('.', ',')));
                            com.Parameters.AddWithValue("@Taxes_and_fees", Convert.ToDecimal(s[42].Replace('.', ',')));
                            com.Parameters.AddWithValue("@access_cabin", isYes(s[43]));
                            com.Parameters.AddWithValue("@relase_access_cabin", isYes(s[44]));
                            com.Parameters.AddWithValue("@best_value_single", isYes(s[45]));
                            com.Parameters.AddWithValue("@best_value_double", isYes(s[46]));
                            com.Parameters.AddWithValue("@best_rate_single", isYes(s[47]));
                            com.Parameters.AddWithValue("@best_rate_double", isYes(s[48]));
                            com.Parameters.AddWithValue("@best_value_triple", isYes(s[49]));
                            com.Parameters.AddWithValue("@best_rate_triple", isYes(s[50]));
                            com.Parameters.AddWithValue("@best_value_quad", isYes(s[51]));
                            com.Parameters.AddWithValue("@best_rate_quad", isYes(s[52]));
                            com.Parameters.AddWithValue("@offer_type", s[53]);
                            com.Parameters.AddWithValue("@value_currency", s[54]);
                            com.Parameters.AddWithValue("@value_single", s[55]);
                            com.Parameters.AddWithValue("@value_guest1", s[56]);
                            com.Parameters.AddWithValue("@value_guest2", s[57]);
                            com.Parameters.AddWithValue("@value_guest3", s[58]);
                            com.Parameters.AddWithValue("@value_guest4", s[59]);
                            com.Parameters.AddWithValue("@value_child", s[60]);
                            com.Parameters.AddWithValue("@value_infant", s[61]);
                            com.Parameters.AddWithValue("@sequence_nuber", Convert.ToInt32(s[62]));
                            com.ExecuteNonQuery();


                        }

                    }
                }
                _logFile.WriteLine("RC: Цены закачены");

            }
            catch (Exception ex)
            {
                
                _logFile.WriteLine(
                    string.Format("RC: Ошибка при получении цен \n Exception:{0}\nInnerException:{1}\nStackTrace:{2}",
                                  ex.Message, ex.InnerException, ex.StackTrace));
                new System.Net.Mail.SmtpClient("mail.mcruises.ru").Send("errorreport@mcruises.ru",
                                                                                   "tech_error@mcruises.ru", "RC",
                                                                                   string.Format(
                       "RC: Ошибка при получении цен, причина: {0}\n",
                       ex.Message));
                throw;
            }
        }


        private FtpClient GetFtpClient()
        {

            
            
            try
            {
                ExeConfigurationFileMap configMap = new ExeConfigurationFileMap();
                configMap.ExeConfigFilename = Path.Combine(Path.GetDirectoryName(Assembly.GetEntryAssembly().Location),
                                                           "lanta.sqlconfig.dll.config");
                Configuration config = ConfigurationManager.OpenMappedExeConfiguration(configMap,
                                                                                       ConfigurationUserLevel.None);
                server = config.AppSettings.Settings["rcFtpServer"].Value;
                user = config.AppSettings.Settings["rcFtpUser"].Value;
                pass = config.AppSettings.Settings["rcFtpPass"].Value;
                port = int.Parse(config.AppSettings.Settings["rcFtpPort"].Value);
                _logFile.WriteLine(string.Format("RC: подключение к Ftp {0}@{1}", user, server));
                FtpClient ftpClient = new FtpClient();
                passiveMode = isYes(config.AppSettings.Settings["ftpPassiveMode"].Value);
                ftpClient.PassiveMode = passiveMode;
                
                
                return ftpClient;
            }
            catch (Exception)
            {
                _logFile.WriteLine(string.Format("RC: Подключение к ftp не удалось.\n Сервер:{0}, Пользователь:{1}", server, user));
                return null;
            }
        }


        Stream UnpackZip(Stream zipStream)
        {
            //Stream f = File.Create("c:\\1.txt");
            //long im = 0;
            //StreamWriter wr = new StreamWriter(f);
          
            using (ZipInputStream zipInputStream = new ZipInputStream(zipStream))
            {
                ZipEntry entry = zipInputStream.GetNextEntry();
                try
                {
                    MemoryStream result = new MemoryStream();
                    int size;

                    byte[] buffer = new byte[4096];
                    DateTime begin = DateTime.Now;
                    do
                    {
                        size = zipInputStream.Read(buffer, 0, buffer.Length);
                        //im +=size;
                        //wr.WriteLine(im.ToString());
                        if (begin.AddMinutes(30) < DateTime.Now)
                        {
                            new System.Net.Mail.SmtpClient("mail.mcruises.ru").Send("errorreport@mcruises.ru",
                                                                                    "tech_error@mcruises.ru", "RC",
                                                                                    string.Format(
                                                                                        "RC: Произошло зависание программы при распаковке прайсов"));
                            return null;
                        }
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

        Stream UnpackZip(byte[] zipByteArray)
        {
            //Stream f = File.Create("c:\\1.txt");
            //long im = 0;
            //StreamWriter wr = new StreamWriter(f);
            Stream zipStream = new MemoryStream(zipByteArray);
            using (ZipInputStream zipInputStream = new ZipInputStream(zipStream))
            {
                ZipEntry entry = zipInputStream.GetNextEntry();
                try
                {
                    MemoryStream result = new MemoryStream();
                    int size;
                 
                    byte[] buffer = new byte[4096];
                    DateTime begin = DateTime.Now;
                    do
                    {
                        size = zipInputStream.Read(buffer, 0, buffer.Length);
                        //im +=size;
                        //wr.WriteLine(im.ToString());
                        if (begin.AddMinutes(30) < DateTime.Now)
                        {
                            new System.Net.Mail.SmtpClient("mail.mcruises.ru").Send("errorreport@mcruises.ru",
                                                                                    "tech_error@mcruises.ru", "RC",
                                                                                    string.Format(
                                                                                        "RC: Произошло зависание программы при распаковке прайсов"));
                            return null;
                        }
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
    }
}