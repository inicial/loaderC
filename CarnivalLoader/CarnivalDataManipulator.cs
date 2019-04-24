using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Reflection;
using System.Text;
using System.Xml;
using BytesRoad.Net.Ftp;
using DxHelpersLib;
using PluginInteractionLib;

namespace CarnivalLoader
{
    class CarnivalDataManipulator:DataManipulator
    {
        private SqlConnection _connection;
        private Logger _log;
        string server = "", user = "", pass = "";
        int port =21;
        bool passiveMode = false;

        private const int FTP_TIMEOUT = 600000;
        private FtpClient _ftpClient;


        private const String insertPort = @"INSERT INTO [dbo].[Carnival_ports]
           ([RES_PORT_ID]
           ,[RES_PORT_CODE]
           ,[PORT_NAME]
           ,[STATUS]
           ,[STATUS_DATE]
           ,[IATA_XREF_CODE]
           ,[PORT_ID]
           ,[LAST_ACTIVITY_TYPE])
     VALUES
           (@RES_PORT_ID
           ,@RES_PORT_CODE
           ,@PORT_NAME
           ,@STATUS
           ,@STATUS_DATE
           ,@IATA_XREF_CODE
           ,@PORT_ID
           ,@LAST_ACTIVITY_TYPE)
",
            insertItenDtl = @"INSERT INTO [dbo].[Carnival_iten_dtl]
           ([ITIN_DTL_ID]
           ,[DAY_ABBREV]
           ,[DAY_NUMBER]
           ,[ARRIVAL_TIME]
           ,[DEPART_TIME]
           ,[DAY_SORT_VALUE]
           ,[ITIN_ID]
           ,[STATUS]
           ,[STATUS_DATE]
           ,[LAST_ACTIVITY_TYPE]
           ,[ITIN_PORT_ID])
     VALUES
           (@ITIN_DTL_ID
           ,@DAY_ABBREV
           ,@DAY_NUMBER
           ,@ARRIVAL_TIME
           ,@DEPART_TIME
           ,@DAY_SORT_VALUE
           ,@ITIN_ID
           ,@STATUS
           ,@STATUS_DATE
           ,@LAST_ACTIVITY_TYPE
           ,@ITIN_PORT_ID)",
            insertIten = @"INSERT INTO [dbo].[Carnival_itinary]
           ([ITIN_ID]
           ,[ITIN_CODE]
           ,[BEGIN_TIME]
           ,[END_TIME]
           ,[EMBK_PIER_NAME]
           ,[EMBARK_PORT_ID]
           ,[DUR_DAYS]
           ,[SUB_REGION_ID]
           ,[STATUS_DATE]
           ,[STATUS]
           ,[LAST_ACTIVITY_TYPE])
     VALUES
           (@ITIN_ID
           ,@ITIN_CODE
           ,@BEGIN_TIME
           ,@END_TIME
           ,@EMBK_PIER_NAME
           ,@EMBARK_PORT_ID
           ,@DUR_DAYS
           ,@SUB_REGION_ID
           ,@STATUS_DATE
           ,@STATUS
           ,@LAST_ACTIVITY_TYPE)",
           insertPrice = @"INSERT INTO [dbo].[Carnival_Price]
           ([ship]
           ,[Date]
           ,[port]
           ,[duration]
           ,[itinerary]
           ,[region]
           ,[child]
           ,[adult]
           ,[ncf]
           ,[tax]
           ,[nrd]
           ,[rate]
           ,[category]
           ,[states]
           ,[rate_code]
           ,[obc]
           ,[avail_to_sell]
           ,[from_file])
     VALUES
           (@ship
           ,@Date
           ,@port
           ,@duration
           ,@itinerary
           ,@region
           ,@child
           ,@adult
           ,@ncf
           ,@tax
           ,@nrd
           ,@rate
           ,@category
           ,@states
           ,@rate_code
           ,@obc
           ,@avail_to_sell
           ,@from_file)";
        private FtpClient GetFtpClient()
        {


            try
            {
                ExeConfigurationFileMap configMap = new ExeConfigurationFileMap();
                configMap.ExeConfigFilename = Path.Combine(Path.GetDirectoryName(Assembly.GetEntryAssembly().Location),
                                                           "lanta.sqlconfig.dll.config");
                Configuration config = ConfigurationManager.OpenMappedExeConfiguration(configMap,
                                                                                       ConfigurationUserLevel.None);
                server = config.AppSettings.Settings["CarFtp"].Value;
                user = config.AppSettings.Settings["CarFtpLogin"].Value;
                pass = config.AppSettings.Settings["CarFtpPass"].Value;
               // port = int.Parse(config.AppSettings.Settings["rcFtpPort"].Value);
                _logFile.WriteLine(string.Format("Carnival: подключение к Ftp {0}@{1}", user, server));
                FtpClient ftpClient = new FtpClient();
                passiveMode = isYes(config.AppSettings.Settings["ftpPassiveMode"].Value);
                ftpClient.PassiveMode = passiveMode;

                return ftpClient;
            }
            catch (Exception)
            {
                _logFile.WriteLine(string.Format("Carnival: Подключение к ftp не удалось.\n Сервер:{0}, Пользователь:{1}", server, user));
                return null;
            }
        }

        private bool isYes(string st)
        {
            if (st == "N") return false;
            else
            {
                return true;
            }
        }

        public CarnivalDataManipulator(SqlConnection con, Logger log) : base(con, log)
        {
            _connection = con;
            _log = log;
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
        public void GetPorts()
        {
            SqlCommand delet = new SqlCommand(@"delete from Carnival_ports", _connection);
            delet.ExecuteNonQuery();
            _ftpClient.Connect(FTP_TIMEOUT, server, port);
            _logFile.WriteLine("Carnival: Ftp подключен");
            _ftpClient.Login(FTP_TIMEOUT, user, pass);
            _logFile.WriteLine("Carnival: Ftp вход выполнен");
            byte[] ports = _ftpClient.GetFile(FTP_TIMEOUT, "TB_RES_PORT.csv");
            _ftpClient.Disconnect(FTP_TIMEOUT);
            Stream ports_stream = new MemoryStream(ports);
            using (StreamReader sr = new StreamReader(ports_stream))
            {
                var t = sr.ReadToEnd();
                t=t.Replace("\",\"", ";");
                t=t.Replace("\"", "");
                var items = t.Split(new[] { '\n' });
                for (int i = 1; i < items.Length - 2; i++)
                {
                    string[] s = items[i].Split(';');
                    if (s.Length < 8) continue;
                    int res_port_id = Convert.ToInt32(s[0]),port_id = Convert.ToInt32(s[6]);
                    string port_code = s[1],name = s[2],status_date = s[4],iata_code =s[5],last_activity = s[7];
                    bool status, dd = bool.TryParse(s[3],out status);
                    using (SqlCommand com = new SqlCommand(insertPort, _connection))
                    {
                        com.Parameters.AddWithValue("@RES_PORT_ID",res_port_id);
                        com.Parameters.AddWithValue("@RES_PORT_CODE", port_code);
                        com.Parameters.AddWithValue("@PORT_NAME", name);
                        com.Parameters.AddWithValue("@STATUS", status);
                        com.Parameters.AddWithValue("@STATUS_DATE", status_date);
                        com.Parameters.AddWithValue("@IATA_XREF_CODE", iata_code);
                        com.Parameters.AddWithValue("@PORT_ID", port_id);
                        com.Parameters.AddWithValue("@LAST_ACTIVITY_TYPE", last_activity);
                        com.ExecuteNonQuery();
                    }
                }

            }
        }
        public void GetPrice()
        {
            List<string> files = new List<string>();//new string[] { "A1157833-RU.txt", "A1157833-P.txt", "A1157833-1A.txt", "A1157833-2.txt", "A1157833-2A.txt" };
            _ftpClient.Connect(FTP_TIMEOUT, server, port);
            _logFile.WriteLine("Carnival: Ftp подключен");
            _ftpClient.Login(FTP_TIMEOUT, user, pass);
            _logFile.WriteLine("Carnival: Ftp вход выполнен проверка файлов");
            FtpItem[] itemsf= _ftpClient.GetDirectoryList(FTP_TIMEOUT);
            _ftpClient.Disconnect(FTP_TIMEOUT);
            foreach (FtpItem item in itemsf)
            {
                if (item.Name.IndexOf("C1157833-S") >= 0)
                {
                    files.Add(item.Name);
                }
            }
            SqlCommand com = new SqlCommand(@"delete from Carnival_Price", _connection);
            com.ExecuteNonQuery();
            foreach (string file in files)
            {

                _ftpClient.Connect(FTP_TIMEOUT, server, port);
                _logFile.WriteLine("Carnival: Ftp подключен");
                _ftpClient.Login(FTP_TIMEOUT, user, pass);
                _logFile.WriteLine("Carnival: Ftp вход выполнен " + file);
                byte[] price = _ftpClient.GetFile(FTP_TIMEOUT, file);
                _ftpClient.Disconnect(FTP_TIMEOUT);
                Stream pri = new MemoryStream(price);
                SqlCommand priceIns = new SqlCommand(insertPrice, _connection);
                priceIns.Parameters.Add(new SqlParameter("@ship", SqlDbType.VarChar));
                priceIns.Parameters.Add(new SqlParameter("@Date", SqlDbType.DateTime));
                priceIns.Parameters.Add(new SqlParameter("@port", SqlDbType.VarChar));
                priceIns.Parameters.Add(new SqlParameter("@duration", SqlDbType.Int));
                priceIns.Parameters.Add(new SqlParameter("@itinerary", SqlDbType.VarChar));
                priceIns.Parameters.Add(new SqlParameter("@region", SqlDbType.VarChar));
                priceIns.Parameters.Add(new SqlParameter("@child", SqlDbType.VarChar));
                priceIns.Parameters.Add(new SqlParameter("@adult", SqlDbType.VarChar));
                priceIns.Parameters.Add(new SqlParameter("@ncf", SqlDbType.Decimal));
                priceIns.Parameters.Add(new SqlParameter("@tax", SqlDbType.Decimal));
                priceIns.Parameters.Add(new SqlParameter("@rate", SqlDbType.Decimal));
                priceIns.Parameters.Add(new SqlParameter("@obc", SqlDbType.Decimal));
                priceIns.Parameters.Add(new SqlParameter("@category", SqlDbType.VarChar));
                priceIns.Parameters.Add(new SqlParameter("@states", SqlDbType.VarChar));
                priceIns.Parameters.Add(new SqlParameter("@rate_code", SqlDbType.VarChar));
                priceIns.Parameters.Add(new SqlParameter("@avail_to_sell", SqlDbType.VarChar));
                priceIns.Parameters.Add(new SqlParameter("@from_file", SqlDbType.VarChar));
                priceIns.Parameters.Add(new SqlParameter("@nrd", SqlDbType.VarChar));
                using (StreamReader sr = new StreamReader(pri))
                {
                    var t = sr.ReadToEnd();

                    var items = t.Split(new[] {'\n'});
                    // Пропускаем первую строку с заголовком
                    for (int i = 1; i < items.Length - 1; i++)
                    {

                        var s = items[i].Split(new char[] {';'});
                        String ship = s[0],
                               portcode = s[2],
                               itin = s[5],
                               region = s[6].Trim(),
                               child = s[7],
                               adult = s[8],
                               nrd = s[12];
                        DateTime date = Date(s[1]);
                        int duration = Convert.ToInt32(s[3]);
                        decimal NCF = Decimal(s[10]), otherTax = Decimal(s[11]);
                        int j = 13;
                        priceIns.Parameters["@ship"].Value = ship;
                        priceIns.Parameters["@Date"].Value = date;
                        priceIns.Parameters["@port"].Value = portcode;
                        priceIns.Parameters["@duration"].Value = duration;
                        priceIns.Parameters["@itinerary"].Value = itin;
                        priceIns.Parameters["@region"].Value = region;
                        priceIns.Parameters["@child"].Value = child;
                        priceIns.Parameters["@adult"].Value = adult;
                        priceIns.Parameters["@ncf"].Value = NCF;
                        priceIns.Parameters["@tax"].Value = otherTax;
                        priceIns.Parameters["@nrd"].Value = nrd;
                        priceIns.Parameters["@from_file"].Value = file;
                        while (j <= s.Length - 7)
                        {
                            String cabinCode = s[j + 2],
                                   status = s[j + 3],
                                   avail_to_sell = s[j + 5],
                                   rate_code = s[j + 4];
                            decimal rate = Convert.ToDecimal(s[j + 1]);
                            decimal obc = Convert.ToDecimal(s[j + 6]);
                            j += 7;
                            if (status == "\0")
                            {
                                j = s.Length;
                                continue;
                            }
                            if ((status == "X") || cabinCode.Trim() == "") continue;
                            priceIns.Parameters["@rate"].Value = rate;
                            priceIns.Parameters["@category"].Value = cabinCode;
                            priceIns.Parameters["@states"].Value = status;
                            priceIns.Parameters["@avail_to_sell"].Value = avail_to_sell;
                            priceIns.Parameters["@rate_code"].Value = rate_code;
                            priceIns.Parameters["@obc"].Value = obc;
                            priceIns.ExecuteNonQuery();
                        }
                    }
                }
            }
            _logFile.WriteLine("Carnival: Цены загружены");
        }

        public override string GetItineraryData()
        {
            _ftpClient.Connect(FTP_TIMEOUT, server, port);
            _logFile.WriteLine("Carnival: Ftp подключен");
            _ftpClient.Login(FTP_TIMEOUT, user, pass);
            _logFile.WriteLine("Carnival: Ftp вход выполнен");
            byte[] itenerary = _ftpClient.GetFile(FTP_TIMEOUT, "TB_ITIN.csv");
            byte[] itenerary_dtl = _ftpClient.GetFile(FTP_TIMEOUT, "TB_ITIN_DTL.csv");
            _ftpClient.Disconnect(FTP_TIMEOUT);
            Stream iten_dtl = new MemoryStream(itenerary_dtl);
            SqlCommand delet = new SqlCommand("delete from Carnival_itinary delete from Carnival_iten_dtl",_connection);
            delet.ExecuteNonQuery();
            using (StreamReader sr = new StreamReader(iten_dtl))
            {
                var t = sr.ReadToEnd();

                var items = t.Split(new[] { '\n' });
                for (int i = 1; i < items.Length - 2; i++)
                {

                    String[] s = items[i].Split(new char[] {','});
                    if (s.Length < 11) continue;

                    for (int d = 0; d < s.Length;d++)
                    {
                        s[d] = s[d].Replace('"', ' ').Trim();

                    }

                    string day_abb = s[1],arrival = s[4],depature =s[5],status_date =s[9],last_actinity = s[10];
                    int itin_dtl_id = Convert.ToInt32(s[0]),day_num= Convert.ToInt32(s[2]),port_id =Convert.ToInt32(s[3]),day_sort = Convert.ToInt32(s[6]),itin_id= Convert.ToInt32(s[7]);
                    bool  status ,dd= bool.TryParse(s[8], out status);
                    using (SqlCommand com = new SqlCommand (insertItenDtl,_connection))
                    {
                        com.Parameters.AddWithValue("@ITIN_DTL_ID", itin_dtl_id);
                        com.Parameters.AddWithValue("@DAY_ABBREV", day_abb);
                        com.Parameters.AddWithValue("@DAY_NUMBER", day_num);
                        com.Parameters.AddWithValue("@ARRIVAL_TIME", arrival);
                        com.Parameters.AddWithValue("@DEPART_TIME", depature);
                        com.Parameters.AddWithValue("@DAY_SORT_VALUE", day_sort);
                        com.Parameters.AddWithValue("@ITIN_ID", itin_id);
                        com.Parameters.AddWithValue("@STATUS", status);
                        com.Parameters.AddWithValue("@STATUS_DATE", status_date);
                        com.Parameters.AddWithValue("@LAST_ACTIVITY_TYPE", last_actinity);
                        com.Parameters.AddWithValue("@ITIN_PORT_ID", port_id);
                        com.ExecuteNonQuery();
                        //com
                    }

                }


            }

            Stream iten = new MemoryStream(itenerary);
            using (StreamReader sr = new StreamReader(iten))
            {
                var t = sr.ReadToEnd();

                var items = t.Split(new[] { '\n' });
                for (int i = 1; i < items.Length - 1; i++)
                {

                    String[] s = items[i].Split(new char[] { ',' });
                    if(s.Length<13) continue;

                    for (int d = 0; d < s.Length; d++)
                    {
                        s[d] = s[d].Replace('"', ' ').Trim();

                    }
                    bool status, dd = bool.TryParse(s[10], out status);
                    int itin_id = Convert.ToInt32(s[0]), embark_port_id = Convert.ToInt32(s[1]),dur_days = Convert.ToInt32(s[3]),sub_reg_code =Convert.ToInt32(s[9]);
                    string itin_code = s[2], begtime = s[4], endtime = s[5], name = s[6], status_day = s[11], last_actinity = s[12];
                    using (SqlCommand com = new SqlCommand(insertIten, _connection))
                    {
                        com.Parameters.AddWithValue("@ITIN_ID", itin_id);
                        com.Parameters.AddWithValue("@ITIN_CODE", itin_code);
                        com.Parameters.AddWithValue("@BEGIN_TIME", begtime);
                        com.Parameters.AddWithValue("@END_TIME", endtime);
                        com.Parameters.AddWithValue("@EMBK_PIER_NAME", name);
                        com.Parameters.AddWithValue("@EMBARK_PORT_ID", embark_port_id);
                        com.Parameters.AddWithValue("@DUR_DAYS", dur_days);
                        com.Parameters.AddWithValue("@SUB_REGION_ID", sub_reg_code);
                        com.Parameters.AddWithValue("@STATUS_DATE", status_day);
                        com.Parameters.AddWithValue("@STATUS", status);
                        com.Parameters.AddWithValue("@LAST_ACTIVITY_TYPE", last_actinity);
                        com.ExecuteNonQuery();

                    }
                }

            }

            return "Carnival: Маршруты успешно загружены";
        }
        DateTime Date(String str )
        {
            return Convert.ToDateTime(str.Substring(4, 4) + "." + str.Substring(0, 2) + "." + str.Substring(2, 2));
        }
        Decimal Decimal(String str)
        {
            return Convert.ToDecimal(str.Substring(0, str.Length - 2) + "," + str.Substring(str.Length - 2));
        }
        void SetChangesDB()
        {
            _log.WriteLine("Carnival: Применение изменений в БД");
            SqlCommand com = new SqlCommand(@"INSERT INTO [mk_job_prices](
            [crlines]
           ,[date_job])  VALUES( '1', getdate())",_connection);
            //com.CommandType= CommandType.StoredProcedure;
            com.ExecuteNonQuery();
        }
        public override void GetData()
        {
            try
            {
                _log.WriteLine("Carnival: Начало загрузки");
               _ftpClient= GetFtpClient();
                GetPrice();
                _log.WriteLine(GetItineraryData());
                GetPorts();
                SetChangesDB();

            }
            catch(Exception ex)
            {

                _log.WriteLine("Carnival:" + ex.Message + "  " + ex.StackTrace);
                throw;
            }

        }
    }
}
