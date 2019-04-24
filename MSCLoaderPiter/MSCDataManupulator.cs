using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Text;
using DxHelpersLib;
using PluginInteractionLib;

namespace MSCLoaderPiter
{
    class MSCDataManupulator:DataManipulator
    {
        private string SavePath;
        private string reloadtable = @"

DELETE FROM [msc_price_piter_new] 
INSERT INTO [dbo].[msc_price_piter_new]
           ([CRUISE-ID]
           ,[SHIP-CD]
           ,[SAILING-PORT]
           ,[TERMINATION-PORT]
           ,[SHIP-NAME]
           ,[SAILING-DATE]
           ,[NIGHTS]
           ,[ITIN-CD]
           ,[ITIN-DESC]
           ,[FARE-CD]
           ,[FARE-DESC]
           ,[ITEMS]
           ,[CATEGORY]
           ,[1A]
           ,[2A]
           ,[3A]
           ,[4A]
           ,[2A1C]
           ,[2A2C]
           ,[1A1J]
           ,[2A1J]
           ,[2A1C1J]
           ,[2A2J]
           ,[NCF-A]
           ,[NCF-C]
           ,[NCF-J]
           ,[GFT-A]
           ,[GFT-C]
           ,[SAIL-TIME-EMBK]
           ,[SAIL-TIME-DISMBK]
           ,[CRUISE-ONLY]
           ,[NOW-AVAIL]
           ,[CLUB-DISCOUNT]
           ,[FLIGHT-STATUS]
           ,[FLIGHT-PRICE-TYPE]
           ,[ROUTING-OUT]
           ,[ROUTING-RET]
           ,[AIR-COST-A]
           ,[AIR-COST-C]
           ,[AIR-COST-I]
           ,[AIR-OUT-DEP-DT]
           ,[AIR-OUT-DEP-CHECK-IN]
           ,[AIR-OUT-DEP-TIME]
           ,[AIR-OUT-ARR-DT]
           ,[AIR-OUT-ARR-TIME]
           ,[AIR-IN-DEP-DT]
           ,[AIR-IN-DEP-CHECK-IN]
           ,[AIR-IN-DEP-TIME]
           ,[AIR-IN-ARR-DT]
           ,[AIR-IN-ARR-TIME]
           ,[AIR-MAX-INF-AGE]
           ,[AIR-MAX-CHD-AGE]
           ,[AIR-MIN-SEN-AGE])
SELECT [CRUISE-ID]
      ,[SHIP-CD]
      ,[SAILING-PORT]
      ,[TERMINATION-PORT]
      ,[SHIP-NAME]
      ,[SAILING-DATE]
      ,[NIGHTS]
      ,[ITIN-CD]
      ,[ITIN-DESC]
      ,[FARE-CD]
      ,[FARE-DESC]
      ,[ITEMS]
      ,[CATEGORY]
      ,[1A]
      ,[2A]
      ,[3A]
      ,[4A]
      ,[2A1C]
      ,[2A2C]
      ,[1A1J]
      ,[2A1J]
      ,[2A1C1J]
      ,[2A2J]
      ,[NCF-A]
      ,[NCF-C]
      ,[NCF-J]
      ,[GFT-A]
      ,[GFT-C]
      ,[SAIL-TIME-EMBK]
      ,[SAIL-TIME-DISMBK]
      ,[CRUISE-ONLY]
      ,[NOW-AVAIL]
      ,[CLUB-DISCOUNT]
      ,[FLIGHT-STATUS]
      ,[FLIGHT-PRICE-TYPE]
      ,[ROUTING-OUT]
      ,[ROUTING-RET]
      ,[AIR-COST-A]
      ,[AIR-COST-C]
      ,[AIR-COST-I]
      ,[AIR-OUT-DEP-DT]
      ,[AIR-OUT-DEP-CHECK-IN]
      ,[AIR-OUT-DEP-TIME]
      ,[AIR-OUT-ARR-DT]
      ,[AIR-OUT-ARR-TIME]
      ,[AIR-IN-DEP-DT]
      ,[AIR-IN-DEP-CHECK-IN]
      ,[AIR-IN-DEP-TIME]
      ,[AIR-IN-ARR-DT]
      ,[AIR-IN-ARR-TIME]
      ,[AIR-MAX-INF-AGE]
      ,[AIR-MAX-CHD-AGE]
      ,[AIR-MIN-SEN-AGE]
  FROM [##Temp_msc_price_piter_new]
DROP TABLE [##Temp_msc_price_piter_new] ";
        private const string insPrice = @"INSERT INTO [##Temp_msc_price_piter_new]
           ([CRUISE-ID]
           ,[SHIP-CD]
           ,[SAILING-PORT]
           ,[TERMINATION-PORT]
           ,[SHIP-NAME]
           ,[SAILING-DATE]
           ,[NIGHTS]
           ,[ITIN-CD]
           ,[ITIN-DESC]
           ,[FARE-CD]
           ,[FARE-DESC]
           ,[ITEMS]
           ,[CATEGORY]
           ,[1A]
           ,[2A]
           ,[3A]
           ,[4A]
           ,[2A1C]
           ,[2A2C]
           ,[1A1J]
           ,[2A1J]
           ,[2A1C1J]
           ,[2A2J]
           ,[NCF-A]
           ,[NCF-C]
           ,[NCF-J]
           ,[GFT-A]
           ,[GFT-C]
           ,[SAIL-TIME-EMBK]
           ,[SAIL-TIME-DISMBK]
           ,[CRUISE-ONLY]
           ,[NOW-AVAIL]
           ,[CLUB-DISCOUNT]
           ,[FLIGHT-STATUS]
           ,[FLIGHT-PRICE-TYPE]
           ,[ROUTING-OUT]
           ,[ROUTING-RET]
           ,[AIR-COST-A]
           ,[AIR-COST-C]
           ,[AIR-COST-I]
           ,[AIR-OUT-DEP-DT]
           ,[AIR-OUT-DEP-CHECK-IN]
           ,[AIR-OUT-DEP-TIME]
           ,[AIR-OUT-ARR-DT]
           ,[AIR-OUT-ARR-TIME]
           ,[AIR-IN-DEP-DT]
           ,[AIR-IN-DEP-CHECK-IN]
           ,[AIR-IN-DEP-TIME]
           ,[AIR-IN-ARR-DT]
           ,[AIR-IN-ARR-TIME]
           ,[AIR-MAX-INF-AGE]
           ,[AIR-MAX-CHD-AGE]
           ,[AIR-MIN-SEN-AGE])
     VALUES
           (@CRUISE_ID
           ,@SHIP_CD
           ,@SAILING_PORT
           ,@TERMINATION_PORT
           ,@SHIP_NAME
           ,@SAILING_DATE
           ,@NIGHTS
           ,@ITIN_CD
           ,@ITIN_DESC
           ,@FARE_CD
           ,@FARE_DESC
           ,@ITEMS
           ,@CATEGORY
           ,@1A
           ,@2A
           ,@3A
           ,@4A
           ,@2A1C
           ,@2A2C
           ,@1A1J
           ,@2A1J
           ,@2A1C1J
           ,@2A2J
           ,@NCF_A
           ,@NCF_C
           ,@NCF_J
           ,@GFT_A
           ,@GFT_C
           ,@SAIL_TIME_EMBK
           ,@SAIL_TIME_DISMBK
           ,@CRUISE_ONLY
           ,@NOW_AVAIL
           ,@CLUB_DISCOUNT
           ,@FLIGHT_STATUS
           ,@FLIGHT_PRICE_TYPE
           ,@ROUTING_OUT
           ,@ROUTING_RET
           ,@AIR_COST_A
           ,@AIR_COST_C
           ,@AIR_COST_I
           ,@AIR_OUT_DEP_DT
           ,@AIR_OUT_DEP_CHECK_IN
           ,@AIR_OUT_DEP_TIME
           ,@AIR_OUT_ARR_DT
           ,@AIR_OUT_ARR_TIME
           ,@AIR_IN_DEP_DT
           ,@AIR_IN_DEP_CHECK_IN
           ,@AIR_IN_DEP_TIME
           ,@AIR_IN_ARR_DT
           ,@AIR_IN_ARR_TIME
           ,@AIR_MAX_INF_AGE
           ,@AIR_MAX_CHD_AGE
           ,@AIR_MIN_SEN_AGE)";
        private const string createTable = @"CREATE TABLE [##Temp_msc_price_piter_new](
	[CRUISE-ID] [varchar](50) NULL,
	[SHIP-CD] [varchar](5) NULL,
	[SAILING-PORT] [varchar](5) NULL,
	[TERMINATION-PORT] [varchar](5) NULL,
	[SHIP-NAME] [varchar](50) NULL,
	[SAILING-DATE] [datetime] NULL,
	[NIGHTS] [int] NULL,
	[ITIN-CD] [varchar](50) NULL,
	[ITIN-DESC] [varchar](50) NULL,
	[FARE-CD] [varchar](50) NULL,
	[FARE-DESC] [varchar](50) NULL,
	[ITEMS] [varchar](50) NULL,
	[CATEGORY] [varchar](100) NULL,
	[1A] [float] NULL,
	[2A] [float] NULL,
	[3A] [float] NULL,
	[4A] [float] NULL,
	[2A1C] [float] NULL,
	[2A2C] [float] NULL,
	[1A1J] [float] NULL,
	[2A1J] [float] NULL,
	[2A1C1J] [float] NULL,
	[2A2J] [float] NULL,
	[NCF-A] [float] NULL,
	[NCF-C] [float] NULL,
	[NCF-J] [float] NULL,
	[GFT-A] [float] NULL,
	[GFT-C] [float] NULL,
	[SAIL-TIME-EMBK] [varchar](10) NULL,
	[SAIL-TIME-DISMBK] [varchar](10) NULL,
	[CRUISE-ONLY] [bit] NULL,
	[NOW-AVAIL] [bit] NULL,
	[CLUB-DISCOUNT] [varchar](50) NULL,
	[FLIGHT-STATUS] [bit] NULL,
	[FLIGHT-PRICE-TYPE] [varchar](50) NULL,
	[ROUTING-OUT] [varchar](50) NULL,
	[ROUTING-RET] [varchar](50) NULL,
	[AIR-COST-A] [varchar](50) NULL,
	[AIR-COST-C] [varchar](50) NULL,
	[AIR-COST-I] [varchar](50) NULL,
	[AIR-OUT-DEP-DT] [varchar](50) NULL,
	[AIR-OUT-DEP-CHECK-IN] [varchar](50) NULL,
	[AIR-OUT-DEP-TIME] [varchar](50) NULL,
	[AIR-OUT-ARR-DT] [varchar](50) NULL,
	[AIR-OUT-ARR-TIME] [varchar](50) NULL,
	[AIR-IN-DEP-DT] [varchar](50) NULL,
	[AIR-IN-DEP-CHECK-IN] [varchar](50) NULL,
	[AIR-IN-DEP-TIME] [varchar](50) NULL,
	[AIR-IN-ARR-DT] [varchar](50) NULL,
	[AIR-IN-ARR-TIME] [varchar](50) NULL,
	[AIR-MAX-INF-AGE] [varchar](50) NULL,
	[AIR-MAX-CHD-AGE] [varchar](50) NULL,
	[AIR-MIN-SEN-AGE] [varchar](50) NULL
)";
        public MSCDataManupulator(SqlConnection con, Logger log) : base(con, log)
        {
            ExeConfigurationFileMap configMap = new ExeConfigurationFileMap();
            configMap.ExeConfigFilename = Path.Combine(Path.GetDirectoryName(Assembly.GetEntryAssembly().Location),
                                                       "lanta.sqlconfig.dll.config");
            Configuration config = ConfigurationManager.OpenMappedExeConfiguration(configMap,
                                                                                   ConfigurationUserLevel.None);
            SavePath = config.AppSettings.Settings["MSCPiterPath"].Value;
            if (!Directory.Exists(SavePath))
            {
                Directory.CreateDirectory(SavePath);
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

        public override void GetData()
        {
            try
            {



                _logFile.WriteLine("MscPiter : Начало загрузки");
                WebClient client = new WebClient();
                byte[] data =client.DownloadData("http://www.gocruise.ru/pac_feed/led-ods-new.php");
               // WebRequest load = WebRequest.Create("http://www.gocruise.ru/pac_feed/led-ods-new.php");
              //  HttpWebResponse response = (HttpWebResponse) load.GetResponse();
                MemoryStream stream =new MemoryStream(data);
                string res = new StreamReader(stream,Encoding.Default).ReadToEnd();
                BinaryWriter bw = new BinaryWriter(File.Create(SavePath + DateTime.Now.ToString("ddMMyyyyHHmm") + ".csv"));
                
                bw.Write(data);
                bw.Close();
                string[] ss = res.Split(new[] {'\n'});
                using (SqlCommand com = new SqlCommand(createTable, _connection))
                {
                    com.ExecuteNonQuery();
                }
                for (int i = 1; i < ss.Length - 1; i++)
                {
                    string[] items = ss[i].Split(';');
                    using (SqlCommand com = new SqlCommand(insPrice, _connection))
                    {
                        com.Parameters.AddWithValue("@CRUISE_ID", items[0]);
                        com.Parameters.AddWithValue("@SHIP_CD", items[1]);
                        com.Parameters.AddWithValue("@SAILING_PORT", items[2]);
                        com.Parameters.AddWithValue("@TERMINATION_PORT", items[3]);
                        com.Parameters.AddWithValue("@SHIP_NAME", items[4]);
                        com.Parameters.AddWithValue("@SAILING_DATE", items[5]);
                        com.Parameters.AddWithValue("@NIGHTS", items[6]);
                        com.Parameters.AddWithValue("@ITIN_CD", items[7]);
                        com.Parameters.AddWithValue("@ITIN_DESC", items[8]);
                        com.Parameters.AddWithValue("@FARE_CD", items[9]);
                        com.Parameters.AddWithValue("@FARE_DESC", items[10]);
                        com.Parameters.AddWithValue("@ITEMS",items[11]);
                        com.Parameters.AddWithValue("@CATEGORY", items[12]);


                        
                        com.Parameters.AddWithValue("@1A",ParseDouble(items[13].Replace('.', ',')));
                        com.Parameters.AddWithValue("@2A",ParseDouble(items[14].Replace('.', ',')));
                        com.Parameters.AddWithValue("@3A",ParseDouble(items[15].Replace('.', ',')));
                        com.Parameters.AddWithValue("@4A",ParseDouble(items[16].Replace('.', ',')));
                        com.Parameters.AddWithValue("@2A1C",ParseDouble(items[17].Replace('.', ',')));
                        com.Parameters.AddWithValue("@2A2C",ParseDouble(items[18].Replace('.', ',')));
                        com.Parameters.AddWithValue("@1A1J", ParseDouble(items[19].Replace('.', ',')));
                        com.Parameters.AddWithValue("@2A1J", ParseDouble(items[20].Replace('.', ',')));
                        com.Parameters.AddWithValue("@2A1C1J", ParseDouble(items[21].Replace('.', ',')));
                        com.Parameters.AddWithValue("@2A2J", ParseDouble(items[22].Replace('.', ',')));
                        com.Parameters.AddWithValue("@NCF_A", ParseDouble(items[23].Replace('.', ',')));
                        com.Parameters.AddWithValue("@NCF_C", ParseDouble(items[24].Replace('.', ',')));
                        com.Parameters.AddWithValue("@NCF_J", ParseDouble(items[25].Replace('.', ',')));
                        com.Parameters.AddWithValue("@GFT_A", ParseDouble(items[26].Replace('.', ',')));
                        com.Parameters.AddWithValue("@GFT_C", ParseDouble(items[27].Replace('.', ',')));

                        com.Parameters.AddWithValue("@SAIL_TIME_EMBK", items[28]);
                        com.Parameters.AddWithValue("@SAIL_TIME_DISMBK", items[29]);
                        com.Parameters.AddWithValue("@CRUISE_ONLY", IsYes(items[30]));

                        com.Parameters.AddWithValue("@NOW_AVAIL", IsYes(items[31]));
                        com.Parameters.AddWithValue("@CLUB_DISCOUNT", items[32]);
                        com.Parameters.AddWithValue("@FLIGHT_STATUS", IsYes(items[33]));
                        com.Parameters.AddWithValue("@FLIGHT_PRICE_TYPE", items[34]);
                        com.Parameters.AddWithValue("@ROUTING_OUT", items[35]);
                        com.Parameters.AddWithValue("@ROUTING_RET", items[36]);
                        com.Parameters.AddWithValue("@AIR_COST_A", items[37]);
                        com.Parameters.AddWithValue("@AIR_COST_C", items[38]);
                        com.Parameters.AddWithValue("@AIR_COST_I", items[39]);
                        com.Parameters.AddWithValue("@AIR_OUT_DEP_DT", items[40]);
                        com.Parameters.AddWithValue("@AIR_OUT_DEP_CHECK_IN", items[41]);
                        com.Parameters.AddWithValue("@AIR_OUT_DEP_TIME", items[42]);
                        com.Parameters.AddWithValue("@AIR_OUT_ARR_DT", items[43]);
                        com.Parameters.AddWithValue("@AIR_OUT_ARR_TIME", items[44]);
                       
                        com.Parameters.AddWithValue("@AIR_IN_DEP_DT", items[45]);
                        com.Parameters.AddWithValue("@AIR_IN_DEP_CHECK_IN", items[46]);
                        com.Parameters.AddWithValue("@AIR_IN_DEP_TIME", items[47]);

                        com.Parameters.AddWithValue("@AIR_IN_ARR_DT", items[48]);
                        com.Parameters.AddWithValue("@AIR_IN_ARR_TIME", items[49]);

                        com.Parameters.AddWithValue("@AIR_MAX_INF_AGE", items[50]);
                        com.Parameters.AddWithValue("@AIR_MAX_CHD_AGE", items[51]);
                        com.Parameters.AddWithValue("@AIR_MIN_SEN_AGE", items[52]);

                        com.ExecuteNonQuery();


                    }
                }
                using (SqlCommand com = new SqlCommand(reloadtable, _connection))
                {
                    com.ExecuteNonQuery();
                }
                _logFile.WriteLine("MscPiter : Загрузка окончена");
            }
            catch (Exception ex)
            {
                new System.Net.Mail.SmtpClient("mail.mcruises.ru").Send("errorreport@mcruises.ru",
                                                                                   "tech_error@mcruises.ru", "MSCPiter",
                                                                                   string.Format(
                                                                                       "MSCPiter: Произошла ошибка {0} StackTrace: {1}", ex.Message, ex.StackTrace));
                throw;
            }
        }

        object IsYes(string a)
        {
            if (a == string.Empty)
            {
                return DBNull.Value;
            }
            else if (a == "YES")
            {
                return true;
            }
            else return false;
        }
        object ParseDouble(string a)
        {
            if (a == string.Empty)
            {
                return DBNull.Value;
            }
            else
            {
                try
                {
                    return Double.Parse(a.Replace('.', ','));
                }
                catch (Exception)
                {

                    return DBNull.Value;
                }
            }
        }
    }
}
