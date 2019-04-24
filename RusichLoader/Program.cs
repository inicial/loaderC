using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using DxHelpersLib;
using lanta.SQLConfig;

namespace RusichLoader
{
    class Program
    {
        static void Main(string[] args)
        {
            bool state;
            SqlConnection _connection;
            _connection = LantaSQLConnection.Open_LantaSQLConnection("dbConnection", "REALCURSES", "qwerty654321", out state);
            ExeConfigurationFileMap configMap = new ExeConfigurationFileMap();
            configMap.ExeConfigFilename = Path.Combine(Path.GetDirectoryName(Assembly.GetEntryAssembly().Location),
                                                       "lanta.sqlconfig.dll.config");
            Configuration config = ConfigurationManager.OpenMappedExeConfiguration(configMap,
                                                                                   ConfigurationUserLevel.None);
            string file = config.AppSettings.Settings["LogPath"].Value;
            Logger log = new FileLogger(file);
            RusichCruiseLoader load = new RusichCruiseLoader(_connection, log);
            load.StartLoader();
        }
    }
}
