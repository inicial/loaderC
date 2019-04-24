using System.Configuration;
using System.Data.SqlClient;
using System.IO;
using System.Reflection;
using DxHelpersLib;
using lanta.SQLConfig;

namespace InfoflotLoader
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
            InfoFlotCruiseLoader load = new InfoFlotCruiseLoader(_connection,log);
            load.StartLoader();
 

        }
        

    }
}
