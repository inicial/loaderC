using System.Configuration;
using System.Data.SqlClient;
using System.IO;
using System.Reflection;
using System.ServiceProcess;
using CostaNewPlugin;
using DxHelpersLib;
using lanta.SQLConfig;

namespace CostaServise
{
    public partial class Costa : ServiceBase
    {
        private SqlConnection _connection;
        public Costa()
        {
            InitializeComponent();
            OnStart();
        }

        protected void OnStart()
        {
            ExeConfigurationFileMap configMap = new ExeConfigurationFileMap();
            configMap.ExeConfigFilename = Path.Combine(Path.GetDirectoryName(Assembly.GetEntryAssembly().Location),
                "lanta.sqlconfig.dll.config");
            Configuration config = ConfigurationManager.OpenMappedExeConfiguration(configMap,
                ConfigurationUserLevel.None);
            string _path = config.AppSettings.Settings["LogPath"].Value;
           
            Logger log = new FileLogger(_path);
            bool state;
            _connection = LantaSQLConnection.Open_LantaSQLConnection("dbConnection", "REALCURSES", "qwerty654321", out state);
            CostaNewPlugin.CostaNewLoader loadder = new CostaNewLoader(_connection,log);
            loadder.StartLoader();
        }

        protected void OnStop()
        {
        }
    }
}
