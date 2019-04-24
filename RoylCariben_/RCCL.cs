using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.ServiceProcess;
using System.Text;
using DxHelpersLib;
using RoyalCaribbeanPlugin;
using lanta.SQLConfig;

namespace RoylCariben
{
    public partial class RCCL : ServiceBase
    {
        public RCCL()
        {
            InitializeComponent();
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
            RoyalCaribbeanLoader load = new RoyalCaribbeanLoader(_connection,log);
            load.StartLoader();
        }

        
        protected override void OnStart(string[] args)
        {
            
        }

        protected override void OnStop()
        {
        }
    }
}
