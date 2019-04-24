using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Xml;
using System.Configuration;
using System.Data.SqlClient;
using System.IO;
using System.Reflection;
using System.ServiceProcess;
using System.Text;
using DxHelpersLib;
using lanta.SQLConfig;

namespace MSCPack
{
    public partial class Service1 : ServiceBase
    {
        private SqlConnection _connection;
        public Service1()
        {
            InitializeComponent();
        }

        public  void OnStart()
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
            PackMSCPlugin.PackMscCruiseLoader loadder = new PackMSCPlugin.PackMscCruiseLoader(_connection, log);
            loadder.StartLoader();
        }

        protected override void OnStop()
        {
        }
    }
}
