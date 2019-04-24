using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Text;
using DxHelpersLib;
using PluginInteractionLib;


namespace CostaNewPlugin
{
    class CostaNewLoader:CruiseLoader
    {
        public CostaNewLoader(SqlConnection con, Logger log)
        {
            Connection = new SqlConnection(con.ConnectionString);
            Log = log;
            IsAvaliable = CreateDataManipulator(con,log);
        }
        public override string ID
        {
            get { return Assembly.GetExecutingAssembly().GetName().Name; }
        }

        public override long RepeatInterval { get; set; }

        
        bool CreateDataManipulator(SqlConnection con, Logger log)
        {
            try
            {
                _dataManipulator = new CostaNewDatamanipulator(con, log);
                return true;
            }
            catch (Exception)
            {
                log.WriteLine("CostaNew :Получение данных невозможно");
                return false;
            }

        }
        private void GetData()
        {
            if (!IsAvaliable)
            {
                IsAvaliable = CreateDataManipulator(Connection, Log);
            }
            if (!IsAvaliable) return;
            _dataManipulator.GetData();
        }
         
        public override void StartLoader()
        {
            GetData();
        }

        public override string LoadShips()
        {
            if (!IsAvaliable)
            {
                IsAvaliable = CreateDataManipulator(Connection, Log);
            }
            return !IsAvaliable ? "CostaNew: Плагин не доступен" : _dataManipulator.GetShipsData();
        }

        public override string LoadDecks()
        {
            if (!IsAvaliable)
            {
                IsAvaliable = CreateDataManipulator(Connection, Log);
            }
            return !IsAvaliable ? "CostaNew: Плагин не доступен" : _dataManipulator.GetDecksData();
        }

        public override string LoadCabins()
        {
            if (!IsAvaliable)
            {
                IsAvaliable = CreateDataManipulator(Connection, Log);
            }
            return !IsAvaliable ? "CostaNew: Плагин не доступен" : _dataManipulator.GetCabinsData();
        }

        public override string LoadItinerary()
        {
            if (!IsAvaliable)
            {
                IsAvaliable = CreateDataManipulator(Connection, Log);
            }
            return !IsAvaliable ? "CostaNew: Плагин не доступен" : _dataManipulator.GetItineraryData();
        }
    }
}
