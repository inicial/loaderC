﻿using System.Data.SqlClient;
using System.Reflection;
using DxHelpersLib;
using PluginInteractionLib;

namespace VodohodLoader
{
    class VodohodCruiseLoader : CruiseLoader
    {
        public VodohodCruiseLoader(SqlConnection con, Logger log)
        {
            Log = log;
            Connection = new SqlConnection(con.ConnectionString);

            _dataManipulator = new VodohodDataManipulator(con, log);
        }

        private void GetData()
        {
            _dataManipulator.GetData();
        }
        public override void StartLoader()
        {
            GetData();
        }

        public override string LoadShips()
        {
            return _dataManipulator.GetShipsData();
        }

        public override string LoadDecks()
        {
            return _dataManipulator.GetDecksData();
        }

        public override string LoadCabins()
        {
            return _dataManipulator.GetCabinsData();
        }

        public override string LoadItinerary()
        {
            return _dataManipulator.GetItineraryData();
        }

        public override string ID
        {
            get { return Assembly.GetExecutingAssembly().GetName().Name; }
        }

        public override long RepeatInterval { get; set; }
    }
}
