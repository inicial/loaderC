using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Text;
using DxHelpersLib;
using PluginInteractionLib;


namespace PrincessLoader
{
    class PrincessDataLoader:CruiseLoader

    {
        public PrincessDataLoader(SqlConnection con, Logger log)
        {
            Log = log;
            Connection = new SqlConnection(con.ConnectionString);

            _dataManipulator = new PricessDataManipulator(con, log);
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
        private void GetData()
        {
            _dataManipulator.GetData();
        }
        public override long RepeatInterval { get; set; }
    }
}
