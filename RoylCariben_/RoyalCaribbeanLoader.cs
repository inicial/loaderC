using System;
using System.Data.SqlClient;
using System.Reflection;
using DxHelpersLib;
using PluginInteractionLib;

namespace RoyalCaribbeanPlugin
{
    public class RoyalCaribbeanLoader:CruiseLoader
    {
        sealed public override string ID
        {
            get { return Assembly.GetExecutingAssembly().GetName().Name; }
        }

        public override long RepeatInterval { get; set; }


        public RoyalCaribbeanLoader(SqlConnection con,Logger log)
        {
            Log = log;
            Connection = new SqlConnection(con.ConnectionString);
            _dataManipulator = new RoyalCaribbeanDataManipulator(con,log);
        }

        public override void StartLoader()
        {
            _dataManipulator.GetData();
        }

        public override string LoadShips()
        {
            throw new NotImplementedException();
        }

        public override string LoadDecks()
        {
            throw new NotImplementedException();
        }

        public override string LoadCabins()
        {
            throw new NotImplementedException();
        }

        public override string LoadItinerary()
        {
            throw new NotImplementedException();
        }

    }
}