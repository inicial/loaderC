//#define SPOTEST
using System;
using System.Data.SqlClient;
using System.Reflection;
using System.Timers;
using DxHelpersLib;
using PluginInteractionLib;

namespace PackMSCPlugin
{
    public class PackMscCruiseLoader:CruiseLoader
    {
        public override sealed string ID{
            get { return Assembly.GetExecutingAssembly().GetName().Name; }
        }

        public override long RepeatInterval
        {
            get; set;
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

        //readonly DataManipulator _dataManipulator;

        public PackMscCruiseLoader(SqlConnection con,Logger log)
        {
            Log = log;
            Connection = new SqlConnection(con.ConnectionString);
           
            _dataManipulator = new MSCDataManipulator(con,log);
        }
        public override void StartLoader()
        {
            try
            {
               GetData();
            }
            catch (Exception)
            {
                throw;
            }
          
        }
        private void GetData()
        {
            try
            {
# if SPOTEST
                ((MSCDataManipulator)_dataManipulator).GetCruisesInfoSPOTEST();
#endif
                 _dataManipulator.GetData();
            }
            catch (Exception)
            {
                return;
            }
            
        }
    }
}
