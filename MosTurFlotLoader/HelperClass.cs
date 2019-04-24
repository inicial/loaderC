using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MosTurFlotLoader
{
    class ShipRequest
    {
        public string createdate { get; set; }
        public string version { get; set; }
        public string status { get; set; }
        public Ship answer { get; set; }
        public dynamic request { get; set; }
        public dynamic errors { get; set; }
    }

    class RoutDetailRequest
    {
        public string createdate { get; set; }
        public string version { get; set; }
        public string status { get; set; }
        public Cruise answer { get; set; }
        public dynamic request { get; set; }
        public dynamic errors { get; set; }
    }
    class Ship
    {
        public string shipid { get; set; }
        public string shipname { get; set; }
        public string shipown { get; set; }
        public string shipdesc { get; set; }
        public string shipclass { get; set; }
        public string shipdeckplan { get; set; }
        public string shiptitleimage { get; set; }
        public string shipcabinsqty { get; set; }
        public string shiptoursqty { get; set; }
        public string shiptourmindate { get; set; }
        public string shiptourmaxdate { get; set; }
        public Dictionary<string,Cabin> shipcabins { get; set; }
    }
    class Cabin
    {
        public string cabinnumber { get; set; }
        public string cabincategoryid { get; set; }
        public string cabincategoryname { get; set; }
        public string cabinclass { get; set; }
        public string cabindesc { get; set; }
        public string cabinmainpass { get; set; }
        public string cabinupperpass { get; set; }
        public string cabinadvpass { get; set; }
        public string cabinmaxpass { get; set; }
        public dynamic cabinimages { get; set; }
    }
    class Request
    {
        public string method { get; set; }
        public string format { get; set; }
        public string userhash { get; set; }
        public string selection { get; set; }
        public string request { get; set; }
        public string routedetail { get; set; }
        public string tariffs { get; set; }
        public string loading { get; set; }
    }
    class HelperClass
    {
        public string createdate { get; set; }
        public string version { get; set; }
        public string status { get; set; }
        public Dictionary<string, Cruise> answer { get; set; } 
        public Request request { get; set;}
        public dynamic errors { get; set; }
        public dynamic warnings { get; set; }
    }
    class RouteDetail
    {
        public string pointname { get; set; }
        public string cityid { get; set; }
        public string cityname { get; set; }
        public string arrival { get; set; }
        public string departure { get; set; }
        public string note { get; set; }
        public string date { get; set; }
        public Dictionary<string,Excursion> excursions { get; set; }
    }
    class Excursion
    {
        public string desc { get; set; }
        public string type { get; set; }
        public string typename { get; set; }
    }
    class TourTariff
    {
        public string categoryid { get; set; }
        public string categoryname { get; set; }
        public string categoryminprice { get; set; }
        public string categorynote { get; set; }
        public Dictionary<string,CategoryTariffs> categorytariffs { get; set; } 
        public string tariffminprice { get; set; }
    }

    class  CategoryTariffs
    {
        public string tariffid { get; set; }
        public string tariffname { get; set; }
        public string tariffminprice { get; set; }
        public string tariffpassqty { get; set; }
        public Dictionary<string, Meal> meals { get; set; } 
    }

    class Meal
    {
        public string mealid { get; set; }
        public string mealname { get; set; }
        public string mainprice { get; set; }
        public string upperprice { get; set; }
        public string advprice { get; set; }
    }

    class Cruise
    {
        public string tourid { get; set; }
        public string shipid { get; set; }
        public string shipname { get; set; }
        public string shipown { get; set; }
        public string tourstart { get; set; }
        public string tourfinish { get; set; }
        public string tourroute { get; set; }
        public string tourdays { get; set; }
        public string tourholiday { get; set; }
        public string touronline { get; set; }
        public string tourminprice { get; set; }
        public string tourdiscount { get; set; }
        public string tourdiscountext { get; set; }
        public Dictionary<string,RouteDetail> tourroutedetail { get; set; }
        public Dictionary<string,TourTariff> tourtariffs { get; set; }
        public string tourcabinstotal { get; set; }
        public string tourcabinsbusy { get; set; }
        public string tourcabinsfree { get; set; }
    }


}
