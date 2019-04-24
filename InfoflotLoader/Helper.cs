using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace InfoflotLoader
{
    class ship
    {
        public int id;
        public string name;
        public ship(int pid, string pname)
        {
            id = pid;
            name = pname;
        }
        public override string ToString()
        {
            return name;
        }
    }
    class CruisePrice
    {
        public string name { get; set; }
        public string price { get; set; }
        public string price_eur { get; set; }
        public string price_usd { get; set; }
        public string places_total { get; set; }
        public string places_free { get; set; }
    }
    class Cruise
    {
        public string name { get; set; }
        public string date_start { get; set;}
        public string date_end { get; set; }
        public string nights { get; set; }
        public string days { get; set; }
        public string weekend { get; set; }
        public string cities { get; set; }
        public string route { get; set; }
        public string surchage_meal_rub { get; set; }
        public string surcharge_excursions_rub { get; set; }
        public Dictionary<string,CruisePrice> prices { get; set; }

    }
    
    class ItinaryStep
    {
        public string city { get; set; }
        public string date_start { get; set; }
        public string time_start { get; set; }
        public string date_end { get; set; }
        public string time_end { get; set; }
        public string description { get; set; }
        public Excursions[] additional_excursions { get; set; }
    }
    class Excursions
    {
        public string name { get; set; }
        public string description { get; set; }
        public string price { get; set; }
        public string currency { get; set; }
    }
    class Place
    {
        public string name { get; set; }
        public string type { get; set; }
        public string position { get; set; }
        public string status { get; set; }
    }

    class CabinStatus
    {
        public string name { get; set; }
        public string deck { get; set; }
        public string type { get; set; }
        public string price { get; set; }
        public string separate { get; set; }
        public string status { get; set; }
        public string gender { get; set; }
        public Dictionary<string, Place> places { get; set; }
    }

    class Plase
    {
        public int name { get; set; }
        public int type { get; set; }
        public int position{ get; set; }
    }
    class Cabin
    {
        public string name { get; set; }
        public string type { get; set; }
        public Plase[] places { get; set; }
    }
}
