using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace VodohodLoader
{
    class Ship
    {
        public string type { get; set; }
        public string name { get; set; }
        public string code { get; set; }
        public string image { get; set; }
        public string decks { get; set; }
        public string decks_pre { get; set; }
        public string description { get; set; }

    }

    class Cruise
    {
        public string motorship_id { get; set; }
        public string name { get; set; }
        public string days { get; set; }
        public string date_start { get; set; }
        public string date_stop { get; set; }
        public string availability_count { get; set; }
        public string[] directions { get; set; }
        
    }
    class ItineraryStep
    {
        public string day { get; set; }
        public string port { get; set; }
        public string time_start { get; set; }
        public string time_stop { get; set; }
        public string excursion { get; set; }
    }
    class Price
    {
        public string deck_name { get; set; }
        public string rt_name { get; set; }
        public string rp_name { get; set; }
        public string price_value { get; set; }
    }
    class Tariff
    {
        public string tariff_name { get; set; }
        public Price[] prices { get; set; }
    }


    class PriceList
    {
        public Tariff[] tariffs { get; set; }
        public dynamic room_availability { get; set; }
    }
}
