using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace PrincessLoader
{
    class Cruise
    {
        public int id_cruise;
        public string name;
        public string region;
        public string ship;
        public bool sold_to;
        public bool hit_sales;
        public bool drop_price;
        public bool last_stateroom;
        public bool newship;
        public string category;
        public string information;
    }
    class DayItinerary
    {
        public int cruise_id;
        public int day_number;
        public string location;
        public string arrival;
        public string depature;
    }

    class Price
    {
        public int cruise_stert_id;
        public string name;
        public string category ;
        public decimal price;
        public string currensy;
    }
    class CruiseStartDay
    {
        public int cruise_id;
        public int cruise_start_id;
        public string datestr;
        public DateTime date;
        public string curency;
        public decimal price;
        public string comment;
        public decimal port_fee;
        public decimal tax;
        public bool group;
        public bool Guide;
    }
    
}
