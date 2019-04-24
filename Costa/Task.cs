using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Costa
{
    public class Task
    {
        private String id;

        private String type;

        private DateTime date;

        public string Id { get { return id; } set { id = value; } }
        public string Type { get { return type; } set { type = value; } }
        public DateTime Date { get { return date; } set { date = value; } }

        public Task()
        {

        }

        public Task(String id, DateTime date, String type = null)
        {
            this.id = id;
            this.type = type;
            this.date = date;
        }

        public Task(String id, String type = null) : this (id, DateTime.Now, type)
        {
        }
    }
}
