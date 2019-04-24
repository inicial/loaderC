using System;
using System.Collections.Generic;
using System.ServiceProcess;
using System.Text;

namespace MSCPack
{
    static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        static void Main()
        {
           // ServiceBase[] ServicesToRun;
           // ServicesToRun = new ServiceBase[] 
           // { 
           //     new Service1() 
           // };
           // ServiceBase.Run(ServicesToRun);
            Service1 service1 = new Service1();
            service1.OnStart();
        }
    }
}
