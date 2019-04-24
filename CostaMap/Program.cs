using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Net;

using System.Net.Mime;
using System.Text;
using BytesRoad.Net.Ftp;

namespace CostaMap
{
    class Program
    {
        static void Main(string[] args)
        {
            SqlConnection connection = new SqlConnection("Data Source=192.168.10.4;Initial Catalog=total_services;User ID=realcurses; pwd=qwerty654321; Timeout=30;");
            SqlCommand com = new SqlCommand(@"Select distinct itinerary,ITI_URL from cruises
                                            inner join cruises_costa as cr on package=C_CODE
                                            inner join itinerary_costa as it on cr.ITI_CODE=it.ITI_CODE
                                            Where itinerary is not null 
                                            and  ITI_URL is not null", connection);
            SqlDataAdapter ada = new SqlDataAdapter(com);
            DataTable dt = new DataTable();
            ada.Fill(dt);
            WebClient cl = new WebClient();
            FtpClient ftp = new FtpClient();
            ftp.PassiveMode = false;
            ftp.Connect(60000,"qa.mcruises.ru",21);
            ftp.Login(60000, "master_mcruises_maps", "KCj9ZXPkXRmCllQmLHVz7K8P9CHtF3DCXWQ6ztNx");
            FtpItem[] ftplist = ftp.GetDirectoryList(60000);
            List<string> ftpfilesname = new List<string>();
            foreach (FtpItem ftpItem in ftplist)
            {
                ftpfilesname.Add(ftpItem.Name);
            }
            foreach (DataRow row  in dt.Rows)
            {
                if (ftpfilesname.IndexOf(row["itinerary"] + ".jpg")>=0) continue;
                

                try
                {
                    byte[] buuf = cl.DownloadData(row["ITI_URL"].ToString());
                    Stream st = new MemoryStream(buuf);
                    ftp.PutFile(60000, row["itinerary"] + ".jpg",buuf);
                    
                }
                catch (Exception)
                {
                    
                   
               
                }
                
                

            }
        }
    }
}
