using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RAKIADocUpdate
{
    internal class Program
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        static void Main(string[] args)
        {
            log.Info("***********************APReadXMLUploadApp Start******************************");
            BLA _BL = null;

            try
            {
                //_BL = new BLA();
                eDocUtil.eDocEnDe clseDoc = new eDocUtil.eDocEnDe("Ejq9pF@2091");

                string DBServerName = ConfigurationManager.AppSettings["DBServerName"];               
                string DWServerName = ConfigurationManager.AppSettings["DWServerName"];
                string DBName = ConfigurationManager.AppSettings["DBName"];
                
                string DBUserName = ConfigurationManager.AppSettings["DBUserName"];
                string DBPassword = clseDoc.DecryptData(ConfigurationManager.AppSettings["DBPassword"]);
               
                string DWFileCabinet = ConfigurationManager.AppSettings["DWFileCabinet"];
                string DWUserName = clseDoc.DecryptData(ConfigurationManager.AppSettings["DWUserName"]);
                string DWPassword = clseDoc.DecryptData(ConfigurationManager.AppSettings["DWPassword"]);

                string PropertyName= ConfigurationManager.AppSettings["PropertyName"];

                _BL = new BLA(DBServerName,DWServerName,DBName, DBUserName, DBPassword,DWFileCabinet,DWUserName,DWPassword, PropertyName);
                _BL.UpdateKeyWordFieldCompForm();
                _BL.UpdateKeyWordFieldCompReport();
            }
            catch (Exception ex)
            {
                log.Error("Error in Main method. Exception: "+ex.Message);
            }
            finally
            {
                _BL.Dispose();
            }
        }
    }
}
