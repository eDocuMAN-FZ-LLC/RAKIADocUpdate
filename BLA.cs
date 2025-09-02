using DocuWare.Platform.ServerClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace RAKIADocUpdate
{
    internal class BLA : IDisposable
    {
        string _DBServerName;
        string _DWServerName;
        string _DBName;
        string _DBUserName;
        string _DBPassword;
        string _DWFileCabinet;
        string _DWUserName;
        string _DWPassword;
        string _PropertyName;

        private SqlConnection conn;

        ServiceConnection dwConn;
        Organization dwOrg;
        FileCabinet dwFileCabinet;
        DocuWare.Platform.ServerClient.Dialog Dia;

        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        

        public BLA(string DBServerName, string DWServerName, string DBName, string DBUserName, string DBPassword, string DWFileCabinet, string DWUserName, string DWPassword,string PropertyName)
        {
            try
            {
                _DBServerName = DBServerName;
                _DWServerName = DWServerName;
                _DBName = DBName;
                _DBUserName = DBUserName;
                _DBPassword = DBPassword;
                _DWFileCabinet = DWFileCabinet;
                _DWUserName = DWUserName;
                _DWPassword = DWPassword;
                _PropertyName = PropertyName;

                conn = new SqlConnection("Data Source= " + _DBServerName + "; Initial Catalog = " + _DBName + "; User ID=" + _DBUserName + ";Password=" + _DBPassword + ";");
                conn.Open();
                log.Debug("Database Connection Opened Successfully");

                Uri dwurl = new Uri("http://" + _DWServerName + "/docuware/platform");
                dwConn = ServiceConnection.Create(dwurl, _DWUserName, _DWPassword);
                dwOrg = dwConn.Organizations[0];
                var fileCabinets = dwOrg.GetFileCabinetsFromFilecabinetsRelation().FileCabinet;
                dwFileCabinet = fileCabinets.Where(f => f.Name.ToUpper() == _DWFileCabinet.ToUpper()).First<FileCabinet>();
                var dialogInfoItems = dwFileCabinet.GetDialogInfosFromSearchesRelation();
                Dia = dialogInfoItems.Dialog[0].GetDialogFromSelfRelation();
            }
            catch (Exception ex)
            {

                log.Error("Error in constructor. Exception: " + ex.Message); 
            }
        }//public BLA(string DBServerName, string DWServerName, string DBName, string DBUserName, string DBPassword, string DWFileCabinet, string DWUserName, string DWPassword,string PropertyName)

        public void UpdateKeyWordFieldCompForm()
        {
            log.Debug("***************************************************************************Start Process to link CompForm to Comp Report*****************************************************************************************************");
            try
            {
                List<string> propertyList = new List<string>();
                propertyList = PropertyInfo(_PropertyName);

                int propertyCount = propertyList.Count;

                log.Debug("Property Count: " + propertyCount);

                int propertyCountValue = 1;

                foreach (var prop in propertyList)
                {

                    try
                    {
                        log.Debug("Property: " + prop + " PropertyCount: " + propertyCountValue);
                        string MainSelectSQL = "select document_type,property, audit_date, document_status,[CASE],dwdocid,ARRIVAL_DATE,DEPARTURE_DATE  from [dwdata].[dbo].[INCOME_AUDIT_PROCESS] where document_type='Comp Room Request Form'  and property='" + prop + "' and [CASE]='False' and (islinked is null or islinked='False') order by dwdocid desc";
                        using (DataSet CompFormDataSet = new DataSet())
                        {
                            using (SqlDataAdapter CompFormDataAdapter = new SqlDataAdapter(MainSelectSQL, conn))
                            {
                                CompFormDataAdapter.Fill(CompFormDataSet, "myTable");

                                if (CompFormDataSet.Tables.Count > 0)
                                {
                                    log.Debug("Number of Comp Form: " + CompFormDataSet.Tables[0].Rows.Count);
                                    if (CompFormDataSet.Tables[0].Rows.Count > 0)
                                    {
                                        log.Debug("Number of Comp Form: " + CompFormDataSet.Tables[0].Rows.Count);

                                        for (int i = 0; i < CompFormDataSet.Tables[0].Rows.Count; i++)
                                        {

                                            try
                                            {
                                                List<string> _dwDocIDList = new List<string>();

                                                log.Debug("Arrival Date: " + CompFormDataSet.Tables[0].Rows[i]["ARRIVAL_DATE"].ToString());
                                                log.Debug("Departure Date: " + CompFormDataSet.Tables[0].Rows[i]["DEPARTURE_DATE"].ToString());

                                                string dwCompFormDocID = CompFormDataSet.Tables[0].Rows[i]["DWDOCID"].ToString();

                                                log.Debug("Comp form: " + dwCompFormDocID);

                                                string CompFormdatetimeArrivalVal = CompFormDataSet.Tables[0].Rows[i]["ARRIVAL_DATE"].ToString();
                                                string CompFormdatetimeDepartureVal = CompFormDataSet.Tables[0].Rows[i]["DEPARTURE_DATE"].ToString();

                                                string[] CompFormdatetimeArrivalVal1 = CompFormdatetimeArrivalVal.Split(' ');
                                                string[] CompFormdatetimeArrivalVal2 = CompFormdatetimeArrivalVal1[0].Split('/');
                                                //string ddArrival = CompFormdatetimeArrivalVal2[0].ToString();//test
                                                //string mmArrival = CompFormdatetimeArrivalVal2[1].ToString();//test
                                                                                                             string ddArrival = CompFormdatetimeArrivalVal2[1].ToString();//original
                                                                                                             string mmArrival = CompFormdatetimeArrivalVal2[0].ToString();//original
                                                string yyArrival = CompFormdatetimeArrivalVal2[2].ToString();

                                                log.Debug("ddArrival: " + ddArrival + " mmArrival: " + mmArrival + " yyArrival: " + yyArrival);

                                                string[] CompFormdatetimeDepartureVal1 = CompFormdatetimeDepartureVal.Split(' ');
                                                string[] CompFormdatetimeDepartureVal2 = CompFormdatetimeDepartureVal1[0].Split('/');
                                                //string ddDeparture = CompFormdatetimeDepartureVal2[0].ToString();//test
                                                //string mmDeparture = CompFormdatetimeDepartureVal2[1].ToString();//test
                                                                                                                 string ddDeparture = CompFormdatetimeDepartureVal2[1].ToString();//original
                                                                                                                 string mmDeparture = CompFormdatetimeDepartureVal2[0].ToString();//original
                                                string yyDeparture = CompFormdatetimeDepartureVal2[2].ToString();

                                                log.Debug("ddDeparture: " + ddDeparture + " mmDeparture: " + mmDeparture + " yyDeparture: " + yyDeparture);

                                                string arrivalDateVal = yyArrival + "-" + mmArrival + "-" + ddArrival;
                                                string departureDateVal = yyDeparture + "-" + mmDeparture + "-" + ddDeparture;
                                                //string[] dateVal=datetimeVal.Split(' ');
                                                //string[] dateVal1 = dateVal[0].Split('/');
                                                string sqlQry = string.Empty;

                                                if (arrivalDateVal == departureDateVal)
                                                {
                                                    sqlQry = "select document_type,property, audit_date, document_status, ARRIVAL_DATE,DEPARTURE_DATE,DWDOCID from [dwdata].[dbo].[INCOME_AUDIT_PROCESS] where document_type='Comp & House Report' and (audit_date between '" + arrivalDateVal + "' and '" + departureDateVal + "')  and property='" + prop + "' and [CASE]='True'";
                                                }
                                                else
                                                {
                                                    sqlQry = "select document_type,property, audit_date, document_status, ARRIVAL_DATE,DEPARTURE_DATE,DWDOCID from [dwdata].[dbo].[INCOME_AUDIT_PROCESS] where document_type='Comp & House Report' and (audit_date between '" + arrivalDateVal + "' and '" + departureDateVal + "') and audit_date < '" + departureDateVal + "'  and property='" + prop + "' and [CASE]='True'";
                                                }

                                                log.Debug("Query: " + sqlQry);

                                                string dwdocIDsCompReport = string.Empty;

                                                using (DataSet CompReportDataSet = new DataSet())
                                                {
                                                    using (SqlDataAdapter CompReportDa = new SqlDataAdapter(sqlQry, conn))
                                                    {
                                                        CompReportDa.Fill(CompReportDataSet, "myTableCompForm");

                                                        if (CompReportDataSet.Tables.Count > 0)
                                                        {
                                                            log.Debug("Number of Comp Report: " + CompReportDataSet.Tables[0].Rows.Count);
                                                            if (CompReportDataSet.Tables[0].Rows.Count > 0)
                                                            {
                                                                log.Debug("Number of Comp Report: " + CompReportDataSet.Tables[0].Rows.Count);

                                                                for (int j = 0; j < CompReportDataSet.Tables[0].Rows.Count; j++)
                                                                {
                                                                    _dwDocIDList.Add(CompReportDataSet.Tables[0].Rows[j]["dwdocid"].ToString());
                                                                    log.Debug("Comp Report: " + CompReportDataSet.Tables[0].Rows[j]["dwdocid"].ToString());
                                                                    log.Debug("Count: " + j + " Audit Date: " + CompReportDataSet.Tables[0].Rows[j]["audit_date"].ToString());
                                                                    log.Debug("Audit Date: " + CompReportDataSet.Tables[0].Rows[j]["audit_date"].ToString() + " Arrival Date: " + arrivalDateVal + " Departure Date: " + departureDateVal);
                                                                    dwdocIDsCompReport = CompReportDataSet.Tables[0].Rows[j]["dwdocid"].ToString() + "," + dwdocIDsCompReport;
                                                                }

                                                            }
                                                        }

                                                    }
                                                }//using (DataSet CompReportDataSet = new DataSet())

                                                dwdocIDsCompReport = dwdocIDsCompReport.TrimEnd(',');

                                                var q = new DialogExpression()
                                                {
                                                    Operation = DialogExpressionOperation.And,
                                                    Condition = new List<DialogExpressionCondition>()
                                                        {
                                                            DialogExpressionCondition.Create("DWDOCID",dwCompFormDocID)
                                                        }
                                                };

                                                var queryResult = Dia.Query.PostToDialogExpressionRelationForDocumentsQueryResult(q);



                                                if (_dwDocIDList != null)
                                                {


                                                    log.Debug("DocIDList is not null");
                                                    foreach (var d in queryResult.Items)
                                                    {
                                                        //Console.WriteLine("Hit {0}: \"{1}\" on {2}", d.Id, (d["SENDER"].Item as string) ?? "-", d.CreatedAt);
                                                        var fieldsFinal = new DocumentIndexFields()
                                                        {
                                                            Field = new List<DocumentIndexField>()
                                                        {
                                                            DocumentIndexField.Create("ISLINKED","True"),
                                                            DocumentIndexField.Create("DOCIDLINK",
                                                            new DocumentIndexFieldKeywords() {
                                                                                                Keyword = _dwDocIDList
                                                                                             }),
                                                        }
                                                        };

                                                        var resultFinalVal = d.PutToFieldsRelationForDocumentIndexFields(fieldsFinal);
                                                    }//foreach (var d in queryResult.Items)

                                                    try
                                                    {
                                                        SqlCommand cmdForNull = new SqlCommand("UPDATE [dwdata].[dbo].[INCOME_AUDIT_PROCESS] SET islinked = 'True'  where dwdocid in (" + dwdocIDsCompReport + ") ", conn);
                                                        cmdForNull.ExecuteNonQuery();
                                                    }
                                                    catch (Exception ex)
                                                    {

                                                        log.Error("DB update failed. Exception: "+ex.Message+" DocID: "+ CompFormDataSet.Tables[0].Rows[i]["DWDOCID"].ToString());
                                                        var q1 = new DialogExpression()
                                                        {
                                                            Operation = DialogExpressionOperation.And,
                                                            Condition = new List<DialogExpressionCondition>()
                                                        {
                                                            DialogExpressionCondition.Create("DWDOCID",dwCompFormDocID)
                                                        }
                                                        };

                                                        var queryResult1 = Dia.Query.PostToDialogExpressionRelationForDocumentsQueryResult(q1);

                                                        foreach (var dd in queryResult1.Items)
                                                        {
                                                            var fieldsFinal = new DocumentIndexFields()
                                                            {
                                                                Field = new List<DocumentIndexField>()
                                                        {
                                                            DocumentIndexField.Create("ISLINKED","False"),

                                                        }
                                                            };
                                                            var resultFinalVal = dd.PutToFieldsRelationForDocumentIndexFields(fieldsFinal);
                                                        }
                                                            
                                                    }

                                                    

                                                }//if(_dwDocIDList != null)
                                                else
                                                {
                                                    log.Debug("DocIDList is null");
                                                }
                                            }
                                            catch (Exception ex)
                                            {

                                                log.Error("Error in loop for comp form. DocID: "+ CompFormDataSet.Tables[0].Rows[i]["DWDOCID"].ToString());
                                            }
                                            


                                        }//for (int i = 0;i< PropertyNamesDataSet.Tables[0].Rows.Count;i++)                                                               
                                    }//if (CompFormDataSet.Tables[0].Rows.Count > 0)
                                }//if (CompFormDataSet.Tables.Count > 0)
                            }//using (SqlDataAdapter CompFormDataAdapter = new SqlDataAdapter(MainSelectSQL, conn))
                        }//using (DataSet CompFormDataSet = new DataSet())
                    }
                    catch (Exception ex)
                    {

                        log.Error("Error in loop for property: "+ prop);
                    }
                    
                    propertyCountValue++;
                }//foreach (var prop in propertyList)
            }
            catch (Exception ex)
            {
                log.Error("Error in method UpdateKeyWordFieldCompForm. Exception: " + ex.Message);
            }
            log.Debug("***************************************************************************End Process to link CompForm to Comp Report*****************************************************************************************************");
        }// public void UpdateKeyWordField()

        public void UpdateKeyWordFieldCompReport()
        {
            log.Debug("***************************************************************************Start Process to link Comp Report to CompForm*****************************************************************************************************");
            try
            {
                List<string> propertyList = new List<string>();
                propertyList = PropertyInfo(_PropertyName);

                int propertyCount = propertyList.Count;

                log.Debug("Property Count: " + propertyCount);

                int propertyCountValue = 1;

                foreach (var prop in propertyList)
                {
                    log.Debug("Property: " + prop+" PropertyCount: "+ propertyCountValue);

                    try
                    {
                        string MainSelectSQL = "select document_type,property, audit_date, document_status, ARRIVAL_DATE,DEPARTURE_DATE,DWDOCID from [dwdata].[dbo].[INCOME_AUDIT_PROCESS] where document_type='Comp & House Report' and property='" + prop + "' and (islinked is null or islinked='False') and [case]='True'";
                        //string MainSelectSQL = "select document_type,property, audit_date, document_status, ARRIVAL_DATE,DEPARTURE_DATE,DWDOCID from [dwdata].[dbo].[INCOME_AUDIT_PROCESS] where document_type='Comp & House Report' and property='" + prop + "' and (islinked is null or islinked='False') and [case]='True' and dwdocid=1359295";
                        using (DataSet CompReportDataSet = new DataSet())
                        {
                            using (SqlDataAdapter CompReportDataAdapter = new SqlDataAdapter(MainSelectSQL, conn))
                            {
                                CompReportDataAdapter.Fill(CompReportDataSet, "myTable");

                                if (CompReportDataSet.Tables.Count > 0)
                                {
                                    log.Debug("Number of Comp Form: " + CompReportDataSet.Tables[0].Rows.Count);
                                    if (CompReportDataSet.Tables[0].Rows.Count > 0)
                                    {
                                        log.Debug("Number of Comp Form: " + CompReportDataSet.Tables[0].Rows.Count);

                                        for (int i = 0; i < CompReportDataSet.Tables[0].Rows.Count; i++)
                                        {
                                            string dwdocidCompReport = CompReportDataSet.Tables[0].Rows[i]["dwdocid"].ToString();
                                            log.Debug("Comp Report doc id: " + dwdocidCompReport);

                                            try
                                            {
                                                string CompReportdatetimeAuditVal = CompReportDataSet.Tables[0].Rows[i]["audit_date"].ToString();

                                                log.Debug("Comp Report Audit Date: " + CompReportdatetimeAuditVal);

                                                string[] CompReportdatetimeAuditVal1 = CompReportdatetimeAuditVal.Split(' ');
                                                string[] CompReportdatetimeAuditVal2 = CompReportdatetimeAuditVal1[0].Split('/');
                                                //string ddAudit = CompReportdatetimeAuditVal2[0].ToString();//test
                                                //string mmAudit = CompReportdatetimeAuditVal2[1].ToString();//test
                                                string ddAudit = CompReportdatetimeAuditVal2[1].ToString();//Original
                                                string mmAudit = CompReportdatetimeAuditVal2[0].ToString();//Original
                                                string yyAudit = CompReportdatetimeAuditVal2[2].ToString();

                                                log.Debug("ddArrival: " + ddAudit + " mmArrival: " + mmAudit + " yyArrival: " + yyAudit);



                                                string auditDateVal = yyAudit + "-" + mmAudit + "-" + ddAudit;


                                                //string sqlQry = "select document_type,property, audit_date, document_status, ARRIVAL_DATE,DEPARTURE_DATE,DWDOCID from [dwdata].[dbo].[INCOME_AUDIT_PROCESS] where document_type='Comp Room Request Form' and ('" + auditDateVal + "' between ARRIVAL_DATE and DEPARTURE_DATE) and DEPARTURE_DATE> '" + auditDateVal + "' and property='" + prop + "'";
                                                string sqlQry = "select document_type,property, audit_date, document_status, ARRIVAL_DATE,DEPARTURE_DATE,DWDOCID from [dwdata].[dbo].[INCOME_AUDIT_PROCESS] where document_type='Comp Room Request Form' and ('" + auditDateVal + "' between ARRIVAL_DATE and DEPARTURE_DATE) and (DEPARTURE_DATE> '" + auditDateVal + "' or (DEPARTURE_DATE='" + auditDateVal + "' and ARRIVAL_DATE=DEPARTURE_DATE)) and property='" + prop + "'";

                                                log.Debug("Query:" + sqlQry);

                                                using (DataSet CompFormDataSet = new DataSet())
                                                {
                                                    using (SqlDataAdapter CompFormDataAdapter = new SqlDataAdapter(sqlQry, conn))
                                                    {
                                                        CompFormDataAdapter.Fill(CompFormDataSet, "myTable");

                                                        if (CompFormDataSet.Tables.Count > 0)
                                                        {
                                                            log.Debug("Number of Comp Form: " + CompFormDataSet.Tables[0].Rows.Count);
                                                            if (CompFormDataSet.Tables[0].Rows.Count > 0)
                                                            {
                                                                log.Debug("Number of Comp Form: " + CompFormDataSet.Tables[0].Rows.Count);

                                                                for (int j = 0; j < CompFormDataSet.Tables[0].Rows.Count; j++)
                                                                {
                                                                    string dwdocidCompForm = CompFormDataSet.Tables[0].Rows[i]["dwdocid"].ToString();

                                                                    log.Debug("Comp Form doc id: " + dwdocidCompForm);

                                                                    var q = new DialogExpression()
                                                                    {
                                                                        Operation = DialogExpressionOperation.And,
                                                                        Condition = new List<DialogExpressionCondition>()
                                                        {
                                                            DialogExpressionCondition.Create("DWDOCID",dwdocidCompForm)
                                                        }
                                                                    };

                                                                    var queryResult = Dia.Query.PostToDialogExpressionRelationForDocumentsQueryResult(q);

                                                                    foreach (var d in queryResult.Items)
                                                                    {
                                                                        List<string> keyList = new List<string>();
                                                                        Document document = d.GetDocumentFromSelfRelation();
                                                                        foreach (DocumentIndexField field in document.Fields)
                                                                        {
                                                                            if (field.ItemElementName.ToString() == "Keywords")
                                                                            {
                                                                                if (field.FieldName == "DOCIDLINK")
                                                                                {
                                                                                    DocumentIndexFieldKeywords keywords = (DocumentIndexFieldKeywords)field.Item;
                                                                                    int keywordVal = keywords.Keyword.Count;

                                                                                    log.Debug("Keyword Count: " + keywordVal);

                                                                                    foreach (var key in keywords.Keyword)
                                                                                    {
                                                                                        keyList.Add(key.ToString());
                                                                                        log.Debug("Keyword Value: " + key.ToString());
                                                                                    }

                                                                                    //Console.WriteLine(field.FieldName);
                                                                                }

                                                                            }
                                                                        }
                                                                        keyList.Add(dwdocidCompReport);
                                                                        log.Debug("Keyword Last Value: " + dwdocidCompReport);

                                                                        var fieldsFinal = new DocumentIndexFields()
                                                                        {
                                                                            Field = new List<DocumentIndexField>()
                                                                            {
                                                                                DocumentIndexField.Create("ISLINKED","True"),
                                                                                DocumentIndexField.Create("DOCIDLINK",
                                                                                new DocumentIndexFieldKeywords() {
                                                                                                                    Keyword = keyList
                                                                                                                  }),
                                                                             }
                                                                        };

                                                                        var resultFinalVal = d.PutToFieldsRelationForDocumentIndexFields(fieldsFinal);

                                                                        try
                                                                        {
                                                                            SqlCommand cmdForNull = new SqlCommand("UPDATE [dwdata].[dbo].[INCOME_AUDIT_PROCESS] SET islinked = 'True'  where dwdocid in (" + dwdocidCompReport + ") ", conn);
                                                                            cmdForNull.ExecuteNonQuery();
                                                                        }
                                                                        catch (Exception ex)
                                                                        {

                                                                            log.Error("Error in DB update: "+ dwdocidCompReport+" Exception: "+ex.Message);

                                                                            var q1 = new DialogExpression()
                                                                            {
                                                                                Operation = DialogExpressionOperation.And,
                                                                                Condition = new List<DialogExpressionCondition>()
                                                        {
                                                            DialogExpressionCondition.Create("DWDOCID",dwdocidCompForm)
                                                        }
                                                                            };

                                                                            var queryResult1 = Dia.Query.PostToDialogExpressionRelationForDocumentsQueryResult(q1);

                                                                            foreach (var d1 in queryResult1.Items)
                                                                            {
                                                                                var fieldsFinal1 = new DocumentIndexFields()
                                                                                {
                                                                                    Field = new List<DocumentIndexField>()
                                                                            {
                                                                                DocumentIndexField.Create("ISLINKED","False"),
                                                                                
                                                                             }
                                                                                };

                                                                                var resultFinalVal1 = d.PutToFieldsRelationForDocumentIndexFields(fieldsFinal1);
                                                                            }
                                                                            }

                                                                        
                                                                    }//foreach (var d in queryResult.Items)
                                                                }//for (int j = 0; j < CompFormDataSet.Tables[0].Rows.Count; j++)
                                                            }//if (CompFormDataSet.Tables[0].Rows.Count > 0)
                                                        }//if (CompFormDataSet.Tables.Count > 0)
                                                    }//using (SqlDataAdapter CompFormDataAdapter = new SqlDataAdapter(MainSelectSQL, conn))
                                                }//using (DataSet CompFormDataSet = new DataSet())
                                            }
                                            catch (Exception ex)
                                            {

                                                log.Error("Error in loop compreport: "+ dwdocidCompReport+" Exception: "+ex.Message);
                                            }

                                            
                                        }//for (int i = 0; i < CompReportDataSet.Tables[0].Rows.Count; i++)
                                    }//if (CompReportDataSet.Tables[0].Rows.Count > 0)
                                }//if (CompReportDataSet.Tables.Count > 0)
                            }//using (SqlDataAdapter CompReportDataAdapter = new SqlDataAdapter(MainSelectSQL, conn))
                        }// using (DataSet CompReportDataSet = new DataSet())
                    }
                    catch (Exception ex)
                    {

                        log.Error("Error in property loop: "+ prop+" Exception: "+ex.Message);
                    }

                    
                    propertyCountValue++;
                }//foreach (var prop in propertyList)
            }
            catch (Exception ex)
            {

                log.Error("Error in method UpdateKeyWordFieldCompReport. Exception: " + ex.Message); ;
            }
            log.Debug("***************************************************************************End Process to link Comp Report to CompForm*****************************************************************************************************");
        }


            private List<string> PropertyInfo(string property)
        {
            List<string> propertyList = new List<string>();
            try
            {
                string query = "";
                if (property == "NA") 
                {
                    query = "SELECT * FROM [APLog].[dbo].[IncomeAuditDateVal] where status='ACTIVE'";
                }
                else
                {
                    query = "SELECT * FROM [APLog].[dbo].[IncomeAuditDateVal] where status='ACTIVE' and property='"+ property + "'";
                }

                using (DataSet CompReportDataSet = new DataSet())
                {
                    using (SqlDataAdapter CompReportDa = new SqlDataAdapter(query, conn))
                    {
                        CompReportDa.Fill(CompReportDataSet, "myTableCompForm");

                        if (CompReportDataSet.Tables.Count > 0)
                        {

                            if (CompReportDataSet.Tables[0].Rows.Count > 0)
                            {
                                for (int j = 0; j < CompReportDataSet.Tables[0].Rows.Count; j++)
                                {
                                    propertyList.Add(CompReportDataSet.Tables[0].Rows[j]["property"].ToString());
                                }

                            }
                        }

                    }
                }
            }
            catch (Exception ex)
            {

                log.Error("Error in PropertyInfo. Exception: " + ex.Message);
            }

            return propertyList;
        }

        private bool disposedValue;

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    if (conn != null)
                    {
                        if (conn.State == ConnectionState.Open)
                        {

                            conn.Close();
                        }
                        conn.Dispose();
                    }

                    if (dwConn != null)
                    {
                        dwConn.Disconnect();
                    }
                    // TODO: dispose managed state (managed objects)
                }

                // TODO: free unmanaged resources (unmanaged objects) and override finalizer
                // TODO: set large fields to null
                disposedValue = true;
            }
        }

        // // TODO: override finalizer only if 'Dispose(bool disposing)' has code to free unmanaged resources
        // ~BLA()
        // {
        //     // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        //     Dispose(disposing: false);
        // }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
