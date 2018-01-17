using System;
using System.Collections;
using System.Configuration;
using System.Diagnostics;
using System.Net.Mail;
using System.ServiceProcess;
using System.Xml;

namespace MSMQListner
{
    public partial class MSMQListner : ServiceBase
    {
        private Hashtable workerReferences = new Hashtable();
#if !DEBUG
        public static readonly string ServiceControlName = "QueueListner";
        public static readonly string PerformanceCategory = "Email Service";
#else
        public static readonly string ServiceControlName = "StagingQueueListner";
        public static readonly string PerformanceCategory = "Staging Email Service";
#endif
        
        public static readonly string ServiceControlDesc = "QueueListner Email service";
        // public variables that define the perfmon categories and description
        public static readonly string PerformanceCounterMsgTot = "Messages/Total";
        public static readonly string PerformanceCounterMsgSec = "Messages/Second";
        public static readonly string PerformanceCounterTotal = "_Total";

        // private variables that reference the service constants
#if DEBUG
        private string serviceName = MSMQListner.ServiceControlName;
#else
        private string serviceName = MSMQListner.ServiceControlName;
#endif
        
        private string serviceDesc = MSMQListner.ServiceControlDesc;
        private string perfCounterCatName = MSMQListner.PerformanceCategory;
        private string perfCounterSecName = MSMQListner.PerformanceCounterMsgSec;
        private string perfCounterTotName = MSMQListner.PerformanceCounterMsgTot;

        

        public MSMQListner()
        {
            CanPauseAndContinue = true;
            CanStop = true;
            ServiceName = serviceName;
            AutoLog = true;
            
            //InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            LogInformation("Service to Start");
            // register the perfmon counters
            PerfmonInstall();

            XmlDocument configXmlDoc;
            WorkerFormatter workerDefinition;

	        // access the XML configuration file and validate against the schema
            try
            {
                configXmlDoc = new XmlDocument();
                //Load the the document with the last book node.
                
                configXmlDoc.Load(ConfigurationSettings.AppSettings["ConfigurationFile"].ToString());
            }
            catch (Exception ex)
            {
                PerfmonUninstall();
                LogError("Invalid XML Configuration File: " + ex.Message);
                throw ex;
            }
            try
            {
                //LogInformation("File loaded and ready to add");
                foreach (XmlNode processXmlDefinition in configXmlDoc.GetElementsByTagName("ThreadDefinition"))
                {
                    workerDefinition = new WorkerFormatter();
                    string processName = processXmlDefinition.Attributes.GetNamedItem("ThreadName").Value;
                    workerDefinition.ProcessName = processName;
                    workerDefinition.NumberThreads = Convert.ToInt32(processXmlDefinition.Attributes.GetNamedItem("NumberThreads").Value);
                    // determine the transaction status of the processing
                    switch (Convert.ToBoolean(processXmlDefinition.Attributes.GetNamedItem("Transactions").Value))
                    {
                        case false:
                            workerDefinition.Transactions = WorkerFormatter.SFTransactions.NotRequired;
                            break;
                        case true:
                            workerDefinition.Transactions = WorkerFormatter.SFTransactions.Required;
                            break;
                        default:
                            throw new ApplicationException("Unknown Required Transaction State");
                    }
                    workerDefinition.ProcessDesc = processXmlDefinition.ChildNodes[0].InnerText;
                    workerDefinition.InputQueue = processXmlDefinition.ChildNodes[1].InnerText;
                    workerDefinition.ErrorQueue = processXmlDefinition.ChildNodes[2].InnerText;
                    workerDefinition.SmtpServer = processXmlDefinition.ChildNodes[3].InnerText;
                    workerDefinition.ErrorEmails = processXmlDefinition.ChildNodes[4].InnerText;
                    workerDefinition.Subject = processXmlDefinition.ChildNodes[5].InnerText;
                    workerDefinition.MessageLimit = int.Parse(processXmlDefinition.ChildNodes[6].InnerText);
                    workerDefinition.Delay = int.Parse(processXmlDefinition.ChildNodes[7].InnerText);
                    AddProcess(workerDefinition);
                   // LogInformation("adding: " + processName + " Threads: " + workerDefinition.NumberThreads); 
                }
            }
            catch (Exception ex)
            {
                PerfmonUninstall();
                LogError("Service Start Failed: " + ex.Message);
                workerReferences.Clear();
                throw ex;
            }
            // call the start method on each worker object
            foreach (WorkerInstance workerReference in workerReferences.Values)
            {
                try
                {
                    workerReference.StartService();
                }
                catch (Exception)
                {
                    // a start failed but continue with other creations
                    // one could decide to abort the start process at this point
                }
            }

            // indicate the service has started
            LogInformation("Service Started");
        }

        protected override void OnStop()
        {
            PerfmonInstall();
            SendPanicEmail();
            LogInformation("Service Stopped");

            // as all objects thrown away force a Garbage Collection
            GC.Collect();
        }

        protected override void OnPause()
        {
            // indicate the service has been paused
            LogInformation("Service Paused");

        }

        protected override void OnContinue()
        {

            // indicate the service has been resumed
            LogInformation("Service Resumed");
        }

        // Log event for recording informaiton messages
        private void LogInformation(string logMessage)
        {
            try
            {
                EventLog.WriteEntry(serviceDesc + ": " + logMessage, EventLogEntryType.Information);
            }
            catch (Exception)
            {
            }
        }

        // Log event for recording error messages
        private void LogError(string logMessage)
        {
            try
            {
                EventLog.WriteEntry(serviceDesc + ": " + logMessage, EventLogEntryType.Error);
            }
            catch (Exception)
            {
            }
        }

        // method to define the perfmon categories and counters
        private void PerfmonInstall()
        {
            try
            {
                // define the category and counter names instances to be defined on a worker process level
                if (!PerformanceCounterCategory.Exists(perfCounterCatName))
                {
                    CounterCreationDataCollection messagePerfCounters = new CounterCreationDataCollection();
                    messagePerfCounters.Add(new CounterCreationData(perfCounterTotName, "Total Messages Processed", PerformanceCounterType.NumberOfItems64));
                    messagePerfCounters.Add(new CounterCreationData(perfCounterSecName, "Messages Processed a Second", PerformanceCounterType.RateOfCountsPerSecond32));
                    //PerformanceCounterCategory.Create(perfCounterCatName, "MSDN Message Service Sample Counters", messagePerfCounters);
                    PerformanceCounterCategory.Create(perfCounterCatName, "MSMQ Email Service Sample Counters", PerformanceCounterCategoryType.Unknown, messagePerfCounters);
                    
                }
            }
            catch (Exception ex)
            {
                LogError("Cannot Register Perfmon Categories: " + ex.Message);
            }
        }

        // method to remove the custom perfmon numbers
        private void PerfmonUninstall()
        {
            try
            {
                PerformanceCounterCategory.Delete(perfCounterCatName);
            }
            catch (Exception ex)
            {
                LogError("Cannot UnRegister Perfmon Categories: " + ex.Message);
            }
        }

        // public method to add a new worker into the collection
        private void AddProcess(WorkerFormatter workerDefinition)
        {
            if (workerReferences != null)
            {
                // validate the name is unique
                string processName = workerDefinition.ProcessName;
                if (workerReferences.ContainsKey(processName))
                {
                    throw new ArgumentException("Process Name Must be Unique: " + processName);
                }

                // create a worker object in the worker array
                workerReferences.Add(processName, new WorkerInstance(workerDefinition));
            }
        }

        // send panic emails on ServiceStop
        private void SendPanicEmail()
        {
            MailMessage eMail = new MailMessage();
            SmtpClient sClient = new SmtpClient(ConfigurationSettings.AppSettings["Smtp"].ToString());
            LogInformation("Sending panic email");
            try
            {
                
                foreach (string s in ConfigurationSettings.AppSettings["ErrorEmails"].ToString().Split(new char[] { ';' }, StringSplitOptions.RemoveEmptyEntries))
                {
                    eMail.To.Add(s);
                }
                eMail.From = new MailAddress("service@youdomain.com", "Email Service");
                eMail.Priority = MailPriority.High;
                eMail.Body = "MSMQListner service is stopped";
                eMail.Subject = ConfigurationSettings.AppSettings["ErrorSubject"].ToString();

                sClient.Send(eMail);
                eMail.Dispose();
                sClient = null;

            }
            catch (Exception e)
            {
                bool success = false;
                int i = 0;
                while (i < 3)
                {
                    System.Threading.Thread.Sleep(1000);
                    try
                    {
                        sClient.Send(eMail);
                        success = true;
                        break;

                    }
                    catch (Exception ex)
                    {
                        i++;
                    }
                }
                if (success)
                {
                    LogInformation("Sent panic email after : " + i);
                }
                else
                {
                    LogError("Unable to send panic email, Error :" + e.Message);
                }
            }
        }

    }
}
