using System;
using System.Diagnostics;
using System.Collections;
using System.Threading;
using System.Net.Mail;

namespace MSMQListner
{
    internal class WorkerInstance 
    {
        // private flags to indicate the state of the service worker object
        private bool workerRunning, workerPaused;

        // descriptions needed for logging
        private string serviceDesc, workerDesc;

        // private access to the definition structure
        private WorkerFormatter workerDefinition;

        // array list to hold the references to the worker thread objects
        private ArrayList threadReferences;

        // the event logging class variables
        private EventLog eventLog;
        private bool eventLogCreated = false;
        private string eventLogName = "Application";
        private string eventLogSource = MSMQListner.ServiceControlName;

        // the work object constructor that validates and saves the constructor defintion
        public WorkerInstance(WorkerFormatter workerFormatter)
        {
            // save the constructor definition and indicate process starting
            workerDefinition = workerFormatter;
            workerDefinition.ProcessStatus = WorkerFormatter.SFProcessStatus.StatusInitialized;

            // indicate the worker class has not been started or paused
            workerRunning = false;
            workerPaused = false;

            // validate the number of threads is within an acceptable range
            if (workerDefinition.NumberThreads <= 0 || workerDefinition.NumberThreads > 255)
            {
                throw new ArgumentException("Must have a postive value for the number of threads");
            }

            // validate the worker object has been correctly assigned a name
            if (workerDefinition.ProcessName == null)
            {
                throw new ArgumentException("Must have a non-null value for the process name");
            }
            // ensure that the description has a value
            if (workerDefinition.ProcessDesc == null)
            {
                workerDefinition.ProcessDesc = workerDefinition.ProcessName;
            }

            // validate the input and error queue path has been passed
            if (workerDefinition.InputQueue == null || workerDefinition.ErrorQueue == null)
            {
                throw new ArgumentException("Must have a non-null value for the queues");
            }

            
            // save a reference to the parent service and worker description
            serviceDesc = MSMQListner.ServiceControlDesc;
            workerDesc = workerDefinition.ProcessDesc;

        }


        // destructor for the class
        // if the service shuts down without calling the stop method it is gracefully terminated
        ~WorkerInstance()
        {
            StopService();
        }


        // the work object start method that creates all the thread objects
        // each thread object then has it corresponding start method called
        internal void StartService()
        {
            // ensure the start method has not already been called
            if (!workerRunning)
            {
                // flag the worker as started
                workerRunning = true;
                workerDefinition.ProcessStatus = WorkerFormatter.SFProcessStatus.StatusStarted;

                // attempt to allocate all needed resources
                try
                {
                    // call the event logging initilization class
                    CreateLogClass();

                    // indicate the worker process has started
                    LogInformation("Process Starting - Queue " + workerDefinition.InputQueue);

                    // create the array list for the thread objects
                    // insert into the array list the required number of thread objects
                    threadReferences = new ArrayList();

                    // for each worker create and start the appropriate worker threads
                    for (int idx = 0; idx < workerDefinition.NumberThreads; idx++)
                    {
                        WorkerThreadFormatter threadDefinition = new WorkerThreadFormatter();
                        threadDefinition.ProcessName = workerDefinition.ProcessName;
                        threadDefinition.ProcessDesc = workerDefinition.ProcessDesc;
                        threadDefinition.ThreadNumber = idx;
                        threadDefinition.InputQueue = workerDefinition.InputQueue;
                        threadDefinition.ErrorQueue = workerDefinition.ErrorQueue;
                        threadDefinition.SmtpServer = workerDefinition.SmtpServer;
                        threadDefinition.ErrorEmails = workerDefinition.ErrorEmails;
                        threadDefinition.Subject = workerDefinition.Subject;
                        threadDefinition.MessageLimit = workerDefinition.MessageLimit;
                        threadDefinition.Delay = workerDefinition.Delay;
                     

                        // define the worker type and insert into the work thread struct
                        WorkerThread workerThread = new WorkerThreadDerived(this, threadDefinition);
                                               
                        threadReferences.Insert(idx, workerThread);
                    }

                    // call the start method on each thread object
                    foreach (WorkerThread threadReference in threadReferences)
                    {
                        threadReference.StartService();
                    }

                    // indicate the worker process has started
                    LogInformation("Process Started");

                }
                // on error call the stop method to tidyup created objects
                catch (Exception ex)
                {
                    StopService();
                    LogError("Unable to Start Process");
                    throw new ApplicationException(ex.Message);
                }

            }
        }

       // the work object stop method that calls corresponding stop method on each thread
		internal void StopService() 
		{
			// ensure the start method has been called
			if (workerRunning) 
			{

				// indicate the worker process has been stopped
				LogInformation("Process Stoping");
                SendPanicEmail();
				// flag the worker object status as not running
				workerRunning = false;
				workerDefinition.ProcessStatus = WorkerFormatter.SFProcessStatus.StatusStopped;

				// call the stop method on each thread object
				try 
				{
					if (threadReferences != null) 
					{
						foreach (WorkerThread threadReference in threadReferences) 
						{
							threadReference.StopService();
						}
						threadReferences = null;
					}
				}
				catch (Exception ex) 
				{
					LogError("Unable to Stop all Threads : " + ex.Message);
				}

				// indicate the worker process has been stopped
				LogInformation("Process Stopped");

				// de-reference the logging class
				DeleteLogClass();

				// prevent finialize method from being called
				GC.SuppressFinalize(this);
			}
		}

        // if a thread object has an error then this method will terminate the worker
        internal void StopOnError()
        {
            // as executed on the processing thread call on the seperate thread
            Thread errorProcessor = new Thread(new ThreadStart(StopService));
            errorProcessor.Start();
        }


        // the worker object stop method that calls corresponding stop method on each thread
        internal void PauseService()
        {
            // ensure the pause method has not already been called
            if (!workerPaused && workerRunning)
            {
                // indicate the worker process has been paused
                LogInformation("Process Paused");

                // call the pause method on each thread object
                foreach (WorkerThread threadReference in threadReferences)
                {
                    threadReference.PauseService();
                }

                // flag the worker object as being paused
                workerDefinition.ProcessStatus = WorkerFormatter.SFProcessStatus.StatusPaused;
                workerPaused = true;
            }
        }

        // send panic emails on ServiceStop
        private void SendPanicEmail()
        {
            LogInformation("Sending panic email");
            try
            {
                MailMessage eMail = new MailMessage() ;
                SmtpClient sClient = new SmtpClient(workerDefinition.SmtpServer);
                foreach (string s in workerDefinition.ErrorEmails.Split(new char[] { ';' }, StringSplitOptions.RemoveEmptyEntries))
                {
                    eMail.To.Add(s);
                }
                eMail.From = new MailAddress("service@yourdomain.com", "Email Service");
                eMail.Priority = MailPriority.High;
                eMail.Body = "MSMQListner Worker instance is stopped";
                eMail.Subject = workerDefinition.Subject;

                sClient.Send(eMail);

                
            }
            catch (Exception e)
            {
                LogInformation("Unable to send panic email " + e.Message);
            }
        }

        // the work object stop method that calls corresponding stop method on each thread
        internal void ContinueService()
        {
            // ensure the pause method has been called
            if (workerPaused && workerRunning)
            {
                // call the continue method on each thread object
                foreach (WorkerThread threadReference in threadReferences)
                {
                    threadReference.ContinueService();
                }

                // flag the worker object as not being paused
                workerDefinition.ProcessStatus = WorkerFormatter.SFProcessStatus.StatusStarted;
                workerPaused = false;

                // indicate the worker process has been resumed
                LogInformation("Process Resumed");
            }
        }

        // initialize the event log source
        private void CreateLogClass()
        {
            try
            {
                // see if the source exists creating it if not
                if (!EventLog.SourceExists(eventLogSource))
                {
                    eventLogCreated = true;
                    EventLog.CreateEventSource(eventLogSource, eventLogName);
                }
                // create the log object and reference the now defined source
                eventLog = new EventLog();
                eventLog.Source = eventLogSource;
            }
            catch (Exception)
            {
                // if the log cannot be initialized null the event object
                eventLog = null;
            }
        }

        // deletes the event log source
        private void DeleteLogClass()
        {
            try
            {
                // close the event log
                eventLog.Close();
                // delete any previously created source
                if (eventLogCreated)
                {
                    EventLog.DeleteEventSource(eventLogSource);
                }
            }
            catch (Exception)
            {
            }
        }

        // Log event for recording informaiton messages
        internal void LogInformation(string logMessage)
        {
            try
            {
                // write a log entry if the log class has been previously defined
                // this will ensure the service will not terminate if eventing errors
                if (eventLog != null)
                {
                    eventLog.WriteEntry(serviceDesc + ": " + workerDesc + ": " + logMessage, EventLogEntryType.Information);
                }
            }
            catch (Exception)
            {
            }
        }

        // Log event for recording warning messages
        internal void LogWarning(string logMessage)
        {
            try
            {
                // write a log entry if the log class has been previously defined
                // this will ensure the service will not terminate if eventing warnings
                if (eventLog != null)
                {
                    eventLog.WriteEntry(serviceDesc + ": " + workerDesc + ": " + logMessage, EventLogEntryType.Warning);
                }
            }
            catch (Exception)
            {
            }
        }

        // Log event for recording error messages
        internal void LogError(string logMessage)
        {
            try
            {
                // write a log entry if the log class has been previously defined
                // this will ensure the service will not terminate if eventing errors
                if (eventLog != null)
                {
                    eventLog.WriteEntry(serviceDesc + ": " + workerDesc + ": " + logMessage, EventLogEntryType.Error);
                }
            }
            catch (Exception)
            {
            }
        }
        // return the formatter structure containing the worker status
        internal WorkerFormatter WorkerInfo
        {
            get
            {
                return workerDefinition;
            }
        }

    }
    // public structure defintion for the layout of a worker object
    internal struct WorkerFormatter
    {
        

        // enum for the worker status
        public enum SFProcessStatus
        {
            StatusInitialized,
            StatusStarted,
            StatusPaused,
            StatusStopped,
            StatusUnknown
        }

        // enum for transaction status
        public enum SFTransactions
        {
            NotRequired,
            Required
        }

        // process description information
        public string ProcessName;
        public string ProcessDesc;
       
        // transaction status of the process
        public SFTransactions Transactions;
        // number of threads on which to process
        public int NumberThreads;
        // queue information
        public string InputQueue;
        public string ErrorQueue;

        //smtp server information
        public string SmtpServer;
        public string ErrorEmails;
        public string Subject;

        public int MessageLimit;
        public int Delay;
        // status of the worker object
        public SFProcessStatus ProcessStatus;
    }
}
