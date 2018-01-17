using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Messaging;
using System.Net.Mail;
using System.Threading;


namespace MSMQListner
{
    internal abstract class WorkerThread 
    {
        // private flags to indicate the state of the service worker thread object
        private bool workerRunning;
        private bool workerPaused;

        // define a thread object
        private Thread messageProcessor;

        // reference to the thread description
        private string threadName;

        // reference to the parent worker class
        private WorkerInstance workerInstance;

        // refernece to objects to be accessed by implemeted classes
        protected WorkerThreadFormatter threadDefinition;

        // the input and error queue the derived class is processing
        protected MessageQueue inputQueue, errorQueue;

        // the input and error queue the derived class is processing
      
        protected string smtpServer;
        protected int messageCountAtError;

        protected SmtpClient Client;

        protected int MessageLimit;
        protected int Delay;
        // the message being processed by the derived class
        protected Message inputMessage;

        // table to maintaine count of processed message based on auditid
        protected Dictionary<string, int> messageCount = new Dictionary<string, int>();

        // the transaction that the derived class must honour
        protected MessageQueueTransaction queueTransaction;

        // indicates if transactions are required
        protected bool transactionalQueue;

        // timespan object for reading messages - 1 second
        private TimeSpan queueTimeout = new TimeSpan(0, 0, 0, 1);

        // reference to the performance counter objects
        PerformanceCounter perfCounterTotThread, perfCounterTotWorker, perfCounterTotService;
        PerformanceCounter perfCounterSecThread, perfCounterSecWorker, perfCounterSecService;

        // private variables that define the services constants
        private string perfCounterCatName = MSMQListner.PerformanceCategory;
        private string perfCounterTotName = MSMQListner.PerformanceCounterMsgTot;
        private string perfCounterSecName = MSMQListner.PerformanceCounterMsgSec;
        private string perfCounterServiceName;
        private string perfCounterWorkerName;
        private string perfCounterThreadName;

        abstract protected void OnStart();
        abstract protected void OnStop();
        abstract protected void OnPause();
        abstract protected void OnContinue();
        // abstract method to perform the processing of the message
        abstract protected void ProcessMessage();

        // constructor method for the class that saves the constructor parameters
        public WorkerThread(WorkerInstance workerInstance, WorkerThreadFormatter workerThreadFormatter)
        {
            // save the constructor references
            this.workerInstance = workerInstance;
            threadDefinition = workerThreadFormatter;
            threadName = "Thread " + threadDefinition.ThreadNumber.ToString();

            // define the perfmon counter instance descriptions
            perfCounterServiceName = MSMQListner.PerformanceCounterTotal;
            perfCounterWorkerName = threadDefinition.ProcessName;
            perfCounterThreadName = threadDefinition.ProcessName + "_" + threadDefinition.ThreadNumber.ToString();

            // indicate the thread is not running
            workerRunning = false;
            workerPaused = false;
        }

        // destructor for the class
        // if the service shuts down without calling the stop method it is gracefully terminated
        ~WorkerThread()
        {
            StopService();
        }
        // the thread object start method that creates the processing thread
        internal void StartService()
        {
            LogInformation("Worker therad StartService()");
            // ensure the start method has not already been called
            if (!workerRunning)
            {
                // flag the thread object as started
                workerRunning = true;

                // attempt to allocate needed resources
                try
                {
                    // initialize the perfmon counters
                    PerfmonInit();

                    // before starting the thread open the queues that require processing
                    string inputQueueName = threadDefinition.InputQueue;
                    string errorQueueName = threadDefinition.ErrorQueue;
                    smtpServer = threadDefinition.SmtpServer;
                    messageCountAtError = 0;
                   
                    if (!MessageQueue.Exists(inputQueueName) || !MessageQueue.Exists(errorQueueName))
                    {
                        // queue does not exist so through an error
                        throw new ArgumentException("The Input/Error Queue does not Exist");
                    }

                    // try and open the input queue and set the default read and write properties
                    inputQueue = new MessageQueue(inputQueueName);
                    inputQueue.MessageReadPropertyFilter.Body = true;
                    inputQueue.MessageReadPropertyFilter.AppSpecific = true;
                    // open the error queue
                    errorQueue = new MessageQueue(errorQueueName);

                    // set the formatter to be activex if using MSMQ COM to load the messages
                    inputQueue.Formatter = new XmlMessageFormatter(new Type[] { typeof(EmailMessage) });
                    errorQueue.Formatter = new XmlMessageFormatter(new Type[] { typeof(EmailMessage) });

                    //smtpClient = new SmtpClient(smtpServer);
                    
                  //  Client = new SmtpClient();
                 //   Client.DeliveryMethod = SmtpDeliveryMethod.PickupDirectoryFromIis;

                   MessageLimit = threadDefinition.MessageLimit;
                    Delay = threadDefinition.Delay;
                    // validate the transactional status of the queues
                    // property taken from the configuration file for each worker process
                    if (workerInstance.WorkerInfo.Transactions == WorkerFormatter.SFTransactions.NotRequired)
                    {
                        transactionalQueue = false;
                    }
                    else
                    {
                        transactionalQueue = true;
                    }
                    if ((inputQueue.Transactional != transactionalQueue) || (errorQueue.Transactional != transactionalQueue))
                    {
                        throw new ApplicationException("Queues do not have Consistent Transactional Status");
                    }

                    // if require transactions create a message queue transaction
                    if (transactionalQueue)
                    {
                        queueTransaction = new MessageQueueTransaction();
                    }
                    else
                    {
                        queueTransaction = null;
                    }

                    // call an abstract start method
                    OnStart();

                    // create a thread and start executing the process method
                    // it is this method that reads the input queue and class an abstract process method
                    messageProcessor = new Thread(new ThreadStart(ProcessMessages));
                    messageProcessor.Start();

                }
                // on error call the stop method to tidyup created objects
                catch (Exception ex)
                {
                    LogError(ex.Message);
                    StopService();
                    throw ex;
                }
            }
        }

        // the thread object stop method that terminates the thread
        internal void StopService()
        {
            // ensure the start method has already been called
            if (workerRunning)
            {
                // flag the thread object as/to stopped
                workerRunning = false;

                // join the service thread and the processing thread
                try
                {
                    if (messageProcessor != null)
                    {
                        messageProcessor.Join();
                    }
                }
                catch (Exception ex)
                {
                    LogError("Unable to Terminate Thread Normally : " + ex.Message);
                }

                // if transactional dispose of the transaction
                try
                {
                    if (transactionalQueue)
                    {
                        queueTransaction.Dispose();
                    }
                }
                catch (Exception ex)
                {
                    LogError("Unable to Dispose of Message Transaction : " + ex.Message);
                }

                // close the message queues
                try
                {
                    if (inputQueue != null)
                    {
                        inputQueue.Close();
                    }
                }
                catch (Exception ex)
                {
                    LogError("Unable to Close Input Queue : " + ex.Message);
                }
                try
                {
                    if (errorQueue != null)
                    {
                        errorQueue.Close();
                    }
                }
                catch (Exception ex)
                {
                    LogError("Unable to Close Error Queue : " + ex.Message);
                }

                // process the implemented stop method
                try
                {
                    OnStop();
                }
                catch (Exception ex)
                {
                    LogError("Unable to Call OnStop Normally : " + ex.Message);
                }

                // close the perfmon counters
                PerfmonClose();

                // prevent the finalize method from being called
                // terminator only calls the stop method - why call twice
                GC.SuppressFinalize(this);

            }
        }

        // the pause method that sets the pause flag to pause the processing thread
        internal void PauseService()
        {
            workerPaused = true;
            OnPause();
        }

        // the continue method that sets the pause flag to continue the processing thread
        internal void ContinueService()
        {
            workerPaused = false;
            OnContinue();
        }

        // the worker thread that operates off the service thread to perform the service work
        // the thread is stopped when the running flag is set to false
        // the thread is paused when the paused flag is set to true
        private void ProcessMessages()
        {
            LogInformation("Thread Started");
           // Client = new SmtpClient(smtpServer);
           
           // Client.ServicePoint.ConnectionLeaseTimeout = 5;
           // Client.ServicePoint.MaxIdleTime = 2;
           // Client.ServicePoint.ConnectionLimit = 1;
            // process work as log as the thread should run
            while (workerRunning)
            {
                // if the thread requires pausing sleep for half a second
                if (workerPaused)
                {
                    Thread.Sleep(500);
                }
                else
                {
                    /*
                    if (messageCountAtError == MessageLimit)
                    {
                        LogInformation("Reached MessageLimit: MessageLimit:" + MessageLimit + ";messageCountAtError:" + messageCountAtError + ";Connection Count:" + Client.ServicePoint.CurrentConnections);
                        messageCountAtError = 0;
                        Thread.Sleep(Delay);

                    }*/
                    // within this loop a single input message is processed
                    // a derived class wil actually process the message

                    // if a a transaction required create one
                    // reads from the input queue and calls process message
                    // if an error thrown the transaction is aborted
                   // Client = new SmtpClient(smtpServer);
                    
                    MessageTransactionStart();
                    MessageReceive();

                    // once have a message call the process message abstract method
                    // any error at this point will force a send to the error queue
                    if (inputMessage != null)
                    {
                        // LogInformation("input mesaage got");
                        // if a terminate error is caught the transaction is aborted
                        try
                        {
                            // call the method to process the message
                            // iMail = (EmailMessage)inputMessage.Body;
                           
                            ProcessMessage();
                            // iMail = null;
                        }
                        // catch error thrown where exception status known
                        catch (WorkerThreadException ex)
                        {
                            ProcessError(ex.Terminate, ex.Message);
                        }
                        // catch an unknown exception and call terminate
                        catch (Exception ex)
                        {
                            ProcessError(true, ex.Message);
                        }
                        // successfully completed a processing of a message
                        // this includes writing to the error queue
                        MessageTransactionComplete(true);
                        // write out the performance counter information
                        PerfmonInc();
                    }
                }
            }
        }

        // method to process the failed message
        private void ProcessError(bool terminateProcessing, string logMessage)
        {
            // attempt to send the failed error to the error queue
            // upon failure to write to the error queue log the error
          // Client = new SmtpClient(smtpServer);
           // Client.ServicePoint.MaxIdleTime = 2;
           // Client.ServicePoint.ConnectionLimit = 1;
            try
            {
              //  messageCountAtError = 0;
                LogWarning("Sending Message to Error Queue : " + inputMessage.Label + " : " + logMessage);
                MessageSend(errorQueue);
            }
            catch (Exception ex)
            {
                //messageCountAtError = 0;
                LogError("Cannot Process Message : " + inputMessage.Label + " : " + ex.Message);
                // as one cannot write to error queue terminate the thread
                terminateProcessing = true;
                MessageTransactionComplete(false);
            }

            // if required terminate the thread and associated worker
            //if (terminateProcessing)
            //{
            //    // abort transaction as cannot place the message into the error queue
            //    MessageTransactionComplete(false);
            //    LogError("Thread Terminated Abnormally : " + logMessage);
            //    // an error here should also terminate the processing thread gracefully
            //    workerInstance.StopOnError();
            //    throw new ApplicationException("Terminate Thread");
            //}
        }

        // message queue receive method that will honour the transaction in process
        // protected to allow derived classes to read from other queues if neccessary
        protected void MessageReceive()
        {
            try
            {
                if (queueTransaction == null)
                {
                    inputMessage = inputQueue.Receive(queueTimeout);
                }
                else
                {
                    inputMessage = inputQueue.Receive(queueTimeout, queueTransaction);
                }
            }
            catch (MessageQueueException ex)
            {
                // set the message to null as not to be processed
                inputMessage = null;
                // as message has not been read terminate the transaction
                MessageTransactionComplete(false);
                // look at the error code and see if there was a timeout
                // if not a timeout throw an error and log the error number
                if (ex.MessageQueueErrorCode != MessageQueueErrorCode.IOTimeout)
                {
                    LogError("Error : " + ex.Message);
                    // an error here should also terminate the processing thread gracefully
                    workerInstance.StopOnError();
                    throw ex;
                }
            }
        }

        // start any required message queue transaction
        private void MessageTransactionStart()
        {
            try
            {
                if (transactionalQueue)
                {
                    queueTransaction.Begin();
                }
            }
            catch (Exception ex)
            {
                LogError("Cannot Create Message Transaction " + perfCounterThreadName + ": " + ex.Message);
                // an error here should also terminate the processing thread gracefully
                workerInstance.StopOnError();
                throw ex;
            }
        }

        // complete the message queue transaction
        // based on the existence of the transaction and it success requirement
        private void MessageTransactionComplete(bool transactionSuccess)
        {
            if (transactionalQueue)
            {
                try
                {
                    // committing/aborting a transactions must be successful
                    if (transactionSuccess)
                    {
                        queueTransaction.Commit();
                    }
                    else
                    {
                        queueTransaction.Abort();
                    }
                }
                catch (Exception ex)
                {
                    LogError("Cannot Complete Message Transaction " + perfCounterThreadName + ": " + ex.Message);
                    // an error here should also terminate the processing thread gracefully
                    workerInstance.StopOnError();
                    throw ex;
                }
            }
        }

        // message queue send method that will honour the transaction in process
        // protected to allow derived classes to send to remote queues
        protected void MessageSend(MessageQueue messageQueue)
        {
            EmailMessage em = (EmailMessage)inputMessage.Body;
            if (em.attempt < 5)
            {
                em.attempt = em.attempt + 1;
                Message m = new Message(em);
                if (queueTransaction == null)
                {
                    inputQueue.Send(m, inputMessage.Label);

                }
                else
                {
                    inputQueue.Send(m, inputMessage.Label, queueTransaction);
                }
            }
            else
            {
                if (queueTransaction == null)
                {
                    errorQueue.Send(inputMessage);

                }
                else
                {
                    errorQueue.Send(inputMessage, queueTransaction);
                }
            }
        }

        // initialize the performance monitor counters
        private void PerfmonInit()
        {
            try
            {
                perfCounterTotThread = new PerformanceCounter(perfCounterCatName, perfCounterTotName, perfCounterThreadName, false);
                perfCounterSecThread = new PerformanceCounter(perfCounterCatName, perfCounterSecName, perfCounterThreadName, false);
                perfCounterTotThread.RawValue = 0;
                perfCounterSecThread.RawValue = 0;
            }
            catch (Exception ex)
            {
                LogWarning("Cannot Init Thread Perfmon Instance " + perfCounterThreadName + ": " + ex.Message);
            }
            try
            {
                perfCounterTotWorker = new PerformanceCounter(perfCounterCatName, perfCounterTotName, perfCounterWorkerName, false);
                perfCounterSecWorker = new PerformanceCounter(perfCounterCatName, perfCounterSecName, perfCounterWorkerName, false);
                perfCounterTotWorker.RawValue = 0;
                perfCounterSecWorker.RawValue = 0;
            }
            catch (Exception ex)
            {
                LogWarning("Cannot Init Worker Perfmon Instance " + perfCounterWorkerName + ": " + ex.Message);
            }
            try
            {
                perfCounterTotService = new PerformanceCounter(perfCounterCatName, perfCounterTotName, perfCounterServiceName, false);
                perfCounterSecService = new PerformanceCounter(perfCounterCatName, perfCounterSecName, perfCounterServiceName, false);
                perfCounterTotService.RawValue = 0;
                perfCounterSecService.RawValue = 0;
            }
            catch (Exception ex)
            {
                LogWarning("Cannot Init Service Perfmon Instance " + perfCounterServiceName + ": " + ex.Message);
            }
        }

        // increment the performance monitor counters
        private void PerfmonInc()
        {
            // increment the counter for the thread
            try
            {
                perfCounterTotThread.IncrementBy(1);
                perfCounterSecThread.IncrementBy(1);
            }
            catch (Exception)
            {
                // ignore error incrementing perfmon counter
            }
            // increment the counter for the worker process
            try
            {
                perfCounterTotWorker.IncrementBy(1);
                perfCounterSecWorker.IncrementBy(1);
            }
            catch (Exception)
            {
                // ignore error incrementing perfmon counter
            }
            // increment the counter for the windows service
            try
            {
                perfCounterTotService.IncrementBy(1);
                perfCounterSecService.IncrementBy(1);
            }
            catch (Exception)
            {
                // ignore error incrementing perfmon counter
            }
        }

        // close the performance monitor counters
        private void PerfmonClose()
        {
            try
            {
                perfCounterTotThread.Close();
                perfCounterSecThread.Close();
            }
            catch (Exception ex)
            {
                LogWarning("Cannot Close Thread Perfmon: " + ex.Message);
            }
            try
            {
                perfCounterTotWorker.Close();
                perfCounterSecWorker.Close();
            }
            catch (Exception ex)
            {
                LogWarning("Cannot Close Worker Perfmon: " + ex.Message);
            }
            try
            {
                perfCounterTotService.Close();
                perfCounterSecService.Close();
            }
            catch (Exception ex)
            {
                LogWarning("Cannot Close Service Perfmon: " + ex.Message);
            }
        }


        // write any error to the event log
        protected void LogError(string logMessage)
        {
            workerInstance.LogError(threadName + " : " + logMessage);
        }

        // write any error to the event log
        protected void LogWarning(string logMessage)
        {
            workerInstance.LogWarning(threadName + " : " + logMessage);
        }

        // write any information to the event log
        protected void LogInformation(string logMessage)
        {
            workerInstance.LogInformation(threadName + " : " + logMessage);
        }
    }

    // internal structure defintion for the layout of a worker thread object
    internal struct WorkerThreadFormatter
    {
        // process description information
        public string ProcessName;
        public string ProcessDesc;
        public int ThreadNumber;
        // queue information
        public string InputQueue;
        public string ErrorQueue;
        // smptp server info

        public string SmtpServer;
        public string ErrorEmails;
        public string Subject;

        public int MessageLimit;
        public int Delay;
    }


    // exception class used in the thread process to determine if the thread should terminate
    internal class WorkerThreadException : ApplicationException
    {
        // indicator of whether the thread should continue
        private bool terminateProcessing;

        // base constructor
        public WorkerThreadException()
            : base()
        {
            terminateProcessing = false;
        }

        // constructors to set base exception message
        public WorkerThreadException(string logMessage)
            : base(logMessage)
        {
            terminateProcessing = false;
        }
        public WorkerThreadException(string logMessage, Exception inner)
            : base(logMessage, inner)
        {
            terminateProcessing = false;
        }

        // constructor to set base exception message and processing flag
        public WorkerThreadException(string logMessage, bool terminateProcessing)
            : base(logMessage)
        {
            this.terminateProcessing = terminateProcessing;
        }

        // terminate property
        public bool Terminate
        {
            get
            {
                return terminateProcessing;
            }
            set
            {
                terminateProcessing = value;
            }
        }

    }
}
