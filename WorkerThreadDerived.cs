using System;
using System.Net.Mail;

namespace MSMQListner
{
    internal class WorkerThreadDerived : WorkerThread
    {
        // calls the base class constructor
        public WorkerThreadDerived(WorkerInstance workerInstance, WorkerThreadFormatter workerThreadFormatter)
            : base(workerInstance, workerThreadFormatter) { }


        // when starting obtain a reference to the assembly and construct an object
        override protected void OnStart()
        {

        }


        // when stopping release the resources
        override protected void OnStop()
        {

        }


        // override the pause and continue methods
        override protected void OnPause()
        {

        }
        override protected void OnContinue()
        {

        }

        // method to perform the processing of the message
        override protected void ProcessMessage()
        {
            MailMessage oMail = null;
            EmailMessage iMail = null;
            SmtpClient s = null;
            // attempt to call the required Process method
            try
            {
                //// define the parameters for the method call
                iMail = (EmailMessage)inputMessage.Body;

                //if (iMail != null && iMail.attempt < 3)
                {
                    int current = 0;
                    if (messageCount.ContainsKey(inputMessage.Label))
                    {
                        current = messageCount[inputMessage.Label];
                        messageCount[inputMessage.Label] = current + 1;
                    }
                    else
                    {
                        messageCount.Add(inputMessage.Label, 1);
                    }
                    MailAddress to = new MailAddress(iMail.to);
                    MailAddress from = new MailAddress(iMail.from, iMail.displayName);
                    string body = iMail.body;
                    string subject = iMail.sub;
                    oMail = new MailMessage(from, to);
                    if (oMail != null)
                    {
                        oMail.Subject = subject;
                        oMail.Body = body;
                        oMail.IsBodyHtml = true;
                        oMail.Priority = MailPriority.Normal;
                        //oMail.Sender = from;
                        //oMail.Headers.Add("X-VirtualServerGroup", "mail1.domain.com");
                        //oMail.Headers.Add("DomainKey-Signature", domainKey);
                        //oMail.Headers.Add("DKIM-Signature", DKIM);

                        s = new SmtpClient(smtpServer);
                        //  s.DeliveryMethod = SmtpDeliveryMethod.PickupDirectoryFromIis;
                        s.ServicePoint.MaxIdleTime = 2;
                        s.ServicePoint.ConnectionLeaseTimeout = 0;
                        if (s != null)
                        {
                            s.Send(oMail);
                        }
                    }
                    oMail.Dispose();
                }
                iMail = null;
            }
            catch (InvalidCastException ex)
            {
                string er = "Unable to Process message - " + inputMessage.Label + ";Email - " + iMail.to + "; Current Attempt - " + iMail.attempt + "; Current Error Count - " + messageCountAtError + "; Error - " + ex.Message + ";Inner Ex - ";
                if (ex.InnerException != null)
                {
                    er = er + ex.InnerException;
                }
                // if an error in casting message details force a non critical error
                throw new WorkerThreadException(er, false);
            }
            catch (SmtpException ex)
            {
                // if an error calling the assembly termiate the thread processing
                string er = "Unable to Process message - " + inputMessage.Label + ";Email - " + iMail.to + "; Current Attempt - " + iMail.attempt + "; Current Error Count - " + messageCountAtError + "; Error - " + ex.Message + ";Status Code:" + ex.StatusCode;
                if (ex.InnerException != null)
                {
                    er = er + ";Inner Ex - " + ex.InnerException;
                }
                throw new WorkerThreadException(er, false);
            }
            catch (Exception ex)
            {
                // if an error calling the assembly termiate the thread processing
                string er = "Unable to Process message - " + inputMessage.Label + ";Email - " + iMail.to + "; Current Attempt - " + iMail.attempt + "; Current Error Count - " + messageCountAtError + "; Error - " + ex.Message + ";Inner Ex - ";
                if (ex.InnerException != null)
                {
                    er = er + ex.InnerException;
                }
                throw new WorkerThreadException(er, false);
            }

            // if no error review the return status of the object call

        }
    }
}
