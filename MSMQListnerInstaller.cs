
using System;
using System.Collections;
using System.Configuration;
using System.Configuration.Install;
using System.ServiceProcess;
using System.ComponentModel;
using System.Diagnostics;

namespace MSMQListner
{
    [RunInstaller(true)]
    public partial class MSMQListnerInstaller : Installer
    {

        private ServiceInstaller serviceInstaller;
        private ServiceProcessInstaller processInstaller;

        public MSMQListnerInstaller()
        {
            // define and create the service installer
            serviceInstaller = new ServiceInstaller();
            serviceInstaller.StartType = ServiceStartMode.Manual;
#if DEBUG
            serviceInstaller.ServiceName = MSMQListner.ServiceControlName;
            serviceInstaller.DisplayName = MSMQListner.ServiceControlName;
#else
            serviceInstaller.ServiceName = MSMQListner.ServiceControlName;
            serviceInstaller.DisplayName = MSMQListner.ServiceControlName;
#endif
            
            serviceInstaller.Description = MSMQListner.ServiceControlDesc;
            Installers.Add(serviceInstaller);

            // define and create the process installer
            processInstaller = new ServiceProcessInstaller();
#if RUNUNDERSYSTEM
            processInstaller.Account = ServiceAccount.LocalSystem;
#else
            // should prompt for user on install
                        processInstaller.Account = ServiceAccount.User;
                        processInstaller.Username = null;
                        processInstaller.Password = null;
#endif
            // processInstaller.Account = ServiceAccount.LocalSystem;
            Installers.Add(processInstaller);
        }
    }
}
