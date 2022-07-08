# Install and Configure Winlogbeat

## Installing and Configuring Winlogbeat
### Installing Winlogbeat
Note: These steps are taken from Elastic's documentation [Winlogbeat quick start: installation and configuration](https://www.elastic.co/guide/en/beats/winlogbeat/current/winlogbeat-installation-configuration.html) page.  Not all steps on that page apply to this specific deployment pattern.

1. Download Winlogbeat [here](https://www.elastic.co/downloads/beats/winlogbeat "Download Winlogbeat")
2. Extract the contents into `C:\Program Files`
3. Rename the `winlogbeat-<version>` directory to `Winlogbeat`
4. Run PowerShell as Administrator by right-clicking on the PowerShell icon and selecting Run as Administrator.
5. From the PowerShell prompt, run the following command to install the service:
```dos
cd 'C:\Program Files\Winlogbeat'
.\install-service-winlogbeat.ps1
```
If you get an error re: Script Execution, see this note from the [Winlogbeat Installation docs](https://www.elastic.co/guide/en/beats/winlogbeat/current/winlogbeat-installation-configuration.html#installation):
> If script execution is disabled on your system, you need to set the execution policy for the current session to allow the script to run. For example: `PowerShell.exe -ExecutionPolicy UnRestricted -File .\install-service-winlogbeat.ps1` 

### Configuring Winlogbeat
The supplied [winlogbeat.yml](../blob/master/winlogbeat.yml) file is a great place to start.

Edit the following line to reflect your Kafka broker's IP:
```yaml
  hosts: ["pkc-XXXXX.us-east-2.aws.confluent.cloud:9092"]
```
Optionaly, edit the topic name.  If you are using Confluent Cloud, be sure to create the topic now, before you start Winlogbeat.
```yaml
  topic: winlogbeat
```  

Edit the username and password fields.  If you're using Confluent Cloud, these are the Key and Secret values of an API key.
```yml
  # Authentication details. Password is required if username is set.
  username: 'XXXXXXXXXXXXXXXX'
  password: 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
```

Start Winlogbeat (from an Administrator PowerShell console):
```dos
cd 'C:\Program Files\Winlogbeat'
Start-Service winlogbeat
```


### Optional (Recommended): Install Sysmon
Download [Sysmon](https://docs.microsoft.com/en-us/sysinternals/downloads/sysmon), the System Monitor from Window Sysinternals.

From the [Windows Sysinternals docs](https://docs.microsoft.com/en-us/sysinternals/downloads/sysmon):
>System Monitor (Sysmon) is a Windows system service and device driver that, once installed on a system, remains resident across system reboots to monitor and log system activity to the Windows event log. It provides detailed information about process creations, network connections, and changes to file creation time. By collecting the events it generates using [Windows Event Collection](https://msdn.microsoft.com/library/windows/desktop/bb427443(v=vs.85).aspx) or [SIEM](https://en.wikipedia.org/wiki/security_information_and_event_management) agents and subsequently analyzing them, you can identify malicious or anomalous activity and understand how intruders and malware operate on your network.

Download Sysmon.zip and uncompress its contents.

Sysmon can optionally be run with a configuration file for finer tuning of what gets logged.  One of the best publicly available sysmon-config files is maintained by Infosec Twitter icon [@SwiftOnSecurity](https://twitter.com/SwiftOnSecurity) and is available [here](https://github.com/SwiftOnSecurity/sysmon-config/blob/master/sysmonconfig-export.xml).  

Save this file as `sysmonconfig-export.xml` in the same directory as the `Sysmon64.exe` binary.

Now install Sysmon to run using this config file (Run as Administrator):
```dos
.\Sysmon64.exe -accepteula -i sysmonconfig-export.xml
```

You should now have Sysmon events written to the Windows Event Logs with Winlogbeat sending those events to Confluent.

You can now [analyze the data with ksqlDB](https://github.com/confluentinc/cyber/blob/bhayes-elastic/quickstart/winlogbeat/winlogbeat-ksql.md).