In order to use the Credential Delegation Service (CDS) to renew job
x509 proxies which are about to expire, follow the instructions below:

Requirements:
-------------
 - The 'curl' package must be installed on the Cloud Scheduler system.
 - The CDS must be configured to accept the Cloud Scheduler host cert DN for
 authentication.
 - GSI authentication must be enabled and properly configured.  See
   https://wiki.heprc.uvic.ca/twiki/bin/view/Restricted/CsGsiSupport for more
   information about this.


Job Submission:
---------------
You need to specify in your job classad the full URL of your proxy.  This is
done via the CDSCredsURL classad attribute, as shown below:

x509userproxy = /tmp/x509up_u20200
+CDSCredsURL = "https://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/cred/priv/DN/cn%3Dcadc+authtest2+10635%2Cou%3Dcadc%2Co%3Dhia"

(don't forget to change the value of the x509userproxy to point to your
proxy's full path on your system)

    IMPORTANT: Note that the Cloud Scheduler assumes that the proxy found at
    the location specified by the CDSCredsURL is valid, short-lived and will
    be periodically refreshed by the user.  The Cloud Scheduler does not check
    the lifetime of the proxy fetched from the CDS; if the proxy lifetime is
    invalid, the user's job will fail.


Security Notes:
---------------
The Cloud Scheduler will check to make sure that the DN of the proxy fetched
from the CDS matches the DN of the job's owner.  This is to prevent users from
accessing other users's credentials.  If the DNs do not match, then a warning
message is displayed in the Cloud Scheduler logs and the credential renewal is
aborted.
