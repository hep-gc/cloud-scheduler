Name:		cloud-scheduler
Version:	1.7
Release:	1%{?dist}
Summary:	Cloud-enabled distributed resource manager

License:	GPLv3 or ASL-2.0
URL:		http://github.com/hep-gc/cloud-scheduler
Source0:	http://pypi.python.org/packages/source/c/%{name}/%{name}-%{version}.tar.gz

BuildArch:      noarch

BuildRequires:  python-setuptools

Requires:	python-lxml
Requires:	python-boto >= 2.0
Requires:	python-suds >= 0.3.9
Requires:	nimbus-cloud-client
Requires:	condor

Requires(post):	  chkconfig
Requires(preun):  chkconfig
Requires(preun):  initscripts
Requires(postun): initscripts


%description
Cloud Scheduler manages virtual machines on clouds configured with
Nimbus, Eucalyptus, or Amazon EC2 to create an environment for HTC
batch job execution. Users submit their jobs to a Condor job queue,
and Cloud Scheduler boots VMs to suit those jobs.

%prep
%setup -q

%build
%{__python} setup.py build

%install
%{__python} setup.py install --skip-build --root %{buildroot}
install -m 755 -d %{buildroot}/%{_sbindir}
install -m 755 %{SOURCE1} %{buildroot}/%{_sbindir}
install -m 755 -d %{buildroot}/%{_initrddir}
install -m 755 -d %{buildroot}/%{_sysconfdir}/condor
install -m0755 scripts/cloud_scheduler.init.d %{buildroot}/%{_initrddir}/cloud_scheduler
install -m0644 scripts/cloud_scheduler.sysconf %{buildroot}/%{_sysconfdir}/condor/cloud_scheduler

# hack to fix a setup.py error when install user is not root
install -m 755 -d %{buildroot}/%{_sysconfdir}/cloudscheduler
mv %{buildroot}/${HOME}/.cloudscheduler/*.conf %{buildroot}/%{_sysconfdir}/cloudscheduler
rm -r %{buildroot}/${HOME}


%files
%defattr(-,root,root)
%doc README.md LICENSE PKG-INFO 
%{_bindir}/cloud_*
%{_sbindir}/cloud_admin
%{python_sitelib}/cloudscheduler/*
%{python_sitelib}/cloud_scheduler*.egg-info/

%config(noreplace) %{_sysconfdir}/cloudscheduler/cloud_resources.conf
%config(noreplace) %{_sysconfdir}/cloudscheduler/cloud_scheduler.conf
%config(noreplace) %{_initrddir}/cloud_scheduler
%config(noreplace) %{_sysconfdir}/condor/cloud_scheduler

%post
if [ $1 = 0 ]; then
    /sbin/chkconfig --add cloud_scheduler
fi

%preun
if [ $1 = 0 ]; then
    /sbin/service cloud_scheduler stop >/dev/null 2>&1
    /sbin/chkconfig --del cloud_scheduler
fi

%postun
if [ $1 = 0 ]; then
    /sbin/service cloud_scheduler quickrestart >/dev/null 2>&1 || :
fi


%changelog
* Wed Jun 05 2013 Sebastien Fabbro <sfabbro@uvic.ca> - 1.7-1
Version bump. Move configuration to /etc/condor for debian
compatibility. cloud_admin now on tar ball.
* Wed Jun 05 2013 Sebastien Fabbro <sfabbro@uvic.ca> - 1.5-1
Version bump. Added sysconfig config script.
* Wed Jun 05 2013 Sebastien Fabbro <sfabbro@uvic.ca> - 1.4-2
Added installation of init scripts and doc files.
Lower dependency on python-lxml
Updated description
* Tue Jun 04 2013 Sebastien Fabbro <sfabbro@uvic.ca> - 1.4-1
First build.
