Name:		cloud-scheduler
Version:	1.4
Release:	1%{?dist}
Summary:	Cloud-enabled distributed resource manager

License:	GPLv3 or ASL-2.0
URL:		http://github.com/hep-gc/cloud-scheduler
Source0:	http://pypi.python.org/packages/source/c/%{name}/%{name}-%{version}.tar.gz
Source1:        https://raw.github.com/hep-gc/%{name}/%{version}/cloud_admin

BuildArch:      noarch

BuildRequires:  python-setuptools

Requires:	python-lxml >= 2.2.6
Requires:       python-boto >= 2.0
Requires:       python-suds >= 0.3.9
Requires:       condor

%description
Cloud Scheduler manages virtual machines on clouds configured with
Eucalyptus, or Amazon EC2 to create an environment for HTC
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

# hack to fix a setup.py error when install user is not root
install -m 755 -d %{buildroot}/%{_sysconfdir}/cloudscheduler
mv %{buildroot}/${HOME}/.cloudscheduler/*.conf %{buildroot}/%{_sysconfdir}/cloudscheduler
rm -r %{buildroot}/${HOME}

%files
%defattr(-,root,root)
%doc
%{_bindir}/cloud_*
%{_sbindir}/cloud_admin
%{python_sitelib}/cloudscheduler/*
%{python_sitelib}/cloud_scheduler*.egg-info/

%config(noreplace) %{_sysconfdir}/cloudscheduler/cloud_resources.conf
%config(noreplace) %{_sysconfdir}/cloudscheduler/cloud_scheduler.conf

%changelog
* Tue Jun 04 2013 Sebastien Fabbro <sfabbro@uvic.ca> - 1.4
First build.
