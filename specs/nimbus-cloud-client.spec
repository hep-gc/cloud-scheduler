Name:		nimbus-cloud-client
Version:	022
Release:	1%{?dist}
Summary:	Nimbus cloud client

License:	ASL-2.0
URL:		http://www.nimbusproject.org/
Source0:	http://www.nimbusproject.org/downloads/%{name}-%{version}.tar.gz

BuildArch:      noarch
Requires:	jre >= 1.5.0

%description
Client for the Nimbus cloud computing platform.

%prep
%setup -q

%install
rm -rf $RPM_BUILD_ROOT
mkdir -p $RPM_BUILD_ROOT/opt/%{name}
cp -ar bin conf lib samples  $RPM_BUILD_ROOT/opt/%{name}
chmod +x $RPM_BUILD_ROOT/opt/%{name}/lib/workspace.sh


%files
%defattr(-,root,root)
%doc CHANGES.txt LICENSE.txt

/opt/%{name}

%changelog
* Mon Jun 03 2013 Sebastien Fabbro <sfabbro@uvic.ca> - 022
Version bump.
* Mon Jun 03 2013 Sebastien Fabbro <sfabbro@uvic.ca> - 021
First build. This package is a horrendous java blob, waste of time to unbundle it.
