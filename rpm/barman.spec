%if 0%{?rhel} == 7
  %global pybasever 2.7
%else
  %if 0%{?fedora}>=21
    %global pybasever 2.7
  %else
    %global pybasever 2.6
  %endif
%endif

%if 0%{?rhel} == 5
%global with_python26 1
%endif

%if 0%{?with_python26}
%global __python_ver python26
%global __python %{_bindir}/python%{pybasever}
%global __os_install_post %{__multiple_python_os_install_post}
%else
%global __python_ver python
%endif

%global main_version 2.4
# comment out the next line if not a pre-release (use '#%%global ...')
#%%global extra_version a1
# Usually 1 - unique sequence for all pre-release version
%global package_release 1

%{!?pybasever: %define pybasever %(%{__python} -c "import sys;print(sys.version[0:3])")}
%{!?python_sitelib: %define python_sitelib %(%{__python} -c "from distutils.sysconfig import get_python_lib; print get_python_lib()")}
%{!?python_sitearch: %define python_sitearch %(%{__python} -c "from distutils.sysconfig import get_python_lib; print get_python_lib(1)")}

Summary:	Backup and Recovery Manager for PostgreSQL
Name:		barman
Version:	%{main_version}
Release:	%{?extra_version:0.}%{package_release}%{?extra_version:.%{extra_version}}%{?dist}
License:	GPLv3
Group:		Applications/Databases
Url:		http://www.pgbarman.org/
Source0:	%{name}-%{version}%{?extra_version:%{extra_version}}.tar.gz
BuildRoot:	%{_tmppath}/%{name}-%{version}-%{release}-buildroot-%(%{__id_u} -n)
BuildArch:	noarch
Vendor:		2ndQuadrant Limited <info@2ndquadrant.com>
Requires:	python-abi = %{pybasever}, %{__python_ver}-psycopg2 >= 2.4.2, %{__python_ver}-argh >= 0.21.2, %{__python_ver}-argcomplete, %{__python_ver}-dateutil
Requires:	/usr/sbin/useradd
Requires:	rsync >= 3.0.4

%description
Barman (Backup and Recovery Manager) is an open-source
administration tool for disaster recovery of PostgreSQL
servers written in Python.
It allows your organisation to perform remote backups of
multiple servers in business critical environments to
reduce risk and help DBAs during the recovery phase.

Barman is distributed under GNU GPL 3 and maintained
by 2ndQuadrant.

%prep
%setup -n barman-%{version}%{?extra_version:%{extra_version}} -q

%build
%{__python} setup.py build
cat > barman.cron << EOF
# m h  dom mon dow   user     command
  * *    *   *   *   barman   [ -x %{_bindir}/barman ] && %{_bindir}/barman -q cron
EOF
cat > barman.logrotate << EOF
/var/log/barman/barman.log {
    missingok
    notifempty
    create 0600 barman barman
}
EOF

%install
%{__python} setup.py install -O1 --skip-build --root %{buildroot}
mkdir -p %{buildroot}%{_sysconfdir}/bash_completion.d
mkdir -p %{buildroot}%{_sysconfdir}/cron.d/
mkdir -p %{buildroot}%{_sysconfdir}/logrotate.d/
mkdir -p %{buildroot}%{_sysconfdir}/barman.d/
mkdir -p %{buildroot}/var/lib/barman
mkdir -p %{buildroot}/var/log/barman
install -pm 644 doc/barman.conf %{buildroot}%{_sysconfdir}/barman.conf
install -pm 644 doc/barman.d/* %{buildroot}%{_sysconfdir}/barman.d/
install -pm 644 scripts/barman.bash_completion %{buildroot}%{_sysconfdir}/bash_completion.d/barman
install -pm 644 barman.cron %{buildroot}%{_sysconfdir}/cron.d/barman
install -pm 644 barman.logrotate %{buildroot}%{_sysconfdir}/logrotate.d/barman
touch %{buildroot}/var/log/barman/barman.log

%clean
rm -rf %{buildroot}

%files
%defattr(-,root,root)
%doc INSTALL NEWS README.rst
%{python_sitelib}/%{name}-%{version}%{?extra_version:%{extra_version}}-py%{pybasever}.egg-info
%{python_sitelib}/%{name}/
%{_bindir}/%{name}
%doc %{_mandir}/man1/%{name}.1.gz
%doc %{_mandir}/man5/%{name}.5.gz
%config(noreplace) %{_sysconfdir}/bash_completion.d/
%config(noreplace) %{_sysconfdir}/%{name}.conf
%config(noreplace) %{_sysconfdir}/cron.d/%{name}
%config(noreplace) %{_sysconfdir}/logrotate.d/%{name}
%config(noreplace) %{_sysconfdir}/barman.d/
%attr(700,barman,barman) %dir /var/lib/%{name}
%attr(755,barman,barman) %dir /var/log/%{name}
%attr(600,barman,barman) %ghost /var/log/%{name}/%{name}.log

%pre
groupadd -f -r barman >/dev/null 2>&1 || :
useradd -M -n -g barman -r -d /var/lib/barman -s /bin/bash \
	-c "Backup and Recovery Manager for PostgreSQL" barman >/dev/null 2>&1 || :

%changelog
* Fri May 25 2018 - Marco Nenciarini <marco.nenciarini@2ndquadrant.it> 2.4-1
- New release 2.4-1

* Tue Sep 5 2017 - Marco Nenciarini <marco.nenciarini@2ndquadrant.it> 2.3-1
- New release 2.3-1

* Mon Jul 17 2017 - Marco Nenciarini <marco.nenciarini@2ndquadrant.it> 2.2-1
- New release 2.2-1

* Thu Jan 5 2017 - Giulio Calacoci <giulio.calacoci@2ndquadrant.it> 2.1-1
- New release 2.1-1

* Tue Dec 27 2016 - Gabriele Bartolini <gabriele.bartolini@2ndquadrant.it> 2.1-0.1.alpha.1
- New release 2.1-0.1.alpha.1

* Tue Sep 27 2016 - Gabriele Bartolini <gabriele.bartolini@2ndquadrant.it> 2.0-1
- New release 2.0-1
- Trim changelog for releases 1.X
