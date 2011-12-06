%define name barman
%define version 0.3.0
%define unmangled_version 0.3.0
%define release 1

Summary: Backup and Recovery Manager for PostgreSQL
Name: %{name}
Version: %{version}
Release: %{release}
Source0: %{name}-%{unmangled_version}.tar.gz
License: GPL-3.0
Group: Applications/Databases
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-buildroot
Prefix: %{_prefix}
BuildArch: noarch
Vendor: 2ndQuadrant Italia (Devise.IT S.r.l.) <info@2ndquadrant.it>
Requires: python26-psycopg2 python26-argh python26-dateutil

%description
BaRman is a tool to backup and recovery PostgreSQL clusters.


%prep
%setup -n %{name}-%{unmangled_version} -n %{name}-%{unmangled_version}

%build
python setup.py build

%install
python setup.py install --single-version-externally-managed -O1 --root=$RPM_BUILD_ROOT --record=INSTALLED_FILES

%clean
rm -rf $RPM_BUILD_ROOT

%files -f INSTALLED_FILES
%defattr(-,root,root)
