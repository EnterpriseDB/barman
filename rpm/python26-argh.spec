%define name python26-argh
%define version 0.14.2
%define unmangled_version 0.14.2
%define release 1

Summary: A simple argparse wrapper.
Name: %{name}
Version: %{version}
Release: %{release}
Source0: http://pypi.python.org/packages/source/a/argh/argh-%{unmangled_version}.tar.gz
License: GNU Lesser General Public License (LGPL), Version 3
Group: Development/Libraries
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-buildroot
Prefix: %{_prefix}
BuildArch: noarch
Vendor: Andrey Mikhaylenko <andy@neithere.net>
Url: http://bitbucket.org/neithere/argh/

%description
Agrh, argparse!
===============

Did you ever say "argh" trying to remember the details of optparse or argparse
API? If yes, this package may be useful for you. It provides a very simple
wrapper for argparse with support for hierarchical commands that can be bound
to modules or classes. Argparse can do it; argh makes it easy.

%prep
%setup -q -n argh-%{version}

%build
python setup.py build

%install
python setup.py install --single-version-externally-managed -O1 --root=$RPM_BUILD_ROOT --record=INSTALLED_FILES

%clean
rm -rf $RPM_BUILD_ROOT

%files -f INSTALLED_FILES
%defattr(-,root,root)

%changelog
* Sat Dec 4 2011 - Marco Neciarini <marco.nenciarini@2ndquadrant.it> 0.3.0-1
- Initial packaging.
