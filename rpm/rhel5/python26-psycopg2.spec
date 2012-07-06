# Use Python 2.6
%global pybasever 2.6
%global __python_ver 26
%global __python %{_bindir}/python%{pybasever}
%global __os_install_post %{__multiple_python_os_install_post}

%{!?python_sitelib: %define python_sitelib %(%{__python} -c "from distutils.sysconfig import get_python_lib; print get_python_lib()")}
%{!?python_sitearch: %define python_sitearch %(%{__python} -c "from distutils.sysconfig import get_python_lib; print get_python_lib(1)")}

%define ZPsycopgDAdir %{_localstatedir}/lib/zope/Products/ZPsycopgDA

%global pgmajorversion 90
%global pginstdir /usr/pgsql-9.0
%global sname psycopg2

Summary:	A PostgreSQL database adapter for Python
Name:		python26-%{sname}
Version:	2.4.5
Release:	1%{?dist}
License:	LGPLv3 with exceptions
Group:		Applications/Databases
Url:		http://www.psycopg.org/psycopg/
Source0:	http://initd.org/psycopg/tarballs/PSYCOPG-2-4/%{sname}-%{version}.tar.gz
Patch0:		setup.cfg.patch
BuildRequires:	python%{__python_ver}-devel postgresql%{pgmajorversion}-devel
BuildRoot:	%{_tmppath}/%{name}-%{version}-%{release}-buildroot-%(%{__id_u} -n)
Requires:	python-abi = %(%{__python} -c "import sys ; print sys.version[:3]")

%description
psycopg is a PostgreSQL database adapter for the Python programming
language (just like pygresql and popy.) It was written from scratch
with the aim of being very small and fast, and stable as a rock. The
main advantages of psycopg are that it supports the full Python
DBAPI-2.0 and being thread safe at level 2.

%package doc
Summary:	Documentation for psycopg python PostgreSQL database adapter
Group:		Documentation
Requires:	%{name} = %{version}-%{release}

%description doc
Documentation and example files for the psycopg python PostgreSQL
database adapter.

%package test
Summary:	Tests for psycopg2
Group:		Development/Libraries
Requires:	%{name} = %{version}-%{release}

%description test
Tests for psycopg2.

%package zope
Summary:	Zope Database Adapter ZPsycopgDA
Group:		Applications/Databases
Requires:	%{name} = %{version}-%{release} zope

%description zope
Zope Database Adapter for PostgreSQL, called ZPsycopgDA

%prep
%setup -q -n psycopg2-%{version}
%patch0 -p0

%build
%{__python} setup.py build
# Fix for wrong-file-end-of-line-encoding problem; upstream also must fix this.
for i in `find doc -iname "*.html"`; do sed -i 's/\r//' $i; done
for i in `find doc -iname "*.css"`; do sed -i 's/\r//' $i; done

%install
rm -Rf %{buildroot}
mkdir -p %{buildroot}%{python_sitearch}/psycopg2
%{__python} setup.py install --no-compile --root %{buildroot}

install -d %{buildroot}%{ZPsycopgDAdir}
cp -pr ZPsycopgDA/* %{buildroot}%{ZPsycopgDAdir}

%clean
rm -rf %{buildroot}

%files
%defattr(-,root,root)
%doc AUTHORS ChangeLog INSTALL LICENSE README
%dir %{python_sitearch}/psycopg2
%{python_sitearch}/psycopg2/*.py
%{python_sitearch}/psycopg2/*.pyc
%{python_sitearch}/psycopg2/*.so
%{python_sitearch}/psycopg2/*.pyo
%{python_sitearch}/psycopg2-*.egg-info

%files doc
%defattr(-,root,root)
%doc doc examples/

%files test
%defattr(-,root,root)
%{python_sitearch}/%{sname}/tests/*

%files zope
%defattr(-,root,root)
%dir %{ZPsycopgDAdir}
%{ZPsycopgDAdir}/*.py
%{ZPsycopgDAdir}/*.pyo
%{ZPsycopgDAdir}/*.pyc
%{ZPsycopgDAdir}/dtml/*
%{ZPsycopgDAdir}/icons/*

%changelog
* Wed May 9 2012 - Marco Neciarini <marco.nenciarini@2ndquadrant.it> 2.4.5-1
- Update to version 2.4.5

* Mon Aug 22 2011 Devrim GUNDUZ <devrim@gunduz.org> 2.4.2-1
- Update to 2.4.2
- Add a patch for pg_config path.
- Add new subpackage: test

* Tue Mar 16 2010 Devrim GUNDUZ <devrim@gunduz.org> 2.0.14-1
- Update to 2.0.14

* Mon Oct 19 2009 Devrim GUNDUZ <devrim@commandprompt.com> 2.0.13-1
- Update to 2.0.13

* Mon Sep 7 2009 Devrim GUNDUZ <devrim@commandprompt.com> 2.0.12-1
- Update to 2.0.12

* Tue May 26 2009 Devrim GUNDUZ <devrim@commandprompt.com> 2.0.11-1
- Update to 2.0.11

* Fri Apr 24 2009 Devrim GUNDUZ <devrim@commandprompt.com> 2.0.10-1
- Update to 2.0.10

* Thu Mar 2 2009 Devrim GUNDUZ <devrim@commandprompt.com> 2.0.9-1
- Update to 2.0.9

* Wed Apr 30 2008 - Devrim GUNDUZ <devrim@commandprompt.com> 2.0.7-1
- Update to 2.0.7

* Fri Jun 15 2007 - Devrim GUNDUZ <devrim@commandprompt.com> 2.0.6-1
- Update to 2.0.6

* Sun May 06 2007 Thorsten Leemhuis <fedora [AT] leemhuis [DOT] info>
- rebuilt for RHEL5 final

* Wed Dec 6 2006 - Devrim GUNDUZ <devrim@commandprompt.com> 2.0.5.1-4
- Rebuilt for PostgreSQL 8.2.0

* Mon Sep 11 2006 - Devrim GUNDUZ <devrim@commandprompt.com> 2.0.5.1-3
- Rebuilt

* Wed Sep 6 2006 - Devrim GUNDUZ <devrim@commandprompt.com> 2.0.5.1-2
- Remove ghost'ing, per Python Packaging Guidelines

* Mon Sep 4 2006 - Devrim GUNDUZ <devrim@commandprompt.com> 2.0.5.1-1
- Update to 2.0.5.1

* Sun Aug 6 2006 - Devrim GUNDUZ <devrim@commandprompt.com> 2.0.3-3
- Fixed zope package dependencies and macro definition, per bugzilla review (#199784)
- Fixed zope package directory ownership, per bugzilla review (#199784)
- Fixed cp usage for zope subpackage, per bugzilla review (#199784)

* Mon Jul 31 2006 - Devrim GUNDUZ <devrim@commandprompt.com> 2.0.3-2
- Fixed 64 bit builds
- Fixed license
- Added Zope subpackage
- Fixed typo in doc description
- Added macro for zope subpackage dir

* Mon Jul 31 2006 - Devrim GUNDUZ <devrim@commandprompt.com> 2.0.3-1
- Update to 2.0.3
- Fixed spec file, per bugzilla review (#199784)

* Sat Jul 22 2006 - Devrim GUNDUZ <devrim@commandprompt.com> 2.0.2-3
- Removed python dependency, per bugzilla review. (#199784)
- Changed doc package group, per bugzilla review. (#199784)
- Replaced dos2unix with sed, per guidelines and bugzilla review (#199784)
- Fix changelog dates

* Sat Jul 21 2006 - Devrim GUNDUZ <devrim@commandprompt.com> 2.0.2-2
- Added dos2unix to buildrequires
- removed python related part from package name

* Fri Jul 20 2006 - Devrim GUNDUZ <devrim@commandprompt.com> 2.0.2-1
- Fix rpmlint errors, including dos2unix solution
- Re-engineered spec file

* Fri Jan 23 2006 - Devrim GUNDUZ <devrim@commandprompt.com>
- First 2.0.X build

* Fri Jan 23 2006 - Devrim GUNDUZ <devrim@commandprompt.com>
- Update to 1.2.21

* Tue Dec 06 2005 - Devrim GUNDUZ <devrim@commandprompt.com>
- Initial release for 1.1.20
