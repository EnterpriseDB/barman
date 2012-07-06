# Use Python 2.6
%global pybasever 2.6
%global __python_ver 26
%global __python %{_bindir}/python%{pybasever}
%global __os_install_post %{__multiple_python_os_install_post}

%{!?python_sitelib: %define python_sitelib %(%{__python} -c "from distutils.sysconfig import get_python_lib; print get_python_lib()")}
%{!?python_sitearch: %define python_sitearch %(%{__python} -c "from distutils.sysconfig import get_python_lib; print get_python_lib(1)")}

Name:           python%{__python_ver}-dateutil
Version:        1.4.1
Release:        6%{?dist}
Summary:        Powerful extensions to the standard datetime module

Group:          Development/Languages
License:        Python
URL:            http://labix.org/python-dateutil
Source0:        http://labix.org/download/python-dateutil/python-dateutil-%{version}.tar.gz
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

# Redirect the exposed parts of the dateutil.zoneinfo API to remove references
# to the embedded copy of zoneinfo-2008e.tar.gz and instead use the system
# data from the "tzdata" package (rhbz#559309):
Patch0:         python-dateutil-1.4.1-remove-embedded-timezone-data.patch

BuildArch:      noarch
BuildRequires:  python%{__python_ver}-devel,python%{__python_ver}-setuptools

Requires:       tzdata

%description
The dateutil module provides powerful extensions to the standard datetime
module available in Python 2.3+.

%prep
%setup -n python-dateutil-%{version} -q

# Remove embedded copy of timezone data:
%patch0 -p1
rm dateutil/zoneinfo/zoneinfo-2008e.tar.gz

# Change encoding of NEWS file to UTF-8, preserving timestamp:
iconv -f ISO-8859-1 -t utf8 NEWS > NEWS.utf8 && \
  touch -r NEWS NEWS.utf8 && \
  mv NEWS.utf8 NEWS

%build
%{__python} setup.py build


%install
rm -rf $RPM_BUILD_ROOT
%{__python} setup.py install -O1 --skip-build --root $RPM_BUILD_ROOT


%clean
rm -rf $RPM_BUILD_ROOT

%check
%{__python} test.py

%files
%defattr(-,root,root,-)
%doc example.py LICENSE NEWS README
%{python_sitelib}/dateutil/
%{python_sitelib}/*.egg-info

%changelog
* Tue Jul 13 2010 David Malcolm <dmalcolm@redhat.com> - 1.4.1-6
- remove embedded copy of timezone data, and redirect the dateutil.zoneinfo
API accordingly
Resolves: rhbz#559309
- add a %%check, running the upstream selftest suite

* Tue Jul 13 2010 David Malcolm <dmalcolm@redhat.com> - 1.4.1-5
- add requirement on tzdata
Resolves: rhbz#559309
- fix encoding of the NEWS file

* Mon Nov 30 2009 Dennis Gregorovic <dgregor@redhat.com> - 1.4.1-4.1
- Rebuilt for RHEL 6

* Sun Jul 26 2009 Fedora Release Engineering <rel-eng@lists.fedoraproject.org> - 1.4.1-4
- Rebuilt for https://fedoraproject.org/wiki/Fedora_12_Mass_Rebuild

* Thu Feb 26 2009 Fedora Release Engineering <rel-eng@lists.fedoraproject.org> - 1.4.1-3
- Rebuilt for https://fedoraproject.org/wiki/Fedora_11_Mass_Rebuild

* Fri Feb 20 2009 Jef Spaleta <jspaleta AT fedoraproject DOT org> - 1.4.1-2
- small specfile fix

* Fri Feb 20 2009 Jef Spaleta <jspaleta AT fedoraproject DOT org> - 1.4.1-2
- New upstream version

* Sat Nov 29 2008 Ignacio Vazquez-Abrams <ivazqueznet+rpm@gmail.com> - 1.4-3
- Rebuild for Python 2.6

* Fri Aug 29 2008 Tom "spot" Callaway <tcallawa@redhat.com> - 1.4-2
- fix license tag

* Tue Jul 01 2008 Jef Spaleta <jspaleta AT fedoraproject DOT org> 1.4-1
- Latest upstream release

* Fri Jan 04 2008 Jef Spaleta <jspaleta@fedoraproject.org> 1.2-2
- Fix for egg-info file creation

* Thu Jun 28 2007 Orion Poplawski <orion@cora.nwra.com> 1.2-1
- Update to 1.2

* Mon Dec 11 2006 Jef Spaleta <jspaleta@gmail.com> 1.1-5
- Fix python-devel BR, as per discussion in maintainers-list

* Mon Dec 11 2006 Jef Spaleta <jspaleta@gmail.com> 1.1-4
- Release bump for rebuild against python 2.5 in devel tree

* Wed Jul 26 2006 Orion Poplawski <orion@cora.nwra.com> 1.1-3
- Add patch to fix building on x86_64

* Wed Feb 15 2006 Orion Poplawski <orion@cora.nwra.com> 1.1-2
- Rebuild for gcc/glibc changes

* Thu Dec 22 2005 Orion Poplawski <orion@cora.nwra.com> 1.1-1
- Update to 1.1

* Thu Jul 28 2005 Orion Poplawski <orion@cora.nwra.com> 1.0-1
- Update to 1.0

* Tue Jul 05 2005 Orion Poplawski <orion@cora.nwra.com> 0.9-1
- Initial Fedora Extras package
