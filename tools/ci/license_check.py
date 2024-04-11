#!/usr/bin/env python3

import zipfile
from os import path as osp
from xml.etree.ElementTree import parse

XML_HEADER = '{http://maven.apache.org/POM/4.0.0}'


# Extract CDC revision from global pom.xml version

def extract_cdc_version():
    with open('./pom.xml') as _f:
        return parse(_f).getroot().find(f'{XML_HEADER}properties').find(f'{XML_HEADER}revision').text


CDC_VERSION = extract_cdc_version()
print("Checking CDC version:", CDC_VERSION)

suspicious_licenses = []
excluded_packages = {'flink-cdc-source-e2e-tests', 'flink-cdc-pipeline-e2e-tests'}


def check_module_license(path, depth=0):
    jar_file = osp.join(path, 'target', f'{osp.basename(path)}-{CDC_VERSION}.jar')

    if osp.basename(path) in excluded_packages:
        # skip excluded jars
        return
    if osp.isfile(jar_file):
        check_jar_license(jar_file, depth)

    with open(osp.join(path, 'pom.xml')) as _f:
        submodules = parse(_f).getroot().find(f'{XML_HEADER}modules')
        if submodules is None:
            return
        for module in submodules:
            check_module_license(osp.join(path, module.text), depth + 1)




def check_jar_license(jar_file, depth):
    print(' ' * depth + "Checking jar", jar_file)
    with zipfile.ZipFile(jar_file) as jar:
        for name in jar.namelist():

            data = jar.read(name)
            print(name, len(data), repr(data[:10]))



if __name__ == '__main__':
    check_module_license('.')
