FROM mattf/spark-base

MAINTAINER Matthew Farrellee <matt@cs.wisc.edu>

RUN yum install -y epel-release && yum clean all
# need proton 0.9, 0.8 has a bug in the int codec
RUN yum install -y python-setuptools https://kojipkgs.fedoraproject.org//packages/qpid-proton/0.9/3.el7/x86_64/python-qpid-proton-0.9-3.el7.x86_64.rpm https://kojipkgs.fedoraproject.org//packages/qpid-proton/0.9/3.el7/x86_64/qpid-proton-c-0.9-3.el7.x86_64.rpm && yum clean all
RUN easy_install null
RUN yum install -y numpy && yum clean all
