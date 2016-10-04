FROM golang:1.7.1
MAINTAINER Travis Rhoden "trhoden@gmail.com"

ENV CEPH_VERSION jewel

RUN echo deb http://download.ceph.com/debian-$CEPH_VERSION/ jessie main | tee /etc/apt/sources.list.d/ceph-$CEPH_VERSION.list

# Running wget with no certificate checks, alternatively ssl-cert package should be installed
RUN wget --no-check-certificate -q -O- 'https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/release.asc' | apt-key add - \
    && apt-get update \
    && apt-get install -y --no-install-recommends librados-dev librbd-dev \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean
