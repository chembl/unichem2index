FROM debian:stretch-slim

LABEL maintainer="arcila@ebi.ac.uk"

ENV TNS_ADMIN=/usr/lib/oracle/18.3/client64 \
    ORACLE_HOME=/usr/lib/oracle/18.3/client64 \
    PATH=$PATH:/usr/lib/oracle/18.3/client64/bin:/usr/local/go/bin \
    UNICHEM=/opt/unichem

SHELL ["/bin/bash", "-c"]

RUN apt-get -qq update && apt-get -y -qq upgrade && \
    apt-get -y -qq install alien wget libaio1 gettext git locales && \
    sed -i 's/^# *\(en_US.UTF-8\)/\1/' /etc/locale.gen && \
    locale-gen && \
    echo "export LC_ALL=en_US.UTF-8" >> ~/.bashrc && \
    echo "export LANG=en_US.UTF-8" >> ~/.bashrc && \
    echo "export LANGUAGE=en_US.UTF-8" >> ~/.bashrc

WORKDIR ${UNICHEM}
ADD . ${UNICHEM}

RUN alien oracle-instantclient18.3-* && \
    rm -f oracle-instantclient*.rpm

RUN dpkg -i oracle-instantclient*.deb && \
    echo /usr/lib/oracle/18.3/client64/lib > /etc/ld.so.conf.d/oracle-instantclient18.3.conf && \
    ldconfig

RUN mv tnsnames.ora ${TNS_ADMIN}}} && \
    mv sqlnet.ora ${TNS_ADMIN}}}

RUN chmod 755 unichem2index
RUN chmod 755 fireitup.sh

EXPOSE 8000

ENTRYPOINT [ "./fireitup.sh" ]