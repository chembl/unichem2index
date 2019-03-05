FROM debian:stretch-slim

LABEL maintainer="arcila@ebi.ac.uk"

ENV TNS_ADMIN=/usr/lib/oracle/18.3/client64 \
    ORACLE_HOME=/usr/lib/oracle/18.3/client64 \
    PATH=$PATH:/usr/lib/oracle/18.3/client64/bin:/usr/local/go/bin \
    UNICHEM_ORA_SETUP=/opt/orasetup

SHELL ["/bin/bash", "-c"]

RUN apt-get -qq update && apt-get -y -qq upgrade && \
    apt-get -y -qq install alien wget libaio1 gettext git locales && \
    sed -i 's/^# *\(en_US.UTF-8\)/\1/' /etc/locale.gen && \
    locale-gen && \
    echo "export LC_ALL=en_US.UTF-8" >> ~/.bashrc && \
    echo "export LANG=en_US.UTF-8" >> ~/.bashrc && \
    echo "export LANGUAGE=en_US.UTF-8" >> ~/.bashrc

RUN wget https://dl.google.com/go/go1.12.linux-amd64.tar.gz && \
    tar -C /usr/local -xzf go1.12.linux-amd64.tar.gz && \
    rm -f go1.12.linux-amd64.tar.gz && \
    echo "export PATH=$PATH:/usr/local/go/bin" >> ~/.bashrc

WORKDIR ${UNICHEM_ORA_SETUP}
ADD . ${UNICHEM_ORA_SETUP}

RUN alien oracle-instantclient18.3-* && \
    rm -f oracle-instantclient*.rpm

RUN dpkg -i oracle-instantclient*.deb && \
    echo /usr/lib/oracle/18.3/client64/lib > /etc/ld.so.conf.d/oracle-instantclient18.3.conf && \
    ldconfig

RUN mv tnsnames.ora /usr/lib/oracle/18.3/client64

RUN chmod 755 fireitup.sh

EXPOSE 8000

ENTRYPOINT [ "./fireitup.sh" ]