FROM python:3.6

# install google chrome
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -
RUN sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list'
RUN apt-get -y update
RUN apt-get install -y google-chrome-stable


RUN PHANTOMJS_VERSION=2.1.1 && \
    PHANTOMJS_PLATFORM=linux-x86_64 && \
    wget -q https://bitbucket.org/ariya/phantomjs/downloads/phantomjs-${PHANTOMJS_VERSION}-${PHANTOMJS_PLATFORM}.tar.bz2 && \
    echo "86dd9a4bf4aee45f1a84c9f61cf1947c1d6dce9b9e8d2a907105da7852460d2f *phantomjs-${PHANTOMJS_VERSION}-${PHANTOMJS_PLATFORM}.tar.bz2" > phantomjs.sha256 && \
    sha256sum --check phantomjs.sha256 && \
    rm -f phantomjs.sha256 && \
    mkdir -p /usr/local/phantomjs-${PHANTOMJS_VERSION} && \
    tar -xjf phantomjs-${PHANTOMJS_VERSION}-${PHANTOMJS_PLATFORM}.tar.bz2 -C /usr/local/phantomjs-$PHANTOMJS_VERSION && \
    ln -s /usr/local/phantomjs-${PHANTOMJS_VERSION}/phantomjs-${PHANTOMJS_VERSION}-${PHANTOMJS_PLATFORM}/bin/phantomjs /usr/local/bin/phantomjs && \
    rm -f phantomjs-${PHANTOMJS_VERSION}-${PHANTOMJS_PLATFORM}.tar.bz2 
# install chromedriver
RUN apt-get install -yqq unzip
RUN wget -O /tmp/chromedriver.zip http://chromedriver.storage.googleapis.com/`curl -sS chromedriver.storage.googleapis.com/LATEST_RELEASE`/chromedriver_linux64.zip
RUN unzip /tmp/chromedriver.zip chromedriver -d /usr/local/bin/

# set display port to avoid crash
ENV DISPLAY=:99


RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

# install selenium
RUN pip install git+https://github.com/scrapy/scrapy@master --no-dependencies --upgrade
COPY ./requirements.txt /usr/src/app/requirements.txt
RUN pip install -r requirements.txt
COPY . /usr/src/app
