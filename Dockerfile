FROM usgswma/python:debian-slim-buster-python-3.7-e923cf03e5573ba531a90edc9b8ae0f5f7cb6e13

RUN apt-get install -y curl
RUN curl -sL https://deb.nodesource.com/setup_13.x | bash - && apt-get install -y nodejs

RUN mkdir $HOME/.npm && chmod 777 $HOME/.npm/ && chmod 777 $HOME/

USER $USER
