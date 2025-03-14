FROM ruby:2.7.8-slim

USER root

# Get Ubuntu packages
RUN apt-get update -yqq && \
    apt-get install -yqq \
    build-essential \
    libclang-dev \
    clang \
    curl \
    git

# Update new packages
RUN apt-get update

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs| bash -s -- -y --default-toolchain=1.85 --profile minimal

WORKDIR /snow_duck_dir

COPY Gemfile Gemfile.lock snow_duck.gemspec ./

COPY lib/snow_duck/version.rb lib/snow_duck/version.rb

RUN gem update --system 3.4.14

RUN bundle install

COPY . ./
