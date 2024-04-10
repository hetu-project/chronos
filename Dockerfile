FROM rust:1.67

WORKDIR /usr/src/myapp
COPY . .

RUN cargo --version

CMD ["echo","-C","hello"]