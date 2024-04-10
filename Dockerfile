FROM rust:1.67

WORKDIR /usr/src/myapp
COPY . .

RUN cargo build

CMD ["echo","-C","hello"]