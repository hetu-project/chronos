FROM ubuntu

WORKDIR /app

RUN cargo build --release

CMD ["echo","-C","hello"]