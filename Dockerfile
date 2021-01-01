FROM registry.cn-hongkong.scoin.one/token/token-lua:master AS openresty-env

RUN mkdir /app

COPY ./* /app/

ENTRYPOINT [ "/app/tezos_index" ]