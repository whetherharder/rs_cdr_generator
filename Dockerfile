# Сборка образа для изолированного (air-gapped) контура.
#
# В контуре нет доступа к crates.io, поэтому сборка идёт офлайн:
# зависимости берутся не из сети, а из каталога vendor/, который должен
# лежать в контексте сборки рядом с этим Dockerfile (см. README —
# `cargo vendor vendor` выполняется ОДИН РАЗ на машине с интернетом,
# результат передаётся в контур вместе с исходниками, а не собирается
# заново в самом контуре). Committed vendor-каталог в git не кладём —
# это ~140 МБ бинарных данных чужого кода, раздувающих историю
# репозитория без пользы: диффать их незачем, а актуальность
# гарантирует не git, а `Cargo.lock` (зафиксирован в репозитории) —
# `cargo vendor` детерминированно восстанавливает из него тот же набор.
#
# Многоступенчатая сборка: тяжёлый toolchain и vendor-каталог остаются
# в builder-слое, в итоговый образ попадает только статически
# слинкованный (со стандартной libc контейнера) бинарник.

FROM rust:1-bookworm AS builder

WORKDIR /build

# Конфиг офлайн-источника и зафиксированные версии — раньше исходников,
# чтобы слой с зависимостями кэшировался отдельно от слоя с кодом.
COPY .cargo/ .cargo/
COPY Cargo.toml Cargo.lock ./
COPY vendor/ vendor/
COPY src/ src/

# --locked: сборка обязана использовать ровно версии из Cargo.lock,
# --offline: обращение к crates.io должно быть невозможно даже случайно —
# так расхождение с контуром (недостающий крейт в vendor/) ловится здесь,
# а не на площадке клиента.
RUN cargo build --release --locked --offline

FROM debian:bookworm-slim AS runtime

# Генератор пишет CSV.gz локально и в сеть не ходит — ca-certificates
# и прочий TLS-стек рантайму не нужен, ставить нечего.
COPY --from=builder /build/target/release/rs_cdr_generator /usr/local/bin/rs_cdr_generator

WORKDIR /data
ENTRYPOINT ["/usr/local/bin/rs_cdr_generator"]
CMD ["--help"]
