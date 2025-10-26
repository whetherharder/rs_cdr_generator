# Memory Profiling with Heaptrack

Данный проект настроен для профилирования памяти через Heaptrack в Docker контейнере.

## Требования

- Docker Desktop для macOS
- Файлы test_db_1m.arrow и test_config.yaml в корне проекта

## Быстрый старт

### 1. Собрать Docker образ

```bash
docker-compose build
```

Это займет несколько минут при первом запуске (установка Rust и сборка проекта).

### 2. Запустить контейнер

```bash
docker-compose up -d
```

### 3. Запустить профилирование

```bash
# Войти в контейнер
docker-compose exec heaptrack /bin/bash

# Внутри контейнера запустить профилирование (1M subscribers)
./profile_bytehound.sh 1000000

# Или для другого количества абонентов
./profile_bytehound.sh 100000
```

### 4. Просмотреть результаты

Найти .gz файл:
```bash
ls -lh prof_output/heaptrack.out.*.gz
```

Распечатать текстовый отчет:
```bash
heaptrack_print prof_output/heaptrack.out.*.gz | less
```

Или скопировать .gz файл на macOS и открыть в heaptrack GUI (если установлен)

### 5. Остановить и очистить

```bash
# Выйти из контейнера (Ctrl+D)
# Остановить контейнер
docker-compose down

# Очистить профили
rm -rf prof_output/heaptrack.out.*.gz
```

## Альтернативный способ (без docker-compose)

### Собрать образ напрямую

```bash
docker build -f Dockerfile.bytehound -t rs_cdr_bytehound .
```

### Запустить контейнер вручную

```bash
docker run -it --rm \
  -v $(pwd):/work \
  -v $(pwd)/test_db_1m.arrow:/work/test_db_1m.arrow:ro \
  -v $(pwd)/test_config.yaml:/work/test_config.yaml:ro \
  -v $(pwd)/prof_output:/work/prof_output \
  rs_cdr_heaptrack \
  /bin/bash
```

Затем внутри контейнера:
```bash
./profile_bytehound.sh 1000000
heaptrack_print prof_output/heaptrack.out.*.gz
```

## Анализ результатов

Heaptrack предоставляет:
- **heaptrack_print**: текстовый отчет с топ аллокаторов
- **heaptrack GUI**: графический интерфейс с flame graphs (требует установки на macOS)
- Детальную информацию о каждой аллокации с callstacks
- Временную динамику использования памяти
- Группировку по функциям

Пример вывода heaptrack_print:
```
MOST CALLS TO ALLOCATION FUNCTIONS
343518 calls to allocation functions with 1.2GB peak consumption from:
  in /work/target/release/rs_cdr_generator
  ...
```

## Troubleshooting

### Docker не запускается
Убедитесь что Docker Desktop запущен:
```bash
docker ps
```

### Файлы не найдены
Проверьте что test_db_1m.arrow и test_config.yaml есть в корне проекта:
```bash
ls -lh test_db_1m.arrow test_config.yaml
```

### Heaptrack не работает
Убедитесь что используете Linux контейнер (heaptrack требует Linux).

### Ошибки сборки
Проверьте логи сборки:
```bash
docker-compose build --no-cache
```
