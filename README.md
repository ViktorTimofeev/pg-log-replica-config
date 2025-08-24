# Логическая репликация PostgreSQL 14 для WMS системы

## Обзор архитектуры

Данная система представляет собой дизайн логической репликации PostgreSQL 14 для высоконагруженной OLTP системы управления складом (WMS) с целью построения сложных аналитических отчетов.

### Архитектурная схема

```
[WMS OLTP Primary] ←→ [Logical Replica] ←→ [Analytics Layer]
       ↓                       ↓                ↓
   High TPS              Data Processing    BI Reports
   Operations            (ETL/Transform)    (Complex Queries)
```

## Компоненты системы

### 1. Primary Server (OLTP)
- **Назначение**: Основная рабочая нагрузка WMS
- **Конфигурация**: `postgresql.conf.primary`
- **Особенности**: Оптимизирован для транзакций, минимальный WAL overhead

### 2. Replica Server (Analytics)
- **Назначение**: Аналитические запросы и отчеты
- **Конфигурация**: `postgresql.conf.replica`
- **Особенности**: Оптимизирован для сложных запросов, материализованные представления

### 3. Аналитические схемы
- `warehouse_analytics` - аналитика складов
- `inventory_analytics` - аналитика инвентаря
- `order_analytics` - аналитика заказов
- `customer_analytics` - аналитика клиентов
- `analytics` - общие аналитические данные

## Развертывание

### Шаг 1: Настройка Primary Server

1. **Остановите PostgreSQL**:
```bash
sudo systemctl stop postgresql
```

2. **Скопируйте конфигурацию**:
```bash
sudo cp postgresql.conf.primary /etc/postgresql/14/main/postgresql.conf
```

3. **Настройте pg_hba.conf** для репликации:
```bash
# Добавьте в pg_hba.conf
host    wms_db    wms_repl_user    0.0.0.0/0    md5
```

4. **Запустите PostgreSQL**:
```bash
sudo systemctl start postgresql
```

5. **Выполните настройку репликации**:
```bash
psql -U postgres -d wms_db -f setup_logical_replication.sql
```

### Шаг 2: Настройка Replica Server

1. **Установите PostgreSQL 14** на отдельном сервере

2. **Скопируйте конфигурацию**:
```bash
sudo cp postgresql.conf.replica /etc/postgresql/14/main/postgresql.conf
```

3. **Создайте базу данных**:
```bash
createdb -U postgres wms_db
```

4. **Настройте подписку**:
```bash
psql -U postgres -d wms_db -f setup_replica.sql
```

5. **Создайте аналитические представления**:
```bash
psql -U postgres -d wms_db -f create_analytics_views.sql
```

6. **Создайте материализованные представления**:
```bash
psql -U postgres -d wms_db -f create_materialized_views.sql
```

## Конфигурация

### Ключевые параметры Primary Server

```ini
# WAL Configuration
wal_level = logical                    # Критично для логической репликации
max_wal_senders = 10                  # Поддержка множественных реплик
max_replication_slots = 10            # Слоты логической репликации
wal_keep_segments = 1000             # Удержание WAL сегментов

# Logical Replication
max_logical_replication_workers = 4   # Работники репликации
max_worker_processes = 20             # Общее количество работников
```

### Ключевые параметры Replica Server

```ini
# Memory Configuration
shared_buffers = 16GB                 # 50% RAM для аналитики
work_mem = 256MB                      # Больше памяти для сложных запросов
effective_cache_size = 32GB           # 80% RAM

# Analytics Optimization
enable_partitionwise_join = on        # Партиционированные соединения
enable_parallel_hash = on             # Параллельные hash соединения
default_statistics_target = 500       # Лучшая статистика для планировщика
```

## Мониторинг и обслуживание

### Ежедневные проверки

1. **Статус репликации**:
```sql
-- На Primary
SELECT * FROM pg_stat_replication WHERE application_name = 'wms_analytics_replica';

-- На Replica
SELECT * FROM pg_stat_subscription WHERE subname = 'wms_analytics_sub';
```

2. **Задержка репликации**:
```sql
-- На Primary
SELECT 
    pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) as lag_bytes,
    pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) / 1024.0 / 1024.0 / 16.0 as lag_minutes
FROM pg_stat_replication 
WHERE application_name = 'wms_analytics_replica';
```

### Еженедельные задачи

1. **Обновление материализованных представлений**:
```sql
-- Ежедневные представления
SELECT refresh_analytics_views();

-- Еженедельные представления
SELECT refresh_weekly_analytics_views();
```

2. **Анализ статистики**:
```sql
-- Проверка использования индексов
SELECT schemaname, tablename, indexname, idx_scan, idx_tup_read
FROM pg_stat_user_indexes 
ORDER BY idx_scan DESC;
```

### Ежемесячные задачи

1. **Обновление месячных представлений**:
```sql
SELECT refresh_monthly_analytics_views();
```

2. **Анализ производительности**:
```sql
-- Медленные запросы
SELECT query, calls, total_time, mean_time
FROM pg_stat_statements 
ORDER BY mean_time DESC 
LIMIT 20;
```

## Автоматизация

### Cron задачи для обновления представлений

```bash
# Ежедневно в 2:00
0 2 * * * psql -U wms_analytics_user -d wms_db -c "SELECT refresh_analytics_views();"

# Еженедельно в понедельник в 3:00
0 3 * * 1 psql -U wms_analytics_user -d wms_db -c "SELECT refresh_weekly_analytics_views();"

# Ежемесячно 1-го числа в 4:00
0 4 1 * * psql -U wms_analytics_user -d wms_db -c "SELECT refresh_monthly_analytics_views();"
```

### Prometheus метрики

Создайте файл `postgres_exporter.yml`:

```yaml
pg_stat_replication:
  query: "SELECT pid, usename, application_name, state, sent_lsn, write_lsn, flush_lsn, replay_lsn FROM pg_stat_replication"
  master: true
  metrics:
    - pid:
        usage: "LABEL"
        description: "Process ID of the WAL sender process"
    - usename:
        usage: "LABEL"
        description: "Name of the user logged into this WAL sender process"
    - application_name:
        usage: "LABEL"
        description: "Name of the application that is connected to this WAL sender"
    - state:
        usage: "LABEL"
        description: "Current WAL sender state"
    - sent_lsn:
        usage: "GAUGE"
        description: "Last transaction log position sent on this connection"
    - write_lsn:
        usage: "GAUGE"
        description: "Last transaction log position written to disk by this standby server"
    - flush_lsn:
        usage: "GAUGE"
        description: "Last transaction log position flushed to disk by this standby server"
    - replay_lsn:
        usage: "GAUGE"
        description: "Last transaction log position replayed into the database on this standby server"
```

## Устранение неполадок

### Частые проблемы

1. **Задержка репликации**:
   - Проверьте сетевую связность
   - Увеличьте `max_wal_senders` и `max_replication_slots`
   - Проверьте диск I/O на replica

2. **Конфликты репликации**:
   - Проверьте логи PostgreSQL
   - Убедитесь в отсутствии прямых изменений на replica
   - Проверьте права доступа пользователя репликации

3. **Нехватка памяти**:
   - Увеличьте `shared_buffers` и `work_mem`
   - Настройте `effective_cache_size`
   - Мониторьте использование swap

### Логи и диагностика

1. **Включите детальное логирование**:
```ini
log_min_duration_statement = 1000
log_checkpoints = on
log_connections = on
log_disconnections = on
log_lock_waits = on
```

2. **Мониторинг WAL**:
```sql
-- Размер WAL файлов
SELECT pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0'));

-- Статус слотов репликации
SELECT slot_name, active, restart_lsn, confirmed_flush_lsn
FROM pg_replication_slots;
```

## Производительность

### Оптимизация аналитических запросов

1. **Используйте материализованные представления** для часто запрашиваемых данных
2. **Создавайте составные индексы** для сложных WHERE условий
3. **Применяйте партиционирование** для больших таблиц по дате
4. **Настройте параллельные запросы** для больших таблиц

### Мониторинг производительности

```sql
-- Статистика по таблицам
SELECT schemaname, tablename, n_tup_ins, n_tup_upd, n_tup_del, n_live_tup, n_dead_tup
FROM pg_stat_user_tables
ORDER BY n_live_tup DESC;

-- Статистика по индексам
SELECT schemaname, tablename, indexname, idx_scan, idx_tup_read, idx_tup_fetch
FROM pg_stat_user_indexes
ORDER BY idx_scan DESC;
```

## Безопасность

### Рекомендации по безопасности

1. **Используйте отдельного пользователя** для репликации
2. **Ограничьте доступ** к replica только для аналитических пользователей
3. **Шифруйте соединения** между серверами
4. **Регулярно обновляйте пароли** пользователей
5. **Мониторьте доступ** к базе данных

### Настройка SSL

```ini
# В postgresql.conf
ssl = on
ssl_cert_file = '/etc/ssl/certs/ssl-cert-snakeoil.pem'
ssl_key_file = '/etc/ssl/private/ssl-cert-snakeoil.key'
```

## Резервное копирование

### Стратегия бэкапов

1. **Primary Server**: Ежедневные полные бэкапы + WAL архивирование
2. **Replica Server**: Еженедельные полные бэкапы
3. **Аналитические данные**: Экспорт материализованных представлений

### Скрипт бэкапа

```bash
#!/bin/bash
# backup_replica.sh

DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR="/backup/replica"
DB_NAME="wms_db"

# Создание бэкапа
pg_dump -U postgres -h localhost -d $DB_NAME \
  --schema=warehouse_analytics \
  --schema=inventory_analytics \
  --schema=order_analytics \
  --schema=customer_analytics \
  --schema=analytics \
  -f "$BACKUP_DIR/analytics_backup_$DATE.sql"

# Сжатие
gzip "$BACKUP_DIR/analytics_backup_$DATE.sql"

# Удаление старых бэкапов (старше 30 дней)
find $BACKUP_DIR -name "analytics_backup_*.sql.gz" -mtime +30 -delete
```

## Заключение

Данная архитектура логической репликации обеспечивает:

- **Высокую производительность** OLTP операций на primary сервере
- **Мощные аналитические возможности** на replica сервере
- **Минимальное влияние** аналитических запросов на основную систему
- **Масштабируемость** для добавления дополнительных реплик
- **Отказоустойчивость** и возможность быстрого восстановления

Регулярный мониторинг и обслуживание обеспечат стабильную работу системы в долгосрочной перспективе.
