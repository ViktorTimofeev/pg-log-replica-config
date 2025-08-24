#!/bin/bash

# ===================================================
# Автоматическое развертывание логической репликации
# PostgreSQL 14 для WMS системы
# ===================================================

set -e  # Остановка при ошибке

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Функции логирования
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Проверка прав root
check_root() {
    if [[ $EUID -ne 0 ]]; then
        log_error "Этот скрипт должен быть запущен с правами root"
        exit 1
    fi
}

# Проверка операционной системы
check_os() {
    if [[ -f /etc/debian_version ]]; then
        OS="debian"
        log_info "Обнаружена Debian/Ubuntu система"
    elif [[ -f /etc/redhat-release ]]; then
        OS="redhat"
        log_info "Обнаружена RedHat/CentOS система"
    else
        log_error "Неподдерживаемая операционная система"
        exit 1
    fi
}

# Проверка PostgreSQL
check_postgresql() {
    if ! command -v psql &> /dev/null; then
        log_error "PostgreSQL не установлен"
        exit 1
    fi
    
    PG_VERSION=$(psql --version | grep -oP '\d+\.\d+' | head -1)
    log_info "Обнаружен PostgreSQL версии $PG_VERSION"
    
    if [[ $(echo "$PG_VERSION < 14" | bc -l) -eq 1 ]]; then
        log_error "Требуется PostgreSQL 14 или выше"
        exit 1
    fi
}

# Установка зависимостей
install_dependencies() {
    log_info "Установка зависимостей..."
    
    if [[ "$OS" == "debian" ]]; then
        apt-get update
        apt-get install -y postgresql-client postgresql-common bc
    elif [[ "$OS" == "redhat" ]]; then
        yum install -y postgresql postgresql-contrib bc
    fi
    
    log_success "Зависимости установлены"
}

# Настройка Primary Server
setup_primary() {
    log_info "Настройка Primary Server..."
    
    # Остановка PostgreSQL
    log_info "Остановка PostgreSQL..."
    systemctl stop postgresql
    
    # Резервное копирование конфигурации
    if [[ -f /etc/postgresql/14/main/postgresql.conf ]]; then
        cp /etc/postgresql/14/main/postgresql.conf /etc/postgresql/14/main/postgresql.conf.backup.$(date +%Y%m%d_%H%M%S)
        log_info "Создан бэкап конфигурации"
    fi
    
    # Копирование новой конфигурации
    cp postgresql.conf.primary /etc/postgresql/14/main/postgresql.conf
    chown postgres:postgres /etc/postgresql/14/main/postgresql.conf
    chmod 644 /etc/postgresql/14/main/postgresql.conf
    
    # Настройка pg_hba.conf для репликации
    if ! grep -q "wms_repl_user" /etc/postgresql/14/main/pg_hba.conf; then
        echo "host    wms_db    wms_repl_user    0.0.0.0/0    md5" >> /etc/postgresql/14/main/pg_hba.conf
        log_info "Добавлены права доступа для репликации"
    fi
    
    # Запуск PostgreSQL
    log_info "Запуск PostgreSQL..."
    systemctl start postgresql
    
    # Ожидание запуска
    sleep 5
    
    # Проверка статуса
    if systemctl is-active --quiet postgresql; then
        log_success "PostgreSQL успешно запущен"
    else
        log_error "Ошибка запуска PostgreSQL"
        exit 1
    fi
    
    # Настройка репликации
    log_info "Настройка логической репликации..."
    sudo -u postgres psql -d wms_db -f setup_logical_replication.sql
    
    log_success "Primary Server настроен"
}

# Настройка Replica Server
setup_replica() {
    log_info "Настройка Replica Server..."
    
    # Остановка PostgreSQL
    log_info "Остановка PostgreSQL..."
    systemctl stop postgresql
    
    # Резервное копирование конфигурации
    if [[ -f /etc/postgresql/14/main/postgresql.conf ]]; then
        cp /etc/postgresql/14/main/postgresql.conf /etc/postgresql/14/main/postgresql.conf.backup.$(date +%Y%m%d_%H%M%S)
        log_info "Создан бэкап конфигурации"
    fi
    
    # Копирование новой конфигурации
    cp postgresql.conf.replica /etc/postgresql/14/main/postgresql.conf
    chown postgres:postgres /etc/postgresql/14/main/postgresql.conf
    chmod 644 /etc/postgresql/14/main/postgresql.conf
    
    # Запуск PostgreSQL
    log_info "Запуск PostgreSQL..."
    systemctl start postgresql
    
    # Ожидание запуска
    sleep 5
    
    # Проверка статуса
    if systemctl is-active --quiet postgresql; then
        log_success "PostgreSQL успешно запущен"
    else
        log_error "Ошибка запуска PostgreSQL"
        exit 1
    fi
    
    # Создание базы данных
    log_info "Создание базы данных..."
    sudo -u postgres createdb wms_db
    
    # Настройка подписки
    log_info "Настройка подписки..."
    sudo -u postgres psql -d wms_db -f setup_replica.sql
    
    # Создание аналитических представлений
    log_info "Создание аналитических представлений..."
    sudo -u postgres psql -d wms_db -f create_analytics_views.sql
    
    # Создание материализованных представлений
    log_info "Создание материализованных представлений..."
    sudo -u postgres psql -d wms_db -f create_materialized_views.sql
    
    log_success "Replica Server настроен"
}

# Проверка репликации
check_replication() {
    log_info "Проверка статуса репликации..."
    
    # Проверка на Primary
    log_info "Статус на Primary Server:"
    sudo -u postgres psql -d wms_db -c "
    SELECT 
        pid,
        usename,
        application_name,
        state,
        pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) as lag_bytes
    FROM pg_stat_replication 
    WHERE application_name = 'wms_analytics_replica';
    "
    
    # Проверка на Replica
    log_info "Статус на Replica Server:"
    sudo -u postgres psql -d wms_db -c "
    SELECT 
        subname,
        pid,
        status,
        received_lsn,
        latest_end_lsn
    FROM pg_stat_subscription;
    "
}

# Создание пользователей для мониторинга
create_monitoring_users() {
    log_info "Создание пользователей для мониторинга..."
    
    # Создание пользователя для Prometheus
    sudo -u postgres psql -d wms_db -c "
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'prometheus_user') THEN
            CREATE ROLE prometheus_user WITH LOGIN PASSWORD 'prometheus_password_123';
        END IF;
    END
    \$\$;
    "
    
    # Создание пользователя для Grafana
    sudo -u postgres psql -d wms_db -c "
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'grafana_user') THEN
            CREATE ROLE grafana_user WITH LOGIN PASSWORD 'grafana_password_123';
        END IF;
    END
    \$\$;
    "
    
    # Предоставление прав
    sudo -u postgres psql -d wms_db -c "
    GRANT CONNECT ON DATABASE wms_db TO prometheus_user, grafana_user;
    GRANT USAGE ON ALL SCHEMAS IN DATABASE wms_db TO prometheus_user, grafana_user;
    GRANT SELECT ON ALL TABLES IN DATABASE wms_db TO prometheus_user, grafana_user;
    "
    
    log_success "Пользователи для мониторинга созданы"
}

# Настройка автоматического обновления представлений
setup_automation() {
    log_info "Настройка автоматизации..."
    
    # Создание cron задач
    (crontab -l 2>/dev/null; echo "0 2 * * * sudo -u postgres psql -d wms_db -c \"SELECT refresh_analytics_views();\"") | crontab -
    (crontab -l 2>/dev/null; echo "0 3 * * 1 sudo -u postgres psql -d wms_db -c \"SELECT refresh_weekly_analytics_views();\"") | crontab -
    (crontab -l 2>/dev/null; echo "0 4 1 * * sudo -u postgres psql -d wms_db -c \"SELECT refresh_monthly_analytics_views();\"") | crontab -
    
    log_success "Cron задачи настроены"
}

# Создание скриптов мониторинга
create_monitoring_scripts() {
    log_info "Создание скриптов мониторинга..."
    
    # Скрипт проверки репликации
    cat > /usr/local/bin/check_replication.sh << 'EOF'
#!/bin/bash
# Проверка статуса репликации

PRIMARY_HOST="localhost"
REPLICA_HOST="localhost"
DB_NAME="wms_db"
REPL_USER="wms_repl_user"

echo "=== Проверка репликации $(date) ==="

echo "Primary Server Status:"
psql -h $PRIMARY_HOST -U postgres -d $DB_NAME -c "
SELECT 
    pid,
    usename,
    application_name,
    state,
    pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) as lag_bytes,
    ROUND(pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) / 1024.0 / 1024.0 / 16.0, 2) as lag_minutes
FROM pg_stat_replication 
WHERE application_name = 'wms_analytics_replica';
"

echo "Replica Server Status:"
psql -h $REPLICA_HOST -U postgres -d $DB_NAME -c "
SELECT 
    subname,
    pid,
    status,
    received_lsn,
    latest_end_lsn,
    latest_end_time
FROM pg_stat_subscription;
"
EOF
    
    chmod +x /usr/local/bin/check_replication.sh
    
    # Скрипт обновления представлений
    cat > /usr/local/bin/refresh_views.sh << 'EOF'
#!/bin/bash
# Обновление материализованных представлений

DB_NAME="wms_db"
REFRESH_TYPE="${1:-daily}"

echo "=== Обновление представлений: $REFRESH_TYPE $(date) ==="

case $REFRESH_TYPE in
    "daily")
        psql -U postgres -d $DB_NAME -c "SELECT refresh_analytics_views();"
        ;;
    "weekly")
        psql -U postgres -d $DB_NAME -c "SELECT refresh_weekly_analytics_views();"
        ;;
    "monthly")
        psql -U postgres -d $DB_NAME -c "SELECT refresh_monthly_analytics_views();"
        ;;
    *)
        echo "Использование: $0 [daily|weekly|monthly]"
        exit 1
        ;;
esac

echo "Обновление завершено"
EOF
    
    chmod +x /usr/local/bin/refresh_views.sh
    
    log_success "Скрипты мониторинга созданы"
}

# Основная функция
main() {
    log_info "Начало развертывания логической репликации PostgreSQL 14 для WMS системы"
    
    # Проверки
    check_root
    check_os
    check_postgresql
    install_dependencies
    
    # Определение роли сервера
    echo
    echo "Выберите роль сервера:"
    echo "1) Primary Server (OLTP)"
    echo "2) Replica Server (Analytics)"
    echo "3) Оба сервера (для тестирования)"
    read -p "Введите номер (1-3): " server_role
    
    case $server_role in
        1)
            setup_primary
            ;;
        2)
            setup_replica
            ;;
        3)
            setup_primary
            setup_replica
            ;;
        *)
            log_error "Неверный выбор"
            exit 1
            ;;
    esac
    
    # Дополнительная настройка
    create_monitoring_users
    setup_automation
    create_monitoring_scripts
    
    # Проверка репликации
    if [[ "$server_role" == "3" ]] || [[ "$server_role" == "2" ]]; then
        check_replication
    fi
    
    log_success "Развертывание завершено успешно!"
    
    echo
    echo "=== Следующие шаги ==="
    echo "1. Проверьте статус репликации: /usr/local/bin/check_replication.sh"
    echo "2. Обновите представления: /usr/local/bin/refresh_views.sh [daily|weekly|monthly]"
    echo "3. Настройте мониторинг в Prometheus/Grafana"
    echo "4. Проверьте логи: tail -f /var/log/postgresql/postgresql-*.log"
    echo
    echo "=== Пользователи ==="
    echo "Репликация: wms_repl_user / secure_repl_password_123"
    echo "Аналитика: wms_analytics_user / analytics_password_123"
    echo "Отчеты: wms_reports_user / reports_password_123"
    echo "Мониторинг: prometheus_user / prometheus_password_123"
    echo "Grafana: grafana_user / grafana_password_123"
}

# Запуск основной функции
main "$@"
