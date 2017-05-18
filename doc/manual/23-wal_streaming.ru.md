## Потоковая передача WAL

Потоковая передача журналов транзакций в дополнении к стандартной
процедуре архивирования журналов позволяет уменьшить RPO.

Barman опирается на [`pg_receivexlog`] [25], утилиту, которая стала
доступна в PostgreSQL версии 9.2. Она использует собственный протокол
потоковой репликации и непрерывно получает журналы транзакций с сервера
PostgreSQL (основного или резервного).

> **Внимание:**
> Утилита `pg_receivexlog` должна быть установлена на том же сервере
> что и Barman. Для серверов PostgreSQL версии 9.2 вам понадобится
> `pg_receivexlog` версии 9.2. Для PostgreSQL версии 9.3 и выше
> рекомендуется установить последнюю доступную версию `pg_receivexlog`,
> так как они обратно совместимы. В качестве альтернативы можно установить
> несколько версий файла `pg_receivexlog` на сервере Barman и правильно
> указать конкретную версию для сервера, используя опцию` path_prefix`
> в файле конфигурации.

Для включения потоковой передачи журналов транзакций необходимо:

1. настроить потоковое соединение, как описано выше
2. установить для параметра `streaming_archiver` значение` on`

Если вышеупомянутые требования выполнены, команда `cron`,
прозрачно управляет потоком журнала через выполнение `Receive-wal`.
Это рекомендуемый сценарий.

Однако, можно вручную выполнить команду `receive-wal`:

``` bash
barman receive-wal <server_name>
```

> **Замечание:**
> Комманда `receive-wal` работает в фоновом режиме.

Журналы транзакций транслируются непосредственно в каталог, указанный
параметром конфигурации `streaming wals directory`, и затем
архивируются командой `archive-wal`.

Если иное не указано в параметре `streaming_archiver_name` и только для
PostgreSQL 9.3 или выше, Barman установит `application_name` процесса
потокой передачи WAL на `barman_receive_wal`, что позволит вам
отслеживать его статус в системном представлении pg_stat_replication`
сервера PostgreSQL

### Replication slots

> **IMPORTANT:** replication slots are available since PostgreSQL 9.4

Replication slots are an automated way to ensure that the PostgreSQL
server will not remove WAL files until they were received by all
archivers. Barman uses this mechanism to receive the transaction logs
from PostgreSQL.

You can find more information about replication slots in the
[PostgreSQL manual][replication-slots].

You can even base your backup architecture on streaming connection
only. This scenario is useful to configure Docker-based PostgreSQL
servers and even to work with PostgreSQL servers running on Windows.

> **IMPORTANT:**
> In this moment, the Windows support is still experimental, as it is
> not yet part of our continuous integration system.


### How to configure the WAL streaming

First, the PostgreSQL server must be configured to stream the
transaction log files to the Barman server.

To configure the streaming connection from Barman to the PostgreSQL
server you need to enable the `streaming_archiver`, as already said,
including this line in the server configuration file:

``` ini
streaming_archiver = on
```

If you plan to use replication slots (recommended),
another essential option for the setup of the streaming-based
transaction log archiving is the `slot_name` option:

``` ini
slot_name = barman
```

This option defines the name of the replication slot that will be
used by Barman. It is mandatory if you want to use replication slots.

When you configure the replication slot name, you can create a
replication slot for Barman with this command:

``` bash
barman@backup$ barman receive-wal --create-slot pg
Creating physical replication slot 'barman' on server 'pg'
Replication slot 'barman' created
```

