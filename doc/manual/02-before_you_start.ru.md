\newpage

# Перед тем как начать

Прежде чем начать использовать Barman, необходимо познакомится с концепциями физического резервного копирования в PostgreSQL, восстановлением на момент времени, репликацией и т. д.

Ниже вы можете найти исчерпывающий список ресурсов, которые мы рекомендуем вам прочитать:

- _Документация по PostgreSQL_:
    - [Выгрузка в SQL] [https://postgrespro.ru/docs/postgrespro/9.6/backup-dump.html] [^pgdump]
    - [Резервное копирование на уровне файлов] [https://postgrespro.ru/docs/postgrespro/9.6/backup-file.html]
    - [Непрерывное архивирование и восстановление на момент времени (Point-in-Time Recovery, PITR)] [https://postgrespro.ru/docs/postgrespro/9.6/continuous-archiving.html]
    - [Конфигурация восстановления] [https://postgrespro.ru/docs/postgrespro/9.6/recovery-config.html]
    - [Надёжность и журнал упреждающей записи] [https://postgrespro.ru/docs/postgrespro/9.6/wal.html]
- _Book_: [PostgreSQL 9 Administration Cookbook - 2nd edition] [adminbook]

  [^pgdump]: Важно понимать разницу между логическим и физическим резервным копированием, например между `pg_dump` и инструментом вроде Barman.

Профессиональная подготовка по этим темам - еще один эффективный способ изучения этих концепций. В любое время года вы можете найти множество курсов по всему миру, поставляемых компаниями работающими с PostgreSQL, такими как 2ndQuadrant.