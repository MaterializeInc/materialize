---
title: "to_char function"
description: "Converts a timestamp into a string using the specified format."
menu:
  main:
    parent: 'sql-functions'
---

`to_char` converts a timestamp into a string using the specified format.

The format string can be composed of any number of [format
specifiers](#format-specifiers), interspersed with regular text. You can place a
specifier token inside of double-quotes to emit it literally.

## Examples

#### RFC 2822 format

```sql
SELECT to_char(TIMESTAMPTZ '2019-11-26 15:56:46 +00:00', 'Dy, Mon DD YYYY HH24:MI:SS +0000') AS formatted
```
```nofmt
             formatted
 ---------------------------------
  Tue, Nov 26 2019 15:56:46 +0000
```

#### Additional non-interpreted text

Normally the `W` in "Welcome" would be converted to the week number, so we must quote it.
The "to" doesn't match any format specifiers, so quotes are optional.

```sql
SELECT to_char(TIMESTAMPTZ '2019-11-26 15:56:46 +00:00', '"Welcome" to Mon, YYYY') AS formatted
```
```nofmt
       formatted
 ----------------------
  Welcome to Nov, 2019
```

#### Ordinal modifiers

```sql
SELECT to_char(TIMESTAMPTZ '2019-11-1 15:56:46 +00:00', 'Dth of Mon') AS formatted
```
```nofmt
  formatted
 ------------
  6th of Nov
```

## Format specifiers

| Specifier     | Description                                                                                      |
|---------------|--------------------------------------------------------------------------------------------------|
| `HH`          | hour of day (01-12)                                                                              |
| `HH12`        | hour of day (01-12)                                                                              |
| `HH24`        | hour of day (00-23)                                                                              |
| `MI`          | minute (00-59)                                                                                   |
| `SS`          | second (00-59)                                                                                   |
| `MS`          | millisecond (000-999)                                                                            |
| `US`          | microsecond (000000-999999)                                                                      |
| `SSSS`        | seconds past midnight (0-86399)                                                                  |
| `AM`/`PM`     | uppercase meridiem indicator (without periods)                                                   |
| `am`/`pm`     | lowercase meridiem indicator (without periods)                                                   |
| `A.M.`/`P.M.` | uppercase meridiem indicator (with periods)                                                      |
| `a.m.`/`p.m.` | lowercase meridiem indicator (with periods)                                                      |
| `Y,YYY`       | Y,YYY year (4 or more digits) with comma                                                         |
| `YYYY`        | year (4 or more digits)                                                                          |
| `YYY`         | last 3 digits of year                                                                            |
| `YY`          | last 2 digits of year                                                                            |
| `Y`           | last digit of year                                                                               |
| `IYYY`        | ISO 8601 week-numbering year (4 or more digits)                                                  |
| `IYY`         | last 3 digits of ISO 8601 week-numbering year                                                    |
| `IY`          | last 2 digits of ISO 8601 week-numbering year                                                    |
| `I`           | last digit of ISO 8601 week-numbering year                                                       |
| `BC`/`AD`     | uppercase era indicator (without periods)                                                        |
| `bc`/`ad`     | lowercase era indicator (without periods)                                                        |
| `B.C.`/`A.D.` | uppercase era indicator (with periods)                                                           |
| `b.c.`/`a.d.` | lowercase era indicator (with periods)                                                           |
| `MONTH`       | full upper case month name (blank-padded to 9 chars)                                             |
| `Month`       | full capitalized month name (blank-padded to 9 chars)                                            |
| `month`       | full lower case month name (blank-padded to 9 chars)                                             |
| `MON`         | abbreviated upper case month name (3 chars in English, localized lengths vary)                   |
| `Mon`         | abbreviated capitalized month name (3 chars in English, localized lengths vary)                  |
| `mon`         | abbreviated lower case month name (3 chars in English, localized lengths vary)                   |
| `MM`          | month number (01-12)                                                                             |
| `DAY`         | full upper case day name (blank-padded to 9 chars)                                               |
| `Day`         | full capitalized day name (blank-padded to 9 chars)                                              |
| `day`         | full lower case day name (blank-padded to 9 chars)                                               |
| `DY`          | abbreviated upper case day name (3 chars in English, localized lengths vary)                     |
| `Dy`          | abbreviated capitalized day name (3 chars in English, localized lengths vary)                    |
| `dy`          | abbreviated lower case day name (3 chars in English, localized lengths vary)                     |
| `DDD`         | day of year (001-366)                                                                            |
| `IDDD`        | day of ISO 8601 week-numbering year (001-371; day 1 of the year is Monday of the first ISO week) |
| `DD`          | day of month (01-31)                                                                             |
| `D`           | day of the week, Sunday (1) to Saturday (7)                                                      |
| `ID`          | ISO 8601 day of the week, Monday (1) to Sunday (7)                                               |
| `W`           | week of month (1-5) (the first week starts on the first day of the month)                        |
| `WW`          | week number of year (1-53) (the first week starts on the first day of the year)                  |
| `IW`          | week number of ISO 8601 week-numbering year (01-53; the first Thursday of the year is in week 1) |
| `CC`          | century (2 digits) (the twenty-first century starts on 2001-01-01)                               |
| `J`           | Julian Day (days since November 24, 4714 BC at midnight)                                         |
| `Q`           | quarter (ignored by to_date and to_timestamp)                                                    |
| `RM`          | month in upper case Roman numerals (I-XII; I=January)                                            |
| `rm`          | month in lower case Roman numerals (i-xii; i=January)                                            |
| `TZ`          | upper case time-zone name                                                                        |
| `tz`          | lower case time-zone name                                                                        |

### Specifier modifiers

| Modifier         | Description                                            | Example | Without Modification | With Modification |
|------------------|--------------------------------------------------------|---------|----------------------|-------------------|
| `FM` prefix      | fill mode (suppress leading zeroes and padding blanks) | FMMonth | `July    `           | `July`            |
| `TH`/`th` suffix | upper/lower case ordinal number suffix                 | Dth     | `1`                  | `1st`             |
