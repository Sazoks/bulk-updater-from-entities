# bulk-updater-from-entities

Утилита для массового обновления записей в RDBMS одним запросом. Основана на операциях `UPDATE + USING + VALUES`. 


## Проблема

Операция `UPDATE` позволяет обновить N сущностей одним и тем же набором значений. Например, запрос `UPDATE my_table SET field_1 = 'ABC' WHERE field_2 in (...)`
обновит все строки, где `field_2` входит в какой-то диапазон значений, **одним и тем же значением** `'ABC'`.

Но что, если у нас есть N сущностей, но каждая сущность имеет **свое собственное новое значение** для поля `field_1`?
Придется делать запросы `UPDATE` в цикле. Это неэффективно.

Либо же можно под каждый такой случае писать вручную `UPDATE` запрос, используя `USING + VALUES`. Но это довольно нетривиальная конструкция и писать каждый раз
её заново под каждый случай - долго и не так-то просто. Есть риск, что рядовые разработчики не будут этого делать и просто сделают запросы в цикле.


## Пример

Ниже приведен пример с запросами в цикле.
Код следует воспринимать как псевдо-код. Его цель - показать идею.

```python
class CartItemEntity:
    ...

# Совпадает по полям с `CartItemEntity`.
class ActualCartItemDTO:
    ...

async def sync_cart_items(cart_id: UUID) -> None:
    actual_cart_items: list[ActualCartItemDTO] = await SyncClient().get_actual_cart_items(cart_id)
    db_cart_items: list[CartItemEntity] = await cart_repo.get_cart_items_by_cart_id(cart_id)

    for db_item, actual_item in zip(db_cart_items, actual_cart_items):
        db_item.field_1 = actual_item.field_1
        db_item.field_2 = actual_item.field_2

        # Сопоставление остальных полей...
        ...

        # Запрос в цикле.
        await cart_repo.update_cart_item(db_item)
```

Такая реализация приведет к N запросам к БД, что может быть неэффективно (сетевые запросы, ожидание соединения из пула соединений).


## Решение

Используя эту утилиту можно было бы переписать код так, чтобы запрос был один.

```python
class BaseAlchemyRepository:
    @property
    def _session(self) -> AsyncSession:
        # Получить сессию из `ContextVar`...
        ...

class CartRepo(BaseAlchemyRepository):
    # Создадим в слое репозиториев наш специальный объект, который умеет обновлять всё одним запросом.
    _items_updater = AsyncBulkUpdaterFromEntities[CartItemOrm, CartItemEntity](
        model_class=CartItemOrm,
        entity_class=CartItemEntity,
        # Если на уровне схемы явно не указаны `primary keys`, можно объяснить утилите, какие поля воспринимать как ключевые.
        primary_fields={"cart_id", "plu"},
    )

    # Теперь репозиторий будет просто делегировать операцию массового обновления утилите.
    async def bulk_update_items(self, items: list[CartItemEntity]) -> list[CartItemEntity] | None:
        return await self._items_updater(self._session).update_from_entities(items)


async def sync_cart_items(cart_id: UUID) -> None:
    actual_cart_items: list[ActualCartItemDTO] = await SyncClient().get_actual_cart_items(cart_id)
    db_cart_items: list[CartItemEntity] = await cart_repo.get_cart_items_by_cart_id(cart_id)

    for db_item, actual_item in zip(db_cart_items, actual_cart_items):
        db_item.field_1 = actual_item.field_1
        db_item.field_2 = actual_item.field_2

        # Сопоставление остальных полей...
        ...

    # Обновление одним запросом.
    await cart_repo.bulk_update(db_cart_items)
```

Благодаря этой утилите можно не тратить время на написание логики обновления сущностей одним запросом, а просто переиспользовать её для любых сущностей.
Ключевым требованием здесь является, чтобы ОРМ-модель совпадала по полям с Domain-моделью. Это нужно, чтобы утилита могла извлечь данные из Domain-модели
и сохранить их в ОРМ-модель, чтобы затем с помощью ОРМ сделать запрос.


## Визуализация, что делает утилита

Чтобы идея окончательно стала ясна, проиллюстрирую пример схематично.

1. Предположим, что есть следующая таблица `Users` со следующими значениями:

    | id (PK) | username |    city   |
    |---------|----------|-----------|
    | 1       | Alexey   | Moscow    |
    | 2       | Sasha    | Krasnodar |
    | 3       | Pasha    | Ufa       |

2. Меняется поле `city`. Мы построим `VALUES` - виртуальное отношение. Ниже приведены новые значения для каждой строки для поля `city`.

    | id (PK) | city      |
    |---------|-----------|
    | 1       | Kazan     |
    | 2       | Volgograd |
    | 3       | Rostov    |

3. Раз `VALUES` - это отношение, значит, мы можем применять к нему `JOINS`. Многие современные RDBMS поддерживают операцию `USING`. Она работает схожим образом, что и `INNER JOIN`.
Применяем эту операцию к виртуальной таблице.
    
    Применив эту операцию, теперь в опреации `UPDATE` мы можем сделать что-то вроде `SET city = v.city`, где `v` - `alias` виртуальной таблицы.

    Ниже приведено отношение, которое получится после использования `USING`.

    | id (PK) | username |    city   | <span style="color:green"> v.city </span>     |
    |---------|----------|-----------|-----------------------------------------------|
    | 1       | Alexey   | Moscow    | <span style="color:green"> Kazan </span>      |
    | 2       | Sasha    | Krasnodar | <span style="color:green"> Volgograd </span>  |
    | 3       | Pasha    | Ufa       | <span style="color:green"> Rostov </span>     |

4. Таблица `VALUES` формируется на стороне клиентского приложения (в коде). Таким образом, можно обновлять любое кол-во полей любыми данными в одном запросе.


## Итого

Используя эту утилиту, команда получает удобный единый механизм для решения описанной задачи - обновления нескольких строк своими значениями в одном запросе.

Теперь, когда рядовой разработчик столкнется с такой задачей, он не будет тратить время на написание запроса с `UPDATE + USING + VALUES`, а также не будет
делать неэффективное решение **с запросами в цикле**. Потому что есть удобный общий механизм, который можно применять **`К ЛЮБЫМ OrmModel + EntityModel`**.
Достаточно лишь того, чтобы `OrmModel` и `EntityModel` совпадали по полям.

В будущем, возможно, добавится возможность определять стратегию `маппинга` значений из `EntityModel` в `OrmModel`.

