from collections.abc import Iterable
from itertools import chain
from typing import Any, Generic, Self, TypeVar

import sqlalchemy as sa
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeBase

_OrmModel = TypeVar("_OrmModel", bound=DeclarativeBase)
_Entity = TypeVar("_Entity", bound=BaseModel)


class AsyncBulkUpdaterFromEntities(Generic[_OrmModel, _Entity]):
    """
    Утилита для эффективного массового обновления записей разными значениями.
    Используется, когда есть N записей, каждая из которых обновлена своими уникальными
    значениями. Данная утилита позволяет обновить N записей одним запросом.

    Поддерживает сущности с составным первичным ключом.

    Обновляться будут только те поля, которые есть и в `_OrmModel`, и в `_Entity`.

    Пример:
    ```python
    class CartRepo(BaseAlchemyRepository):
        _carts_updater = AsyncBulkUpdaterFromEntities[CartOrm, CartEntity](
            model_class=CartOrm,
            entity_class=CartEntity,
        )
        _items_updater = AsyncBulkUpdaterFromEntities[CartItemOrm, CartItemEntity](
            model_class=CartItemOrm,
            entity_class=CartItemEntity,
        )

        async def bulk_update_carts(self, carts: list[CartEntity]) -> list[CartEntity]:
            return await self._carts_updater(self._session).update_from_entities(carts)

        async def bulk_update_items(self, items: list[CartItemEntity]) -> list[CartItemEntity] | None:
            return await self._items_updater(self._session).update_from_entities(items)
    ```
    """

    __slots__ = (
        "_model_class",
        "_entity_class",
        "_session",
        "_primary_fields",
        "_orm_model_fields",
        "_primary_fields_by_name",
        "_default_update_fields_by_name",
    )

    def __init__(
        self,
        model_class: type[_OrmModel],
        entity_class: type[_Entity],
        primary_fields: set[str] | None = None,
    ) -> None:
        self._model_class = model_class
        self._entity_class = entity_class
        self._primary_fields = primary_fields
        self._orm_model_fields = self._model_class.__table__.columns

        # Столбцы, входящие в первичный ключ.
        self._primary_fields_by_name = {
            field_name: field
            for field_name, field in self._orm_model_fields.items()
            if self._is_primary_key(field_name, field)
        }

        # Столбцы для обновления по умолчанию.
        self._default_update_fields_by_name = {
            field_name: self._orm_model_fields[field_name]
            for field_name in self._entity_class.model_fields
            if field_name in self._orm_model_fields and field_name not in self._primary_fields_by_name
        }

    def __call__(self, session: AsyncSession) -> Self:
        self._session = session
        return self

    async def update_from_entity(
        self,
        entity: _Entity,
        update_fields: Iterable[str] | None = None,
    ) -> _Entity:
        """Обновление данных с помощью одной сущности."""

        update_fields = self._clear_update_fields(update_fields)
        stmt = (
            sa.update(self._model_class)  # type: ignore[attr-defined]
            .where(self._build_single_lookup_expr_for_table(entity))
            .values(**{str(field_name): getattr(entity, field_name) for field_name in update_fields})
        )
        await self._session.execute(stmt)  # type: ignore[attr-defined]

        return entity

    async def update_from_entities(
        self,
        entities: list[_Entity],
        update_fields: Iterable[str] | None = None,
    ) -> list[_Entity] | None:
        """Обновление данных с помощью нескольких сущностей."""

        if len(entities) == 0:
            return None

        update_fields = self._clear_update_fields(update_fields)
        new_data_values = self._build_virtual_table_with_values_to_update(entities, update_fields)
        stmt = self._build_update_statement(entities, update_fields, new_data_values)

        await self._session.execute(stmt)  # type: ignore[attr-defined]
        return entities

    def _is_primary_key(self, field_name: str, field: sa.ColumnElement[Any]) -> bool:
        if self._primary_fields is None:
            return field.primary_key
        return field_name in self._primary_fields

    def _clear_update_fields(self, raw_update_fields: Iterable[str] | None) -> dict[str, sa.ColumnElement[Any]]:
        """
        Получение валидных полей для обновления.

        Поле не является ключевым, а значит может быть
        обновлено с помощью операции `UPDATE`.
        """

        if raw_update_fields is None:
            return self._default_update_fields_by_name  # type: ignore[return-value]

        return {
            field_name: self._default_update_fields_by_name[field_name]
            for field_name in raw_update_fields
            if field_name not in self._primary_fields_by_name and field_name in self._default_update_fields_by_name
        }

    def _build_virtual_table_with_values_to_update(
        self,
        entities: list[_Entity],
        update_fields: dict[str, sa.ColumnElement[Any]],
    ) -> sa.Values:
        """
        Создание объекта виртуальной таблицы (VALUES) со значениями, которыми будут
        обновлены строки в целевой таблице.
        """

        # Формирование списка кортежей. Каждый кортеж содержит в себе
        # значения для первичного ключа + значения только для обновляемых полей.
        data_for_values: list[tuple[Any]] = []
        for entity in entities:
            row_data: list[Any] = []

            for field_name in chain(self._primary_fields_by_name.keys(), update_fields):
                value = getattr(entity, field_name)
                field = self._orm_model_fields[field_name]  # type: ignore[attr-defined]

                # В некоторых ситуациях `sqlalchemy` обрабатывает значения `NULL`
                # как текст. Это может вызывать ошибку типа. Например, когда
                # идет попытка записать `NULL` значение в поле `sa.Enum`.
                # Это свойственно как минимум для выражения `sa.values`.
                # Поэтому значения `None` необходимо явным образом привести к
                # типу поля.
                if value is None:
                    value = sa.null().cast(field.type)

                row_data.append(value)

            data_for_values.append(tuple(row_data))  # type: ignore[arg-type]

        new_data_values = sa.values(
            *[
                sa.column(field_name, field.type)
                for field_name, field in chain(self._primary_fields_by_name.items(), update_fields.items())
            ],
            name="new_data",
        ).data(data_for_values)

        return new_data_values

    def _build_update_statement(
        self,
        entities: list[_Entity],
        update_fields: dict[str, sa.ColumnElement[Any]],
        new_data_values: sa.Values,
    ) -> sa.Update:
        """
        Формирование запроса на обновление.

        Значения для обновления из `new_data_values` будут присоединены к целевым
        строкам с помощью конструкции `UPDATE..FROM`.
        """
        return (
            sa.update(self._model_class)  # type: ignore[attr-defined]
            .where(
                sa.and_(
                    self._build_many_lookup_expr_for_table(entities),
                    self._build_lookup_expr_for_join_table_and_values(new_data_values),
                ),
            )
            .values(**{field_name: getattr(new_data_values.c, field_name) for field_name in update_fields})
        )

    def _build_single_lookup_expr_for_table(self, entity: _Entity) -> sa.ColumnElement[bool]:
        """
        Генерация выражения, которое фильтрует строки из основной таблицы.
        Это выражение смотрит, чтобы все ключевые столбцы были равны значениям,
        которые определяются в `entity`.

        Пример:
        ```python
            sa.and_(
                order_id == 1,
                attmept_id == 2,
            )
        ```
        """
        return sa.and_(*[field == getattr(entity, field.key) for field in self._primary_fields_by_name.values()])  # type: ignore[arg-type]

    def _build_many_lookup_expr_for_table(self, entities: list[_Entity]) -> sa.ColumnElement[bool]:
        """
        Генерация выражения, которое фильтрует строки из основной таблицы.
        Это выражение смотрит, чтобы все ключевые столбцы входили в диапазоны
        значений, которые определяют сущности `entities`.

        Пример:
        ```python
            sa.and_(
                order_id.in_([1, 2, 3, 4]),
                attmept_id.in_([9, 8, 7, 6]),
            )
        ```
        """
        return sa.and_(
            *[
                field.in_([getattr(entity, field.key) for entity in entities])  # type: ignore[arg-type]
                for field in self._primary_fields_by_name.values()
            ],
        )

    def _build_lookup_expr_for_join_table_and_values(self, values: sa.Values) -> sa.ColumnElement[bool]:
        """
        Генерация выражения для соединения строк из таблицы и из `VALUES`.
        Грубо говоря, в `UPDATE` запросе происходит `INNER JOIN` строк из таблицы
        со строками из `VALUES`. Этот метод возвращает условие, которое определяет,
        по какому признаку соединять строки.

        Соединение происходит, если строки равны по всем ключевым столбцам.
        """
        return sa.and_(
            *[
                table_field == getattr(values.c, table_field.key)  # type: ignore[arg-type]
                for table_field in self._primary_fields_by_name.values()
            ],
        )
