from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from datetime import datetime
from pytz import timezone
import time

from typing import TYPE_CHECKING, Optional, Sequence
if TYPE_CHECKING:
    from airflow.utils.context import Context


class MySqlToPostgresOperator(BaseOperator):


    template_fields: Sequence[str] = (
        'query',
    )
    template_ext: Sequence[str] = ('.sql', '.json')
    template_fields_renderers = {
        "query": "sql",
    }

    def __init__(
            self,
            *,
            query: str = None,
            target_table: str = None,
            identifier: [str] = None,
            mysql_conn: str = None,
            postgres_conn: str = None,
            postgres_conn_target: str = None,
            replace: Optional[bool] = False,
            db_query_from: str = 'mysql',
            **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.query = query
        self.target_table = target_table
        self.identifier = identifier
        self.mysql_conn = mysql_conn
        self.postgres_conn = postgres_conn
        self.replace = replace,
        self.db_query_from = db_query_from
        self.postgres_conn_target = postgres_conn_target

        # params that will be passed
        self.row_count = 0
        self.current_time = datetime.now(timezone('Asia/Jakarta'))
        self.duration = 0

    @staticmethod
    def _serialize_cell(cell) -> str:

        if cell is None:
            return "NULL"
        if isinstance(cell, datetime):
            return "'" + cell.isoformat() + "'"
        if isinstance(cell, str):
            return "'" + str(cell).replace("'", "''") + "'"
        return "'" + str(cell) + "'"

    @staticmethod
    def get_column(target_fields) -> str:
        if target_fields:
            target_fields_fragment = ", ".join(target_fields)
            target_fields_fragment = f"({target_fields_fragment})"
        else:
            target_fields_fragment = ""

        return target_fields_fragment

    def get_value(self, rows) -> str:
        values = ""
        for i, row in enumerate(rows, 1):
            lst = []
            for cell in row:
                cell = cell.replace('\0', '') if isinstance(cell, str) else cell
                lst.append(self._serialize_cell(cell))
            lsj = '(' + ','.join(lst).replace("None", "") + ')'
            values += "" + lsj + ","
        values = values[:-1]
        return values

    def generate_query(self, rows, target_fields) -> str:

        sql = f"""
            INSERT INTO {self.target_table}
            {self.get_column(target_fields)}
            VALUES {self.get_value(rows)}
        """

        if str(self.replace) == "(True,)":
            if target_fields is None:
                raise ValueError("PostgreSQL ON CONFLICT upsert syntax requires column names")
            if self.identifier is None:
                raise ValueError("PostgreSQL ON CONFLICT upsert syntax requires an unique index")
            if isinstance(self.identifier, list):
                replace_index = ",".join(self.identifier)
            else:
                replace_index = self.identifier

            replace_target = [
                "{0} = excluded.{0}".format(col) for col in target_fields if col not in self.identifier
            ]

            sql += f"ON CONFLICT ({replace_index}) DO UPDATE SET {', '.join(replace_target)}"
        else:
            if isinstance(self.identifier, list):
                replace_index = ",".join(self.identifier)
                sql += f"ON CONFLICT ({replace_index}) DO NOTHING"
            elif isinstance(self.identifier, str):
                replace_index = self.identifier
                sql += f"ON CONFLICT ({replace_index}) DO NOTHING"
            else:
                pass

        return sql

    def execute(self, context: 'Context') -> None:
        self.current_time = datetime.now(timezone('Asia/Jakarta'))
        dateStart = (time.time() * 1000)

        target = PostgresHook(self.postgres_conn, log_sql=False)

        if(self.postgres_conn_target):
            target = PostgresHook(self.postgres_conn_target, log_sql=False)

        # Check if mysql-to-postgres(raw) or postgres-to-postgres(mart)
        if self.db_query_from == 'postgres':
            source = PostgresHook(self.postgres_conn, log_sql=False)
            conn = source.get_conn()
            cursor = conn.cursor()
        else:
            source = MySqlHook(self.mysql_conn)
            conn = source.get_conn()
            cursor = conn.cursor()
            # cursor.execute('')

        self.log.info(self.query)
        # Execute query
        cursor.execute(self.query)

        # Estimate row count
        self.row_count = cursor.rowcount

        # Find column name based on query result
        target_fields = [x[0] for x in cursor.description]

        # Store query result to rows
        rows = cursor.fetchall()

        # Perform inserting data
        if (rows):
            target.run(
                sql=self.generate_query(rows, target_fields)
            )
        else:
            self.log.info("There is no data to insert/update.")

        cursor.close()
        conn.close()

        self.duration = (time.time() * 1000) - dateStart