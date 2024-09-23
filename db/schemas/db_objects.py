import re


class DBObjects:

    @staticmethod
    def normalize_definition(definition):
        if definition is None:
            return ''
        # Remove comments and extra whitespace
        definition = re.sub(r'/\*.*?\*/', '', definition, flags=re.DOTALL)  # Remove C-style comments
        definition = re.sub(r'--.*?$', '', definition, flags=re.MULTILINE)  # Remove SQL-line comments
        definition = re.sub(r'\s+', ' ', definition).strip()  # Replace multiple whitespace with single space
        return definition.lower()

    @staticmethod
    def get_tables(conn, schema=None):
        """Retrieve tables from the database connection."""
        query = """
        SELECT table_schema as schema, table_name as name
        FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
          AND table_type='BASE TABLE'
        """
        params = ()
        if schema:
            query += " AND table_schema = %s"
            params = (schema,)
        query += " ORDER BY schema, table_name;"
        with conn.cursor() as cur:
            cur.execute(query, params)
            tables = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in tables]

    @staticmethod
    def get_columns(conn, schema=None):
        """Retrieve columns from the database connection."""
        query = """
        SELECT table_schema as schema, table_name, column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        """
        params = ()
        if schema:
            query += " AND table_schema = %s"
            params = (schema,)
        query += " ORDER BY schema, table_name, column_name;"
        with conn.cursor() as cur:
            cur.execute(query, params)
            columns = cur.fetchall()
            col_names = [desc[0] for desc in cur.description]
        return [dict(zip(col_names, row)) for row in columns]

    @staticmethod
    def get_indexes(conn, schema=None):
        """Retrieve indexes from the database connection."""
        query = """
        SELECT
            schemaname AS schema,
            tablename AS table_name,
            indexname AS name,
            indexdef AS definition
        FROM pg_indexes
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
        """
        params = ()
        if schema:
            query += " AND schemaname = %s"
            params = (schema,)
        query += " ORDER BY schema, table_name, name;"
        with conn.cursor() as cur:
            cur.execute(query, params)
            indexes = cur.fetchall()
            col_names = [desc[0] for desc in cur.description]
            # Apply normalization to the 'definition' field
            results = []
            for row in indexes:
                row_dict = dict(zip(col_names, row))
                row_dict['definition'] = DBObjects.normalize_definition(row_dict['definition'])
                results.append(row_dict)
            return results

    @staticmethod
    def get_functions(conn, schema=None):
        """Retrieve functions from the database connection."""
        query = """
        SELECT n.nspname AS schema,
               p.proname AS name,
               pg_catalog.pg_get_function_identity_arguments(p.oid) AS arguments,
               pg_catalog.pg_get_functiondef(p.oid) AS definition
        FROM pg_catalog.pg_proc p
             LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
          AND p.prokind = 'f'
        """
        params = ()
        if schema:
            query += " AND n.nspname = %s"
            params = (schema,)
        query += " ORDER BY schema, name;"
        with conn.cursor() as cur:
            cur.execute(query, params)
            functions = cur.fetchall()
            col_names = [desc[0] for desc in cur.description]
            # Apply normalization to the 'definition' field
            results = []
            for row in functions:
                row_dict = dict(zip(col_names, row))
                row_dict['definition'] = DBObjects.normalize_definition(row_dict['definition'])
                results.append(row_dict)
            return results

    @staticmethod
    def get_procedures(conn, schema=None):
        """Retrieve procedures from the database connection."""
        query = """
        SELECT n.nspname AS schema,
               p.proname AS name,
               pg_catalog.pg_get_function_identity_arguments(p.oid) AS arguments,
               pg_catalog.pg_get_functiondef(p.oid) AS definition
        FROM pg_catalog.pg_proc p
             LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
          AND p.prokind = 'p'
        """
        params = ()
        if schema:
            query += " AND n.nspname = %s"
            params = (schema,)
        query += " ORDER BY schema, name;"
        with conn.cursor() as cur:
            cur.execute(query, params)
            procedures = cur.fetchall()
            col_names = [desc[0] for desc in cur.description]
            # Apply normalization to the 'definition' field
            results = []
            for row in procedures:
                row_dict = dict(zip(col_names, row))
                row_dict['definition'] = DBObjects.normalize_definition(row_dict['definition'])
                results.append(row_dict)
            return results

    @staticmethod
    def get_triggers(conn, schema=None):
        """Retrieve triggers from the database connection."""
        query = """
        SELECT
            n.nspname AS schema,
            c.relname AS table_name,
            t.tgname AS name,
            pg_get_triggerdef(t.oid, true) AS definition
        FROM pg_trigger t
        JOIN pg_class c ON c.oid = t.tgrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE NOT t.tgisinternal
        """
        params = ()
        if schema:
            query += " AND n.nspname = %s"
            params = (schema,)
        query += " ORDER BY schema, table_name, name;"
        with conn.cursor() as cur:
            cur.execute(query, params)
            triggers = cur.fetchall()
            col_names = [desc[0] for desc in cur.description]
            # Apply normalization to the 'definition' field
            results = []
            for row in triggers:
                row_dict = dict(zip(col_names, row))
                row_dict['definition'] = DBObjects.normalize_definition(row_dict['definition'])
                results.append(row_dict)
            return results
